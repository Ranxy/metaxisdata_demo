# metaxisdata demo — design

> Audience: the people implementing and reviewing this repo.
> One-line summary: **a single `docker compose up -d` brings up MySQL + PostgreSQL + StarRocks + Airflow +
> metaxisdata, seeds data, runs the pipelines and ships OpenLineage events, so a newcomer opens
> <http://localhost:8083> and sees a populated demo.**
>
> Status: agreed design, implementation in progress. Agreed decisions are in §4 and §12.1.

---

## 0. TL;DR

| Item | Decision |
| --- | --- |
| Deliverable | A public repo, `metaxisdata-demo`, that runs the whole stack from one compose command |
| Platform | Official image `ghcr.io/ranxy/metaxisdata:v0.1.1` — **never built from source**, so the demo stays small and focused on *how to use the product* |
| Databases | MySQL 8 (`shop`, OLTP) → PostgreSQL 16 (`warehouse`, ods/dwd/ads) → StarRocks (`serving`, OLAP) + the platform's own PostgreSQL store |
| ETL | Airflow 3.1 in a single `airflow standalone` container, with a small `SqlCopyOperator` for the cross-engine hops |
| Lineage sources | SQL analyzers (views / materialized views), OpenLineage (SQL facet + column-level facet), manual SQL, application SQL |
| Docs | English first (`README.md`, `docs/design.md`) with a Chinese mirror (`README.zh.md`, `docs/design.zh.md`); all code comments in English |
| Verification | Deliberately light: `make smoke` checks the environment is populated. This is a demo, not a test suite |
| Secrets | None in the repo. Runtime keys are generated into a gitignored `state/` volume |

---

## 1. Goals and non-goals

### Goals

1. Show **metadata collection across different database engines**.
2. Show **lineage analysis** derived from SQL definitions.
3. Show **lineage analysis derived from OpenLineage** events.
4. Bring up databases + ETL + platform with **one `docker compose up`**.
5. **Inject everything automatically** — no manual instance registration, no clicking through a pipeline.
6. Cover **as much of the product surface as possible**, with whatever cannot be integrated stated plainly.
7. Be **reproducible and reset-able**: `make up`, `make reset`, `make smoke`.
8. Be **safe to publish**: no credentials, no tokens, nothing that leaks a real deployment.

### Non-goals

- Replacing the platform's own integration suites. We do not test the product; we demonstrate it.
- Running a production topology. Credentials in this repo are throwaway local values.
- Building metaxisdata from source, or demoing unreleased features.
- Exhaustive coverage of every engine the platform supports.

---

## 2. What we verified before designing

Everything below was checked against the source (with `file:line`), against the published image, or against
this machine's Docker host. Nothing here is assumed.

### 2.1 Platform capabilities we depend on

| Capability | Fact |
| --- | --- |
| Engines accepted | `MSSQL, MYSQL, POSTGRES, TIDB, MARIADB, OCEANBASE, STARROCKS, DORIS` (`proto/store/store/common.proto:16-30`, enforced at `backend/api/v1/instance_service.go:185-194`) |
| Schema sync | All eight engines implement instance + schema sync (`backend/plugin/db/{mysql,pg,starrocks,mssql}`) |
| SQL lineage analyzers | MySQL family, PostgreSQL, StarRocks only — **MSSQL has no analyzer** (`backend/plugin/lineage/engines/engines.go:26-34`) |
| Instance creation | `CreateInstance` requires exactly one `ADMIN` data source and **already triggers a sync** (`backend/api/v1/instance_service.go:158-226`) |
| Sync account | Schema sync always opens the `ADMIN` data source, never a `READ_ONLY` one (`backend/runner/schemasync/syncer.go:448,601`) |
| First account | `CreateUser` is credential-exempt while no end user exists; the first `END_USER` becomes `workspaceAdmin` (`backend/api/v1/user_service.go:243-270`) |
| Password policy | Enforced: length, digits, letters, uppercase, special characters (`backend/api/v1/user_service.go:277-292`) |
| Scripting transport | ConnectRPC `POST /metaxisdata.v1.<Service>/<Method>` (protojson) alongside the grpc-gateway `/v1/*` mirror. Request bodies differ (`body: "user"` vs `{"user": …}`) — **scripts use the Connect paths** |
| CSRF | Any request carrying `Authorization` is exempt; cookie-less requests pass (`backend/server/csrf.go:29-34`) |
| OpenLineage ingestion | `POST /api/v1/lineage` (+ `/batch`), plain REST, **requires `Authorization: Bearer ol_…`**; RunEvents only; **only `COMPLETE` events produce lineage** (`backend/api/v1/openlineage_handler.go:148-166`, `backend/plugin/openlineage/processor.go:48-72`) |
| SQL-facet events | Ingested events that carry `job.facets.sql` are **re-analysed by the server's own analyzer**; the producer's `columnLineage` facet is deliberately ignored (`backend/plugin/openlineage/sql_facet_lineage.go`) |
| Unmapped namespaces | Not an error: the dataset falls back to `external:<namespace>:<name>` and admins get a deduped notification (`backend/plugin/openlineage/resolver.go:120-213`) |
| Airflow deep links | Derived from the event's `run.facets.airflow.taskInstance.log_url`; no extra configuration (`backend/plugin/openlineage/airflow_links.go:9-47`) |
| Lineage read API | `GET /v1/lineages` (one object), `GET /v1/lineages:graph` (depth 1–10), `POST /v1/lineages:counts`. Column lineage is carried inside each relation, not as a separate RPC |
| Health / version | `GET /healthz` → `OK`, `GET /api/version` → `{version,git_commit,build_time}`; both anonymous |
| Process env | The server process reads only `PG_URL` (required), `METAXISDATA_ENCRYPTION_KEY`, `METAXISDATA_ENCRYPTION_KEY_PREVIOUS`; the other `METAXISDATA_*` names are image entrypoint → flag mappings |
| Store bootstrap | The schema is migrated at start-up; the DB user needs `CREATE TABLE` and `CREATE EXTENSION` (`pg_trgm`) |

### 2.2 Published image: what v0.1.1 contains

All GHCR tags were compared and the image binary probed:

- Tags published: `v0.1.1`, `0.1.1`, `sha-ffc38ba`, `latest` (all pointing at `sha256:db3b75d2…`), plus the older `v0.1.0` line.
- `v0.1.1` = commit `ffc38ba`, build time `2026-10-13`.
- Feature probe on the image binary: `applicationSql`, `application-sql`, `identityProviders`,
  `IdentityProviderService`, `manualSqls`, `namespaceMappings`, `deviceLogins`, `mcp_enabled` — all present.
- Consequently **no capability detection is needed**: every scenario in this design is available in the pinned image.

### 2.3 Reference implementation: `test/e2e_etl`

The source repo ships a four-hop lineage fixture that this demo reuses.

| Fact | Detail |
| --- | --- |
| Chain | MySQL `e2e_ods` (8 tables + 3 views) → PG `e2e.e2e_ods` → PG `e2e_dwd` (6 views + 1 materialized view + 2 tables) / `e2e_ads` → StarRocks `e2e_ods` (5 tables) → StarRocks `e2e_ads` (2 views + 1 async materialized view). 4 DAGs, 26 tasks, ~50 s |
| Data (seed `20260407`) | 300 customers, 100 products, 2 000 orders, 4 407 order items, 1 823 payments, 89 refunds, 12 regions, 12 sales reps; derived 2 000 fact / 300 customer / 1 921 daily-sales rows |
| Lineage scale | Per hop: MySQL internal 7 · MySQL→PG 8 · PG internal 34 · PG→StarRocks 5 · StarRocks internal 4; 19 analysed objects; 30 OpenLineage runs per chain |
| Analyzer stress | Multi-CTE `INSERT … SELECT`, three-schema joins, window functions, `FILTER`, `DISTINCT ON`, `GROUPING SETS`, `LATERAL`, `generate_series`, arrays/jsonb, StarRocks async materialized views |
| Host coupling to remove | container names `mysql8-db-1`/`pg16-db-1`/`starrocks`; `localhost`/`127.0.0.1`; Airflow home `/home/ran/airflow`; DataX at `/home/ran/source/datax`; `docker exec` inside tasks |
| Fixture-created accounts | **None** — MySQL `dev/dev` and the PG `dev` role are assumed to exist and must be created by us |

### 2.4 Hard constraints we design around

| # | Constraint | Source |
| --- | --- | --- |
| C1 | OpenLineage ingestion **requires an API key**, and the key only exists after the platform is bootstrapped → Airflow's transport config must be generated at runtime | ingestion handler |
| C2 | Dataset resolution matches the registered instance's `host:port` **string**, with only `localhost`/`127.0.0.1`/`::1` folded; a mismatch silently becomes an external dataset | e2e F2/F9 |
| C3 | SQL that does not schema-qualify its tables silently loses lineage | e2e F3 |
| C4 | `REFRESH MATERIALIZED VIEW` and `CREATE MATERIALIZED VIEW … WITH DATA` are unparseable by Airflow's SQL extractor; the server surfaces this as an extraction warning. Accepted, and documented as expected | e2e F5 |
| C5 | The StarRocks all-in-one image is 3.15 GB, wants ~4 GB of RAM and 30–60 s to start, and is amd64-only | measured |
| C6 | This host already publishes 5432, 9030, 8030 and 1433 → the demo must not publish database ports by default | measured |
| C7 | DataX has no reliable public binary (`datax-opensource…` returns 403) and its Airflow provider is not on PyPI → DataX cannot be the default path | measured |

### 2.5 This machine

Docker 29.9.0 / Compose 5.6.0, 24 cores, 45 GB RAM, 628 GB free on `/var/lib/docker`. Images for
`ghcr.io/ranxy/metaxisdata`, `starrocks/allin1-ubuntu:4.1-latest`, `mysql:8.x`, `postgres:16` and
`mcr.microsoft.com/mssql/server:2019-latest` are already cached. Docker Hub and PyPI are reachable.

---

## 3. Architecture

### 3.1 Topology

```
                        ┌──────────────────────────────────────────────┐
   browser ────────────▶│  metaxisdata :8083  (server + embedded SPA)  │
  localhost:8083        └───────┬────────────────────────┬─────────────┘
                                │ schema sync            │ POST /api/v1/lineage
        ┌───────────────┬───────┴────────┬───────────────┴──────────┐
        ▼               ▼                ▼                          ▼
 ┌─────────────┐ ┌────────────────┐ ┌──────────────────┐  ┌──────────────────┐
 │ mysql-shop  │ │postgres-       │ │ starrocks-serving│  │ metadata-db      │
 │ MySQL 8     │ │warehouse       │ │ StarRocks allin1 │  │ platform store   │
 │ db: shop    │ │ PG 16          │ │ dbs: ods, ads    │  │ (not published)  │
 │ 8 tables    │ │ db: warehouse  │ │ 5 tables         │  └──────────────────┘
 │ 3 views     │ │ ods/dwd/ads    │ │ 2 views + 1 MV   │
 └──────▲──────┘ └───────▲────────┘ └────────▲─────────┘
        │                │                   │
        │        ┌───────┴───────────────────┴────────┐
        └────────┤  airflow :8080 (standalone)        │
                 │  ingest_shop_to_warehouse          │  SQL facet ──┐
                 │  build_warehouse_layers            │  column facets ┤
                 │  publish_to_serving                │                │
                 │  build_serving_aggregates          │                │
                 └────────────────────────────────────┘                │
                                                                       ▼
   one-shot containers:  warehouse-init  →  bootstrap  →  etl-runner  →  smoke
```

### 3.2 Services

| Service | Image | Host port | Role |
| --- | --- | --- | --- |
| `metadata-db` | `postgres:16-alpine` | — | The platform's own store |
| `metaxisdata` | `ghcr.io/ranxy/metaxisdata:${METAXISDATA_VERSION:-v0.1.1}` | `8083:8083` | Platform server + SPA |
| `mysql-shop` | `mysql:8.4` | — (opt-in `13306`) | OLTP source, database `shop`, seeded from `fixture/mysql/` |
| `postgres-warehouse` | `postgres:16` | — (opt-in `15432`) | Warehouse, database `warehouse`, schemas `ods`/`dwd`/`ads` |
| `starrocks-serving` | `starrocks/allin1-ubuntu:4.1-latest`, `mem_limit: 4g` | — (opt-in `19030`/`18030`) | Serving layer, databases `ods`/`ads` |
| `airflow` | built from `docker/Dockerfile.airflow` (`apache/airflow:3.1.8-python3.12` + providers) | `8080:8080` | `airflow standalone`: webserver + scheduler + triggerer |
| `warehouse-init` | `postgres:16-alpine` (client included) | — | Creates the PG schemas |
| `serving-init` | `mysql:8.4` (client included) | — | Creates the StarRocks landing tables |
| `bootstrap` | `python:3.12-alpine` (standard library only) | — | Creates the admin, registers instances, syncs, issues the ingestion key, seeds samples |
| `etl-runner` | the Airflow image | — | Triggers the four DAGs in order and waits for each |
| `smoke` | `python:3.12-alpine` (standard library only) | — | Lightweight check that the environment is populated |
| `[mssql-erp]`, `[mariadb-crm]` | `mcr.microsoft.com/mssql/server:2019-latest`, `mariadb:11` | — | `extra` profile: two more engines, metadata-only and MySQL-family |
| `[ollama]` | `ollama/ollama` | `11434:11434` | `llm` profile: a local OpenAI-compatible endpoint so Explain SQL works offline |
| `[mxd]` | built from the demo toolchain | — | `mcp` profile: device login and read-back through the CLI |

### 3.3 Profiles

| Profile | Adds | Why |
| --- | --- | --- |
| *(default)* | everything above, StarRocks included | The full story: three engines, five-hop lineage, OpenLineage |
| `lite` | drops `starrocks-serving` (DAGs 3–4 skip themselves) | Machines with less than ~10 GB to spare |
| `extra` | `mssql-erp`, `mariadb-crm` | "Many database types" |
| `llm` | `ollama` | Explain SQL without a cloud API key |
| `mcp` | `mxd` | Device login (`/device`), MCP tools, the OAuth consent page |
| `datax` | DataX-based variants of DAGs 1 and 3 | Faithful replay of `test/e2e_etl`; needs a local DataX install |
| `ports` | publishes the demo databases on 1xxxx | Inspecting them with a local client |

### 3.4 Resource budget

| Component | Resident memory | Image size |
| --- | --- | --- |
| `metadata-db` + `metaxisdata` | ~400 MB | ~0.3 GB |
| `mysql-shop` + `postgres-warehouse` | ~800 MB | ~1.1 GB |
| `starrocks-serving` | ~3.5 GB | ~3.15 GB |
| `airflow` | ~1.5 GB | ~1.3 GB |
| **default total** | **≈ 6.2 GB (recommend ≥ 10 GB free)** | **≈ 5.9 GB** |
| `extra` | +2.2 GB | +1.9 GB |
| `datax` | +0.5 GB | +0.4 GB |

---

## 4. Decisions

| # | Decision | Alternatives | Why |
| --- | --- | --- | --- |
| **D1** | Use the **official image only**, pinned by `METAXISDATA_VERSION` (default `v0.1.1`) | Build from source; track `latest` | The demo repo is public and independent of the source repo. A pinned tag is reproducible; `latest` drifts |
| **D2** | Default ETL is **Airflow + a small `SqlCopyOperator` + `SQLExecuteQueryOperator`**; DataX becomes the optional `datax` profile | Make DataX the default | DataX has no reliable binary source and needs a JVM; a demo about metaxisdata should not depend on it. The profile keeps the faithful e2e replay available |
| **D3** | One **`airflow standalone`** container instead of webserver + scheduler + triggerer | Three containers | Fewer moving parts for a demo; `etl-runner` shares the image and drives the CLI, so no API auth is involved |
| **D4** | Bootstrap over **ConnectRPC + a bearer token** | grpc-gateway `/v1/*`; Playwright; the `mxd` CLI | The gateway's `body: "x"` rule makes request shapes differ; bearer requests skip CSRF; the CLI cannot create instances or sync |
| **D5** | The **ingestion key is generated at runtime**: `bootstrap` writes `state/ol.env`, and Airflow's entrypoint waits for it and sources it before starting | Hard-code a key; restart Airflow after bootstrap | Resolves C1 without a second start-up. `depends_on: service_completed_successfully` makes the wait deterministic |
| **D6** | **One source of truth for names**: `.env` feeds instance registration, Airflow connections, the copy tasks and the DAGs | Let each piece spell hosts its own way | This is exactly the e2e F2/F9 failure mode; a mismatch is silent |
| **D7** | **Reuse the fixture's SQL**, renaming only databases and schemas | Write new demo data | 1 MB, deterministic, already covers every analyzer stress case, and row counts reconcile |
| **D8** | Do **not publish database ports** by default; `--profile ports` maps them to 1xxxx | Publish 3306/5432/9030 | This host already uses those ports (C6) |
| **D9** | Verification is a **light `make smoke`**, not a test suite | Port `verify_chain.sql`; keep a numeric baseline | The brief is "a demo, not tests": the check answers "is the demo populated?" and nothing more |
| **D10** | Every one-shot service is **idempotent**: look before creating, treat "already exists" as success, truncate before reloading | Fail and retry by hand | `docker compose up` will be run repeatedly |
| **D11** | Keep StarRocks in the default profile, behind `mem_limit: 4g` and its own healthcheck | Drop it by default | Multi-engine, multi-hop lineage is the centrepiece of the demo |
| **D12** | Seed **manual SQL** and **application SQL** from `bootstrap` | Leave them empty | Two cheap, independent lineage entry points that light up two more pages |
| **D13** | Explain SQL uses a **local OpenAI-compatible container** in the optional `llm` profile; SSO is documented but not wired to a real IdP | Require a cloud key | Keeps the demo self-contained; the identity-provider page renders, the login flow needs an external IdP |
| **D14** | **No secrets in the repo**: throwaway demo credentials only, runtime keys in gitignored `state/`, plus a `make check-secrets` guard | Rely on review | The repo is public |

---

## 5. Demo scenarios

Each scenario is a section of the walkthrough: what to click, and what should be on screen.

### S1 — Multi-engine metadata collection
Open `http://localhost:8083` → sign in → **Data → Instances**.
Expect `mysql-shop` (MySQL), `postgres-warehouse` (PostgreSQL) and `starrocks-serving` (StarRocks), each with a
recent `last_sync_time`. Drill into `shop.orders` for columns, indexes, DDL and size. `make evolve` runs one
`ALTER TABLE` plus a re-sync to show the metadata history/diff view.

### S2 — SQL-derived lineage
Metadata → `postgres-warehouse` → `warehouse.dwd.v_order_line` → **Lineage**.
Expect upstream `warehouse.ods.order_items` and downstream `warehouse.dwd.mv_daily_sales` →
`warehouse.ads.ads_daily_sales`, with both `AGGREGATE` and `DIRECT` relation types. Expanding a column shows
per-column trails.

### S3 — End-to-end, five hops
Metadata → `mysql-shop.shop.orders` → **Lineage**, expanding to StarRocks:
`shop.orders → warehouse.ods.orders → v_order_base/v_order_line → mv_daily_sales/dwd_* →
ods.dwd_order_fact → ads.v_sr_* / ads.mv_sr_region_daily`, unbroken across five hops.

### S4 — OpenLineage ingestion
**OpenLineage → Overview / Jobs / Datasets / Events** → a run of `build_warehouse_layers`.
Expect 30+ runs, SQL facets on the SQL tasks, Airflow deep links back to `localhost:8080`, and the documented
extraction warnings on `REFRESH MATERIALIZED VIEW` (C4 — the platform surfacing an upstream limitation).

### S5 — Namespace mapping and external datasets
With the `ol` profile, a synthetic producer sends a COMPLETE event under a namespace that matches no instance.
Expect an `external:…` dataset and an admin notification; then create a namespace mapping and re-send — the
dataset resolves to the real table.

### S6 — Application SQL
`bootstrap` registers an application, issues an `asql_` key and reports a handful of parameterised statements.
Expect them under **Application SQL**, and on the tables as "which applications read me".

### S7 — Governance
Settings → Users / Groups / Roles / IAM / Audit Log / Notifications. Expect a seeded second user, a group, a
custom role, an audit trail of everything the bootstrap did, and at least one notification.

### S8 — Optional: Explain SQL, MCP, CLI
- `--profile llm`: a CUSTOM LLM profile pointing at `http://ollama:11434` makes Explain SQL work offline.
- `--profile mcp`: `/mcp` plus `/.well-known/*` and `/oauth/*` come alive once `external_url` is set
  (`http://localhost:8083` is accepted for loopback). `mxd auth login` demonstrates device login at `/device`.

### 5.1 Feature coverage

| Feature area | Covered | How |
| --- | --- | --- |
| Home dashboard | ✅ default | Populated by the bootstrap and the pipeline run |
| Instances / data sources / sync | ✅ default | Three instances registered and synced |
| Databases and schemas | ✅ default | Same |
| Metadata browser (tables, columns, indexes, DDL, history, diff) | ✅ default | Sync; `make evolve` for history |
| Table + column lineage | ✅ default | Views, materialized views, OpenLineage, manual SQL |
| Manual SQL | ✅ default | One entry seeded by `bootstrap` |
| Application SQL | ✅ default | Application + key + report seeded by `bootstrap` |
| Explain SQL | ⚙️ `llm` profile | Local OpenAI-compatible endpoint, or a cloud key |
| OpenLineage browse (overview/jobs/datasets/events/run detail) | ✅ default | The four DAGs' events |
| Namespace mapping | ⚙️ `ol` profile | Unmapped event → external dataset → mapping fixes it |
| OpenLineage API keys | ✅ default | One key issued by `bootstrap` (masked in the UI) |
| Airflow deep links | ✅ default | Derived from the event's airflow facet |
| Notifications | ✅ default | Unmapped-namespace notice, plus an ingestion-failure sample |
| MCP + OAuth consent | ⚙️ `mcp` profile | `external_url` on loopback |
| Device login + `mxd` CLI | ⚙️ `mcp` profile | `mxd auth login` |
| Settings: general, environments, users, roles, groups, IAM | ✅ default | Seeded / rendered |
| Settings: LLM providers | ⚙️ `llm` profile | As above |
| Settings: applications | ✅ default | Seeded |
| Settings: OpenLineage (mappings, keys, retention) | ✅ default | Seeded |
| Settings: identity providers | ⚠️ partial | The page renders; a full SSO login needs an external IdP |
| SQL Server / MariaDB collection | ⚙️ `extra` profile | Metadata-only for SQL Server |

---

## 6. Repository layout

```
metaxisdata-demo/
├── docker-compose.yml               # default stack
├── docker-compose.profiles.yml      # extra / llm / mcp / datax / ports
├── .env.example                     # non-secret demo configuration, documented
├── Makefile                         # up, down, reset, logs, smoke, evolve, check-secrets
├── README.md / README.zh.md         # quick start + the S1–S8 walkthrough
├── docs/
│   ├── design.md / design.zh.md     # this document
│   └── images/                      # screenshots referenced by the README
├── docker/
│   ├── Dockerfile.airflow           # Airflow 3.1.8 + providers + openlineage-python
│   ├── Dockerfile.datax             # optional profile
│   ├── entrypoint-airflow.sh        # wait for state/ol.env, source it, exec airflow standalone
│   └── initdb/01-airflow-database.sql  # Airflow's own database on the store server
├── airflow/
│   ├── dags/
│   │   ├── ingest_shop_to_warehouse.py
│   │   ├── build_warehouse_layers.py
│   │   ├── publish_to_serving.py
│   │   └── build_serving_aggregates.py
│   └── plugins/sql_copy_operator.py # cross-engine copy that reports OpenLineage facets
├── fixture/                         # vendored from test/e2e_etl, databases/schemas renamed
│   ├── README.md                    # why it is reused, and what changed
│   ├── UPSTREAM                     # source repo + commit
│   ├── mysql/{10_schema,20_seed,90_views}.sql
│   ├── pg/{05_ods_ddl,10_dwd_ddl,20_dwd_load,30_ads_ddl,40_ads_load,50_experimental_pg_features}.sql
│   └── starrocks/{10_ods_ddl,20_ads_ddl}.sql
├── scripts/
│   ├── vendor_fixture.sh            # re-vendor the fixture from the source repo
│   ├── warehouse_init.sh            # PG schemas  (runs in a postgres:16-alpine container)
│   ├── serving_init.sh              # StarRocks DDL (runs in a mysql:8.4 container)
│   ├── bootstrap.py                 # account, instances, sync, ingestion key, governance, samples
│   ├── etl_runner.sh                # trigger the DAGs in order
│   ├── smoke.py                     # light "is it populated?" check
│   ├── evolve.sh                    # one schema change + re-sync
│   ├── emit_openlineage.py          # optional synthetic producer
│   └── check_secrets.sh             # public-repo guard
├── vendor/                          # only for the datax profile
└── state/                           # runtime keys/tokens, gitignored
```

---

## 7. Startup sequence and bootstrap

### 7.1 Sequence

```
metadata-db (healthy)
  └─▶ metaxisdata (/healthz)
        └─▶ mysql-shop · postgres-warehouse · starrocks-serving (healthy)
              └─▶ warehouse-init      create PG schemas, apply StarRocks DDL
                    └─▶ bootstrap     admin → login → instances → sync → OL key → mappings
                                      → application + manual SQL → governance samples
                                      → write state/ol.env
                          └─▶ airflow  entrypoint waits for state/ol.env, then starts
                                └─▶ etl-runner  trigger the four DAGs, wait for each
                                      └─▶ smoke  assert the demo is populated
```

`docker compose ps` should end with the long-running services healthy and the one-shot services `Exited (0)`.
`make up` finishes by running `make smoke`, so a half-populated environment is never reported as ready.

### 7.2 Bootstrap calls (ConnectRPC, all verified against the source)

```python
C = "http://metaxisdata:8083/metaxisdata.v1."

# 1) First account. The first END_USER becomes workspaceAdmin. Password policy is enforced.
POST C+"UserService/CreateUser"
{"user": {"email": "admin@demo.local", "title": "Demo Admin",
          "password": "Demo-admin-1234!", "userType": "END_USER"}}

# 2) Token. Without `web` the token comes back in the body. Bearer requests skip CSRF.
POST C+"AuthService/Login"   {"email": "admin@demo.local", "password": "Demo-admin-1234!"}
-> {"token": "..."}

# 3) Instances. instance_id matches ^[a-z]([a-z0-9-]{0,61}[a-z0-9])?$; exactly one ADMIN data source.
#    Hosts are the compose service names, so registration, the DAGs and Airflow agree (D6/C2).
POST C+"InstanceService/CreateInstance"
{"instanceId": "mysql-shop",
 "instance": {"title": "Shop (MySQL)", "engine": "MYSQL", "activation": true,
   "dataSources": [{"name": "instances/mysql-shop/dataSources/admin", "type": "ADMIN",
                    "host": "mysql-shop", "port": "3306",
                    "username": "demo", "password": "demo", "database": "shop"}]}}
# postgres-warehouse (POSTGRES, demo/demo, db warehouse) and starrocks-serving (STARROCKS, root, 9030) likewise.
# CreateInstance already syncs; the explicit batch sync only makes it deterministic.

# 4) Deterministic full sync
POST C+"InstanceService/BatchSyncInstances"
{"requests": [{"name": "instances/mysql-shop", "enableFullSync": true}, ...]}

# 5) Ingestion key -> written to state/ol.env, never to the repo
POST C+"OpenLineageService/CreateAPIKey"   {"description": "airflow"}
-> {"key": "ol_..."}

# 6) Application SQL: register an application, issue its key, report parameterised statements
POST C+"ApplicationService/CreateApplication"        {"applicationId": "shop-web", "application": {"title": "shop-web"}}
POST C+"ApplicationService/CreateApplicationAPIKey"  {"parent": "applications/shop-web"}
-> {"apiKey": "asql_..."}
POST /api/v1/applicationSql/report   (Bearer asql_...)
{"instance": "instances/postgres-warehouse", "database": "warehouse", "schema": "dwd",
 "reportId": "demo-1/stmt-batch",
 "statements": [
   {"sql": "INSERT INTO ads.ads_daily_sales (order_date, channel, net_sales) VALUES (?, ?, ?)", "count": 128},
   {"sql": "SELECT id, total FROM dwd.dwd_order_fact WHERE channel = ? AND total > ?"}]}

# 7) Manual SQL (needs an existing instance and database)
POST C+"DatabaseService/CreateManualSQL"
{"parent": "instances/postgres-warehouse/databases/warehouse",
 "manualSql": {"title": "nightly reconciliation", "schemaName": "dwd",
               "sqlText": "INSERT INTO dwd.dwd_order_fact (order_id, net_amount)\nSELECT order_id, net_amount FROM ods.orders"}}

# 8) Governance samples: a second user, a group, a custom role
POST C+"UserService/CreateUser" ... / GroupService/CreateGroup ... / RoleService/CreateRole ...

# 9) Platform settings used by the demo
SettingService/UpdateWorkspaceProfileSetting
  external_url = http://localhost:8083      # accepted on loopback; needed by device login and MCP
```

### 7.3 Naming

Names follow what a real small data platform would use — no `demo_`/`e2e_` prefixes.

| Concept | Value |
| --- | --- |
| Compose project | `metaxisdata-demo` |
| Instances | `mysql-shop`, `postgres-warehouse`, `starrocks-serving` (optional `mssql-erp`, `mariadb-crm`) |
| Databases / schemas | MySQL `shop`; PostgreSQL `warehouse` with `ods`, `dwd`, `ads`; StarRocks `ods`, `ads` |
| Airflow connections | `shop_mysql`, `warehouse_postgres`, `serving_starrocks` |
| DAGs | `ingest_shop_to_warehouse`, `build_warehouse_layers`, `publish_to_serving`, `build_serving_aggregates` |
| Tables / views | Kept from the fixture (`orders`, `v_order_line`, `dwd_order_fact`, `mv_daily_sales`, `ads_daily_sales`, …) — only the database/schema part of every qualified name changes |

### 7.4 Credentials and the public-repo policy

All credentials here are **throwaway local values**, and they are the only ones in the repo:

| Where | Value |
| --- | --- |
| MySQL | `demo` / `demo`, root `demo-root` |
| PostgreSQL (warehouse and store) | `demo` / `demo` |
| StarRocks | `root`, no password |
| Platform admin | `admin@demo.local` / `Demo-admin-1234!` |
| Airflow | `admin` / `admin` |

Rules that keep the repo publishable:

1. Never commit a generated key. `ol_…`, `asql_…` and `sa_…` keys are created at runtime and written to
   `state/`, which is gitignored.
2. `.env` is gitignored; `.env.example` carries only documented demo values.
3. `make check-secrets` greps the working tree for key prefixes and private keys and fails on a hit.
4. The README states plainly that these credentials are for local containers only.

### 7.5 Idempotency and reset

- `bootstrap`: look up before creating; treat "already exists" as success; skip issuing a second key when one
  with the same description exists.
- `warehouse-init`: `CREATE … IF NOT EXISTS`, and drop-then-create as the fixture already does.
- DAGs: truncate/delete before loading; the fixture SQL is idempotent.
- `make reset`: `docker compose --profile "*" down -v`, then `make up`.
- `make up`: re-runs the one-shot containers only.

---

## 8. ETL design

| DAG | Steps | Lineage comes from |
| --- | --- | --- |
| `ingest_shop_to_warehouse` | Create `ods` tables, then eight parallel `SqlCopyOperator` copies, then a row-count check | The copy operator's OpenLineage column facets (validated server-side against the registry) |
| `build_warehouse_layers` | `10_dwd_ddl.sql` (six views + one materialized view + two tables), `20_dwd_load.sql` (multi-CTE `INSERT … SELECT`), `30_ads_ddl.sql`, `40_ads_load.sql`, `50_experimental_pg_features.sql` | The SQL facet → the server's own analyzer (a SQL-carrying event never trusts the producer's column claims) |
| `publish_to_serving` | Create StarRocks `ods` tables, then five copies (one reads a materialized view) | Column facets |
| `build_serving_aggregates` | StarRocks views and the async materialized view | SQL facet (the `REFRESH … WITH DATA` statement records an expected extraction warning, C4) |

`SqlCopyOperator` at a glance:

- Inputs: `src_conn_id`, `dst_conn_id`, `select_sql`, `dst_table`, `column_map`, `batch_size`.
- Runs a streaming DB-API read and a batched `executemany` insert; truncates first. StarRocks is reached over
  the MySQL protocol on 9030 with plain `INSERT`, so the Stream Load port 8030 is not needed.
- Reports lineage through Airflow's native facet hook (`get_openlineage_facets_on_start()` returning
  `OperatorLineage`), so the official provider owns transport and authentication. A manual fallback exists but
  must carry the bearer key — the DataX provider's fallback does not, which is exactly why its events would 401.
- Dataset names are spelled the way the resolver expects: `mysql://mysql-shop:3306` / `shop.orders` and
  `postgres://postgres-warehouse:5432` / `warehouse.ods.orders`.

---

## 9. Verification

`make smoke` (`scripts/smoke.py`) answers one question — *is the demo populated?* — and prints a short report:

1. every long-running service is healthy and every one-shot service exited 0;
2. `GET /healthz` is 200 and the admin can log in;
3. three instances exist with a non-empty `last_sync_time`;
4. each instance exposes tables and at least one view or materialized view;
5. `shop.orders` has both upstream and downstream lineage, and the graph reaches StarRocks;
6. OpenLineage holds at least one run per DAG.

No numeric baselines, no ported `verify_chain.sql`. Anything deeper belongs to the product's own test suites,
not to this repo.

---

## 10. Risks

| # | Risk | Mitigation |
| --- | --- | --- |
| R1 | StarRocks is heavy (3.15 GB image, ~4 GB RAM, slow start) | `mem_limit: 4g`, the image's own healthcheck, and a `lite` profile |
| R2 | Database ports are already taken on this host | Never publish them by default; `--profile ports` maps to 1xxxx |
| R3 | The Airflow image needs PyPI at build time | Document `PIP_INDEX_URL` / `PIP_TRUSTED_HOST` overrides |
| R4 | Airflow cannot start before the ingestion key exists | `bootstrap` writes `state/ol.env`; the entrypoint waits for it; `smoke` fails if no runs arrive |
| R5 | A host-name mismatch silently produces external datasets (C2) | One source of truth for names (D6); `smoke` fails when lineage does not reach StarRocks |
| R6 | Unqualified SQL silently loses lineage (C3) | Every DAG statement is schema-qualified |
| R7 | Materialized-view extraction warnings look like errors (C4) | Documented as expected, and shown as a product feature |
| R8 | DataX has no reliable binary and its provider is not on PyPI (C7) | DataX is an optional profile; its provider is vendored when the profile is used |
| R9 | StarRocks is amd64-only | Noted in the README; `lite` profile for Apple Silicon |
| R10 | A one-shot container fails unnoticed | `make up` ends with `make smoke` and prints the tail of the failed container's log |
| R11 | A secret slips into the repo | `make check-secrets` in the Makefile and in review |

---

## 11. Delivery plan

Work proceeds step by step, **one git commit per step**.

| Step | Scope | Commit |
| --- | --- | --- |
| 0 | This design, the README pair, `.gitignore`, the secrets guard | `docs: add the demo design and repository scaffolding` |
| 1 | `docker-compose.yml` with `metadata-db` + `metaxisdata`, `.env.example`, Makefile | `feat(compose): run the platform with its metadata store` |
| 2 | `mysql-shop`, `postgres-warehouse`, `starrocks-serving`, the vendored fixture, `warehouse-init` | `feat(data): add the shop, warehouse and serving databases` |
| 3 | `bootstrap.py`: account, instances, sync, ingestion key, `state/ol.env` | `feat(bootstrap): register instances, sync metadata and issue the ingestion key` |
| 4 | The Airflow image, the `SqlCopyOperator`, the four DAGs, `etl-runner` | `feat(etl): add the Airflow pipeline and OpenLineage lineage` |
| 5 | Governance samples, manual SQL, application SQL, `smoke.py`, `make evolve` | `feat(samples): seed governance, manual SQL and application SQL` |
| 6 | The `extra` (SQL Server, MariaDB), `ol` (synthetic producer), `llm` and `mcp` profiles | `feat(profiles): add the optional engines, LLM, MCP and OpenLineage profiles` |
| 7 | The walkthrough in both READMEs, screenshots, and a full `make reset && make up` rehearsal | `docs: add the walkthrough and screenshots` |

Each step ends with the stack still runnable (`make up && make smoke`), and commit messages follow the
conventional-commit style used by the source repo.

---

## 12. Agreed decisions and open items

### 12.1 Agreed with the maintainers

| Item | Decision |
| --- | --- |
| ETL component | Airflow with the built-in `SqlCopyOperator`; DataX is an optional profile |
| Engine scope | MySQL + PostgreSQL + StarRocks by default; SQL Server / MariaDB behind `extra` |
| Platform image | Official image only, never built from source; pinned by `METAXISDATA_VERSION` (v0.1.1) |
| Repository | Public — no credentials, no tokens, throwaway demo values only |
| Documentation | English first, with a Chinese mirror; all code comments in English |
| Verification | Light `make smoke`, not a test suite |
| Naming | Normal product naming (§7.3) |
| Process | Step by step, one commit per step (§11) |

### 12.2 Open items

1. Should the optional profiles stay out of the default quick-start so the first run stays fast? *(Assumed: yes.)*
2. Are three engines enough for the "different database types" story, or should SQL Server join the default set
   despite the extra 1.5 GB image and 2 GB of RAM? *(Assumed: three by default.)*
3. Screenshots for the README — captured from this machine, or left as placeholders for the maintainers?
