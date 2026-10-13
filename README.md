# metaxisdata demo

A self-contained demo of [metaxisdata](https://github.com/Ranxy/metaxisdata), the self-hosted data
governance and metadata platform. **One `docker compose up` brings up a small data platform
(MySQL → PostgreSQL → StarRocks), runs an Airflow pipeline over it, feeds the platform, and leaves
you with a populated environment to explore.**

Nothing here is production-shaped, and every credential is a throwaway local value.

- Walkthrough: below · 中文说明: [README.zh.md](README.zh.md)
- Why it is built this way: [docs/design.md](docs/design.md) · [中文](docs/design.zh.md)

---

## What you get

```
                        ┌──────────────────────────────────────────────┐
   browser ────────────▶│  metaxisdata :8083  (server + embedded SPA)  │
  localhost:8083        └───────┬────────────────────────┬─────────────┘
                                │ schema sync            │ POST /api/v1/lineage
        ┌───────────────┬───────┴────────┬───────────────┴──────────┐
        ▼               ▼                ▼                          ▼
 ┌─────────────┐ ┌────────────────┐ ┌──────────────────┐  ┌──────────────────┐
 │ mysql-shop  │ │postgres-       │ │ starrocks-serving│  │ metadata-db      │
 │ MySQL 8     │ │warehouse       │ │ StarRocks        │  │ platform store   │
 │ db: shop    │ │ db: warehouse  │ │ dbs: ods, ads    │  └──────────────────┘
 │ 8 tables    │ │ ods/dwd/ads    │ │ 5 tables         │
 │ 3 views     │ │ 12 views, 1 MV │ │ 2 views, 1 MV    │
 └──────▲──────┘ └───────▲────────┘ └────────▲─────────┘
        │                │                   │
        │        ┌───────┴───────────────────┴────────┐
        └────────┤  airflow :8080 (standalone)        │
                 │  ingest_shop_to_warehouse          │  SQL facet ──┐
                 │  build_warehouse_layers            │  column facets┤
                 │  publish_to_serving                │               │
                 │  build_serving_aggregates          │               │
                 └────────────────────────────────────┘               ▼
   one-shot:  warehouse-init → serving-init → bootstrap → etl-runner → smoke
```

Everything is prepared for you: the databases are seeded, the platform registers and syncs its three
sources, the pipeline runs, and the samples the walkthrough points at are created.

## Requirements

- Docker with Compose v2, BuildKit enabled
- **~10 GB of free RAM** and **~8 GB of disk** — StarRocks takes most of both
- Ports **8083** (platform) and **8080** (Airflow) free. The demo databases are deliberately *not*
  published, so it cannot collide with anything else you have running.

## Quick start

```bash
git clone <this repo> && cd metaxisdata-demo
make up
```

`make up` copies `.env.example` to `.env` on first use, builds the Airflow image, starts everything,
waits for the pipeline, and finishes with a check that the demo is populated. A cold start pulls about
5 GB of images and takes roughly 5–10 minutes; afterwards a restart is much faster.

The last lines of a successful run say what was found. Then open **<http://localhost:8083>** and sign
in:

| Where | Sign in as |
| --- | --- |
| metaxisdata (http://localhost:8083) | `admin@demo.local` / `Demo-admin-1234!` |
| Airflow (http://localhost:8080) | `admin` / `admin` |

```bash
make smoke      # is the demo populated? (short report, exits non-zero if not)
make logs       # follow the stack
make down       # stop it, keep the data
make reset      # stop it and drop the volumes; `make up` then starts clean
```

---

## Walkthrough

### 1. Metadata collected from three different engines

**Data → Instances.** Expect `mysql-shop` (MySQL), `postgres-warehouse` (PostgreSQL) and
`starrocks-serving` (StarRocks), each with a recent sync time — three engines collected by the same
registry.

Open **Metadata**, pick `shop.orders` in the MySQL instance, and look at its columns, indexes, DDL and
size. Then run:

```bash
make evolve
```

It adds one column to `shop.orders` and re-syncs. Back in the same table's **history** there is now an
entry summarised as `+1 column`, with the diff — which is the point of keeping a metadata registry.

### 2. Lineage derived from SQL

**Metadata → postgres-warehouse → `warehouse.dwd.v_order_line` → Lineage.**

The warehouse's views, materialized view and tables were created by the pipeline. The platform reads
their definitions and derives the graph itself: upstream reaches `warehouse.ods.order_items`, and
downstream goes through `warehouse.dwd.mv_daily_sales` to `warehouse.ads.ads_daily_sales`. Edges are
labelled by what they are — `DIRECT`, `AGGREGATE`, `JOIN`, `GROUP` — and expanding a node shows
per-column trails.

The fixture's SQL is deliberately awkward (multi-CTE `INSERT … SELECT`, `GROUPING SETS`, `LATERAL`,
`FILTER` on a window, `generate_series`, arrays and jsonb), so the graph exercises the analyzers
rather than a toy schema.

### 3. The whole chain, five hops

**Metadata → `shop.orders` (MySQL) → Lineage**, then expand downstream. The chain crosses all three
engines:

```
shop.orders → warehouse.ods.orders → v_order_base / v_order_line
  → mv_daily_sales / dwd_* → StarRocks ods.dwd_order_fact → ads.v_sr_* / ads.mv_sr_region_daily
```

Roughly 400 column edges, and no node outside the three instances: had the pipeline addressed its
databases with a different host name than the instances were registered with, the first hop would have
shown up as an *external* dataset instead.

### 4. Lineage derived from OpenLineage

**OpenLineage → Overview / Jobs / Datasets / Events.** The pipeline reports every task as an
OpenLineage run; the runs of the `INSERT … SELECT` tasks carry a SQL facet, and the cross-engine
copies carry column facets.

Open a run of `build_warehouse_layers` and note three things:

- the datasets it read and wrote, resolved against the registry rather than left as opaque strings;
- the **Airflow link** back to the task log on `localhost:8080`;
- an **extraction warning** on the materialized-view task. `REFRESH MATERIALIZED VIEW` is a statement
  Airflow's own SQL parser cannot parse. The platform shows that instead of quietly losing the
  lineage — the warning is expected, not a failure.

### 5. Which applications read which tables

**Application SQL** lists statements reported by an application rather than inferred from a database:
the demo registers an application, issues it a key, and reports the parameterised SQL it runs.
**Manual SQL** holds a statement someone kept in a wiki instead of a view — the registry analyses it
either way.

Both are seeded for you, and both appear in the lineage graph.

### 6. Governance

**Settings → Users / Groups / Roles / IAM / Audit Log**, and **Notifications** in the sidebar. The demo
seeds a second user (`analyst@demo.local` / `Analyst-1234!`), a group and a custom role, and the audit
log already holds everything the bootstrap and the walkthrough itself did — reading the audit log is
itself audited.

### 7. A producer that is not Airflow

The platform accepts OpenLineage events from anything that speaks the protocol. This one is a
synthetic Spark job that reports under a namespace no instance owns:

```bash
make ol-demo
```

The first event lands as an `external:spark-prod:…` dataset, and the workspace administrators get a
notification saying the namespace is unmapped. The script then creates the namespace mapping and sends
the same event again; the datasets resolve to `starrocks-serving`, and the edge appears in the graph
between two real objects. That mapping — not a code change — is how a producer from another team is
brought into the registry.

---

## What is not included

Being explicit about the edges of this demo:

- **Explain SQL** needs an LLM provider. Add one under *Settings → LLM Providers* (any
  OpenAI-compatible endpoint, including one running locally) and the page works.
- **SQL Server and MariaDB** are supported by the platform but not started here; adding them means two
  more services, registered the same way `scripts/bootstrap.py` registers the other three.
- **MCP** and the `mxd` CLI live in the platform repository and need an external URL; see
  [docs/mcp.md](https://github.com/Ranxy/metaxisdata/blob/main/docs/mcp.md).
- **DataX** is not used: its upstream binary has no reliable public source. The demo copies data with
  a small operator instead (`airflow/plugins/sql_copy_operator.py`), and
  `metaxisdata/test/e2e_etl` remains the reference for the DataX path.

## Troubleshooting

| Symptom | What to do |
| --- | --- |
| `make up` fails at the end | Run `make smoke` again; it names the check that failed. `make logs` follows the stack, `docker compose logs etl-runner` shows the pipeline. |
| StarRocks restarts, or the pipeline's last hop fails | StarRocks wants ~4 GB to itself. Give Docker more memory, or drop it by removing `starrocks-serving`, `serving-init`, `publish_to_serving` and `build_serving_aggregates`. |
| Ports already in use | Change `METAXISDATA_PORT` / `AIRFLOW_PORT` in `.env`. |
| The pipeline runs but the platform has nothing new | A sync happens at the end of the pipeline, so a run that died early leaves the registry behind. Re-run it with `docker compose up -d etl-runner && docker compose wait etl-runner`. |
| Apple Silicon | The StarRocks image is `amd64` only, so it runs under emulation: it works, slowly. |

## Credentials

Everything in this repository is a throwaway local value: `demo` / `demo` for the databases,
`admin@demo.local` / `Demo-admin-1234!` for the platform, `admin` / `admin` for Airflow. Do not use any
of them anywhere real.

Generated keys — the OpenLineage ingestion key, the application's key — are created at runtime into the
`state/` Docker volume, never into the repository. `make check-secrets` fails if a real credential ever
looks committed.

## Repository layout

| Path | What it is |
| --- | --- |
| `docker-compose.yml` | The whole stack |
| `fixture/` | The SQL fixture, vendored from the platform's end-to-end lineage test |
| `airflow/dags/`, `airflow/plugins/` | The four pipeline DAGs and the cross-engine copy operator |
| `scripts/` | Bootstrap, samples, smoke check, schema evolution, the OpenLineage producer |
| `docs/design.md` | The design and the decisions behind it |
