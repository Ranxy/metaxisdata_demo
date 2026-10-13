# metaxisdata demo

A self-contained demo of [metaxisdata](https://github.com/Ranxy/metaxisdata), the self-hosted data
governance and metadata platform: **one `docker compose up -d` brings up a small data platform
(MySQL → PostgreSQL → StarRocks), runs an Airflow pipeline over it, feeds the platform, and gives you a
populated environment to explore.**

Nothing here is production-shaped. Every credential is a throwaway local value.

- Design and decisions: [docs/design.md](docs/design.md) · [中文](docs/design.zh.md)
- 中文说明: [README.zh.md](README.zh.md)

## What it shows

| You want to see | Where |
| --- | --- |
| Metadata collected from different engines — MySQL, PostgreSQL and StarRocks | **Data → Instances**, then the metadata browser |
| Lineage derived from SQL — views and materialized views | **Lineage** on `warehouse.dwd.v_order_line` |
| Lineage derived from OpenLineage — runs, jobs, datasets, events, SQL facets, Airflow links | **OpenLineage** |
| The whole chain end to end, five hops from `shop.orders` to the StarRocks serving views | **Lineage** on `shop.orders` |
| Governance — users, groups, roles, audit log, notifications | **Settings** |

## How it is put together

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
 └──────▲──────┘ └───────▲────────┘ └────────▲─────────┘
        │                │                   │
        │        ┌───────┴───────────────────┴────────┐
        └────────┤  airflow :8080 (standalone)        │
                 │  ingest_shop_to_warehouse          │
                 │  build_warehouse_layers            │
                 │  publish_to_serving                │
                 │  build_serving_aggregates          │
                 └────────────────────────────────────┘
```

One-shot containers run the whole set-up for you: `warehouse-init` (schemas) → `bootstrap` (platform
account, instances, sync, ingestion key, sample SQL) → `etl-runner` (the four DAGs) → `smoke`.

## Requirements

- Docker with Compose v2, BuildKit enabled
- ~10 GB of free RAM and ~8 GB of disk for the default stack (StarRocks dominates both)
- Ports `8083` and `8080` free on the host. Database ports are **not** published by default — this keeps
  the demo from colliding with anything already running on your machine.

## Quick start

```bash
cp .env.example .env
make up          # builds the Airflow image, starts everything, seeds it, runs the pipeline
```

Then open <http://localhost:8083> and sign in with the demo admin account printed by `make up`
(see `.env.example`).

Airflow is at <http://localhost:8080> if you want to watch the pipeline itself.

```bash
make smoke       # is the demo populated?  (short report)
make logs        # follow the stack
make reset       # drop everything, including volumes, and start over
```

## Optional profiles

```bash
make up PROFILE=extra   # add SQL Server and MariaDB as two more collected engines
make up PROFILE=llm     # add a local OpenAI-compatible endpoint so Explain SQL works offline
make up PROFILE=mcp     # add the mxd CLI: device login, MCP tools, the OAuth consent page
make up PROFILE=ports   # publish the demo databases on 1xxxx for inspection
```

## Status

Under construction, delivered step by step; each step is its own commit. The plan lives in
[docs/design.md §11](docs/design.md). Steps 0 and 1 are the repository scaffolding and the platform
skeleton; the full walkthrough below lands with step 6.

## Credentials

Everything in this repo is a throwaway local value — see [docs/design.md §7.4](docs/design.md). Generated
keys (OpenLineage ingestion keys, application keys) are created at runtime inside the `state/` volume, which
is gitignored. Please do not use any of these credentials outside this demo.
