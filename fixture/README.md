# Data fixture

The SQL in this directory is **vendored** from the metaxisdata repository's end-to-end
lineage fixture (`test/e2e_etl`), with its databases and schemas renamed to the names this
demo uses. See `UPSTREAM` for the exact commit, and `scripts/vendor_fixture.sh` for the
rewrite rules.

Why reuse it rather than write new data: the chain is deterministic (seed `20260407`), small
(about 1 MB of SQL) and already exercises the constructs the lineage analyzers have to cope
with — multi-CTE `INSERT … SELECT`, three-schema joins, window functions, `FILTER`,
`DISTINCT ON`, `GROUPING SETS`, `LATERAL`, `generate_series`, arrays/jsonb, and StarRocks
async materialized views.

## Layout

| Path | What it is |
| --- | --- |
| `mysql/10_schema.sql` | Source DDL: eight tables in the `shop` database |
| `mysql/20_seed.sql` | Deterministic seed data (generated upstream — do not edit by hand) |
| `mysql/90_views.sql` | Three MySQL views, including a nested-CTE aggregate |
| `pg/05_ods_ddl.sql` | Landing tables in the `ods` schema |
| `pg/10_dwd_ddl.sql` | Six views, one materialized view and two tables in `dwd` |
| `pg/20_dwd_load.sql` | `INSERT … SELECT` loads, fully schema-qualified on purpose |
| `pg/30_ads_ddl.sql`, `pg/40_ads_load.sql` | Serving layer in `ads` |
| `pg/50_experimental_pg_features.sql` | Deliberately exotic PostgreSQL definitions |
| `starrocks/10_ods_ddl.sql` | Landing tables in the StarRocks `ods` database |
| `starrocks/20_ads_ddl.sql` | StarRocks views and the async materialized view in `ads` |

## Notes

- Comments inside the vendored files still refer to **DataX**, because that is how the
  upstream fixture moved data between engines. This demo's default pipeline copies with
  Airflow instead — see `airflow/dags/`. The `datax` profile replays the original path.
- Only qualified names were rewritten. Table and view names are unchanged.
- Re-vendor with `scripts/vendor_fixture.sh [path-to-metaxisdata-checkout]`; it fails if any
  upstream name survives.
