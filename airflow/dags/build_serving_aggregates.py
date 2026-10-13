"""Build the StarRocks serving views and refresh the async materialized view.

The last hop. StarRocks' analyzer reads these definitions exactly as PostgreSQL's reads the
warehouse views, so the objects created here are what closes the five-hop chain.

Two things are worth knowing about this DAG:

* the DDL is sent with its comment lines stripped. StarRocks' parser rejects the fixture's
  leading ``--`` comment lines when a client forwards them verbatim;
* ``REFRESH MATERIALIZED VIEW`` is a statement Airflow's SQL extractor cannot parse, so the
  run shows an extraction warning. That is upstream behaviour, and the platform renders it
  on the run detail page rather than hiding it -- the demo points at it deliberately.
"""
from __future__ import annotations

import os
from datetime import datetime
from pathlib import Path

from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

FIXTURE = Path(os.environ.get("FIXTURE_DIR", "/fixture"))
CONNECTION = "serving_starrocks"
MATERIALIZED_VIEW = "ads.mv_sr_region_daily"


def sql_without_comments(name: str) -> str:
    text = (FIXTURE / "starrocks" / name).read_text()
    return "\n".join(line for line in text.splitlines() if not line.lstrip().startswith("--"))


with DAG(
    dag_id="build_serving_aggregates",
    description="Create the StarRocks serving views and refresh the materialized view",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["serving", "starrocks"],
) as dag:
    create_serving_objects = SQLExecuteQueryOperator(
        task_id="create_serving_objects",
        conn_id=CONNECTION,
        sql=sql_without_comments("20_ads_ddl.sql"),
    )

    refresh_materialized_view = SQLExecuteQueryOperator(
        task_id="refresh_materialized_view",
        conn_id=CONNECTION,
        sql=f"REFRESH MATERIALIZED VIEW {MATERIALIZED_VIEW} WITH SYNC MODE;",
    )

    create_serving_objects >> refresh_materialized_view
