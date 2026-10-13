"""Build the warehouse's transformation and serving layers.

Everything here is plain SQL executed through one PostgreSQL connection: six views, one
materialized view and two tables in ``dwd``, then the ``ads`` serving layer on top of them.

The views and the materialized view are analysed from their stored definitions, so the
platform derives their lineage without any help from this DAG. The two ``INSERT … SELECT``
loads are the interesting part: they carry a SQL facet, and the platform re-analyses that
SQL with its own PostgreSQL analyzer rather than trusting the producer's column claims.

Every statement is schema-qualified on purpose. An unqualified name resolves against the
connection's default schema, which silently produces a run with no lineage at all.
"""
from __future__ import annotations

import os
from datetime import datetime
from pathlib import Path

from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

FIXTURE = Path(os.environ.get("FIXTURE_DIR", "/fixture"))
CONNECTION = "warehouse_postgres"


def sql(name: str) -> str:
    return (FIXTURE / "pg" / name).read_text()


with DAG(
    dag_id="build_warehouse_layers",
    description="Build the warehouse's dwd and ads layers with SQL",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["warehouse", "transform"],
) as dag:
    build_dwd = SQLExecuteQueryOperator(
        task_id="build_dwd_views",
        conn_id=CONNECTION,
        sql=sql("10_dwd_ddl.sql"),
    )
    load_dwd = SQLExecuteQueryOperator(
        task_id="load_dwd_tables",
        conn_id=CONNECTION,
        sql=sql("20_dwd_load.sql"),
    )
    build_ads = SQLExecuteQueryOperator(
        task_id="build_ads_tables",
        conn_id=CONNECTION,
        sql=sql("30_ads_ddl.sql"),
    )
    load_ads = SQLExecuteQueryOperator(
        task_id="load_ads_tables",
        conn_id=CONNECTION,
        sql=sql("40_ads_load.sql"),
    )
    # Deliberately exotic definitions -- GROUPING SETS, LATERAL, generate_series, arrays,
    # jsonb, a FILTER on a window -- so the analyzer's coverage is visible in the graph.
    edge_cases = SQLExecuteQueryOperator(
        task_id="build_experimental_views",
        conn_id=CONNECTION,
        sql=sql("50_experimental_pg_features.sql"),
    )

    build_dwd >> load_dwd >> build_ads >> load_ads
    build_dwd >> edge_cases
