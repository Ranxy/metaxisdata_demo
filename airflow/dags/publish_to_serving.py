"""Publish the warehouse's transformation and serving tables to StarRocks.

The third hop: five tables are copied from PostgreSQL ``dwd``/``ads`` into the StarRocks
``ods`` database, which is the landing area the serving views are built on. One of them is a
materialized view, which is a relation like any other as far as the copy is concerned.

StarRocks speaks the MySQL protocol, so the copy uses the MySQL hook. Every task truncates
its target first, which is also what makes a re-run safe.
"""
from __future__ import annotations

import os
from datetime import datetime

from airflow import DAG
from sql_copy_operator import SqlCopyOperator, Table

WAREHOUSE_DB = os.environ.get("PG_DB", "warehouse")
SERVING_DB = "ods"
PG_NAMESPACE = f"postgres://{os.environ.get('PG_HOST', 'postgres-warehouse')}:{os.environ.get('PG_PORT', '5432')}"
STARROCKS_NAMESPACE = f"starrocks://{os.environ.get('SR_HOST', 'starrocks-serving')}:{os.environ.get('SR_PORT', '9030')}"

# (source schema, source table) -- the target keeps the table's name.
TABLES = [
    ("dwd", "dwd_order_fact"),
    ("dwd", "dwd_customer_360"),
    ("dwd", "mv_daily_sales"),
    ("ads", "ads_daily_sales"),
    ("ads", "ads_customer_segment"),
]

with DAG(
    dag_id="publish_to_serving",
    description="Copy the warehouse's dwd/ads tables into the StarRocks landing layer",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["warehouse", "serving", "publish"],
) as dag:
    copies = [
        SqlCopyOperator(
            task_id=f"publish_{table}",
            source=Table(
                conn_id="warehouse_postgres",
                sql_name=f"{schema}.{table}",
                namespace=PG_NAMESPACE,
                dataset=f"{WAREHOUSE_DB}.{schema}.{table}",
            ),
            target=Table(
                conn_id="serving_starrocks",
                sql_name=f"{SERVING_DB}.{table}",
                namespace=STARROCKS_NAMESPACE,
                dataset=f"{SERVING_DB}.{table}",
            ),
        )
        for schema, table in TABLES
    ]

    copies
