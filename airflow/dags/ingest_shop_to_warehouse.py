"""Load the shop's OLTP tables into the warehouse's landing layer.

This is the first hop of the demo pipeline: eight tables are copied from the MySQL source
into the ``ods`` schema of the warehouse. Each copy reports the columns it moved to
OpenLineage, which is where the hop's table- and column-level lineage comes from.

Re-running is safe: every task truncates its target before it loads.
"""
from __future__ import annotations

import os
from datetime import datetime
from pathlib import Path

from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.standard.operators.python import PythonOperator
from sql_copy_operator import SqlCopyOperator, Table

FIXTURE = Path(os.environ.get("FIXTURE_DIR", "/fixture"))

SHOP_DB = os.environ.get("MYSQL_DB", "shop")
WAREHOUSE_DB = os.environ.get("PG_DB", "warehouse")
MYSQL_NAMESPACE = f"mysql://{os.environ.get('MYSQL_HOST', 'mysql-shop')}:{os.environ.get('MYSQL_PORT', '3306')}"
PG_NAMESPACE = f"postgres://{os.environ.get('PG_HOST', 'postgres-warehouse')}:{os.environ.get('PG_PORT', '5432')}"

TABLES = [
    "regions",
    "sales_reps",
    "customers",
    "products",
    "orders",
    "order_items",
    "payments",
    "refunds",
]


def check_row_counts() -> None:
    """Print both sides of the hop, and fail if they disagree.

    The Airflow task log is the reconciliation evidence: a reader can see the counts the
    copy produced without opening either database.
    """
    source = MySqlHook(mysql_conn_id="shop_mysql")
    target = PostgresHook(postgres_conn_id="warehouse_postgres")

    mismatched = []
    for table in TABLES:
        source_count = source.get_first(f"SELECT count(*) FROM {SHOP_DB}.{table}")[0]
        target_count = target.get_first(f"SELECT count(*) FROM ods.{table}")[0]
        print(f"{table:<12} source={source_count:<7} warehouse={target_count}")
        if source_count != target_count:
            mismatched.append(table)

    if mismatched:
        raise ValueError(f"row counts disagree for: {', '.join(mismatched)}")


with DAG(
    dag_id="ingest_shop_to_warehouse",
    description="Copy the shop's tables from MySQL into the warehouse landing layer",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["shop", "warehouse", "ingest"],
) as dag:
    create_landing_tables = SQLExecuteQueryOperator(
        task_id="create_landing_tables",
        conn_id="warehouse_postgres",
        sql=(FIXTURE / "pg" / "05_ods_ddl.sql").read_text(),
    )

    copies = [
        SqlCopyOperator(
            task_id=f"copy_{table}",
            source=Table(
                conn_id="shop_mysql",
                sql_name=f"{SHOP_DB}.{table}",
                namespace=MYSQL_NAMESPACE,
                dataset=f"{SHOP_DB}.{table}",
            ),
            target=Table(
                conn_id="warehouse_postgres",
                sql_name=f"ods.{table}",
                namespace=PG_NAMESPACE,
                dataset=f"{WAREHOUSE_DB}.ods.{table}",
            ),
        )
        for table in TABLES
    ]

    verify = PythonOperator(task_id="check_row_counts", python_callable=check_row_counts)

    create_landing_tables >> copies >> verify
