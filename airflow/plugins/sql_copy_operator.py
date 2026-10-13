"""Copy one table between two databases, and report the copy to OpenLineage.

The demo moves data across three engines. Airflow's SQL operator runs a statement on one
connection, but a cross-engine copy has to read from one and write to the other, so it needs
a small operator of its own.

Lineage: the operator hands Airflow an ``OperatorLineage`` with one input and one output
dataset, each carrying a ``schema`` facet that lists the columns of the copy. For a
column-for-column copy that is all the platform needs -- it infers the column-level edges
from the two schemas. Airflow's OpenLineage provider then owns the transport and the
authentication; a copy that sent its own events would have to remember the ingestion key,
and would silently produce nothing when it forgot.

Two ends of a copy are described with :class:`Table`: the Airflow connection that reaches
it, the qualified name SQL uses, and the identity OpenLineage uses -- a namespace of the
form ``engine://host:port`` plus a ``database.table`` or ``database.schema.table`` name. The
namespace has to match the instance registration in the platform character for character,
or the platform silently files the dataset as an external one instead of as real lineage.
"""
from __future__ import annotations

import logging
from typing import Any, NamedTuple

from airflow.exceptions import AirflowException
from airflow.hooks.base import BaseHook
from airflow.models import BaseOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.providers.postgres.hooks.postgres import PostgresHook

log = logging.getLogger(__name__)


class Table(NamedTuple):
    """One end of a copy: connection, SQL name, and OpenLineage identity."""

    conn_id: str
    sql_name: str
    namespace: str
    dataset: str


class SqlCopyOperator(BaseOperator):
    """Copy a whole table between two connections.

    The copy is column-for-column: the target takes the same column names, in the same
    order, as the source. It truncates the target first, so re-running a load is safe.
    """

    template_fields = ()

    def __init__(
        self,
        *,
        source: Table,
        target: Table,
        truncate: bool = True,
        batch_size: int = 1000,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.source = source
        self.target = target
        self.truncate = truncate
        self.batch_size = batch_size

    # --- execution ------------------------------------------------------------

    def execute(self, context: dict[str, Any]) -> int:
        rows, columns = self._read_source()
        self._write_target(rows, columns)
        log.info(
            "copied %d rows x %d columns: %s -> %s",
            len(rows),
            len(columns),
            self.source.sql_name,
            self.target.sql_name,
        )
        return len(rows)

    def _read_source(self) -> tuple[list[tuple], list[str]]:
        connection = self._hook(self.source.conn_id).get_conn()
        try:
            cursor = connection.cursor()
            cursor.execute(f"SELECT * FROM {self.source.sql_name}")
            rows = cursor.fetchall()
            columns = [description[0] for description in cursor.description or []]
            cursor.close()
        finally:
            connection.close()
        if not columns:
            raise AirflowException(f"{self.source.sql_name} returned no columns")
        return rows, columns

    def _write_target(self, rows: list[tuple], columns: list[str]) -> None:
        column_list = ", ".join(columns)
        placeholders = ", ".join(["%s"] * len(columns))
        insert = f"INSERT INTO {self.target.sql_name} ({column_list}) VALUES ({placeholders})"

        connection = self._hook(self.target.conn_id).get_conn()
        try:
            cursor = connection.cursor()
            # StarRocks refuses a bare DELETE, and truncating is what makes a re-run safe.
            cursor.execute(f"TRUNCATE TABLE {self.target.sql_name}")
            for start in range(0, len(rows), self.batch_size):
                cursor.executemany(insert, rows[start : start + self.batch_size])
            connection.commit()
            cursor.close()
        finally:
            connection.close()

    @staticmethod
    def _hook(conn_id: str):
        """Pick the hook from the connection's type, so the DAGs stay declarative."""
        conn_type = BaseHook.get_connection(conn_id).conn_type
        if conn_type in ("mysql", "mariadb"):
            return MySqlHook(mysql_conn_id=conn_id)
        if conn_type in ("postgres", "postgresql"):
            return PostgresHook(postgres_conn_id=conn_id)
        raise AirflowException(f"SqlCopyOperator does not support connection type {conn_type!r}")

    # --- lineage --------------------------------------------------------------

    def get_openlineage_facets_on_start(self) -> Any:
        """Describe the copy to Airflow's OpenLineage provider.

        Airflow reuses the facets collected at start when it emits the run's COMPLETE event,
        and the platform derives lineage from COMPLETE events only.
        """
        try:
            from airflow.providers.common.compat.openlineage.facet import (
                Dataset,
                SchemaDatasetFacet,
                SchemaDatasetFacetFields,
            )
            from airflow.providers.openlineage.extractors import OperatorLineage
        except ImportError:
            log.warning("the OpenLineage provider is unavailable; no lineage will be reported")
            return None

        try:
            _, columns = self._read_source_columns_only()
        except Exception:
            log.exception("could not read the source columns; reporting table-level lineage only")
            return OperatorLineage(
                inputs=[Dataset(namespace=self.source.namespace, name=self.source.dataset)],
                outputs=[Dataset(namespace=self.target.namespace, name=self.target.dataset)],
            )

        # The copy is column-for-column, so one schema describes both ends. The platform
        # turns two identical schemas into column-level edges.
        schema = SchemaDatasetFacet(
            fields=[SchemaDatasetFacetFields(name=column) for column in columns]
        )
        return OperatorLineage(
            inputs=[
                Dataset(
                    namespace=self.source.namespace,
                    name=self.source.dataset,
                    facets={"schema": schema},
                )
            ],
            outputs=[
                Dataset(
                    namespace=self.target.namespace,
                    name=self.target.dataset,
                    facets={"schema": schema},
                )
            ],
        )

    def _read_source_columns_only(self) -> tuple[None, list[str]]:
        connection = self._hook(self.source.conn_id).get_conn()
        try:
            cursor = connection.cursor()
            cursor.execute(f"SELECT * FROM {self.source.sql_name} LIMIT 0")
            columns = [description[0] for description in cursor.description or []]
            cursor.close()
        finally:
            connection.close()
        return None, columns
