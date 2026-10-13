#!/usr/bin/env python3
"""Check that the demo is actually populated.

One question only: has everything the walkthrough points at arrived? This is a demo, not a
test suite, so there are no numeric baselines -- each check is a floor ("at least one", "the
chain reaches StarRocks"), not an exact count.

Exits non-zero and prints what failed, so `make up` never reports a half-populated stack as
ready.
"""
from __future__ import annotations

import json
import os
import sys
import urllib.error
import urllib.request

from bootstrap import ADMIN_EMAIL, ADMIN_PASSWORD, Api, require

AIRFLOW_URL = os.environ.get("AIRFLOW_URL", "http://airflow:8080")
WAREHOUSE_INSTANCE = "postgres-warehouse"
DAGS = [
    "ingest_shop_to_warehouse",
    "build_warehouse_layers",
    "publish_to_serving",
    "build_serving_aggregates",
]

FAILURES: list[str] = []


def check(label: str, ok: bool, detail: str = "") -> None:
    print(f"  {'ok  ' if ok else 'FAIL'}  {label}{f'  ({detail})' if detail else ''}")
    if not ok:
        FAILURES.append(label)


def reachable(url: str) -> bool:
    try:
        with urllib.request.urlopen(url, timeout=10) as response:
            return response.status == 200
    except (urllib.error.URLError, OSError):
        return False


def children(api: Api, guid: str) -> list[tuple[str, str]]:
    status, body = api.call("DatabaseService/ListMetadata", {"parentGuid": guid, "pageSize": 500})
    if status != 200:
        return []
    return [
        (group["metaType"], item["guid"])
        for group in body.get("typesStoredMetadata", [])
        for item in group.get("list", [])
    ]


def count_objects(api: Api, instance: str) -> dict[str, int]:
    totals: dict[str, int] = {}

    def walk(guid: str) -> None:
        for meta_type, child in children(api, guid):
            totals[meta_type] = totals.get(meta_type, 0) + 1
            if meta_type in ("SCHEMA", "DATABASE"):
                walk(child)

    status, body = api.call(
        "DatabaseService/ListDatabases", {"parent": f"instances/{instance}", "pageSize": 50}
    )
    if status != 200:
        return totals
    for database in body.get("databases", []):
        if database.get("name", "").rsplit("/", 1)[-1] in ("information_schema", "sys", "postgres"):
            continue
        walk(database["guid"])
    return totals


def main() -> int:
    print("== smoke: is the demo populated?")
    api = Api()

    status, body = api.call("AuthService/Login", {"email": ADMIN_EMAIL, "password": ADMIN_PASSWORD})
    require(status, body, "signing in")
    api.token = body.get("token", "")
    check("the platform answers and the demo admin can sign in", bool(api.token))

    status, body = api.call("InstanceService/ListInstances", {"pageSize": 100})
    instances = body.get("instances", []) if status == 200 else []
    check(
        "the demo's instances are registered",
        len(instances) >= 3,
        ", ".join(instance.get("name", "") for instance in instances),
    )
    check(
        "every instance has synced",
        all(instance.get("lastSyncTime") for instance in instances) and bool(instances),
    )

    total_tables = 0
    total_views = 0
    for instance in instances:
        name = instance.get("name", "").rsplit("/", 1)[-1]
        counts = count_objects(api, name)
        tables = counts.get("TABLE", 0)
        views = counts.get("VIEW", 0) + counts.get("MATERIALIZED_VIEW", 0)
        total_tables += tables
        total_views += views
        check(f"{name}: tables and views were collected", tables >= 1 and views >= 1, f"{tables} tables, {views} views")
    check("the registry holds the demo's tables", total_tables >= 20, f"{total_tables} tables")
    check("the registry holds the demo's views", total_views >= 10, f"{total_views} views")

    status, graph = api.call(
        "LineageService/GetLineageGraph",
        {
            "guid": "mysql-shop;shop;;orders",
            "metaType": "TABLE",
            "lineageType": "TARGET",
            "depth": 10,
        },
    )
    nodes = graph.get("nodes", []) if status == 200 else []
    edges = graph.get("edges", []) if status == 200 else []
    engines = {node.get("guid", "").split(";")[0] for node in nodes}
    check("the lineage chain starts at the MySQL source", status == 200 and bool(edges), f"{len(edges)} edges")
    check(
        "the chain crosses the warehouse into the serving layer",
        {"mysql-shop", "postgres-warehouse", "starrocks-serving"}.issubset(engines),
        ", ".join(sorted(engines)),
    )
    check(
        "no lineage endpoint fell back to an external dataset",
        not any(node.get("guid", "").startswith("external:") for node in nodes),
    )

    status, body = api.call("OpenLineageService/ListOpenLineageRuns", {"pageSize": 500})
    check("OpenLineage runs were ingested", status == 200 and len(body.get("runs", [])) >= 4, f"{len(body.get('runs', []))} runs")
    status, body = api.call("OpenLineageService/ListOpenLineageTasks", {"pageSize": 200})
    tasks = json.dumps(body) if status == 200 else ""
    missing = [dag for dag in DAGS if dag not in tasks]
    check("every pipeline DAG reported to OpenLineage", not missing, f"missing: {', '.join(missing)}" if missing else "")

    status, body = api.call("DatabaseService/ListDatabases", {"parent": f"instances/{WAREHOUSE_INSTANCE}", "pageSize": 50})
    parent = ""
    for database in body.get("databases", []) if status == 200 else []:
        if database.get("name", "").endswith("/" + os.environ.get("PG_DB", "warehouse")):
            parent = database["name"]
    if parent:
        status, body = api.call("DatabaseService/ListManualSQLs", {"parent": parent, "pageSize": 50})
        check("the manual SQL sample is registered", status == 200 and bool(body.get("manualSqls")))
        status, body = api.call("ApplicationService/ListApplicationSQLs", {"parent": parent, "pageSize": 50})
        check("application SQL was reported", status == 200 and bool(body.get("applicationSqls")))
    else:
        check("the manual SQL sample is registered", False, "the warehouse database is not registered")
        check("application SQL was reported", False, "the warehouse database is not registered")

    status, body = api.call("ApplicationService/ListApplications", {"pageSize": 50})
    check("the application sample is registered", status == 200 and bool(body.get("applications")))

    status, body = api.call("AuditLogService/ListAuditLogs", {"parent": "workspaces/-", "pageSize": 1})
    check("the audit log has entries", status == 200 and bool(body.get("auditLogs")))

    check("Airflow is up", reachable(f"{AIRFLOW_URL}/api/v2/monitor/health"))

    print()
    if FAILURES:
        print(f"== smoke: {len(FAILURES)} check(s) failed")
        for failure in FAILURES:
            print(f"   - {failure}")
        return 1
    print("== smoke: the demo is populated")
    print("   open http://localhost:8083 and sign in as the demo admin (see .env.example)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
