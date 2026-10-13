#!/usr/bin/env python3
"""Seed the samples the walkthrough points at.

Runs after the pipeline, because most of these samples name objects the pipeline creates:

* a second user, a group and a custom role, so Settings has something in it;
* one manual SQL entry, so the lineage page has a SQL source that is not a view;
* one application, its key, and a report of the SQL it runs, so the application-SQL pages
  show which applications read and write which tables.

Every step looks before it creates, and a sample that fails is reported and skipped rather
than taking the whole stack down: this is demo content, not a dependency of anything else.
"""
from __future__ import annotations

import json
import os
import sys

from bootstrap import ADMIN_EMAIL, ADMIN_PASSWORD, Api, require

SECOND_USER_EMAIL = os.environ.get("ANALYST_EMAIL", "analyst@demo.local")
SECOND_USER_PASSWORD = os.environ.get("ANALYST_PASSWORD", "Analyst-1234!")
WAREHOUSE_INSTANCE = "postgres-warehouse"

# Application SQL is reported over plain REST, not Connect: the producers are applications.
APPLICATION_SQL_URL = (
    os.environ.get("METAXISDATA_URL", "http://metaxisdata:8083") + "/api/v1/applicationSql"
)

MANUAL_SQL = (
    "-- Regional revenue per month. Registered as SQL rather than as a view, so the\n"
    "-- lineage of a statement someone keeps in a wiki is visible too.\n"
    "INSERT INTO ads.ads_daily_sales\n"
    "  (sales_date, channel, region, currency, order_count, net_line_amount)\n"
    "SELECT date_trunc('month', o.order_date)::date, o.channel, o.region, o.currency,\n"
    "       count(DISTINCT o.order_id), sum(o.net_amount)\n"
    "FROM dwd.dwd_order_fact o\n"
    "GROUP BY 1, 2, 3, 4"
)

REPORT_STATEMENTS = [
    {
        "sql": "INSERT INTO ads.ads_daily_sales (sales_date, channel, region, currency, "
        "order_count, net_line_amount) VALUES (?, ?, ?, ?, ?, ?)",
        "count": 128,
    },
    {
        "sql": "SELECT order_id, net_amount FROM dwd.dwd_order_fact WHERE channel = ? AND net_amount > ?",
        "count": 4213,
    },
    {
        "sql": "SELECT customer_id, count(*) FROM dwd.dwd_order_fact WHERE region = ? GROUP BY customer_id",
        "count": 96,
    },
    {
        "sql": "SELECT o.order_id FROM dwd.dwd_order_fact o "
        "JOIN ods.customers c ON c.customer_id = o.customer_id WHERE c.country = ?",
        "count": 512,
    },
]


def fail(what: str, status: int, body: dict) -> None:
    print(f"   warning: {what} was skipped: HTTP {status} {json.dumps(body)[:200]}")


def ensure_second_user(api: Api) -> None:
    status, body = api.call("UserService/CreateUser", {
        "user": {
            "email": SECOND_USER_EMAIL,
            "title": "Demo Analyst",
            "password": SECOND_USER_PASSWORD,
            "userType": "END_USER",
        }
    })
    if status == 200:
        print(f"   user {SECOND_USER_EMAIL} created")
    elif "exist" in json.dumps(body).lower() or status == 409:
        print(f"   user {SECOND_USER_EMAIL} already exists")
    else:
        fail("creating the second user", status, body)


def ensure_group(api: Api) -> None:
    status, body = api.call("GroupService/ListGroups", {"pageSize": 100})
    if status != 200:
        return fail("listing groups", status, body)
    if any(g.get("name") == "groups/analytics" for g in body.get("groups", [])):
        print("   group analytics already exists")
        return

    status, body = api.call("GroupService/CreateGroup", {
        "group": {"name": "groups/analytics", "title": "Analytics", "description": "Reads the warehouse"}
    })
    if status == 200:
        print("   group analytics created")
    else:
        fail("creating the group", status, body)


def ensure_role(api: Api) -> None:
    status, body = api.call("RoleService/ListRoles", {"pageSize": 100})
    if status != 200:
        return fail("listing roles", status, body)
    if any(r.get("name") == "roles/data-analyst" for r in body.get("roles", [])):
        print("   role data-analyst already exists")
        return

    status, body = api.call("RoleService/CreateRole", {
        "role": {
            "name": "roles/data-analyst",
            "title": "Data analyst",
            "description": "Read the registry and the lineage, write manual SQL",
            "permissions": [
                "metaxisdata.instances.list",
                "metaxisdata.databases.list",
                "metaxisdata.databases.read",
                "metaxisdata.manualSqls.list",
                "metaxisdata.manualSqls.create",
                "metaxisdata.lineage.get",
                "metaxisdata.openlineage.read",
            ],
        }
    })
    if status == 200:
        print("   role data-analyst created")
    else:
        fail("creating the role", status, body)


def warehouse_database(api: Api) -> str:
    status, body = api.call(
        "DatabaseService/ListDatabases", {"parent": f"instances/{WAREHOUSE_INSTANCE}", "pageSize": 50}
    )
    require(status, body, "listing the warehouse's databases")
    for database in body.get("databases", []):
        if database.get("name", "").rsplit("/", 1)[-1] == os.environ.get("PG_DB", "warehouse"):
            return database["name"]
    raise RuntimeError("the warehouse database is not registered yet")


def ensure_manual_sql(api: Api) -> None:
    parent = warehouse_database(api)
    status, body = api.call("DatabaseService/ListManualSQLs", {"parent": parent, "pageSize": 50})
    if status != 200:
        return fail("listing manual SQL", status, body)
    if body.get("manualSqls"):
        print("   manual SQL already registered")
        return

    status, body = api.call("DatabaseService/CreateManualSQL", {
        "parent": parent,
        "manualSqlId": "monthly-regional-revenue",
        "manualSql": {
            "title": "Monthly regional revenue",
            "schemaName": "ads",
            "comment": "Kept out of the database on purpose: the registry analyzes it anyway.",
            "sqlText": MANUAL_SQL,
        },
    })
    if status == 200:
        print("   manual SQL registered")
    else:
        fail("registering manual SQL", status, body)


def ensure_application(api: Api) -> None:
    status, body = api.call("ApplicationService/ListApplications", {"pageSize": 50})
    if status != 200:
        return fail("listing applications", status, body)
    if any(a.get("name") == "applications/shop-web" for a in body.get("applications", [])):
        print("   application shop-web already exists (its key is only shown once)")
        return

    status, body = api.call("ApplicationService/CreateApplication", {
        "application": {"name": "applications/shop-web", "displayName": "shop-web"},
        "apiKeyDescription": "shop-web",
    })
    if status != 200:
        return fail("creating the application", status, body)
    api_key = body.get("apiKey", "")
    print("   application shop-web created with its first key")

    report = {
        "instance": f"instances/{WAREHOUSE_INSTANCE}",
        "database": os.environ.get("PG_DB", "warehouse"),
        "schema": "dwd",
        "reportId": "demo-seed/1",
        "statements": REPORT_STATEMENTS,
    }
    status, body = api.call(f"{APPLICATION_SQL_URL}/report", report, bearer=api_key)
    if status != 200:
        return fail("reporting the application's SQL", status, body)
    print(f"   application SQL reported: {body.get('accepted', 0)} accepted, {body.get('rejected', 0)} rejected")


def main() -> int:
    print("== samples")
    api = Api()
    status, body = api.call("AuthService/Login", {"email": ADMIN_EMAIL, "password": ADMIN_PASSWORD})
    require(status, body, "signing in")
    api.token = body.get("token", "")
    if not api.token:
        raise RuntimeError("sign-in returned no token")

    for step in (
        ensure_second_user,
        ensure_group,
        ensure_role,
        ensure_manual_sql,
        ensure_application,
    ):
        try:
            step(api)
        except Exception as error:  # noqa: BLE001 - a sample must never stop the demo
            print(f"   warning: {step.__name__} failed: {error}")

    print("== samples: done")
    return 0


if __name__ == "__main__":
    sys.exit(main())
