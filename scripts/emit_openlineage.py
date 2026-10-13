#!/usr/bin/env python3
"""Send an OpenLineage event from a producer that is not Airflow.

The demo's pipelines report their own lineage, but the platform accepts events from anything
that speaks OpenLineage. This script stands in for a Spark job that rolls StarRocks data up
on a schedule, and it exercises the one piece of configuration that trips people up:
namespaces.

A producer states a namespace, and the platform has to work out which registered instance the
datasets belong to. It first looks for an administrator-issued namespace mapping, then tries
to match the namespace's host and port against an instance, and otherwise files the dataset as
an *external* one -- and tells the workspace administrators about it.

Run it twice:

  python3 emit_openlineage.py          # no mapping: the datasets land under external:spark-prod
  python3 emit_openlineage.py --map    # create the mapping, then send the same event again

Both runs use a fresh run id, so the second event is a new run rather than a repeat.
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import urllib.error
import urllib.request
import uuid
from datetime import datetime, timezone

BASE = os.environ.get("METAXISDATA_URL", "http://metaxisdata:8083")
CONNECT = f"{BASE}/metaxisdata.v1."
STATE_FILE = os.path.join(os.environ.get("STATE_DIR", "/state"), "ol.env")

NAMESPACE = "spark-prod"
INSTANCE = "starrocks-serving"
DATABASE = "ods"
JOB_NAME = "spark_nightly_rollup"


def ingestion_key() -> str:
    """The key the bootstrap container issued, read from the shared state volume."""
    with open(STATE_FILE, encoding="utf-8") as handle:
        for line in handle:
            if line.startswith("DEMO_OPENLINEAGE_KEY="):
                return line.split("=", 1)[1].strip().strip("'")
    raise SystemExit(f"no ingestion key in {STATE_FILE}; has the bootstrap container run?")


def post(url: str, payload: dict, bearer: str) -> tuple[int, str]:
    request = urllib.request.Request(
        url, data=json.dumps(payload).encode(), method="POST"
    )
    request.add_header("Content-Type", "application/json")
    request.add_header("Authorization", f"Bearer {bearer}")
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            status, text = response.status, response.read().decode()
    except urllib.error.HTTPError as error:
        status, text = error.code, error.read().decode()
    try:
        return status, json.loads(text)
    except json.JSONDecodeError:
        return status, text


def ol_event() -> dict:
    """A COMPLETE run event. Only COMPLETE events produce lineage."""
    return {
        "_schemaURL": "https://openlineage.io/spec/2-0-2/OpenLineage.json#/$defs/RunEvent",
        "eventTime": datetime.now(timezone.utc).isoformat(),
        "eventType": "COMPLETE",
        "producer": "https://github.com/Ranxy/metaxisdata-demo",
        "run": {"runId": str(uuid.uuid4())},
        "job": {"namespace": NAMESPACE, "name": JOB_NAME},
        # Both datasets already exist in the serving layer, so once the namespace resolves the
        # platform draws a real edge between two registered objects.
        "inputs": [{"namespace": NAMESPACE, "name": f"{DATABASE}.mv_daily_sales"}],
        "outputs": [{"namespace": NAMESPACE, "name": f"{DATABASE}.dwd_order_fact"}],
    }


def ensure_mapping(admin_token: str) -> None:
    status, body = post(f"{CONNECT}OpenLineageService/ListNamespaceMappings", {}, admin_token)
    if status == 200 and NAMESPACE in json.dumps(body):
        print(f"   namespace mapping for {NAMESPACE} already exists")
        return
    status, body = post(
        f"{CONNECT}OpenLineageService/CreateNamespaceMapping",
        {"mapping": {"namespace": NAMESPACE, "instanceResourceId": INSTANCE, "databaseName": DATABASE}},
        admin_token,
    )
    if status == 200:
        print(f"   namespace mapping created: {NAMESPACE} -> {INSTANCE} ({DATABASE})")
    else:
        raise SystemExit(f"could not create the mapping: HTTP {status} {json.dumps(body)[:200]}")


def admin_token() -> str:
    status, body = post(
        f"{CONNECT}AuthService/Login",
        {
            "email": os.environ.get("ADMIN_EMAIL", "admin@demo.local"),
            "password": os.environ.get("ADMIN_PASSWORD", "Demo-admin-1234!"),
        },
        "",
    )
    if status != 200 or not isinstance(body, dict) or "token" not in body:
        raise SystemExit(f"could not sign in: HTTP {status} {json.dumps(body)[:200]}")
    return body["token"]


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--map",
        action="store_true",
        help="create the namespace mapping before sending, so the datasets resolve to the instance",
    )
    args = parser.parse_args()

    if args.map:
        print(f"== mapping the {NAMESPACE} namespace")
        ensure_mapping(admin_token())

    print(f"== sending one OpenLineage COMPLETE event for job {JOB_NAME}")
    # The ingestion endpoint is plain REST and is key-gated.
    status, body = post(f"{BASE}/api/v1/lineage", ol_event(), ingestion_key())
    if status != 200:
        raise SystemExit(f"ingestion failed: HTTP {status} {json.dumps(body)[:300]}")

    print("   accepted")
    if args.map:
        print(f"   the datasets should now belong to instances/{INSTANCE}")
    else:
        print(f"   the datasets are filed as external:{NAMESPACE}:... -- run with --map to fix that")
    return 0


if __name__ == "__main__":
    sys.exit(main())
