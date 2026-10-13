#!/usr/bin/env python3
"""Re-sync the instances once the pipeline has created new objects, and wait for the result.

The bootstrap syncs the databases before the pipelines run, because the browser should show
something as soon as the stack is up. The pipelines then create the warehouse's views,
materialized view and tables, and StarRocks' serving views -- objects the platform cannot
know about until it syncs again.

A sync call returns as soon as the platform has *queued* the databases, so it is followed by a
wait: without it, the smoke check that ends `make up` can run while the registry is still
catching up, and report a half-populated stack for a demo that is in fact fine.

Uses only the standard library, and re-runs safely: a full sync is idempotent.
"""
from __future__ import annotations

import sys
import time

from bootstrap import ADMIN_EMAIL, ADMIN_PASSWORD, Api, count_objects, instances, require

# Every instance has objects the pipeline creates, so "no object at all" is the signal that the
# registry has not caught up yet.
WAIT_SECONDS = 240
POLL_SECONDS = 5

# The registry also analyses the views it discovers in the background, and ingests the pipeline's
# OpenLineage events as they arrive. Both take minutes, so `make up` waits for the demo's own
# claim to hold -- the chain that starts at the shop's orders reaches the serving layer -- rather
# than for a number.
ANCHOR_GUID = "mysql-shop;shop;;orders"
SERVING_INSTANCE = "starrocks-serving"
LINEAGE_WAIT_SECONDS = 420


def populated(api: Api, name: str) -> bool:
    counts = count_objects(api, name)
    return counts.get("TABLE", 0) + counts.get("VIEW", 0) + counts.get("MATERIALIZED_VIEW", 0) > 0


def wait_for_registry(api: Api, names: list[str], timeout: int = WAIT_SECONDS) -> bool:
    deadline = time.time() + timeout
    while True:
        pending = [name for name in names if not populated(api, name)]
        if not pending:
            return True
        if time.time() >= deadline:
            print(f"   warning: the registry still has nothing for {', '.join(pending)}")
            return False
        print(f"   waiting for the registry to catch up on: {', '.join(pending)}")
        time.sleep(POLL_SECONDS)


def lineage_edges_to_serving(api: Api) -> int:
    """The chain's column edges, or 0 while it does not reach the serving layer yet."""
    status, body = api.call(
        "LineageService/GetLineageGraph",
        {"guid": ANCHOR_GUID, "metaType": "TABLE", "lineageType": "TARGET", "depth": 10},
    )
    if status != 200:
        return 0
    if not any(node.get("guid", "").startswith(SERVING_INSTANCE + ";") for node in body.get("nodes", [])):
        return 0
    return len(body.get("edges", []))


def wait_for_lineage(api: Api, timeout: int = LINEAGE_WAIT_SECONDS) -> int:
    deadline = time.time() + timeout
    while True:
        edges = lineage_edges_to_serving(api)
        if edges:
            return edges
        if time.time() >= deadline:
            print("   warning: the lineage chain has not reached the serving layer yet")
            return 0
        print("   waiting for the view analysis and the ingested lineage to land")
        time.sleep(10)


def main() -> int:
    api = Api()
    status, body = api.call(
        "AuthService/Login", {"email": ADMIN_EMAIL, "password": ADMIN_PASSWORD}
    )
    require(status, body, "signing in")
    api.token = body.get("token", "")
    if not api.token:
        raise RuntimeError("sign-in returned no token")

    names = [spec["id"] for spec in instances()]
    status, body = api.call(
        "InstanceService/BatchSyncInstances",
        {"requests": [{"name": f"instances/{name}", "enableFullSync": True} for name in names]},
    )
    require(status, body, "syncing instances")

    for result in body.get("results", []):
        if result.get("error"):
            raise RuntimeError(f"syncing {result.get('name')} failed: {result['error']}")

    print(f"== resync: {len(names)} instances synced ({', '.join(names)})")
    wait_for_registry(api, names)
    print("   the registry has the objects the pipeline created; waiting for the lineage")
    edges = wait_for_lineage(api)
    if edges:
        print(f"   the chain from the shop's orders reaches the serving layer ({edges} column edges)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
