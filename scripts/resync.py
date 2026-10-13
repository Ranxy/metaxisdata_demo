#!/usr/bin/env python3
"""Re-sync the instances once the pipeline has created new objects.

The bootstrap syncs the databases before the pipelines run, because the browser should show
something as soon as the stack is up. The pipelines then create the warehouse's views,
materialized view and tables, and StarRocks' serving views -- objects the platform cannot
know about until it syncs again.

Without this step the metadata browser would be missing the layers the demo is about, and the
lineage graph would stop at objects the registry has never seen.

Uses only the standard library, and re-runs safely: a full sync is idempotent.
"""
from __future__ import annotations

import os
import sys

from bootstrap import ADMIN_EMAIL, ADMIN_PASSWORD, Api, instances, require


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
    print("   the platform queues lineage analysis for every view it just discovered")
    return 0


if __name__ == "__main__":
    sys.exit(main())
