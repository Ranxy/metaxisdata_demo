#!/usr/bin/env python3
"""Bring a fresh metaxisdata deployment to the state this demo needs.

Runs once the platform is healthy and the databases are prepared:

  1. create the first account -- the first end user becomes the workspace administrator --
     and sign in;
  2. register the demo's three databases as instances;
  3. run a full metadata sync, so the browser has something to show immediately;
  4. issue the OpenLineage ingestion key and write it to /state/ol.env. Ingestion is
     key-gated and the key can only exist after the platform is up, so the Airflow
     container waits for this file before it starts.

Every step looks before it creates, so `make up` may re-run this container at any time.

The script uses only the standard library, so its container needs no package downloads.

Requests go to the ConnectRPC endpoints (`/metaxisdata.v1.<Service>/<Method>`) rather than
the grpc-gateway mirror under `/v1/*`: the two accept different request bodies, and the
Connect form is the one that matches the protobuf messages. A bearer token is enough --
requests carrying `Authorization` are exempt from the CSRF check.
"""
from __future__ import annotations

import json
import os
import shlex
import sys
import urllib.error
import urllib.request

BASE = os.environ.get("METAXISDATA_URL", "http://metaxisdata:8083")
CONNECT = f"{BASE}/metaxisdata.v1."
STATE_DIR = os.environ.get("STATE_DIR", "/state")

ADMIN_EMAIL = os.environ.get("ADMIN_EMAIL", "admin@demo.local")
ADMIN_PASSWORD = os.environ.get("ADMIN_PASSWORD", "Demo-admin-1234!")
DEMO_USER = os.environ.get("DEMO_USER", "demo")
DEMO_PASSWORD = os.environ.get("DEMO_PASSWORD", "demo")


def instances() -> list[dict]:
    """The instances this demo registers, spelled exactly the way the pipelines address them.

    OpenLineage resolves a dataset by matching its host and port against a registered
    instance, so these values have to agree with the Airflow connections and the pipeline's
    JDBC settings character for character. A mismatch is silent: the dataset then shows up
    as an external one instead of as real lineage.
    """
    return [
        {
            "id": "mysql-shop",
            "title": "Shop (MySQL)",
            "engine": "MYSQL",
            "host": os.environ.get("MYSQL_HOST", "mysql-shop"),
            "port": os.environ.get("MYSQL_PORT", "3306"),
            "database": os.environ.get("MYSQL_DB", "shop"),
            "username": DEMO_USER,
            "password": DEMO_PASSWORD,
        },
        {
            "id": "postgres-warehouse",
            "title": "Warehouse (PostgreSQL)",
            "engine": "POSTGRES",
            "host": os.environ.get("PG_HOST", "postgres-warehouse"),
            "port": os.environ.get("PG_PORT", "5432"),
            "database": os.environ.get("PG_DB", "warehouse"),
            "username": DEMO_USER,
            "password": DEMO_PASSWORD,
        },
        {
            "id": "starrocks-serving",
            "title": "Serving (StarRocks)",
            "engine": "STARROCKS",
            "host": os.environ.get("SR_HOST", "starrocks-serving"),
            "port": os.environ.get("SR_PORT", "9030"),
            "database": "ods",
            "username": "root",
            "password": "",
        },
    ]


class Api:
    """A tiny ConnectRPC client: one POST per call, one bearer token."""

    def __init__(self) -> None:
        self.token = ""

    def call(self, path: str, body: dict | None = None, bearer: str = "") -> tuple[int, dict]:
        url = path if path.startswith("http") else CONNECT + path
        data = None if body is None else json.dumps(body).encode()
        request = urllib.request.Request(url, data=data, method="POST")
        if data is not None:
            request.add_header("Content-Type", "application/json")
        token = bearer or self.token
        if token:
            request.add_header("Authorization", f"Bearer {token}")
        try:
            with urllib.request.urlopen(request, timeout=60) as response:
                payload = response.read().decode()
                return response.status, (json.loads(payload) if payload else {})
        except urllib.error.HTTPError as error:
            payload = error.read().decode()
            try:
                return error.code, json.loads(payload)
            except json.JSONDecodeError:
                return error.code, {"message": payload}


def require(status: int, body: dict, what: str) -> dict:
    if status != 200:
        raise RuntimeError(f"{what} failed: HTTP {status} {json.dumps(body)[:400]}")
    return body


def is_already_exists(status: int, body: dict) -> bool:
    return status == 409 or "exist" in json.dumps(body).lower()


def metadata_children(api: Api, guid: str) -> list[tuple[str, str]]:
    """The `(metaType, guid)` pairs directly under a metadata node."""
    status, body = api.call("DatabaseService/ListMetadata", {"parentGuid": guid, "pageSize": 500})
    if status != 200:
        return []
    return [
        (group["metaType"], item["guid"])
        for group in body.get("typesStoredMetadata", [])
        for item in group.get("list", [])
    ]


def count_objects(api: Api, instance: str) -> dict[str, int]:
    """How many objects of each type the registry holds for one instance.

    Walks database -> schema -> object, skipping the engine's system databases. Shared by the
    resync wait and the smoke check.
    """
    totals: dict[str, int] = {}

    def walk(guid: str) -> None:
        for meta_type, child in metadata_children(api, guid):
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


def ensure_admin(api: Api) -> None:
    user = {
        "email": ADMIN_EMAIL,
        "title": "Demo Admin",
        # The workspace password policy wants length, digits, letters, uppercase and a
        # special character; a weaker value is rejected.
        "password": ADMIN_PASSWORD,
        "userType": "END_USER",
    }
    status, body = api.call("UserService/CreateUser", {"user": user})
    if status != 200 and not is_already_exists(status, body):
        raise RuntimeError(f"creating the first account failed: HTTP {status} {body}")
    if status == 200:
        print(f"   admin account {ADMIN_EMAIL} created (workspace administrator)")

    status, body = api.call(
        "AuthService/Login", {"email": ADMIN_EMAIL, "password": ADMIN_PASSWORD}
    )
    require(status, body, "signing in")
    api.token = body.get("token", "")
    if not api.token:
        raise RuntimeError("sign-in returned no token")
    print("   signed in")


def ensure_instances(api: Api) -> list[str]:
    status, body = api.call("InstanceService/ListInstances", {"pageSize": 100})
    require(status, body, "listing instances")
    known = {i.get("resourceId") or i.get("name", "").split("/")[-1] for i in body.get("instances", [])}

    created = []
    for spec in instances():
        if spec["id"] in known:
            print(f"   instance {spec['id']} already registered")
            continue
        request = {
            "instanceId": spec["id"],
            "instance": {
                "title": spec["title"],
                "engine": spec["engine"],
                "activation": True,
                "dataSources": [
                    {
                        "name": f"instances/{spec['id']}/dataSources/admin",
                        "type": "ADMIN",
                        "host": spec["host"],
                        "port": spec["port"],
                        "username": spec["username"],
                        "password": spec["password"],
                        "database": spec["database"],
                    }
                ],
            },
        }
        status, body = api.call("InstanceService/CreateInstance", request)
        require(status, body, f"registering {spec['id']}")
        created.append(spec["id"])
        print(f"   instance {spec['id']} registered ({spec['engine']} at {spec['host']}:{spec['port']})")

    names = [spec["id"] for spec in instances()]
    status, body = api.call(
        "InstanceService/BatchSyncInstances",
        {"requests": [{"name": f"instances/{name}", "enableFullSync": True} for name in names]},
    )
    require(status, body, "syncing instances")
    for result in body.get("results", []):
        if result.get("error"):
            raise RuntimeError(f"syncing {result.get('name')} failed: {result['error']}")
    print(f"   full metadata sync done for {len(names)} instances")
    return names


STATE_FILE = "ol.env"


def _state_path() -> str:
    return os.path.join(STATE_DIR, STATE_FILE)


def read_state_key() -> str:
    """Read back the ingestion key from a previous run, if there is one."""
    try:
        with open(_state_path(), encoding="utf-8") as handle:
            for line in handle:
                if line.startswith("DEMO_OPENLINEAGE_KEY="):
                    return line.split("=", 1)[1].strip()
    except OSError:
        return ""
    return ""


def write_state(plaintext: str) -> None:
    """Hand the key to the Airflow container through the shared state volume.

    The value is shell-quoted: the Airflow entrypoint sources this file, and the transport is
    a JSON object that must survive that.
    """
    transport = {
        "type": "http",
        "url": BASE,
        "endpoint": "api/v1/lineage",
        "auth": {"type": "api_key", "apiKey": plaintext},
    }
    os.makedirs(STATE_DIR, exist_ok=True)
    path = _state_path()
    with open(path, "w", encoding="utf-8") as handle:
        handle.write(f"AIRFLOW__OPENLINEAGE__TRANSPORT={shlex.quote(json.dumps(transport))}\n")
        handle.write(f"DEMO_OPENLINEAGE_KEY={shlex.quote(plaintext)}\n")
    # The Airflow container runs as its own user and only has to read this file. It lives in
    # a Docker volume on the demo host, never in the repository.
    os.chmod(path, 0o644)


def ensure_ingestion_key(api: Api) -> str:
    """Issue the key Airflow ships events with, once.

    The key is a secret: it is written to the state volume (gitignored) and never to the
    repository. A key already handed out is reused rather than re-issued, so re-running the
    demo does not leave a trail of live keys behind.
    """
    plaintext = read_state_key()
    if plaintext:
        write_state(plaintext)
        print("   ingestion key already handed out; state file refreshed")
        return plaintext

    # No usable key on disk, so any key the platform still holds is orphaned: revoke it and
    # issue a fresh one.
    status, body = api.call("OpenLineageService/ListAPIKeys", {})
    require(status, body, "listing ingestion keys")
    for key in body.get("apiKeys", []):
        if key.get("description") == "airflow":
            api.call("OpenLineageService/RevokeAPIKey", {"name": key["name"]})
            print(f"   revoked key {key['name']}, which nothing holds any more")

    status, body = api.call("OpenLineageService/CreateAPIKey", {"description": "airflow"})
    require(status, body, "issuing the ingestion key")
    plaintext = body.get("key", "")
    if not plaintext:
        raise RuntimeError("the ingestion key response carried no key")

    write_state(plaintext)
    print(f"   ingestion key issued and written to {_state_path()}")
    return plaintext


def main() -> int:
    print(f"== bootstrap: {BASE}")
    api = Api()
    ensure_admin(api)
    ensure_instances(api)
    ensure_ingestion_key(api)
    print("== bootstrap: done")
    return 0


if __name__ == "__main__":
    sys.exit(main())
