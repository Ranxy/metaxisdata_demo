#!/usr/bin/env bash
# Create the StarRocks serving objects.
#
# The landing tables must exist before the pipeline loads them, and the views plus the
# async materialized view are what the StarRocks lineage analyzer reads. Both scripts drop
# before they create, so this is safe to re-run.
set -euo pipefail

# --skip-comments: StarRocks' parser rejects the fixture's leading `--` comment lines when
# the client forwards them, so they are stripped client-side.
mysql_args=(--protocol=TCP --skip-comments -h "${SR_HOST}" -P "${SR_PORT}" -uroot)

echo "== StarRocks: landing tables in ${SR_HOST}:${SR_PORT}"
mysql "${mysql_args[@]}" < /fixture/starrocks/10_ods_ddl.sql

echo "== StarRocks: serving views and async materialized view"
mysql "${mysql_args[@]}" < /fixture/starrocks/20_ads_ddl.sql

echo "== serving-init: done"
