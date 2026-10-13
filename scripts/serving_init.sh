#!/usr/bin/env bash
# Create the StarRocks landing tables.
#
# The landing tables have to exist before the pipeline loads them. The serving views and the
# async materialized view are created later, by the build_serving_aggregates DAG, so that
# this container only prepares what the first load needs.
set -euo pipefail

# --skip-comments: StarRocks' parser rejects the fixture's leading `--` comment lines when
# the client forwards them, so they are stripped client-side.
mysql_args=(--protocol=TCP --skip-comments -h "${SR_HOST}" -P "${SR_PORT}" -uroot)

echo "== StarRocks: landing tables in ${SR_HOST}:${SR_PORT}"
mysql "${mysql_args[@]}" < /fixture/starrocks/10_ods_ddl.sql

echo "== serving-init: done"
