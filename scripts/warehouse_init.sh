#!/usr/bin/env bash
# Create the warehouse's layered schemas.
#
# The pipeline DDL recreates the views and tables on every run, but the schemas come from
# here so that even the very first metadata sync shows the ods/dwd/ads structure.
set -euo pipefail

echo "== PostgreSQL: creating schemas ods, dwd, ads in ${PGDATABASE}"
psql -v ON_ERROR_STOP=1 <<'SQL'
CREATE SCHEMA IF NOT EXISTS ods;
CREATE SCHEMA IF NOT EXISTS dwd;
CREATE SCHEMA IF NOT EXISTS ads;
SQL

echo "== warehouse-init: done"
