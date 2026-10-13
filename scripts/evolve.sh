#!/usr/bin/env bash
# Add one column to the shop's orders table, so the metadata browser's history and diff have
# something to show.
#
# Runs in a MySQL client container (the `schema-evolve` service). `make evolve` follows it
# with a re-sync, which is what makes the platform notice the change.
set -euo pipefail

COLUMN="loyalty_points"
MYSQL_HOST="${MYSQL_HOST:-mysql-shop}"
MYSQL_PORT="${MYSQL_PORT:-3306}"
MYSQL_DB="${MYSQL_DB:-shop}"

echo "== adding ${MYSQL_DB}.orders.${COLUMN}"
mysql --protocol=TCP -h "${MYSQL_HOST}" -P "${MYSQL_PORT}" \
  -u"${DEMO_USER:-demo}" -p"${DEMO_PASSWORD:-demo}" <<SQL
SET @exists := (
  SELECT count(*) FROM information_schema.columns
  WHERE table_schema = '${MYSQL_DB}' AND table_name = 'orders' AND column_name = '${COLUMN}'
);
SET @statement := IF(
  @exists = 0,
  'ALTER TABLE ${MYSQL_DB}.orders ADD COLUMN ${COLUMN} INT NOT NULL DEFAULT 0',
  'SELECT ''the column is already there'''
);
PREPARE migration FROM @statement;
EXECUTE migration;
SQL

echo "== schema-evolve: done"
