#!/usr/bin/env bash
# Wait for the ingestion key, then start Airflow.
#
# OpenLineage ingestion on the platform requires an API key, and the key can only exist
# after the platform has been bootstrapped. The bootstrap container writes the transport
# configuration into the shared state volume; Airflow must not start before that file is
# there, or every event it emits is refused with 401 while the DAGs still look green.
set -euo pipefail

STATE_FILE="${STATE_DIR:-/state}/ol.env"

echo "== airflow: waiting for ${STATE_FILE}"
for _ in $(seq 1 150); do
  if [ -s "$STATE_FILE" ]; then
    break
  fi
  sleep 2
done

if [ ! -s "$STATE_FILE" ]; then
  echo "airflow: ${STATE_FILE} never appeared -- is the bootstrap container running?" >&2
  exit 1
fi

# Export everything the bootstrap wrote (the OpenLineage transport, with its key).
set -a
# shellcheck disable=SC1090
. "$STATE_FILE"
set +a

echo "== airflow: OpenLineage transport configured; starting airflow standalone"
exec airflow standalone
