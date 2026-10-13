#!/usr/bin/env bash
# Trigger the pipeline DAGs in order and wait for each one.
#
# The DAGs are sequential on purpose: every hop reads what the previous hop wrote, and there
# are no cross-DAG dependencies, so firing them together could copy a half-loaded table.
set -euo pipefail

DAGS=(
  ingest_shop_to_warehouse
  build_warehouse_layers
  publish_to_serving
  build_serving_aggregates
)
TIMEOUT="${DAG_TIMEOUT_SECONDS:-900}"
FAILED=0

for dag in "${DAGS[@]}"; do
  echo "== triggering ${dag}"
  airflow dags unpause "$dag" >/dev/null 2>&1 || true

  run_id="demo_$(date +%Y%m%d%H%M%S)_${dag}"
  airflow dags trigger --run-id "$run_id" "$dag" >/dev/null

  deadline=$(( $(date +%s) + TIMEOUT ))
  while true; do
    state="$(airflow dags state "$dag" "$run_id" 2>/dev/null | tail -n 1 | tr -d '[:space:]' || true)"
    case "$state" in
      success)
        echo "   ${dag}: success"
        break
        ;;
      failed)
        echo "   ${dag}: FAILED" >&2
        airflow tasks states-for-dag-run "$dag" "$run_id" >&2 || true
        exit 1
        ;;
      ""|queued|running|scheduled|up_for_retry|up_for_reschedule|restarting)
        ;;
      *)
        echo "   ${dag}: unexpected state '${state}'" >&2
        exit 1
        ;;
    esac
    if [ "$(date +%s)" -gt "$deadline" ]; then
      echo "   ${dag}: timed out after ${TIMEOUT}s" >&2
      exit 1
    fi
    sleep 10
  done
done

echo "== etl-runner: all ${#DAGS[@]} DAGs succeeded"

# The pipelines created objects the platform has not seen yet, so sync once more before the
# walkthrough starts: the metadata browser and the lineage graph depend on it.
echo "== re-syncing the instances"
python3 /scripts/resync.py

# Now that the warehouse has its layers, seed the samples that name them: a manual SQL entry,
# an application and its report, and the governance examples.
python3 /scripts/seed_samples.py
