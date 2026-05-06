#!/usr/bin/env bash
set -euo pipefail

if python3 -c "import yaml" >/dev/null 2>&1; then
  export PYTHON_BIN="python3"
else
  if [[ ! -d ".venv" ]]; then
    python3 -m venv .venv
  fi
  . .venv/bin/activate
  if ! python -c "import yaml" >/dev/null 2>&1; then
    python -m pip install -r backend/requirements.txt >/dev/null
  fi
  export PYTHON_BIN="python"
fi

"$PYTHON_BIN" scripts/validate_scenarios.py

# dependency_down scales cart to 0; run load-spike before it so the next scenario
# is not polluted by dependency_unavailable. Always restore cart on exit.
cleanup_otel_demo_cart() {
  kubectl -n otel-demo scale deploy/cart --replicas=1 >/dev/null 2>&1 || true
  kubectl -n otel-demo rollout status deploy/cart --timeout=3m >/dev/null 2>&1 || true
}
trap cleanup_otel_demo_cart EXIT

SCENARIOS=(
  deploy_latency
  db_slowdown
  traffic_spike_false_positive
  dependency_down
)

echo "warming baseline load..."
if [[ "${SKIP_BASELINE_LOAD:-0}" != "1" ]]; then
  scripts/run_load.sh || true
fi

failures=0
for scenario in "${SCENARIOS[@]}"; do
  echo "=== running scenario: $scenario ==="
  if ! scripts/run_scenario.sh "$scenario"; then
    failures=$((failures + 1))
  fi
done

if [[ "$failures" -gt 0 ]]; then
  echo "scenario suite finished with failures: $failures"
  exit 1
fi

echo "all scenarios passed"
