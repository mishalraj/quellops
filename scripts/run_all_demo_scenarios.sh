#!/usr/bin/env bash
set -euo pipefail

if python3 -c "import yaml" >/dev/null 2>&1; then
  export PYTHON_BIN="python3"
else
  if [[ ! -d ".venv" ]]; then
    python3 -m venv .venv
  fi
  # shellcheck source=/dev/null
  . .venv/bin/activate
  pip install -q pyyaml httpx 2>/dev/null || pip install -q -r backend/requirements.txt
  export PYTHON_BIN="python"
fi

SCENARIOS=(
  01_deploy_latency
  02_dependency_down
  03_traffic_spike
  04_upstream_errors
  05_db_slowdown
)

failures=0
for scenario in "${SCENARIOS[@]}"; do
  echo "=== running demo scenario: $scenario ==="
  scripts/demo_reset.sh || true
  sleep 15
  if ! scripts/run_demo_scenario.sh "$scenario"; then
    failures=$((failures + 1))
  fi
  echo ""
done

scripts/demo_reset.sh || true

if [[ "$failures" -gt 0 ]]; then
  echo "demo scenario suite finished with failures: $failures"
  exit 1
fi

echo "all demo scenarios passed"
