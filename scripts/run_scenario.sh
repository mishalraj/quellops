#!/usr/bin/env bash
set -euo pipefail

SCENARIO="${1:-}"
BACKEND_URL="${BACKEND_URL:-http://localhost:8088}"
ARTIFACT_DIR="${ARTIFACT_DIR:-artifacts}"
WAIT_SECONDS="${WAIT_SECONDS:-30}"

if [[ -z "$SCENARIO" ]]; then
  echo "usage: scripts/run_scenario.sh <scenario_name>"
  exit 1
fi

mkdir -p "$ARTIFACT_DIR/$SCENARIO"

inject() {
  case "$SCENARIO" in
    deploy_latency)
      kubectl -n otel-demo rollout restart deployment/frontend
      kubectl -n otel-demo rollout status deployment/frontend --timeout=5m
      ;;
    db_slowdown)
      kubectl -n otel-demo annotate deployment/productcatalogservice aidbg/test-ts="$(date +%s)" --overwrite
      kubectl -n otel-demo rollout restart deployment/productcatalogservice
      kubectl -n otel-demo rollout status deployment/productcatalogservice --timeout=5m
      ;;
    dependency_down)
      kubectl -n otel-demo scale deployment/cartservice --replicas=0
      local replicas
      replicas="$(kubectl -n otel-demo get deployment/cartservice -o jsonpath='{.spec.replicas}')"
      if [[ "$replicas" != "0" ]]; then
        echo "FAIL: dependency_down injection did not set replicas=0"
        exit 3
      fi
      ;;
    traffic_spike_false_positive)
      echo "load spike scenario: injection performed externally by load generator"
      ;;
    *)
      echo "unknown scenario: $SCENARIO"
      exit 2
      ;;
  esac
}

assert_result() {
  local incident_file="$1"
  python3 - "$SCENARIO" "$incident_file" <<'PY'
import json
import sys
scenario = sys.argv[1]
incident_file = sys.argv[2]
d = json.load(open(incident_file, "r", encoding="utf-8"))
causes = d.get("top_causes", [])
top = causes[0]["type"] if causes else ""
top3 = [c.get("type", "") for c in causes[:3]]
if scenario == "traffic_spike_false_positive":
    if top == "recent_deploy":
        print("FAIL: deploy incorrectly ranked top-1")
        sys.exit(1)
    print("PASS: deploy not top-1")
    sys.exit(0)
if "recent_deploy" not in top3:
    print("FAIL: expected recent_deploy in top-3 causes")
    sys.exit(1)
print(f"PASS: top={top} top3={top3}")
PY
}

inject
sleep "$WAIT_SECONDS"

curl -fsS "$BACKEND_URL/incident/latest" > "$ARTIFACT_DIR/$SCENARIO/incident.json"
curl -fsS "$BACKEND_URL/explain/latest" > "$ARTIFACT_DIR/$SCENARIO/llm_answer.json"
curl -fsS "$BACKEND_URL/graph/snapshot" > "$ARTIFACT_DIR/$SCENARIO/graph_snapshot.json"
curl -fsS "$BACKEND_URL/anomalies/latest" > "$ARTIFACT_DIR/$SCENARIO/anomalies.json"

python3 scripts/validate_incident.py "$ARTIFACT_DIR/$SCENARIO/incident.json"
assert_result "$ARTIFACT_DIR/$SCENARIO/incident.json"
echo "scenario '$SCENARIO' completed; artifacts at $ARTIFACT_DIR/$SCENARIO"
