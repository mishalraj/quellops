#!/usr/bin/env bash
set -euo pipefail

SCENARIO="${1:-}"
BACKEND_URL="${BACKEND_URL:-http://localhost:8088}"
ARTIFACT_DIR="${ARTIFACT_DIR:-artifacts}"
WAIT_SECONDS="${WAIT_SECONDS:-30}"
PYTHON_BIN="${PYTHON_BIN:-python3}"
SCENARIO_FILE=""

if [[ -z "$SCENARIO" ]]; then
  echo "usage: scripts/run_scenario.sh <scenario_name>"
  exit 1
fi

SCENARIO_FILE="scenarios/${SCENARIO}.yaml"
if [[ ! -f "$SCENARIO_FILE" ]]; then
  echo "unknown scenario spec: $SCENARIO_FILE"
  exit 2
fi

mkdir -p "$ARTIFACT_DIR/$SCENARIO"

yaml_eval() {
  local expr="$1"
  "$PYTHON_BIN" - "$SCENARIO_FILE" "$expr" <<'PY'
import sys
import yaml

path = sys.argv[1]
expr = sys.argv[2]
d = yaml.safe_load(open(path, "r", encoding="utf-8")) or {}

def get(obj, dotted):
    cur = obj
    for part in dotted.split("."):
        if not part:
            continue
        if not isinstance(cur, dict):
            return None
        cur = cur.get(part)
    return cur

v = get(d, expr)
if isinstance(v, list):
    print(",".join(str(x) for x in v))
elif v is None:
    print("")
else:
    print(str(v))
PY
}

inject() {
  local inject_type
  local ns
  local deploy
  inject_type="$(yaml_eval 'inject.type')"
  ns="$(yaml_eval 'target.namespace')"
  deploy="$(yaml_eval 'target.deployment')"

  if [[ -z "$inject_type" || -z "$ns" || -z "$deploy" ]]; then
    echo "invalid scenario spec: missing inject.type or target fields"
    exit 2
  fi

  case "$inject_type" in
    rollout_restart)
      kubectl -n "$ns" rollout restart "deployment/$deploy"
      kubectl -n "$ns" rollout status "deployment/$deploy" --timeout=5m
      ;;
    annotate_placeholder)
      kubectl -n "$ns" annotate "deployment/$deploy" aidbg/test-ts="$(date +%s)" --overwrite
      kubectl -n "$ns" rollout restart "deployment/$deploy"
      kubectl -n "$ns" rollout status "deployment/$deploy" --timeout=5m
      ;;
    scale_to_zero)
      kubectl -n "$ns" scale "deployment/$deploy" --replicas=0
      local replicas
      replicas="$(kubectl -n "$ns" get "deployment/$deploy" -o jsonpath='{.spec.replicas}')"
      if [[ "$replicas" != "0" ]]; then
        echo "FAIL: scale_to_zero injection did not set replicas=0"
        exit 3
      fi
      ;;
    load_spike_only)
      echo "load spike scenario: injection performed externally by load generator"
      ;;
    *)
      echo "unknown inject.type: $inject_type"
      exit 2
      ;;
  esac
}

assert_result() {
  local incident_file="$1"
  "$PYTHON_BIN" - "$SCENARIO_FILE" "$incident_file" <<'PY'
import json
import sys

import yaml

scenario_file = sys.argv[1]
incident_file = sys.argv[2]
d = json.load(open(incident_file, "r", encoding="utf-8"))
causes = d.get("top_causes", [])
top = causes[0]["type"] if causes else ""
top3 = [c.get("type", "") for c in causes[:3]]
spec = yaml.safe_load(open(scenario_file, "r", encoding="utf-8")) or {}
assert_cfg = spec.get("assert", {}) or {}
allow_top_k = int(assert_cfg.get("allow_top_k", 3))
expect_top_types = assert_cfg.get("expect_top_types", []) or []
not_top_1_type = assert_cfg.get("not_top_1_type")

if not_top_1_type and top == not_top_1_type:
    print(f"FAIL: {not_top_1_type} incorrectly ranked top-1")
    sys.exit(1)

if expect_top_types:
    top_k = [c.get("type", "") for c in causes[:allow_top_k]]
    if not any(t in top_k for t in expect_top_types):
        print(f"FAIL: expected one of {expect_top_types} in top-{allow_top_k}, got {top_k}")
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

"$PYTHON_BIN" scripts/validate_incident.py "$ARTIFACT_DIR/$SCENARIO/incident.json"
assert_result "$ARTIFACT_DIR/$SCENARIO/incident.json"
echo "scenario '$SCENARIO' completed; artifacts at $ARTIFACT_DIR/$SCENARIO"
