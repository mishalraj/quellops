#!/usr/bin/env bash
set -euo pipefail

SCENARIO="${1:-}"
BACKEND_URL="${BACKEND_URL:-http://localhost:8088}"
EDGE_URL="${EDGE_URL:-http://localhost:8080/}"
ARTIFACT_DIR="${ARTIFACT_DIR:-artifacts/demo}"
PYTHON_BIN="${PYTHON_BIN:-python3}"
USE_IN_CLUSTER_LOAD="${USE_IN_CLUSTER_LOAD:-0}"

if [[ -z "$SCENARIO" ]]; then
  echo "usage: scripts/run_demo_scenario.sh <scenario_name>"
  echo "  example: scripts/run_demo_scenario.sh 01_deploy_latency"
  exit 1
fi

SCENARIO_FILE="scenarios/demo/${SCENARIO}.yaml"
if [[ ! -f "$SCENARIO_FILE" ]]; then
  echo "unknown scenario: $SCENARIO_FILE"
  exit 2
fi

mkdir -p "$ARTIFACT_DIR/$SCENARIO"

yaml_get() {
  "$PYTHON_BIN" - "$SCENARIO_FILE" "$1" <<'PY'
import sys
import yaml
path, key = sys.argv[1], sys.argv[2]
d = yaml.safe_load(open(path, encoding="utf-8")) or {}

def get(obj, dotted):
    cur = obj
    for part in dotted.split("."):
        if not isinstance(cur, dict):
            return None
        cur = cur.get(part)
    return cur

v = get(d, key)
if isinstance(v, list):
    print(",".join(str(x) for x in v))
elif v is None:
    print("")
else:
    print(v)
PY
}

NS="$(yaml_get namespace)"
DEPLOY="$(yaml_get target.deployment)"
INJECT_TYPE="$(yaml_get inject.type)"
WAIT_SECONDS="$(yaml_get wait_seconds)"
WAIT_SECONDS="${WAIT_SECONDS:-45}"

run_load() {
  local dur rps
  dur="$(yaml_get load.duration_seconds)"
  rps="$(yaml_get load.rps)"
  dur="${dur:-90}"
  rps="${rps:-10}"
  if [[ "$USE_IN_CLUSTER_LOAD" == "1" ]]; then
    kubectl -n "$NS" delete job aidbg-demo-loadgen --ignore-not-found >/dev/null 2>&1 || true
    kubectl apply -f deploy/aidbg-demo/loadgen-job.yaml >/dev/null
    kubectl -n "$NS" wait --for=condition=complete job/aidbg-demo-loadgen --timeout=10m || true
    return 0
  fi
  DURATION_SECONDS="$dur" RPS="$rps" EDGE_URL="$EDGE_URL" \
    "$PYTHON_BIN" demo/loadgen/load.py || true
}

inject() {
  case "$INJECT_TYPE" in
    fault_latency_rollout)
      local ms
      ms="$(yaml_get inject.fault_latency_ms)"
      ms="${ms:-400}"
      kubectl -n "$NS" set env "deploy/${DEPLOY}" \
        FAULT_LATENCY_MS="$ms" FAULT_ERROR_RATE=0
      kubectl -n "$NS" rollout restart "deploy/${DEPLOY}"
      kubectl -n "$NS" rollout status "deploy/${DEPLOY}" --timeout=5m
      ;;
    fault_error_rollout)
      local rate
      rate="$(yaml_get inject.fault_error_rate)"
      rate="${rate:-0.5}"
      kubectl -n "$NS" set env "deploy/${DEPLOY}" \
        FAULT_ERROR_RATE="$rate" FAULT_LATENCY_MS=0
      kubectl -n "$NS" rollout restart "deploy/${DEPLOY}"
      kubectl -n "$NS" rollout status "deploy/${DEPLOY}" --timeout=5m
      ;;
    scale_to_zero)
      kubectl -n "$NS" scale "deploy/${DEPLOY}" --replicas=0
      ;;
    load_only)
      echo "load_only: no k8s injection"
      ;;
    db_slow_query_fault)
      local sleep_ms migration
      sleep_ms="$(yaml_get inject.fault_db_sleep_ms)"
      sleep_ms="${sleep_ms:-300}"
      migration="$(yaml_get inject.migration)"
      if [[ -n "$migration" ]]; then
        kubectl -n "$NS" exec "deploy/${DEPLOY}" -- \
          env MIGRATION_ACTION="$migration" SKIP_DB_MIGRATE=1 \
          python -m services.common.migrate || true
      fi
      "$PYTHON_BIN" - "$SCENARIO_FILE" "$BACKEND_URL" <<'PY' || true
import json
import sys
import urllib.request
import yaml

scenario_file, backend_url = sys.argv[1], sys.argv[2]
spec = yaml.safe_load(open(scenario_file, encoding="utf-8")) or {}
for item in (spec.get("inject") or {}).get("post_changes") or []:
    if not isinstance(item, dict):
        continue
    payload = {
        "kind": item.get("kind", "code.push"),
        "entity": item.get("entity", "unknown"),
        "namespace": spec.get("namespace"),
        "metadata": item.get("metadata") or {},
    }
    req = urllib.request.Request(
        f"{backend_url.rstrip('/')}/ingest/change",
        data=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=10) as resp:
        resp.read()
PY
      kubectl -n "$NS" set env "deploy/${DEPLOY}" \
        FAULT_DB_SLEEP_MS="$sleep_ms" FAULT_LATENCY_MS=0 FAULT_ERROR_RATE=0
      kubectl -n "$NS" rollout restart "deploy/${DEPLOY}"
      kubectl -n "$NS" rollout status "deploy/${DEPLOY}" --timeout=5m
      ;;
    post_change)
      "$PYTHON_BIN" - "$SCENARIO_FILE" "$BACKEND_URL" <<'PY'
import json
import sys
import urllib.request
import yaml

scenario_file, backend_url = sys.argv[1], sys.argv[2]
spec = yaml.safe_load(open(scenario_file, encoding="utf-8")) or {}
inject = spec.get("inject") or {}
payload = {
    "kind": inject.get("change_kind", "code.push"),
    "entity": inject.get("entity") or (spec.get("target") or {}).get("deployment", "unknown"),
    "namespace": spec.get("namespace"),
    "metadata": inject.get("metadata") or {},
}
req = urllib.request.Request(
    f"{backend_url.rstrip('/')}/ingest/change",
    data=json.dumps(payload).encode("utf-8"),
    headers={"Content-Type": "application/json"},
    method="POST",
)
with urllib.request.urlopen(req, timeout=10) as resp:
    resp.read()
print("posted change event", payload["kind"], payload["entity"])
PY
      ;;
    *)
      echo "unknown inject.type: $INJECT_TYPE"
      exit 2
      ;;
  esac
}

SPANS_BEFORE=0
if curl -fsS "${BACKEND_URL}/stats/ingest" >/dev/null 2>&1; then
  SPANS_BEFORE="$("$PYTHON_BIN" -c "import json,urllib.request; d=json.load(urllib.request.urlopen('${BACKEND_URL}/stats/ingest')); print(int(d.get('spans_received',0)))" 2>/dev/null || echo 0)"
fi

echo "=== demo scenario: $SCENARIO (ns=$NS deploy=$DEPLOY) ==="
inject
run_load
sleep "$WAIT_SECONDS"

curl -fsS "${BACKEND_URL}/incident/latest" >"$ARTIFACT_DIR/$SCENARIO/incident.json"
curl -fsS "${BACKEND_URL}/anomalies/latest" >"$ARTIFACT_DIR/$SCENARIO/anomalies.json"
curl -fsS "${BACKEND_URL}/graph/snapshot" >"$ARTIFACT_DIR/$SCENARIO/graph_snapshot.json"
curl -fsS "${BACKEND_URL}/explain/latest" >"$ARTIFACT_DIR/$SCENARIO/llm_answer.json" 2>/dev/null || true

"$PYTHON_BIN" scripts/validate_incident.py "$ARTIFACT_DIR/$SCENARIO/incident.json"

"$PYTHON_BIN" - "$SCENARIO_FILE" "$ARTIFACT_DIR/$SCENARIO" "${BACKEND_URL}" "$SPANS_BEFORE" <<'PY'
import json
import sys
import urllib.request

import yaml

scenario_file = sys.argv[1]
artifact_dir = sys.argv[2]
backend_url = sys.argv[3]
spans_before = int(sys.argv[4])

spec = yaml.safe_load(open(scenario_file, encoding="utf-8")) or {}
assert_cfg = spec.get("assert") or {}
target = (spec.get("target") or {}).get("deployment", "")

incident = json.load(open(f"{artifact_dir}/incident.json", encoding="utf-8"))
graph = json.load(open(f"{artifact_dir}/graph_snapshot.json", encoding="utf-8"))
anomalies = json.load(open(f"{artifact_dir}/anomalies.json", encoding="utf-8")).get("anomalies", [])

causes = incident.get("top_causes", [])
top = causes[0]["type"] if causes else ""
top_entity = causes[0].get("entity") if causes else None
allow_k = int(assert_cfg.get("allow_top_k", 3))
top_k = causes[:allow_k]

ingest = json.load(urllib.request.urlopen(f"{backend_url}/stats/ingest"))
spans_after = int(ingest.get("spans_received", 0))
min_spans = int(assert_cfg.get("min_spans_received", 0))
if spans_after - spans_before < min_spans and spans_after < min_spans:
    print(f"FAIL: spans_received {spans_after} (delta {spans_after - spans_before}) < min {min_spans}")
    sys.exit(1)

expect_type = assert_cfg.get("expect_top_type")
expect_types = assert_cfg.get("expect_top_types") or []
if isinstance(expect_types, str):
    expect_types = [x.strip() for x in expect_types.split(",") if x.strip()]
if expect_type:
    expect_types = [expect_type] + list(expect_types)
if expect_types:
    types_k = [c.get("type") for c in top_k]
    if not any(t in types_k for t in expect_types):
        print(f"FAIL: expected one of {expect_types} in top-{allow_k}, got {types_k}")
        sys.exit(1)

expect_entity = assert_cfg.get("expect_entity")
if expect_entity:
    entities_k = [c.get("entity") for c in top_k]
    if expect_entity not in entities_k:
        print(f"FAIL: expected entity {expect_entity} in top-{allow_k}, got {entities_k}")
        sys.exit(1)

not_top = assert_cfg.get("not_top_1_type")
if not_top and top == not_top:
    print(f"FAIL: {not_top} incorrectly ranked top-1")
    sys.exit(1)

if assert_cfg.get("expect_entity_in_top_k") and target:
    entities_k = [c.get("entity") for c in top_k]
    allowed = assert_cfg.get("entities_allow") or [target]
    if not any(e in entities_k for e in allowed):
        print(f"FAIL: none of {allowed} in top-{allow_k} entities {entities_k}")
        sys.exit(1)

min_err = assert_cfg.get("min_graph_error_rate")
if min_err is not None:
    src = assert_cfg.get("graph_edge_source")
    dst = assert_cfg.get("graph_edge_target")
    found = False
    for e in graph.get("edges", []):
        if src and e.get("source") != src:
            continue
        if dst and e.get("target") != dst:
            continue
        if float(e.get("error_rate", 0)) >= float(min_err):
            found = True
            break
    if not found:
        print(f"FAIL: no graph edge with error_rate >= {min_err} ({src} -> {dst})")
        sys.exit(1)

print(f"PASS: top={top} entity={top_entity} spans={spans_after} top{allow_k}={[c.get('type')+':'+str(c.get('entity')) for c in top_k]}")
PY

echo "scenario '$SCENARIO' completed; artifacts at $ARTIFACT_DIR/$SCENARIO"
