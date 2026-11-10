#!/usr/bin/env bash
# One-shot local aidbg: Kind cluster, demo mesh, collector, agent, backend, edge port-forward, warm traffic.
# Optional: run demo scenarios. No Cloudflare tunnel required.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

BACKEND_URL="${BACKEND_URL:-http://localhost:8088}"
EDGE_URL="${EDGE_URL:-http://localhost:8080/}"
CLUSTER_NAME="${CLUSTER_NAME:-aidbg-local}"
DEMO_NS="${AIDBG_DEMO_NS:-aidbg-demo}"
PF_LOCAL_PORT="${PF_LOCAL_PORT:-8080}"
PF_PID_FILE="${AIDBG_PF_PID_FILE:-/tmp/aidbg-edge-port-forward.pid}"
PF_LOG_FILE="${AIDBG_PF_LOG_FILE:-/tmp/aidbg-edge-port-forward.log}"

RUN_SCENARIOS=0
RUN_SCENARIO=""
SKIP_BOOTSTRAP=0
SKIP_BACKEND=0
SKIP_WARM=0
SKIP_PORT_FORWARD=0
WARM_SECONDS="${WARM_SECONDS:-50}"
WARM_RPS="${WARM_RPS:-8}"
SPANS_WAIT_SECONDS="${SPANS_WAIT_SECONDS:-90}"

usage() {
  cat <<'EOF'
Usage:
  scripts/run_local.sh [options]          Start full local stack + warm traffic
  scripts/run_local.sh stop               Stop edge port-forward (backend/cluster stay up)
  scripts/run_local.sh status             Quick health summary

Options:
  --scenarios              After warm-up, run all four demo scenarios (scripts/run_all_demo_scenarios.sh)
  --scenario <name>        Run one scenario (e.g. 01_deploy_latency)
  --skip-bootstrap         Skip Kind/K8s deploy (cluster already bootstrapped)
  --skip-backend           Skip docker compose backend start
  --skip-warm              Skip load generator warm-up
  --no-port-forward        Do not start kubectl port-forward (you manage EDGE_URL yourself)
  -h, --help               Show this help

Environment:
  BACKEND_URL              Default http://localhost:8088
  EDGE_URL                 Default http://localhost:8080/
  DOCKER_HOST_ADDR         Host reachable from Kind pods (auto: host.docker.internal on macOS, 172.18.0.1 on Linux)
  CLUSTER_NAME             kind cluster name (default aidbg-local)

After start:
  UI:      http://localhost:8088/ui/
  Edge:    http://localhost:8080/  (via port-forward)
  Scenarios: EDGE_URL=http://localhost:8080/ scripts/run_demo_scenario.sh 01_deploy_latency
EOF
}

detect_docker_host_addr() {
  if [[ -n "${DOCKER_HOST_ADDR:-}" ]]; then
    return 0
  fi
  case "$(uname -s)" in
    Darwin)
      DOCKER_HOST_ADDR="host.docker.internal"
      ;;
    Linux)
      DOCKER_HOST_ADDR="172.18.0.1"
      ;;
    *)
      DOCKER_HOST_ADDR="host.docker.internal"
      ;;
  esac
}

need_cmd() {
  local cmd="$1"
  if ! command -v "$cmd" >/dev/null 2>&1; then
    echo "error: required command not found: $cmd" >&2
    exit 1
  fi
}

ensure_python_deps() {
  export PYTHON_BIN="${PYTHON_BIN:-python3}"
  if "$PYTHON_BIN" -c "import httpx, yaml" >/dev/null 2>&1; then
    return 0
  fi
  echo "Installing Python deps (httpx, pyyaml) for loadgen/scenarios..."
  if [[ ! -d ".venv" ]]; then
    "$PYTHON_BIN" -m venv .venv
  fi
  # shellcheck source=/dev/null
  . .venv/bin/activate
  pip install -q httpx pyyaml
  export PYTHON_BIN="python"
}

apply_collector_and_agent_hosts() {
  local host="$1"
  local otlp="${BACKEND_OTLP_ENDPOINT:-${host}:4317}"

  echo "Configuring collector OTLP export -> ${otlp}"
  kubectl -n aidbg-otel set env deploy/aidbg-otel-collector \
    "AIDBG_BACKEND_OTLP_ENDPOINT=${otlp}" >/dev/null
  kubectl -n aidbg-otel rollout status deploy/aidbg-otel-collector --timeout=5m >/dev/null

  if kubectl -n aidbg-agent get deploy aidbg-agent >/dev/null 2>&1; then
    echo "Configuring Kubernetes agent -> http://${host}:8088"
    kubectl -n aidbg-agent set env deploy/aidbg-agent \
      "AIDBG_BACKEND_URL=http://${host}:8088" >/dev/null
    kubectl -n aidbg-agent rollout status deploy/aidbg-agent --timeout=3m >/dev/null
  fi
}

port_forward_running() {
  [[ -f "$PF_PID_FILE" ]] || return 1
  local pid
  pid="$(cat "$PF_PID_FILE" 2>/dev/null || true)"
  [[ -n "$pid" ]] && kill -0 "$pid" 2>/dev/null
}

start_port_forward() {
  if port_forward_running; then
    echo "Edge port-forward already running (pid $(cat "$PF_PID_FILE"))"
    return 0
  fi
  if command -v lsof >/dev/null 2>&1 && lsof -nP -iTCP:"$PF_LOCAL_PORT" -sTCP:LISTEN >/dev/null 2>&1; then
    echo "warn: port ${PF_LOCAL_PORT} is in use; assuming edge is already reachable at ${EDGE_URL}"
    return 0
  fi

  echo "Starting port-forward svc/edge ${PF_LOCAL_PORT}:8080 (log: ${PF_LOG_FILE})"
  : >"$PF_LOG_FILE"
  kubectl -n "$DEMO_NS" port-forward "svc/edge" "${PF_LOCAL_PORT}:8080" >>"$PF_LOG_FILE" 2>&1 &
  echo $! >"$PF_PID_FILE"
  sleep 2
  if ! port_forward_running; then
    echo "error: port-forward failed; see ${PF_LOG_FILE}" >&2
    tail -20 "$PF_LOG_FILE" >&2 || true
    exit 1
  fi
}

stop_port_forward() {
  if [[ -f "$PF_PID_FILE" ]]; then
    local pid
    pid="$(cat "$PF_PID_FILE" 2>/dev/null || true)"
    if [[ -n "$pid" ]] && kill -0 "$pid" 2>/dev/null; then
      kill "$pid" 2>/dev/null || true
      echo "Stopped edge port-forward (pid ${pid})"
    fi
    rm -f "$PF_PID_FILE"
  else
    echo "No port-forward pid file at ${PF_PID_FILE}"
  fi
}

wait_for_backend() {
  local wait_secs="${BACKEND_WAIT_SECONDS:-120}"
  local deadline=$((SECONDS + wait_secs))
  while (( SECONDS < deadline )); do
    if curl -fsS "${BACKEND_URL}/healthz" >/dev/null 2>&1; then
      echo "Backend healthy at ${BACKEND_URL}/healthz"
      return 0
    fi
    sleep 2
  done
  echo "error: backend not healthy at ${BACKEND_URL}/healthz after ${wait_secs}s" >&2
  docker logs aidbg-backend --tail=40 2>/dev/null || true
  return 1
}

wait_for_spans() {
  local min="${1:-1}"
  local wait_secs="${SPANS_WAIT_SECONDS:-90}"
  local deadline=$((SECONDS + wait_secs))
  while (( SECONDS < deadline )); do
    if ! curl -fsS "${BACKEND_URL}/stats/ingest" >/dev/null 2>&1; then
      sleep 3
      continue
    fi
    local spans
    spans="$(curl -fsS "${BACKEND_URL}/stats/ingest" | python3 -c 'import json,sys; print(int(json.load(sys.stdin).get("spans_received",0)))' 2>/dev/null || echo 0)"
    if [[ "$spans" -ge "$min" ]]; then
      echo "Traces flowing: spans_received=${spans}"
      return 0
    fi
    sleep 5
  done
  echo "warn: spans_received still low after ${wait_secs}s (check collector logs)" >&2
  kubectl -n aidbg-otel logs deploy/aidbg-otel-collector --tail=40 2>/dev/null || true
  return 1
}

warm_traffic() {
  echo "Warming traffic (${WARM_SECONDS}s @ ${WARM_RPS} rps) -> ${EDGE_URL}"
  DURATION_SECONDS="$WARM_SECONDS" RPS="$WARM_RPS" EDGE_URL="$EDGE_URL" \
    "$PYTHON_BIN" demo/loadgen/load.py || true
}

cmd_status() {
  detect_docker_host_addr
  echo "=== aidbg local status ==="
  echo "docker host (from Kind pods): ${DOCKER_HOST_ADDR}"
  echo "backend: ${BACKEND_URL}"
  echo "edge:    ${EDGE_URL}"
  if port_forward_running; then
    echo "port-forward: running (pid $(cat "$PF_PID_FILE"))"
  else
    echo "port-forward: not running"
  fi
  if curl -fsS "${BACKEND_URL}/healthz" >/dev/null 2>&1; then
    echo "backend healthz: ok"
    curl -fsS "${BACKEND_URL}/stats/ingest" 2>/dev/null && echo "" || true
  else
    echo "backend healthz: unreachable"
  fi
  if kubectl cluster-info >/dev/null 2>&1; then
    kubectl -n "$DEMO_NS" get pods 2>/dev/null || true
  else
    echo "kubectl: cluster not reachable"
  fi
}

cmd_stop() {
  stop_port_forward
  echo "Backend and Kind cluster were left running."
  echo "  Stop backend: docker compose stop aidbg-backend"
  echo "  Delete cluster: kind delete cluster --name ${CLUSTER_NAME}"
}

cmd_start() {
  detect_docker_host_addr
  export BACKEND_OTLP_ENDPOINT="${BACKEND_OTLP_ENDPOINT:-${DOCKER_HOST_ADDR}:4317}"
  export BACKEND_HOST="${BACKEND_HOST:-$DOCKER_HOST_ADDR}"

  need_cmd kind
  need_cmd kubectl
  need_cmd docker
  need_cmd curl
  need_cmd python3

  if ! docker info >/dev/null 2>&1; then
    echo "error: Docker is not running. Start Docker Desktop and retry." >&2
    exit 1
  fi

  ensure_python_deps

  echo "============================================================"
  echo "aidbg local stack"
  echo "  cluster:  ${CLUSTER_NAME}"
  echo "  backend:  ${BACKEND_URL}"
  echo "  edge:     ${EDGE_URL}"
  echo "  k8s host: ${DOCKER_HOST_ADDR} (OTLP ${BACKEND_OTLP_ENDPOINT})"
  echo "============================================================"

  if [[ "$SKIP_BOOTSTRAP" -eq 0 ]]; then
    echo ""
    echo "[1/6] Kind cluster + collector + agent + demo mesh..."
    scripts/setup_kind.sh
    scripts/deploy_collector.sh
    apply_collector_and_agent_hosts "$DOCKER_HOST_ADDR"
    scripts/deploy_agent.sh
    BACKEND_HOST="$DOCKER_HOST_ADDR" scripts/deploy_aidbg_demo.sh
  else
    echo ""
    echo "[1/6] Skipping bootstrap (--skip-bootstrap)"
    apply_collector_and_agent_hosts "$DOCKER_HOST_ADDR" || true
  fi

  if [[ "$SKIP_BACKEND" -eq 0 ]]; then
    echo ""
    echo "[2/6] Starting aidbg backend (docker compose)..."
    docker compose up -d --build aidbg-backend
  else
    echo ""
    echo "[2/6] Skipping backend start (--skip-backend)"
  fi

  wait_for_backend

  if [[ "$SKIP_PORT_FORWARD" -eq 0 ]]; then
    echo ""
    echo "[3/6] Edge port-forward..."
    start_port_forward
  else
    echo ""
    echo "[3/6] Skipping port-forward (--no-port-forward)"
  fi

  echo ""
  echo "[4/6] Waiting for demo pods..."
  kubectl -n "$DEMO_NS" rollout status deploy/edge --timeout=5m >/dev/null
  kubectl -n "$DEMO_NS" rollout status deploy/checkout --timeout=5m >/dev/null

  if [[ "$SKIP_WARM" -eq 0 ]]; then
    echo ""
    echo "[5/6] Warm-up load..."
    warm_traffic
    wait_for_spans 1 || true
  else
    echo ""
    echo "[5/6] Skipping warm-up (--skip-warm)"
  fi

  echo ""
  echo "[6/6] Health summary..."
  scripts/demo_smoke_check.sh || true

  if [[ "$RUN_SCENARIOS" -eq 1 ]]; then
    echo ""
    echo "Running all demo scenarios..."
    export EDGE_URL
    scripts/run_all_demo_scenarios.sh
  elif [[ -n "$RUN_SCENARIO" ]]; then
    echo ""
    echo "Running scenario: ${RUN_SCENARIO}"
    scripts/demo_reset.sh || true
    sleep 10
    export EDGE_URL
    scripts/run_demo_scenario.sh "$RUN_SCENARIO"
  fi

  echo ""
  echo "============================================================"
  echo "Local stack is up."
  echo "  UI:        ${BACKEND_URL%/}/ui/"
  echo "  Ingest:    curl -fsS ${BACKEND_URL}/stats/ingest"
  echo "  Edge:      ${EDGE_URL}"
  echo "  Scenario:  EDGE_URL=${EDGE_URL} scripts/run_demo_scenario.sh 01_deploy_latency"
  echo "  All tests: EDGE_URL=${EDGE_URL} scripts/run_all_demo_scenarios.sh"
  echo "  Stop PF:   scripts/run_local.sh stop"
  echo "============================================================"
}

# --- parse args ---
SUBCMD="${1:-start}"
if [[ "$SUBCMD" == "stop" || "$SUBCMD" == "status" ]]; then
  shift || true
else
  SUBCMD="start"
fi

while [[ $# -gt 0 ]]; do
  case "$1" in
    start) shift ;;
    --scenarios) RUN_SCENARIOS=1; shift ;;
    --scenario) RUN_SCENARIO="${2:?--scenario requires a name}"; shift 2 ;;
    --skip-bootstrap) SKIP_BOOTSTRAP=1; shift ;;
    --skip-backend) SKIP_BACKEND=1; shift ;;
    --skip-warm) SKIP_WARM=1; shift ;;
    --no-port-forward) SKIP_PORT_FORWARD=1; shift ;;
    -h|--help) usage; exit 0 ;;
    *)
      if [[ "$SUBCMD" == "start" && "$1" != "start" ]]; then
        echo "unknown option: $1" >&2
        usage >&2
        exit 2
      fi
      shift
      ;;
  esac
done

case "$SUBCMD" in
  start) cmd_start ;;
  stop) cmd_stop ;;
  status) cmd_status ;;
  *)
    echo "unknown command: $SUBCMD" >&2
    usage >&2
    exit 2
    ;;
esac
