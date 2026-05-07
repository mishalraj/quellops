#!/usr/bin/env bash
set -euo pipefail

# Starts required services in background, applies Linux collector endpoint fix,
# starts Cloudflare quick tunnel, and prints a PASS/FAIL summary.
# Intentionally does NOT run scenarios.

BACKEND_URL="${BACKEND_URL:-http://localhost:8088}"
BACKEND_OTLP_ENDPOINT="${BACKEND_OTLP_ENDPOINT:-172.18.0.1:4317}"
CF_TUNNEL_URL_TARGET="${CF_TUNNEL_URL_TARGET:-http://localhost:8088}"
CF_LOG_FILE="${CF_LOG_FILE:-/tmp/aidbg-cloudflared.log}"
CF_START_WAIT_SECONDS="${CF_START_WAIT_SECONDS:-12}"
CLUSTER_NAME="${CLUSTER_NAME:-aidbg-local}"

PASS_COUNT=0
FAIL_COUNT=0
WARN_COUNT=0

print_hr() {
  echo "------------------------------------------------------------"
}

pass() {
  PASS_COUNT=$((PASS_COUNT + 1))
  echo "[PASS] $*"
}

fail() {
  FAIL_COUNT=$((FAIL_COUNT + 1))
  echo "[FAIL] $*"
}

warn() {
  WARN_COUNT=$((WARN_COUNT + 1))
  echo "[WARN] $*"
}

need_cmd() {
  local cmd="$1"
  if command -v "$cmd" >/dev/null 2>&1; then
    pass "command available: $cmd"
  else
    fail "missing command: $cmd"
  fi
}

try_step() {
  local label="$1"
  shift
  if "$@"; then
    pass "$label"
  else
    fail "$label"
  fi
}

extract_cloudflare_url() {
  local log_file="$1"
  if command -v rg >/dev/null 2>&1; then
    rg -o "https://[-a-zA-Z0-9]+\.trycloudflare\.com" "$log_file" | head -n 1 || true
    return 0
  fi
  grep -Eo "https://[-a-zA-Z0-9]+\.trycloudflare\.com" "$log_file" | head -n 1 || true
}

print_hr
echo "aidbg background start + health checks"
print_hr

need_cmd kind
need_cmd kubectl
need_cmd helm
need_cmd docker
need_cmd curl
need_cmd cloudflared

if [[ "$FAIL_COUNT" -gt 0 ]]; then
  print_hr
  echo "Cannot continue due to missing commands."
  exit 1
fi

print_hr
echo "1) Bootstrapping cluster/demo/collector/agent..."
try_step "kind setup" scripts/setup_kind.sh
try_step "otel demo deploy" scripts/deploy_otel_demo.sh
try_step "collector deploy" scripts/deploy_collector.sh

echo "Applying Linux-friendly collector endpoint: ${BACKEND_OTLP_ENDPOINT}"
if kubectl -n aidbg-otel set env deploy/aidbg-otel-collector "AIDBG_BACKEND_OTLP_ENDPOINT=${BACKEND_OTLP_ENDPOINT}" >/dev/null \
  && kubectl -n aidbg-otel rollout status deploy/aidbg-otel-collector --timeout=5m >/dev/null; then
  pass "collector endpoint set to ${BACKEND_OTLP_ENDPOINT}"
else
  fail "collector endpoint update/rollout"
fi

try_step "agent deploy" scripts/deploy_agent.sh

print_hr
echo "2) Starting backend in background..."
if docker compose up -d --build aidbg-backend >/dev/null; then
  pass "backend started in docker compose"
else
  fail "backend docker compose up"
fi

print_hr
echo "3) Starting Cloudflare quick tunnel in background..."
rm -f "$CF_LOG_FILE"
if nohup cloudflared tunnel --url "$CF_TUNNEL_URL_TARGET" >"$CF_LOG_FILE" 2>&1 & then
  pass "cloudflared launched (log: $CF_LOG_FILE)"
else
  fail "cloudflared launch"
fi

sleep "$CF_START_WAIT_SECONDS"

CF_URL="$(extract_cloudflare_url "$CF_LOG_FILE")"
if [[ -n "$CF_URL" ]]; then
  pass "cloudflare URL discovered: $CF_URL"
else
  fail "cloudflare URL not found in log"
fi

print_hr
echo "4) Health checks..."

if curl -fsS "${BACKEND_URL}/healthz" >/dev/null; then
  pass "backend healthz reachable at ${BACKEND_URL}/healthz"
else
  fail "backend healthz unreachable at ${BACKEND_URL}/healthz"
fi

if kubectl -n aidbg-otel rollout status deploy/aidbg-otel-collector --timeout=30s >/dev/null; then
  pass "aidbg collector rollout healthy"
else
  fail "aidbg collector rollout unhealthy"
fi

if kubectl -n aidbg-agent rollout status deploy/aidbg-agent --timeout=30s >/dev/null; then
  pass "aidbg agent rollout healthy"
else
  fail "aidbg agent rollout unhealthy"
fi

INGEST_JSON="$(curl -fsS "${BACKEND_URL}/stats/ingest" || true)"
if [[ -n "$INGEST_JSON" ]]; then
  pass "stats/ingest reachable"
  SPANS_RX="$(printf "%s" "$INGEST_JSON" | python3 -c 'import json,sys; d=json.load(sys.stdin); print(int(d.get("spans_received",0)))' 2>/dev/null || echo 0)"
  if [[ "$SPANS_RX" -gt 0 ]]; then
    pass "spans ingesting (spans_received=${SPANS_RX})"
  else
    warn "spans_received is 0 (trace flow may still be warming or collector export path is wrong)"
  fi
else
  fail "stats/ingest not reachable"
fi

COL_LOG_TAIL="$(kubectl -n aidbg-otel logs deploy/aidbg-otel-collector --tail=120 2>/dev/null || true)"
if [[ -n "$COL_LOG_TAIL" ]]; then
  if printf "%s" "$COL_LOG_TAIL" | grep -Eqi "no such host|host\.docker\.internal"; then
    fail "collector still shows host.docker.internal DNS errors"
  else
    pass "collector logs do not show host.docker.internal DNS errors"
  fi
  if printf "%s" "$COL_LOG_TAIL" | grep -qi "Exporting failed"; then
    warn "collector is still retrying failed exports (check endpoint/network)"
  else
    pass "collector shows no recent export retry failures"
  fi
else
  warn "could not read collector logs"
fi

if [[ -n "$CF_URL" ]]; then
  if curl -fsS -o /dev/null "$CF_URL/healthz"; then
    pass "cloudflare URL forwards /healthz"
  else
    warn "cloudflare URL exists but /healthz check failed"
  fi
fi

print_hr
echo "Summary"
echo "  PASS: ${PASS_COUNT}"
echo "  WARN: ${WARN_COUNT}"
echo "  FAIL: ${FAIL_COUNT}"
if [[ -n "$CF_URL" ]]; then
  echo "  Cloudflare URL: $CF_URL"
else
  echo "  Cloudflare URL: <not found>"
fi
echo "  Cloudflare log: $CF_LOG_FILE"
print_hr

if [[ "$FAIL_COUNT" -gt 0 ]]; then
  exit 1
fi

exit 0

