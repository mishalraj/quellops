#!/usr/bin/env bash
set -euo pipefail

if [[ -n "${QUELLOPS_PROVIDER_SIMULATOR_PORT:-}" ]]; then
  port="$QUELLOPS_PROVIDER_SIMULATOR_PORT"
else
  port="$(python3 -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1", 0)); print(s.getsockname()[1]); s.close()')"
fi
python3 scripts/provider_api_simulator.py --port "$port" &
simulator_pid=$!

cleanup() {
  kill "$simulator_pid" >/dev/null 2>&1 || true
  wait "$simulator_pid" 2>/dev/null || true
}
trap cleanup EXIT

for _ in $(seq 1 20); do
  if curl -fsS "http://127.0.0.1:${port}/healthz" >/dev/null 2>&1; then
    break
  fi
  sleep 0.25
done
curl -fsS "http://127.0.0.1:${port}/healthz" >/dev/null

export QUELLOPS_LIVE_SPLUNK="{\"values\":{\"url\":\"http://127.0.0.1:${port}\"},\"credentials\":{\"token\":\"simulation-token\"}}"
export QUELLOPS_LIVE_DATADOG="{\"values\":{\"url\":\"http://127.0.0.1:${port}\"},\"credentials\":{\"api_key\":\"simulation-api-key\",\"app_key\":\"simulation-app-key\"}}"

output="$(cd modules/logs && go test ./internal/logs -run 'TestLiveProviders/(splunk|datadog)' -v)"
printf '%s\n' "$output"
grep -Eq 'validated splunk:.*records=1' <<<"$output"
grep -Eq 'validated datadog:.*records=1' <<<"$output"
