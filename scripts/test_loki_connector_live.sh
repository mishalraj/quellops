#!/usr/bin/env bash
set -euo pipefail

container="quellops-loki-test"
port="${QUELLOPS_LOKI_TEST_PORT:-13100}"

cleanup() {
  docker rm -f "$container" >/dev/null 2>&1 || true
}
trap cleanup EXIT
cleanup

docker run -d --name "$container" -p "127.0.0.1:${port}:3100" grafana/loki:3.2.1 -config.file=/etc/loki/local-config.yaml >/dev/null

for _ in $(seq 1 30); do
  if curl -fsS "http://127.0.0.1:${port}/ready" >/dev/null; then
    break
  fi
  sleep 1
done
curl -fsS "http://127.0.0.1:${port}/ready" >/dev/null

now_ns="$(( $(date +%s) * 1000000000 ))"
curl -fsS -H 'Content-Type: application/json' \
  -X POST "http://127.0.0.1:${port}/loki/api/v1/push" \
  --data "{\"streams\":[{\"stream\":{\"service\":\"checkout-service\",\"namespace\":\"production\"},\"values\":[[\"${now_ns}\",\"checkout ready\"]]}]}" >/dev/null

export QUELLOPS_LIVE_LOKI="{\"values\":{\"url\":\"http://127.0.0.1:${port}\"},\"credentials\":{}}"
(cd modules/logs && go test ./internal/logs -run TestLiveProviders/loki -v)

