#!/usr/bin/env bash
set -euo pipefail

DURATION_SECONDS="${DURATION_SECONDS:-60}"
RPS="${RPS:-5}"
TARGET_URL="${TARGET_URL:-http://localhost:8080/}"

end_ts=$(( $(date +%s) + DURATION_SECONDS ))
sleep_s="$(python3 - <<'PY'
import os
rps=max(1,int(os.environ.get("RPS","5")))
print(1.0/rps)
PY
)"

while [[ "$(date +%s)" -lt "$end_ts" ]]; do
  curl -fsS "$TARGET_URL" >/dev/null || true
  sleep "$sleep_s"
done

echo "load generation complete"
