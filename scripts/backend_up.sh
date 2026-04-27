#!/usr/bin/env bash
set -euo pipefail

PY="${PYTHON:-python3}"

if [ ! -d ".venv" ]; then
  "$PY" -m venv .venv
fi

source .venv/bin/activate
pip install -r backend/requirements.txt

export PYTHONPATH="backend/src:${PYTHONPATH:-}"
export AIDBG_OTLP_GRPC_BIND="${AIDBG_OTLP_GRPC_BIND:-0.0.0.0:4317}"
export AIDBG_HTTP_BIND="${AIDBG_HTTP_BIND:-0.0.0.0:8088}"

exec "$PY" -m aidbg.main

