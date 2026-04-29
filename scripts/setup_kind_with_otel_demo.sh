#!/usr/bin/env bash
set -euo pipefail

scripts/setup_kind.sh
scripts/deploy_otel_demo.sh

echo "kind cluster is ready and OpenTelemetry Demo has been deployed"
