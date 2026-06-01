#!/usr/bin/env bash
set -euo pipefail

scripts/setup_kind.sh
scripts/deploy_collector.sh
scripts/deploy_agent.sh
scripts/deploy_aidbg_demo.sh

echo "bootstrap done. Start backend with:"
echo "  BACKEND_MODE=docker scripts/backend_up.sh"
