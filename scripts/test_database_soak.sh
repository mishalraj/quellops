#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
COMPOSE_FILE="$ROOT_DIR/tests/integration/database/docker-compose.yml"
PROJECT_NAME="quellops-db-soak"

cleanup() {
  docker compose -p "$PROJECT_NAME" -f "$COMPOSE_FILE" --profile soak \
    down --volumes --remove-orphans
}
trap cleanup EXIT

docker compose -p "$PROJECT_NAME" -f "$COMPOSE_FILE" --profile soak up -d --wait control-postgres
docker compose -p "$PROJECT_NAME" -f "$COMPOSE_FILE" --profile soak run --rm soak
