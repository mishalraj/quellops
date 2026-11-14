#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
COMPOSE_FILE="$ROOT_DIR/tests/integration/database/docker-compose.yml"
PROJECT_NAME="quellops-db-it"

cleanup() {
  if [[ "${KEEP_DATABASE_TEST_STACK:-false}" != "true" ]]; then
    docker compose -p "$PROJECT_NAME" -f "$COMPOSE_FILE" down --volumes --remove-orphans
  fi
}
trap cleanup EXIT

docker compose -p "$PROJECT_NAME" -f "$COMPOSE_FILE" up -d --wait \
  postgres-primary postgres-replica mysql-primary mysql-replica \
  mongo-1 mongo-2 mongo-3 redis-primary redis-replica
docker compose -p "$PROJECT_NAME" -f "$COMPOSE_FILE" --profile validator run --rm --no-deps \
  validator python /workspace/tests/integration/database/validate.py replicas

# Release completed replica services before starting the JVM-based CDC phase.
docker compose -p "$PROJECT_NAME" -f "$COMPOSE_FILE" stop \
  postgres-primary postgres-replica mysql-replica mongo-1 mongo-2 mongo-3 \
  redis-primary redis-replica
docker compose -p "$PROJECT_NAME" -f "$COMPOSE_FILE" up -d --wait redpanda debezium
docker compose -p "$PROJECT_NAME" -f "$COMPOSE_FILE" --profile validator run --rm --no-deps \
  validator python /workspace/tests/integration/database/validate.py cdc
