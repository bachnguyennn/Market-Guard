#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$PROJECT_ROOT"
DEMO_PYTHON="${MARKETGUARD_PYTHON:-$PROJECT_ROOT/.venv/bin/python}"

if ! command -v docker >/dev/null 2>&1 || ! docker info >/dev/null 2>&1; then
    echo "Docker is unavailable. Start Docker Desktop or the Docker daemon." >&2
    exit 1
fi
if ! "$DEMO_PYTHON" -c 'import marketguard, delta, confluent_kafka, streamlit' >/dev/null 2>&1; then
    echo "Install the project first: python -m pip install -e ." >&2
    exit 1
fi

echo "Starting MarketGuard local demo..."
if docker container inspect marketguard-kafka >/dev/null 2>&1; then
    docker start marketguard-kafka >/dev/null
else
    docker run --detach --name marketguard-kafka \
        --publish 127.0.0.1:9092:9092 apache/kafka:4.3.1 >/dev/null
fi

exec "$DEMO_PYTHON" -m marketguard.local_demo "$@"
