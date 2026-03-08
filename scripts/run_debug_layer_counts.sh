#!/bin/bash
# Run debug_layer_counts.py via Docker (avoids local Spark/JVM compatibility issues)
# Usage: ./scripts/run_debug_layer_counts.sh [--category Gift_Cards]

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$PROJECT_ROOT"

docker compose -f docker/docker-compose.yml run --rm dashboard python3 scripts/debug_layer_counts.py "$@"
