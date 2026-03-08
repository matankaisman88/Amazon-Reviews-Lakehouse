#!/bin/bash
# Drop all raw files after pipeline has ingested them into Bronze.
# Frees disk space; raw data is redundant once in Delta tables.

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

RAW_DIR="${PROJECT_ROOT}/data/raw"

if [ ! -d "$RAW_DIR" ]; then
  echo "Raw directory does not exist: $RAW_DIR"
  exit 0
fi

# Count before delete for logging
COUNT=$(find "$RAW_DIR" -type f 2>/dev/null | wc -l)
echo "Dropping $COUNT raw file(s) from data/raw..."

find "$RAW_DIR" -mindepth 1 -delete

echo "Raw files dropped successfully."
