#!/bin/bash
# Run full Amazon Medallion pipeline: Bronze -> Silver -> Gold
# Usage: ./scripts/run_pipeline.sh [ingestion_date] [--skip-optimize] [--category=Category]
#   --skip-optimize: skip Gold OPTIMIZE (faster backfills)
#   --category=...: optionally scope Silver/Gold processing to one category

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

cd "$PROJECT_ROOT"

# Parse args: first non-flag is ingestion_date; flags pass through to Silver/Gold as relevant
INGESTION_DATE=""
CATEGORY_ARG=""
GOLD_EXTRA=()
SILVER_EXTRA=()
for arg in "$@"; do
  if [[ "$arg" == "--skip-optimize" ]]; then
    GOLD_EXTRA+=("$arg")
  elif [[ "$arg" == --category=* ]]; then
    CATEGORY_ARG="${arg#--category=}"
    SILVER_EXTRA+=("$arg")
    GOLD_EXTRA+=("$arg")
  elif [[ -z "$INGESTION_DATE" && "$arg" != -* ]]; then
    INGESTION_DATE="$arg"
  fi
done

# Default ingestion_date to today if not provided
INGESTION_DATE="${INGESTION_DATE:-$(date -u +%Y-%m-%d)}"

echo "Using ingestion_date=${INGESTION_DATE}"

# Fetch raw data: use requested category when --category passed, else Gift_Cards
if [[ -z "${FETCH_SKIP:-}" ]]; then
  RAW_DIR="$PROJECT_ROOT/data/raw/amazon"
  FETCH_NEEDED=0
  if [[ ! -d "$RAW_DIR" ]] || [[ -z "$(find "$RAW_DIR" -name "*.jsonl.gz" 2>/dev/null | head -1)" ]]; then
    FETCH_NEEDED=1
  elif [[ -n "$CATEGORY_ARG" ]]; then
    # Raw exists; ensure requested category is present
    if [[ ! -f "$RAW_DIR/reviews/${CATEGORY_ARG}.jsonl.gz" ]] || [[ ! -f "$RAW_DIR/metadata/meta_${CATEGORY_ARG}.jsonl.gz" ]]; then
      FETCH_NEEDED=1
    fi
  fi
  if [[ "$FETCH_NEEDED" -eq 1 ]]; then
    FETCH_CATEGORIES="${CATEGORY_ARG:-Gift_Cards}"
    echo "Fetching raw data for ${FETCH_CATEGORIES}..."
    python scripts/fetch_amazon_data.py --categories "$FETCH_CATEGORIES"
  fi
fi

echo "Starting Amazon Bronze Ingestion..."
./scripts/run_amazon_bronze.sh "${INGESTION_DATE}" ${CATEGORY_ARG:+--category=$CATEGORY_ARG}

echo "Starting Amazon Silver Transformation (incremental)..."
./scripts/run_amazon_silver.sh "${INGESTION_DATE}" "${SILVER_EXTRA[@]}"

echo "Starting Amazon Gold Analytics (incremental)..."
./scripts/run_amazon_gold.sh "${INGESTION_DATE}" "${GOLD_EXTRA[@]}"

echo "Amazon Medallion pipeline completed successfully."

