#!/bin/bash
# Run Amazon Silver transformation job
# Usage: ./scripts/run_amazon_silver.sh [ingestion_date] [--category=Category]
set -e
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR/.."

SILVER_ARGS=("$@")

MSYS_NO_PATHCONV=1 docker compose -f docker/docker-compose.yml run --rm spark-master \
  /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  --packages io.delta:delta-spark_2.12:3.2.0 \
  --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
  --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
  /opt/spark-app/src/jobs/amazon_silver_transformation.py "${SILVER_ARGS[@]}"
