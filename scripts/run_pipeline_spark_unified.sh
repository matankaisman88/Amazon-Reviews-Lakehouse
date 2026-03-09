#!/bin/bash
# Run Bronze -> Silver -> Gold in a single Spark session (avoids 3x startup).
# Usage: ./scripts/run_pipeline_spark_unified.sh [ingestion_date] [--category=Category] [--skip-optimize]
set -e
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR/.."

SPARK_ARGS=("$@")

MSYS_NO_PATHCONV=1 docker compose -f docker/docker-compose.yml run --rm spark-master \
  /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  --packages io.delta:delta-spark_2.12:3.2.0 \
  --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
  --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
  /opt/spark-app/scripts/run_pipeline_unified.py "${SPARK_ARGS[@]}"
