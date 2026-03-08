#!/bin/bash
# Run Amazon Bronze ingestion job
# Optional first argument: ingestion_date (e.g. 2024-01-01). If not provided, uses current date.
set -e
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR/.."

INGESTION_DATE="${1:-}"

MSYS_NO_PATHCONV=1 docker compose -f docker/docker-compose.yml run --rm spark-master \
  /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  --packages io.delta:delta-spark_2.12:3.2.0 \
  --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
  --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
  --conf spark.eventLog.enabled=true \
  --conf spark.eventLog.dir=/opt/spark-events \
  /opt/spark-app/src/jobs/amazon_bronze_ingestion.py ${INGESTION_DATE:+$INGESTION_DATE}
