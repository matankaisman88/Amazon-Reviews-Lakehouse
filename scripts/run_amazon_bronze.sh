#!/bin/bash
# Run Amazon Bronze ingestion job
# Args: [ingestion_date] [--category=Category]
#   ingestion_date: optional (default: today)
#   --category=X: scope to one category (reads specific files, partition overwrite)
set -e
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR/.."

INGESTION_DATE=""
EXTRA_ARGS=()
for arg in "$@"; do
  if [[ "$arg" == --category=* ]]; then
    EXTRA_ARGS+=("$arg")
  elif [[ -z "$INGESTION_DATE" && "$arg" != -* ]]; then
    INGESTION_DATE="$arg"
  fi
done

MSYS_NO_PATHCONV=1 docker compose -f docker/docker-compose.yml run --rm spark-master \
  /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  --packages io.delta:delta-spark_2.12:3.2.0 \
  --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
  --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
  --conf spark.eventLog.enabled=true \
  --conf spark.eventLog.dir=/opt/spark-events \
  /opt/spark-app/src/jobs/amazon_bronze_ingestion.py ${INGESTION_DATE:+$INGESTION_DATE} "${EXTRA_ARGS[@]}"
