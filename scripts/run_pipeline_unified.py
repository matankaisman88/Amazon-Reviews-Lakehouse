#!/usr/bin/env python3
"""
Run Bronze -> Silver -> Gold in a single Spark session (avoids 3x startup cost).
Usage: python run_pipeline_unified.py [ingestion_date] [--category=Category] [--skip-optimize]
"""
import sys
from pathlib import Path

# Ensure project root is on path
SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent
sys.path.insert(0, str(PROJECT_ROOT))

from src.jobs.amazon_bronze_ingestion import run as run_bronze
from src.jobs.amazon_silver_transformation import run as run_silver
from src.jobs.amazon_gold_analytics import run as run_gold
from src.utils.spark_session import get_spark_session


def main() -> None:
    args = sys.argv[1:]
    ingestion_date = None
    category = None
    skip_optimize = False

    for arg in args:
        if arg.startswith("--category="):
            category = arg.split("=", 1)[1]
        elif arg in ("--skip-optimize", "-s"):
            skip_optimize = True
        elif not arg.startswith("-"):
            ingestion_date = arg

    spark = get_spark_session("AmazonMedallionPipeline")

    print("Starting Amazon Bronze Ingestion...")
    run_bronze(spark=spark, category=category, ingestion_date=ingestion_date)

    print("Starting Amazon Silver Transformation...")
    run_silver(spark=spark, category=category, ingestion_date=ingestion_date)

    print("Starting Amazon Gold Analytics...")
    run_gold(
        spark=spark,
        category=category,
        ingestion_date=ingestion_date,
        skip_optimize=skip_optimize,
    )

    print("Amazon Medallion pipeline completed successfully.")


if __name__ == "__main__":
    main()
