"""
Bronze Layer: Bulk raw Amazon Reviews 2023 JSONL.gz to Delta.
- Manual schemas (no inferSchema)
- Reviews and metadata ingested separately under bronze/amazon_reviews
- Optional category: scope to one category (reads specific files, partition overwrite)
- ingestion_date: batch label (default: current_date)
"""

import sys
from pathlib import Path
from typing import Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import current_date, input_file_name, lit, regexp_extract, to_date
from pyspark.sql.types import StructType

from src.utils.amazon_schemas import AMAZON_METADATA_SCHEMA, AMAZON_REVIEW_SCHEMA
from src.utils.config_loader import get_max_partition_bytes, get_paths
from src.utils.spark_session import apply_dynamic_config, get_spark_session

AMAZON_BRONZE_ROOT = "amazon_reviews"


def _read_jsonl(
    spark: SparkSession,
    source_path: str,
    schema: StructType,
) -> DataFrame:
    """Read gzipped JSONL with explicit schema. source_path can be dir or single file."""
    return spark.read.schema(schema).json(source_path)


def _with_partition_columns(
    df: DataFrame,
    category: str,
    ingestion_date: Optional[str],
) -> DataFrame:
    """Add partition columns."""
    df = df.withColumn("category", lit(category))
    if ingestion_date:
        df = df.withColumn("ingestion_date", to_date(lit(ingestion_date)))
    else:
        df = df.withColumn("ingestion_date", current_date())
    return df


def _write_delta(
    df: DataFrame,
    path: str,
) -> None:
    """Write with dynamic partition overwrite (replaces only written partitions, idempotent re-run)."""
    df.write.format("delta").partitionBy("category", "ingestion_date").mode("overwrite").save(path)


def run(
    spark: Optional[SparkSession] = None,
    category: Optional[str] = None,
    ingestion_date: Optional[str] = None,
) -> None:
    paths = get_paths()
    raw_root = Path(paths["raw"]) / "amazon"
    bronze_root = Path(paths["bronze"]) / AMAZON_BRONZE_ROOT

    spark = spark or get_spark_session("AmazonBronzeIngestion")
    spark.conf.set("spark.sql.files.maxPartitionBytes", get_max_partition_bytes())

    ingest_date = ingestion_date or None

    if category:
        # Scope to one category: read specific files, partition overwrite
        review_path = str(raw_root / "reviews" / f"{category}.jsonl.gz")
        metadata_path = str(raw_root / "metadata" / f"meta_{category}.jsonl.gz")
        apply_dynamic_config(spark, [review_path, metadata_path])
        if not Path(review_path).exists() or not Path(metadata_path).exists():
            raise FileNotFoundError(
                f"Raw files for {category} not found. Run fetch first: "
                f"python scripts/fetch_amazon_data.py --categories {category}"
            )
        reviews = _read_jsonl(spark, review_path, AMAZON_REVIEW_SCHEMA)
        metadata = _read_jsonl(spark, metadata_path, AMAZON_METADATA_SCHEMA)
        reviews = _with_partition_columns(reviews, category, ingest_date)
        metadata = _with_partition_columns(metadata, category, ingest_date)
        # Dynamic partition overwrite: only overwrites (category, ingestion_date) we write
        spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
        _write_delta(reviews, str(bronze_root / "reviews"))
        _write_delta(metadata, str(bronze_root / "metadata"))
    else:
        # Full ingest: all raw files, append (legacy behavior for run_pipeline.sh)
        review_path = str(raw_root / "reviews")
        metadata_path = str(raw_root / "metadata")
        apply_dynamic_config(spark, [review_path, metadata_path])
        reviews = (
            spark.read.schema(AMAZON_REVIEW_SCHEMA)
            .option("recursiveFileLookup", "true")
            .option("pathGlobFilter", "*.jsonl.gz")
            .json(review_path)
        )
        metadata = (
            spark.read.schema(AMAZON_METADATA_SCHEMA)
            .option("recursiveFileLookup", "true")
            .option("pathGlobFilter", "*.jsonl.gz")
            .json(metadata_path)
        )
        reviews = reviews.withColumn("file_path", input_file_name()).withColumn(
            "category", regexp_extract("file_path", r"[\\/]reviews[\\/]([^\\/]+?)\.jsonl\.gz$", 1)
        ).drop("file_path").filter("category != ''")
        metadata = metadata.withColumn("file_path", input_file_name()).withColumn(
            "category",
            regexp_extract("file_path", r"[\\/]metadata[\\/](?:meta_)?([^\\/]+?)\.jsonl\.gz$", 1),
        ).drop("file_path").filter("category != ''")
        ing_col = to_date(lit(ingest_date)) if ingest_date else current_date()
        reviews = reviews.withColumn("ingestion_date", ing_col)
        metadata = metadata.withColumn("ingestion_date", ing_col)
        reviews.write.format("delta").partitionBy("category", "ingestion_date").mode("append").save(
            str(bronze_root / "reviews")
        )
        metadata.write.format("delta").partitionBy("category", "ingestion_date").mode("append").save(
            str(bronze_root / "metadata")
        )


if __name__ == "__main__":
    ingestion_date = None
    category = None
    for arg in sys.argv[1:]:
        if arg.startswith("--category="):
            category = arg.split("=", 1)[1]
        elif not arg.startswith("-"):
            ingestion_date = arg
    run(category=category, ingestion_date=ingestion_date)
