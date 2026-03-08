"""
Bronze Layer: Bulk raw Amazon Reviews 2023 JSONL.gz to Delta.
- Manual schemas (no inferSchema)
- Reviews and metadata ingested separately under bronze/amazon_reviews
- Category extracted from file path via input_file_name()
- Optional ingestion_date arg: use specific date for batch, else current_date
"""

import sys
from pathlib import Path
from typing import Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import current_date, input_file_name, lit, regexp_extract, to_date
from pyspark.sql.types import StructType

from src.utils.amazon_schemas import AMAZON_METADATA_SCHEMA, AMAZON_REVIEW_SCHEMA
from src.utils.config_loader import get_max_partition_bytes, get_paths
from src.utils.spark_session import get_spark_session

AMAZON_BRONZE_ROOT = "amazon_reviews"


def _read_jsonl(
    spark: SparkSession,
    source_path: str,
    schema: StructType,
) -> DataFrame:
    """Read gzipped JSONL recursively with explicit schema."""
    return (
        spark.read.schema(schema)
        .option("recursiveFileLookup", "true")
        .option("pathGlobFilter", "*.jsonl.gz")
        .json(source_path)
    )


def _with_partition_columns(
    df: DataFrame,
    category_pattern: str,
    ingestion_date: Optional[str],
) -> DataFrame:
    """Add partition columns derived from file path and pipeline argument."""
    df = df.withColumn("file_path", input_file_name()).withColumn(
        "category",
        regexp_extract("file_path", category_pattern, 1),
    )

    if ingestion_date:
        df = df.withColumn("ingestion_date", to_date(lit(ingestion_date)))
    else:
        df = df.withColumn("ingestion_date", current_date())

    return df.drop("file_path").filter("category != ''")


def run(spark: Optional[SparkSession] = None, ingestion_date: Optional[str] = None) -> None:
    paths = get_paths()
    raw_root = Path(paths["raw"]) / "amazon"
    bronze_root = Path(paths["bronze"]) / AMAZON_BRONZE_ROOT

    spark = spark or get_spark_session("AmazonBronzeIngestion")
    # Smaller scan partitions help avoid oversized tasks on 1GB executors.
    spark.conf.set("spark.sql.files.maxPartitionBytes", get_max_partition_bytes())

    review_path = str(raw_root / "reviews")
    metadata_path = str(raw_root / "metadata")
    review_bronze_path = str(bronze_root / "reviews")
    metadata_bronze_path = str(bronze_root / "metadata")

    reviews = _read_jsonl(spark, review_path, AMAZON_REVIEW_SCHEMA)
    reviews = _with_partition_columns(
        reviews,
        r"[\\/]reviews[\\/]([^\\/]+?)\.jsonl\.gz$",
        ingestion_date,
    )
    reviews.write.format("delta").partitionBy("category", "ingestion_date").mode("append").save(
        review_bronze_path
    )

    metadata = _read_jsonl(spark, metadata_path, AMAZON_METADATA_SCHEMA)
    metadata = _with_partition_columns(
        metadata,
        r"[\\/]metadata[\\/](?:meta_)?([^\\/]+?)\.jsonl\.gz$",
        ingestion_date,
    )
    metadata.write.format("delta").partitionBy("category", "ingestion_date").mode(
        "append"
    ).save(metadata_bronze_path)


if __name__ == "__main__":
    ingestion_date = sys.argv[1] if len(sys.argv) > 1 else None
    run(ingestion_date=ingestion_date)
