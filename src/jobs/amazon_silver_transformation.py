"""
Silver Layer: Normalize Amazon Bronze reviews and enrich with product metadata.
- Incremental filtering by category and ingestion_date
- Broadcast metadata join on parent_asin and category
- Idempotent MERGE on review_id
"""

import sys
from pathlib import Path
from typing import Optional

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    broadcast,
    coalesce,
    col,
    concat_ws,
    from_unixtime,
    lit,
    regexp_extract,
    sha2,
    to_date,
    year,
)

from src.utils.amazon_schemas import AMAZON_REVIEW_SILVER_SCHEMA
from src.utils.config_loader import get_max_partition_bytes, get_paths
from src.utils.spark_session import apply_dynamic_config, get_spark_session

AMAZON_ROOT = "amazon_reviews"
PRICE_REGEX = r"(\d+(?:\.\d+)?)"


def _apply_filters(
    df: DataFrame,
    category: Optional[str],
    ingestion_date: Optional[str],
) -> DataFrame:
    """Apply incremental category and ingestion_date filters when provided."""
    if category:
        df = df.filter(col("category") == category)
    if ingestion_date:
        df = df.filter(col("ingestion_date") == ingestion_date)
    return df


def _build_silver_df(reviews_df: DataFrame, metadata_df: DataFrame) -> DataFrame:
    """Normalize Bronze reviews and enrich them with selected product metadata.
    Deduplication runs before the join to reduce shuffle volume."""
    reviews = (
        reviews_df.withColumn(
            "review_id",
            sha2(
                concat_ws(
                    "||",
                    coalesce(col("user_id"), lit("")),
                    coalesce(col("parent_asin"), lit("")),
                    coalesce(col("timestamp").cast("string"), lit("")),
                ),
                256,
            ),
        )
        .withColumn("review_timestamp", from_unixtime(col("timestamp") / 1000).cast("timestamp"))
        .withColumn("review_date", to_date(col("review_timestamp")))
        .withColumnRenamed("title", "review_title")
        .withColumnRenamed("text", "review_text")
        .withColumnRenamed("timestamp", "raw_timestamp")
        .dropDuplicates(["review_id"])
    )

    metadata = (
        metadata_df.withColumn(
            "price",
            regexp_extract(coalesce(col("price"), lit("")), PRICE_REGEX, 1).cast("double"),
        )
        .withColumnRenamed("title", "product_title")
        .withColumnRenamed("average_rating", "product_avg_rating")
        .select(
            "parent_asin",
            "category",
            "product_title",
            "main_category",
            "product_avg_rating",
            "price",
        )
    )

    silver = reviews.join(
        broadcast(metadata),
        on=["parent_asin", "category"],
        how="left",
    )

    silver_cols = [field.name for field in AMAZON_REVIEW_SILVER_SCHEMA.fields]
    result = silver.select(*silver_cols)
    try:
        from src.utils.debug_explain import log_explain
        log_explain(result, label="silver_build", mode="formatted")
    except Exception:
        pass
    return result


def run(
    spark: Optional[SparkSession] = None,
    category: Optional[str] = None,
    ingestion_date: Optional[str] = None,
) -> None:
    paths = get_paths()
    bronze_root = Path(paths["bronze"]) / AMAZON_ROOT
    silver_root = Path(paths["silver"]) / AMAZON_ROOT
    silver_path = str(silver_root)

    spark = spark or get_spark_session("AmazonSilverTransformation")
    spark.conf.set("spark.sql.files.maxPartitionBytes", get_max_partition_bytes())

    reviews_path = str(bronze_root / "reviews")
    metadata_path = str(bronze_root / "metadata")
    apply_dynamic_config(spark, [reviews_path, metadata_path])

    reviews_df = spark.read.format("delta").load(reviews_path)
    metadata_df = spark.read.format("delta").load(metadata_path)

    reviews_df = _apply_filters(reviews_df, category, ingestion_date)
    metadata_df = _apply_filters(metadata_df, category, ingestion_date)

    if reviews_df.isEmpty():
        return

    silver_df = _build_silver_df(reviews_df, metadata_df)

    if silver_df.isEmpty():
        return

    # Partition by year (not review_date) to avoid 500+ partitions for small datasets.
    silver_df = silver_df.withColumn("year", year(col("review_date")))

    # Check the exact target path only.
    # `/data/silver`, and DeltaTable.isDeltaTable() can treat that ancestor as a match.
    if (silver_root / "_delta_log").exists():
        target = DeltaTable.forPath(spark, silver_path)
        (
            target.alias("target")
            .merge(silver_df.alias("source"), "target.review_id = source.review_id")
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
    else:
        (
            silver_df.write.format("delta")
            .option("mergeSchema", "true")
            .partitionBy("category", "year")
            .mode("overwrite")
            .save(silver_path)
        )


if __name__ == "__main__":
    args = sys.argv[1:]
    category = None
    ingestion_date = None

    for arg in args:
        if arg.startswith("--category="):
            category = arg.split("=", 1)[1]
        elif not arg.startswith("-"):
            ingestion_date = arg

    run(category=category, ingestion_date=ingestion_date)
