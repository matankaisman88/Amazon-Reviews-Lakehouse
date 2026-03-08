"""
Gold Layer: Amazon review analytics and storage optimization.
- Incremental-windowed refresh using a 30-day lookback
- Memory-efficient rolling metrics from daily aggregates
- Delta MERGE for rerun safety and scoped OPTIMIZE + Z-ORDER
"""

import sys
from datetime import timedelta
from pathlib import Path
from typing import List, Optional

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from src.utils.amazon_schemas import (
    AMAZON_CATEGORY_TRENDS_GOLD_SCHEMA,
    AMAZON_PRODUCT_METRICS_GOLD_SCHEMA,
    AMAZON_VERIFIED_PURCHASE_IMPACT_GOLD_SCHEMA,
)
from src.utils.config_loader import get_max_partition_bytes, get_paths
from src.utils.spark_session import get_spark_session

AMAZON_ROOT = "amazon_reviews"
ROLLING_DAYS = 30

# Sample Spark SQL for AI-style analytics against the Gold tables:
#
# WITH ranked AS (
#   SELECT
#       parent_asin,
#       review_date,
#       rolling_30d_avg_rating,
#       LAG(rolling_30d_avg_rating) OVER (
#           PARTITION BY parent_asin
#           ORDER BY review_date
#       ) AS previous_rolling_avg
#   FROM product_metrics
#   WHERE category = 'Electronics'
#     AND review_date >= DATE_SUB(CURRENT_DATE(), 30)
# )
# SELECT
#     parent_asin,
#     review_date,
#     rolling_30d_avg_rating,
#     previous_rolling_avg,
#     previous_rolling_avg - rolling_30d_avg_rating AS rating_decline
# FROM ranked
# WHERE previous_rolling_avg IS NOT NULL
#   AND rolling_30d_avg_rating < previous_rolling_avg
# ORDER BY rating_decline DESC
# LIMIT 100;


def _apply_filters(
    df: DataFrame,
    category: Optional[str],
    ingestion_date: Optional[str],
) -> DataFrame:
    """Apply category and ingestion_date filters when provided."""
    if category:
        df = df.filter(F.col("category") == category)
    if ingestion_date:
        df = df.filter(F.col("ingestion_date") == ingestion_date)
    return df


def _get_recompute_window(touched_df: DataFrame) -> tuple:
    """Return the output date range plus the source lookback start date."""
    bounds = touched_df.agg(
        F.min("review_date").alias("min_review_date"),
        F.max("review_date").alias("max_review_date"),
    ).first()
    if bounds is None or bounds.min_review_date is None or bounds.max_review_date is None:
        raise ValueError("Touched Silver batch does not contain valid review_date values.")

    output_start = bounds.min_review_date
    output_end = bounds.max_review_date
    source_start = output_start - timedelta(days=ROLLING_DAYS - 1)
    return source_start, output_start, output_end


def _restrict_output_range(df: DataFrame, output_start, output_end) -> DataFrame:
    """Keep only the Gold rows that belong to the affected output date window."""
    return df.filter(
        (F.col("review_date") >= F.lit(output_start)) & (F.col("review_date") <= F.lit(output_end))
    )


def _build_product_metrics_df(silver_df: DataFrame, output_start, output_end) -> DataFrame:
    """Build cumulative and rolling product metrics from daily aggregates."""
    daily_product = silver_df.groupBy("parent_asin", "category", "review_date").agg(
        F.count("*").alias("daily_review_count"),
        F.sum("rating").alias("daily_rating_sum"),
    )

    order_col = F.unix_date("review_date")
    cumulative_window = (
        Window.partitionBy("parent_asin", "category")
        .orderBy(order_col)
        .rowsBetween(Window.unboundedPreceding, Window.currentRow)
    )
    rolling_window = (
        Window.partitionBy("parent_asin", "category")
        .orderBy(order_col)
        .rangeBetween(-(ROLLING_DAYS - 1), 0)
    )

    product_metrics = (
        daily_product.withColumn("total_reviews", F.sum("daily_review_count").over(cumulative_window))
        .withColumn("cumulative_rating_sum", F.sum("daily_rating_sum").over(cumulative_window))
        .withColumn(
            "average_rating",
            F.col("cumulative_rating_sum") / F.col("total_reviews"),
        )
        .withColumn("rolling_30d_review_count", F.sum("daily_review_count").over(rolling_window))
        .withColumn("rolling_30d_rating_sum", F.sum("daily_rating_sum").over(rolling_window))
        .withColumn(
            "rolling_30d_avg_rating",
            F.col("rolling_30d_rating_sum") / F.col("rolling_30d_review_count"),
        )
        .select(*[field.name for field in AMAZON_PRODUCT_METRICS_GOLD_SCHEMA.fields])
    )

    return _restrict_output_range(product_metrics, output_start, output_end)


def _build_category_trends_df(silver_df: DataFrame, output_start, output_end) -> DataFrame:
    """Build daily category-level trend metrics."""
    category_trends = (
        silver_df.groupBy("category", "review_date")
        .agg(
            F.count("*").alias("daily_review_count"),
            F.avg("rating").alias("daily_avg_rating"),
        )
        .select(*[field.name for field in AMAZON_CATEGORY_TRENDS_GOLD_SCHEMA.fields])
    )
    return _restrict_output_range(category_trends, output_start, output_end)


def _build_verified_purchase_impact_df(
    silver_df: DataFrame,
    output_start,
    output_end,
) -> DataFrame:
    """Build daily verified-purchase impact metrics."""
    verified_impact = (
        silver_df.groupBy("category", "review_date", "verified_purchase")
        .agg(
            F.count("*").alias("daily_review_count"),
            F.avg("rating").alias("avg_rating"),
        )
        .select(*[field.name for field in AMAZON_VERIFIED_PURCHASE_IMPACT_GOLD_SCHEMA.fields])
    )
    return _restrict_output_range(verified_impact, output_start, output_end)


def _write_gold_table(
    spark: SparkSession,
    df: DataFrame,
    target_root: Path,
    merge_condition: str,
) -> None:
    """Write Gold data with bootstrap overwrite and rerun-safe MERGE."""
    target_path = str(target_root)
    if (target_root / "_delta_log").exists():
        target = DeltaTable.forPath(spark, target_path)
        (
            target.alias("target")
            .merge(df.alias("source"), merge_condition)
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
    else:
        (
            df.write.format("delta")
            .partitionBy("category", "review_date")
            .mode("overwrite")
            .save(target_path)
        )


def _optimize_table(
    spark: SparkSession,
    target_root: Path,
    df: DataFrame,
    zorder_cols: Optional[List[str]] = None,
) -> None:
    """OPTIMIZE only the touched partitions and Z-ORDER supported data columns."""
    if not (target_root / "_delta_log").exists():
        return

    dates = [str(row.review_date) for row in df.select("review_date").distinct().collect() if row.review_date]
    categories = [row.category for row in df.select("category").distinct().collect() if row.category]
    if not dates or not categories:
        return

    date_list = "', '".join(dates)
    category_list = "', '".join(categories)
    predicate = f"review_date IN ('{date_list}') AND category IN ('{category_list}')"

    optimize_builder = DeltaTable.forPath(spark, str(target_root)).optimize().where(predicate)
    # Delta cannot Z-ORDER partition columns, so partition pruning handles
    # category/review_date while Z-ORDER is reserved for useful data columns.
    if zorder_cols:
        optimize_builder.executeZOrderBy(zorder_cols)
    else:
        optimize_builder.executeCompaction()


def run(
    spark: Optional[SparkSession] = None,
    category: Optional[str] = None,
    ingestion_date: Optional[str] = None,
    skip_optimize: bool = False,
) -> None:
    paths = get_paths()
    silver_root = Path(paths["silver"]) / AMAZON_ROOT
    gold_root = Path(paths["gold"]) / AMAZON_ROOT

    spark = spark or get_spark_session("AmazonGoldAnalytics")
    # Smaller scan partitions help avoid oversized tasks on constrained executors.
    spark.conf.set("spark.sql.files.maxPartitionBytes", get_max_partition_bytes())

    silver_df = spark.read.format("delta").load(str(silver_root))
    touched_df = _apply_filters(silver_df, category, ingestion_date)
    if touched_df.isEmpty():
        return

    source_start, output_start, output_end = _get_recompute_window(touched_df)

    source_df = silver_df.filter(
        (F.col("review_date") >= F.lit(source_start)) & (F.col("review_date") <= F.lit(output_end))
    )
    if category:
        source_df = source_df.filter(F.col("category") == category)

    product_metrics_df = _build_product_metrics_df(source_df, output_start, output_end)
    category_trends_df = _build_category_trends_df(source_df, output_start, output_end)
    verified_impact_df = _build_verified_purchase_impact_df(source_df, output_start, output_end)

    if product_metrics_df.isEmpty():
        return

    product_root = gold_root / "product_metrics"
    category_root = gold_root / "category_trends"
    verified_root = gold_root / "verified_purchase_impact"

    _write_gold_table(
        spark,
        product_metrics_df,
        product_root,
        " AND ".join(
            [
                "target.parent_asin = source.parent_asin",
                "target.category = source.category",
                "target.review_date = source.review_date",
            ]
        ),
    )
    _write_gold_table(
        spark,
        category_trends_df,
        category_root,
        "target.category = source.category AND target.review_date = source.review_date",
    )
    _write_gold_table(
        spark,
        verified_impact_df,
        verified_root,
        " AND ".join(
            [
                "target.category = source.category",
                "target.review_date = source.review_date",
                "target.verified_purchase <=> source.verified_purchase",
            ]
        ),
    )

    if not skip_optimize:
        _optimize_table(spark, product_root, product_metrics_df, zorder_cols=["parent_asin"])
        _optimize_table(spark, category_root, category_trends_df)
        _optimize_table(spark, verified_root, verified_impact_df, zorder_cols=["verified_purchase"])


if __name__ == "__main__":
    args = sys.argv[1:]
    category = None
    ingestion_date = None
    skip_optimize = False

    for arg in args:
        if arg.startswith("--category="):
            category = arg.split("=", 1)[1]
        elif arg in ("--skip-optimize", "-s"):
            skip_optimize = True
        elif not arg.startswith("-"):
            ingestion_date = arg

    run(category=category, ingestion_date=ingestion_date, skip_optimize=skip_optimize)
