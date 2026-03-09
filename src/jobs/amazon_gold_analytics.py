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
from src.utils.config_loader import (
    get_max_partition_bytes,
    get_paths,
    get_gold_optimize_threshold,
    get_gold_optimize_threshold_size_mb,
    get_shuffle_partitions_for_input,
)
from src.utils.input_size_estimator import estimate_input_size_bytes
from src.utils.spark_session import apply_dynamic_config, get_spark_session

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
        F.avg("price").alias("daily_avg_price"),
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

    # avg_price: use last non-null daily_avg_price in rolling window (product price is stable)
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
        .withColumn(
            "avg_price",
            F.last("daily_avg_price", ignorenulls=True).over(rolling_window),
        )
        .select(*[field.name for field in AMAZON_PRODUCT_METRICS_GOLD_SCHEMA.fields])
    )

    return _restrict_output_range(product_metrics, output_start, output_end)


def _build_category_trends_df(silver_df: DataFrame, output_start, output_end) -> DataFrame:
    """Build daily category-level trend metrics with rating distribution."""
    rounded_rating = F.round(F.col("rating")).cast("long")
    category_trends = (
        silver_df.groupBy("category", "review_date")
        .agg(
            F.count("*").alias("daily_review_count"),
            F.avg("rating").alias("daily_avg_rating"),
            F.sum(F.when(rounded_rating == 1, 1).otherwise(0)).alias("count_1_star"),
            F.sum(F.when(rounded_rating == 2, 1).otherwise(0)).alias("count_2_star"),
            F.sum(F.when(rounded_rating == 3, 1).otherwise(0)).alias("count_3_star"),
            F.sum(F.when(rounded_rating == 4, 1).otherwise(0)).alias("count_4_star"),
            F.sum(F.when(rounded_rating == 5, 1).otherwise(0)).alias("count_5_star"),
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


def _should_optimize_table(target_root: Path) -> bool:
    """Check if table meets size threshold for OPTIMIZE (avoids DBU waste on tiny tables)."""
    size_bytes = estimate_input_size_bytes(target_root)
    size_mb = size_bytes / (1024 * 1024)
    threshold_mb = get_gold_optimize_threshold_size_mb()
    return size_mb >= threshold_mb


def _optimize_table(
    spark: SparkSession,
    target_root: Path,
    output_start,
    output_end,
    categories: List[str],
    zorder_cols: Optional[List[str]] = None,
) -> None:
    """OPTIMIZE only the touched partitions and Z-ORDER when table meets size threshold."""
    if not (target_root / "_delta_log").exists():
        return
    if not categories:
        return
    if not _should_optimize_table(target_root):
        return

    from datetime import timedelta

    dates = []
    d = output_start
    while d <= output_end:
        dates.append(str(d))
        d += timedelta(days=1)
    date_list = "', '".join(dates)
    category_list = "', '".join(categories)
    predicate = f"review_date IN ('{date_list}') AND category IN ('{category_list}')"

    optimize_builder = DeltaTable.forPath(spark, str(target_root)).optimize().where(predicate)
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
    spark.conf.set("spark.sql.files.maxPartitionBytes", get_max_partition_bytes())

    silver_path = str(silver_root)
    apply_dynamic_config(spark, silver_path)
    # Enable schema evolution for merge (adds new columns like avg_price, count_*_star).
    spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")

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

    # Repartition by category for data locality: aligns with Gold partitioning and window partitionBy(parent_asin, category).
    # Auto-tune partition count from Silver table size (same logic as shuffle_partitions).
    silver_size_bytes = estimate_input_size_bytes(silver_path)
    max_partitions = get_shuffle_partitions_for_input(silver_size_bytes)
    source_df = source_df.repartition(max_partitions, "category")

    # Cache source_df: used for 3 Gold tables; avoid re-scanning Silver 3x.
    source_df = source_df.cache()
    if category:
        categories = [category]
    else:
        categories = [r.category for r in source_df.select("category").distinct().collect() if r.category]

    product_metrics_df = _build_product_metrics_df(source_df, output_start, output_end)
    category_trends_df = _build_category_trends_df(source_df, output_start, output_end)
    verified_impact_df = _build_verified_purchase_impact_df(source_df, output_start, output_end)

    try:
        from src.utils.debug_explain import log_explain
        log_explain(product_metrics_df, label="gold_product_metrics", mode="formatted")
    except Exception:
        pass

    if product_metrics_df.isEmpty():
        source_df.unpersist()
        return

    # Cache product_metrics for count + write; count drives conditional OPTIMIZE.
    product_metrics_df = product_metrics_df.cache()
    row_count = product_metrics_df.count()
    source_df.unpersist()

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
    product_metrics_df.unpersist()

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

    optimize_threshold = get_gold_optimize_threshold()
    should_opt = (
        not skip_optimize
        and (row_count >= optimize_threshold or _should_optimize_table(product_root))
    )
    if should_opt:
        _optimize_table(
            spark, product_root, output_start, output_end, categories, zorder_cols=["parent_asin"]
        )
        _optimize_table(spark, category_root, output_start, output_end, categories)
        _optimize_table(
            spark, verified_root, output_start, output_end, categories,
            zorder_cols=["verified_purchase"],
        )


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
