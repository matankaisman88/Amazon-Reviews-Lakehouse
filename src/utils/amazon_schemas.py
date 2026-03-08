"""
Manual StructTypes for Amazon Reviews 2023 raw ingestion.
Do NOT use inferSchema - explicit schemas reduce overhead and keep ingestion stable.
"""

from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    DateType,
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

# Amazon Reviews 2023 review JSONL (raw format)
AMAZON_REVIEW_SCHEMA = StructType(
    [
        StructField("rating", DoubleType(), True),
        StructField("title", StringType(), True),
        StructField("text", StringType(), True),
        StructField("asin", StringType(), True),
        StructField("parent_asin", StringType(), True),
        StructField("user_id", StringType(), True),
        StructField("timestamp", LongType(), True),
        StructField("helpful_vote", LongType(), True),
        StructField("verified_purchase", BooleanType(), True),
    ]
)

# Amazon Reviews 2023 metadata JSONL (raw format)
AMAZON_METADATA_SCHEMA = StructType(
    [
        StructField("main_category", StringType(), True),
        StructField("title", StringType(), True),
        StructField("average_rating", DoubleType(), True),
        StructField("rating_number", LongType(), True),
        StructField("price", StringType(), True),
        StructField("store", StringType(), True),
        StructField("categories", ArrayType(StringType(), True), True),
        StructField("parent_asin", StringType(), True),
    ]
)

# Silver: normalized reviews enriched with product metadata
AMAZON_REVIEW_SILVER_SCHEMA = StructType(
    [
        StructField("review_id", StringType(), False),
        StructField("user_id", StringType(), True),
        StructField("asin", StringType(), True),
        StructField("parent_asin", StringType(), True),
        StructField("rating", DoubleType(), True),
        StructField("review_title", StringType(), True),
        StructField("review_text", StringType(), True),
        StructField("raw_timestamp", LongType(), True),
        StructField("review_timestamp", TimestampType(), True),
        StructField("review_date", DateType(), True),
        StructField("helpful_vote", LongType(), True),
        StructField("verified_purchase", BooleanType(), True),
        StructField("category", StringType(), True),
        StructField("ingestion_date", DateType(), True),
        StructField("product_title", StringType(), True),
        StructField("main_category", StringType(), True),
        StructField("product_avg_rating", DoubleType(), True),
        StructField("price", DoubleType(), True),
    ]
)

# Gold: daily product metrics with cumulative and rolling summaries
AMAZON_PRODUCT_METRICS_GOLD_SCHEMA = StructType(
    [
        StructField("parent_asin", StringType(), True),
        StructField("category", StringType(), True),
        StructField("review_date", DateType(), True),
        StructField("total_reviews", LongType(), True),
        StructField("average_rating", DoubleType(), True),
        StructField("rolling_30d_avg_rating", DoubleType(), True),
        StructField("avg_price", DoubleType(), True),
    ]
)

# Gold: daily category trend metrics
AMAZON_CATEGORY_TRENDS_GOLD_SCHEMA = StructType(
    [
        StructField("category", StringType(), True),
        StructField("review_date", DateType(), True),
        StructField("daily_review_count", LongType(), True),
        StructField("daily_avg_rating", DoubleType(), True),
        StructField("count_1_star", LongType(), True),
        StructField("count_2_star", LongType(), True),
        StructField("count_3_star", LongType(), True),
        StructField("count_4_star", LongType(), True),
        StructField("count_5_star", LongType(), True),
    ]
)

# Gold: verified vs non-verified purchase impact by day
AMAZON_VERIFIED_PURCHASE_IMPACT_GOLD_SCHEMA = StructType(
    [
        StructField("category", StringType(), True),
        StructField("review_date", DateType(), True),
        StructField("verified_purchase", BooleanType(), True),
        StructField("daily_review_count", LongType(), True),
        StructField("avg_rating", DoubleType(), True),
    ]
)
