"""
Unit tests for Amazon Medallion transformation logic.
"""

from src.jobs.amazon_silver_transformation import _build_silver_df
from src.jobs.amazon_gold_analytics import (
    _build_category_trends_df,
    _build_product_metrics_df,
    _build_verified_purchase_impact_df,
)
from pyspark.sql.functions import to_date


def test_amazon_silver_transformation_logic(spark_session):
    """Verify review normalization, metadata enrichment, and price parsing."""
    reviews = spark_session.createDataFrame(
        [
            (
                5.0,
                "Great product",
                "Works as expected.",
                "B001",
                "P001",
                "USER1",
                1_588_687_728_923,
                2,
                True,
                "Gift_Cards",
                "2026-03-08",
            ),
            (
                4.0,
                "No metadata",
                "Still keep the review row.",
                "B999",
                "P999",
                "USER2",
                1_588_690_000_000,
                0,
                False,
                "Gift_Cards",
                "2026-03-08",
            ),
        ],
        [
            "rating",
            "title",
            "text",
            "asin",
            "parent_asin",
            "user_id",
            "timestamp",
            "helpful_vote",
            "verified_purchase",
            "category",
            "ingestion_date",
        ],
    ).withColumn("ingestion_date", to_date("ingestion_date"))

    metadata = spark_session.createDataFrame(
        [
            (
                "Gift Cards",
                "Test Gift Card",
                4.5,
                100,
                "$19.99",
                "TestStore",
                ["Gift Cards"],
                "P001",
                "Gift_Cards",
                "2026-03-08",
            ),
            (
                "Gift Cards",
                "Bad Price",
                3.5,
                50,
                "Not available",
                "OtherStore",
                ["Gift Cards"],
                "P999",
                "Gift_Cards",
                "2026-03-08",
            ),
        ],
        [
            "main_category",
            "title",
            "average_rating",
            "rating_number",
            "price",
            "store",
            "categories",
            "parent_asin",
            "category",
            "ingestion_date",
        ],
    ).withColumn("ingestion_date", to_date("ingestion_date"))

    result = _build_silver_df(reviews, metadata).orderBy("asin").collect()

    assert len(result) == 2

    enriched = result[0]
    assert enriched.review_title == "Great product"
    assert enriched.review_text == "Works as expected."
    assert enriched.raw_timestamp == 1_588_687_728_923
    assert str(enriched.review_date) == "2020-05-05"
    assert enriched.product_title == "Test Gift Card"
    assert enriched.main_category == "Gift Cards"
    assert enriched.product_avg_rating == 4.5
    assert enriched.price == 19.99
    assert enriched.review_id

    bad_price = result[1]
    assert bad_price.product_title == "Bad Price"
    assert bad_price.price is None


def test_amazon_gold_analytics_logic(spark_session):
    """Verify daily product metrics, category trends, and verified split logic."""
    silver = spark_session.createDataFrame(
        [
            ("R1", "U1", "A1", "P1", 5.0, "t1", "x1", 1, None, "2020-05-01", 0, True, "Gift_Cards", "2026-03-08", "Prod1", "Gift Cards", 4.5, 19.99),
            ("R2", "U2", "A1", "P1", 4.0, "t2", "x2", 2, None, "2020-05-01", 0, False, "Gift_Cards", "2026-03-08", "Prod1", "Gift Cards", 4.5, 19.99),
            ("R3", "U3", "A1", "P1", 3.0, "t3", "x3", 3, None, "2020-05-02", 0, True, "Gift_Cards", "2026-03-08", "Prod1", "Gift Cards", 4.5, 19.99),
            ("R4", "U4", "A2", "P2", 2.0, "t4", "x4", 4, None, "2020-05-02", 0, False, "Gift_Cards", "2026-03-08", "Prod2", "Gift Cards", 3.0, 9.99),
        ],
        [
            "review_id",
            "user_id",
            "asin",
            "parent_asin",
            "rating",
            "review_title",
            "review_text",
            "raw_timestamp",
            "review_timestamp",
            "review_date",
            "helpful_vote",
            "verified_purchase",
            "category",
            "ingestion_date",
            "product_title",
            "main_category",
            "product_avg_rating",
            "price",
        ],
    ).withColumn("review_date", to_date("review_date")).withColumn("ingestion_date", to_date("ingestion_date"))

    product_metrics = _build_product_metrics_df(silver, "2020-05-01", "2020-05-02")
    category_trends = _build_category_trends_df(silver, "2020-05-01", "2020-05-02")
    verified_impact = _build_verified_purchase_impact_df(silver, "2020-05-01", "2020-05-02")

    product_rows = {
        (row.parent_asin, str(row.review_date)): row
        for row in product_metrics.orderBy("parent_asin", "review_date").collect()
    }
    assert product_rows[("P1", "2020-05-01")].total_reviews == 2
    assert product_rows[("P1", "2020-05-01")].average_rating == 4.5
    assert product_rows[("P1", "2020-05-01")].rolling_30d_avg_rating == 4.5
    assert product_rows[("P1", "2020-05-02")].total_reviews == 3
    assert product_rows[("P1", "2020-05-02")].average_rating == 4.0
    assert product_rows[("P1", "2020-05-02")].rolling_30d_avg_rating == 4.0
    assert product_rows[("P2", "2020-05-02")].total_reviews == 1
    assert product_rows[("P2", "2020-05-02")].average_rating == 2.0

    category_rows = {str(row.review_date): row for row in category_trends.orderBy("review_date").collect()}
    assert category_rows["2020-05-01"].daily_review_count == 2
    assert category_rows["2020-05-01"].daily_avg_rating == 4.5
    assert category_rows["2020-05-02"].daily_review_count == 2
    assert category_rows["2020-05-02"].daily_avg_rating == 2.5

    verified_rows = {
        (str(row.review_date), row.verified_purchase): row
        for row in verified_impact.orderBy("review_date", "verified_purchase").collect()
    }
    assert verified_rows[("2020-05-01", True)].daily_review_count == 1
    assert verified_rows[("2020-05-01", True)].avg_rating == 5.0
    assert verified_rows[("2020-05-01", False)].avg_rating == 4.0
    assert verified_rows[("2020-05-02", True)].avg_rating == 3.0
    assert verified_rows[("2020-05-02", False)].avg_rating == 2.0
