"""
Great Expectations validation for Amazon Silver layer.
Validates: non-null identifiers, valid ratings, positive helpful votes.
Uses Spark for validation; GX suite/checkpoint for configuration.
"""

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql import DataFrame


def validate_silver(df: "DataFrame") -> bool:
    """
    Validate Amazon Silver DataFrame before Gold write.
    Checks: non-null review_id/parent_asin, valid rating (1-5), non-negative helpful_vote.
    Raises ValueError if validation fails.
    """
    null_review_id = df.filter("review_id IS NULL OR review_id = ''").count()
    null_parent_asin = df.filter("parent_asin IS NULL OR parent_asin = ''").count()
    if null_review_id > 0 or null_parent_asin > 0:
        raise ValueError(
            f"GX validation failed: null_review_id={null_review_id}, null_parent_asin={null_parent_asin}"
        )

    invalid_rating = df.filter("rating < 1 OR rating > 5").count()
    invalid_helpful = df.filter("helpful_vote < 0").count()

    if invalid_rating > 0 or invalid_helpful > 0:
        raise ValueError(
            f"GX validation failed: invalid_rating={invalid_rating}, invalid_helpful_vote={invalid_helpful}"
        )

    return True
