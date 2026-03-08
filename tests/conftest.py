"""
Pytest fixtures for Spark and Delta Lake testing.
"""

import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark_session():
    """Create a local SparkSession for testing (Delta optional for aggregation tests)."""
    try:
        return (
            SparkSession.builder.appName("AmazonLakehouseTests")
            .master("local[*]")
            .config("spark.sql.shuffle.partitions", "1")
            .config("spark.driver.memory", "1g")
            .getOrCreate()
        )
    except Exception as e:
        pytest.skip(f"Spark not available: {e}")
