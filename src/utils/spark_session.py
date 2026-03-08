"""
SparkSession builder with Delta Lake, History Server, and AQE tuning.
All config from config_loader - no hardcoded values.
"""

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

from .config_loader import get_paths, get_spark_config


def get_spark_session(app_name: str = "AmazonReviewsLakehouse") -> SparkSession:
    """Build SparkSession with Delta, event logging, and resource tuning."""
    cfg = get_spark_config()
    paths = get_paths()

    builder = (
        SparkSession.builder.appName(app_name)
        # Delta Lake extensions — set explicitly so they survive getOrCreate()
        # even when a session already exists (configure_spark_with_delta_pip
        # alone is not enough if it re-uses an existing session that lacks them).
        .config(
            "spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension",
        )
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.executor.memory", cfg.get("executor_memory", "1g"))
        .config("spark.driver.memory", cfg.get("driver_memory", "512m"))
        .config("spark.memory.fraction", str(cfg.get("memory_fraction", 0.5)))
        .config("spark.memory.storageFraction", str(cfg.get("memory_storage_fraction", 0.3)))
        .config("spark.sql.shuffle.partitions", str(cfg.get("shuffle_partitions", 8)))
        .config("spark.sql.adaptive.enabled", str(cfg.get("adaptive_enabled", True)).lower())
        .config(
            "spark.serializer", cfg.get("serializer", "org.apache.spark.serializer.KryoSerializer")
        )
    )

    if cfg.get("event_log_enabled"):
        builder = builder.config("spark.eventLog.enabled", "true").config(
            "spark.eventLog.dir",
            cfg.get("event_log_dir", paths.get("spark_events", "/opt/spark-events")),
        )

    # configure_spark_with_delta_pip adds the Delta JAR to the classpath.
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    return spark
