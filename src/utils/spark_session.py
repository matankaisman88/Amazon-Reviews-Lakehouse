"""
SparkSession builder with Delta Lake, History Server, and AQE tuning.
Supports dynamic configuration based on input size.
"""

import os
from pathlib import Path
from typing import List, Optional, Union

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

from .config_loader import get_paths, get_spark_config, get_shuffle_partitions_for_input
from .input_size_estimator import estimate_input_size_bytes


def _apply_aqe_config(spark: SparkSession) -> None:
    """Apply AQE settings from config (advisory partition size, coalesce, skew join)."""
    cfg = get_spark_config()
    advisory = cfg.get("advisory_partition_size", 134217728)  # 128MB
    coalesce = cfg.get("coalesce_partitions_enabled", True)
    skew_join = cfg.get("skew_join_enabled", True)
    spark.conf.set("spark.sql.adaptive.advisoryPartitionSizeInBytes", str(advisory))
    spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", str(coalesce).lower())
    spark.conf.set("spark.sql.adaptive.skewJoin.enabled", str(skew_join).lower())


def apply_dynamic_config(
    spark: SparkSession,
    input_paths: Union[str, Path, List[Union[str, Path]]],
) -> int:
    """
    Estimate input size, set shuffle partitions dynamically, apply AQE.
    Returns the estimated input size in bytes.
    """
    size_bytes = estimate_input_size_bytes(input_paths)
    shuffle_partitions = get_shuffle_partitions_for_input(size_bytes)
    spark.conf.set("spark.sql.shuffle.partitions", str(shuffle_partitions))
    _apply_aqe_config(spark)
    return size_bytes


def get_spark_session(
    app_name: str = "AmazonReviewsLakehouse",
    input_paths: Optional[Union[str, Path, List[Union[str, Path]]]] = None,
) -> SparkSession:
    """
    Build SparkSession with Delta, event logging, and resource tuning.
    If input_paths is provided and dynamic_config.enabled, sets shuffle partitions from input size.
    """
    cfg = get_spark_config()
    paths = get_paths()

    shuffle_partitions = cfg.get("shuffle_partitions", 8)
    if input_paths is not None:
        size_bytes = estimate_input_size_bytes(input_paths)
        shuffle_partitions = get_shuffle_partitions_for_input(size_bytes)

    master = os.getenv("SPARK_MASTER", "local[*]")
    builder = (
        SparkSession.builder.master(master).appName(app_name)
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
        .config("spark.sql.shuffle.partitions", str(shuffle_partitions))
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

    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    _apply_aqe_config(spark)
    return spark
