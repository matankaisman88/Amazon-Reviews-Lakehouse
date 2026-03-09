"""
Externalized configuration loader.
Loads config.yaml and .env. No hardcoded paths in jobs.
"""

import os
from pathlib import Path
from typing import Any, Dict, Optional

import yaml
from dotenv import load_dotenv

# Load .env from project root. override=True ensures values from .env
# overwrite empty env vars (e.g. OPENAI_API_KEY="" from docker-compose).
load_dotenv(override=True)

_CONFIG: Optional[Dict[str, Any]] = None


def _get_project_root() -> Path:
    return Path(__file__).resolve().parent.parent.parent


def _load_config() -> Dict[str, Any]:
    global _CONFIG
    if _CONFIG is not None:
        return _CONFIG
    root = _get_project_root()
    config_path = root / "config" / "config.yaml"
    if not config_path.exists():
        raise FileNotFoundError(f"Config not found: {config_path}")
    with open(config_path) as f:
        _CONFIG = yaml.safe_load(f)
    return _CONFIG


def get_paths() -> Dict[str, str]:
    cfg = _load_config()
    paths = cfg.get("paths", {})
    data_root = os.getenv("DATA_ROOT")
    if data_root:
        base = Path(data_root)
        return {k: str(base / Path(v).name) for k, v in paths.items()}
    return paths


def get_spark_config() -> Dict[str, Any]:
    cfg = _load_config()
    return cfg.get("spark", {})


def get_max_partition_bytes() -> str:
    """Max bytes per partition for file scans; reduces task count for large batches."""
    cfg = _load_config()
    spark = cfg.get("spark", {})
    return str(spark.get("max_partition_bytes", "128m"))


def get_gx_config() -> Dict[str, str]:
    cfg = _load_config()
    return cfg.get("gx", {})


def get_gold_rolling_days() -> int:
    """Rolling window in days for product_metrics (e.g. rolling_30d_avg_rating)."""
    cfg = _load_config()
    gold = cfg.get("gold", {})
    return int(gold.get("rolling_days", 30))


def get_gold_optimize_threshold() -> int:
    """Min rows to run OPTIMIZE/Z-ORDER; skip for small batches to avoid rewrite overhead."""
    cfg = _load_config()
    gold = cfg.get("gold", {})
    return int(gold.get("optimize_threshold_rows", 100000))


def get_gold_optimize_threshold_size_mb() -> float:
    """Min table size (MB) to run OPTIMIZE; skip for small tables."""
    cfg = _load_config()
    gold = cfg.get("gold", {})
    return float(gold.get("optimize_threshold_size_mb", 100))


def get_gold_small_input_threshold_bytes() -> int:
    """Below this Silver size (bytes), use fewer partitions for faster Gold processing."""
    cfg = _load_config()
    gold = cfg.get("gold", {})
    return int(gold.get("small_input_threshold_bytes", 10 * 1024 * 1024))


def get_gold_small_output_coalesce() -> int:
    """Coalesce to N partitions before MERGE when output is small."""
    cfg = _load_config()
    gold = cfg.get("gold", {})
    return int(gold.get("small_output_coalesce", 2))


def get_gold_small_output_threshold_rows() -> int:
    """Below this output row count, coalesce before MERGE (even when input was large)."""
    cfg = _load_config()
    gold = cfg.get("gold", {})
    return int(gold.get("small_output_threshold_rows", 10000))


def get_dynamic_config() -> Dict[str, Any]:
    """Dynamic config: target_partition_size_bytes, min/max shuffle partitions, small-input thresholds."""
    cfg = _load_config()
    dyn = cfg.get("dynamic_config", {})
    return {
        "enabled": dyn.get("enabled", True),
        "target_partition_size_bytes": int(dyn.get("target_partition_size_bytes", 134217728)),
        "min_shuffle_partitions": int(dyn.get("min_shuffle_partitions", 8)),
        "max_shuffle_partitions": int(dyn.get("max_shuffle_partitions", 400)),
        "small_input_threshold_bytes": int(dyn.get("small_input_threshold_bytes", 10485760)),
        "small_input_partitions": int(dyn.get("small_input_partitions", 2)),
    }


def get_shuffle_partitions_for_input(input_size_bytes: int) -> int:
    """Compute shuffle partitions from input size when dynamic_config.enabled; else use static config."""
    cfg = _load_config()
    dyn = get_dynamic_config()
    if not dyn.get("enabled", True):
        return int(cfg.get("spark", {}).get("shuffle_partitions", 8))
    from src.utils.input_size_estimator import compute_shuffle_partitions

    return compute_shuffle_partitions(
        input_size_bytes,
        target_partition_size_bytes=dyn["target_partition_size_bytes"],
        min_partitions=dyn["min_shuffle_partitions"],
        max_partitions=dyn["max_shuffle_partitions"],
        small_input_threshold_bytes=dyn["small_input_threshold_bytes"],
        small_input_partitions=dyn["small_input_partitions"],
    )


def get_fetch_config() -> Dict[str, Any]:
    """Fetch settings: default_categories, max_rows_per_category (None = no limit)."""
    cfg = _load_config()
    fetch = cfg.get("fetch", {})
    max_rows = fetch.get("max_rows_per_category")
    if max_rows is not None:
        max_rows = int(max_rows)
    return {
        "default_categories": fetch.get("default_categories", ["Gift_Cards"]),
        "max_rows_per_category": max_rows,
    }
