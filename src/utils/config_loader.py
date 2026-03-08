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
