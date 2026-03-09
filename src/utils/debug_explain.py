"""
Debug utility: log df.explain() output when debug flag is enabled.
Helps analyze Catalyst Optimizer plans.
"""

import os
import sys
from io import StringIO
from pathlib import Path
from typing import Optional

from pyspark.sql import DataFrame


def _get_debug_folder() -> Path:
    """Return debug output folder; create if needed."""
    root = Path(__file__).resolve().parent.parent.parent
    debug_dir = root / "data" / "debug"
    data_root = os.getenv("DATA_ROOT")
    if data_root:
        debug_dir = Path(data_root) / "debug"
    debug_dir.mkdir(parents=True, exist_ok=True)
    return debug_dir


def is_debug_enabled() -> bool:
    """Check if debug mode is enabled via env or config."""
    if os.getenv("SPARK_DEBUG", "").lower() in ("1", "true", "yes"):
        return True
    try:
        from src.utils.config_loader import _load_config
        cfg = _load_config()
        return cfg.get("debug", {}).get("explain_enabled", False)
    except Exception:
        return False


def log_explain(
    df: DataFrame,
    label: str = "plan",
    mode: str = "formatted",
) -> Optional[Path]:
    """
    Log df.explain(mode) output to data/debug/ when debug is enabled.
    Returns path to log file, or None if debug disabled.
    """
    if not is_debug_enabled():
        return None
    debug_dir = _get_debug_folder()
    safe_label = "".join(c if c.isalnum() or c in "-_" else "_" for c in label)
    log_path = debug_dir / f"explain_{safe_label}.txt"
    try:
        buf = StringIO()
        old_stdout = sys.stdout
        sys.stdout = buf
        try:
            df.explain(mode)
            output = buf.getvalue()
        finally:
            sys.stdout = old_stdout
        with open(log_path, "w") as f:
            f.write(output)
        return log_path
    except Exception:
        return None
