"""
Pipeline orchestrator for the Amazon Reviews dashboard refresh button.

Steps:
  0. Fetch   — download raw JSONL.gz from McAuley Lab (if none staged)
  1. Bronze  — raw Amazon JSONL.gz -> Delta
  2. Silver  — normalize reviews and join product metadata
  3. Gold    — business analytics tables and optimization

All Spark stages run in-process using the shared SparkSession configuration.
Raw data is auto-fetched from UCSD when none is staged.
"""

from datetime import date, timedelta
from pathlib import Path
from typing import Iterator, Optional


def yesterday() -> str:
    """Return yesterday's date as YYYY-MM-DD (UTC)."""
    return (date.today() - timedelta(days=1)).strftime("%Y-%m-%d")


def _project_root() -> Path:
    return Path(__file__).resolve().parent.parent.parent


def _amazon_raw_root() -> Path:
    """Resolve the expected Amazon raw root from config. Always use config path so Docker volume /data is used."""
    from src.utils.config_loader import get_paths

    paths = get_paths()
    raw_root = Path(paths.get("raw", "/data/raw")) / "amazon"
    raw_root.mkdir(parents=True, exist_ok=True)
    return raw_root


def _count_raw_files(raw_root: Path) -> int:
    """Count staged Amazon JSONL.gz files under the raw root."""
    if not raw_root.exists():
        return 0
    return sum(1 for _ in raw_root.rglob("*.jsonl.gz"))


def run_refresh(target_date: Optional[str] = None) -> Iterator[str]:
    """
    Full refresh: Bronze -> Silver -> Gold for the Amazon Reviews pipeline.

    Yields human-readable progress lines throughout.
    Raises RuntimeError on unrecoverable failure.
    """
    ingest_date = target_date or yesterday()

    yield f"[Orchestrator] Target date: {ingest_date}"
    raw_root = _amazon_raw_root()
    raw_count = _count_raw_files(raw_root)
    yield f"[Orchestrator] Amazon raw root: {raw_root}"
    yield f"[Orchestrator] Staged raw files: {raw_count}"

    if raw_count == 0:
        yield "[Orchestrator] No raw data found. Fetching from McAuley Lab (UCSD)..."
        try:
            import sys
            root = _project_root()
            sys.path.insert(0, str(root / "scripts"))
            from src.utils.config_loader import get_fetch_config
            from fetch_amazon_data import run_fetch
            fetch_cfg = get_fetch_config()
            fetched = run_fetch(
                categories=fetch_cfg.get("default_categories", ["Gift_Cards"]),
                raw_root=raw_root,
                overwrite=False,
                max_rows=fetch_cfg.get("max_rows_per_category"),
            )
            yield f"[Orchestrator] Fetched {fetched} file(s)."
        except Exception as exc:
            raise RuntimeError(
                f"Auto-fetch failed: {exc}. You can manually run: python scripts/fetch_amazon_data.py --categories Gift_Cards"
            ) from exc
        raw_count = _count_raw_files(raw_root)
        if raw_count == 0:
            raise RuntimeError("Fetch completed but no raw files were found.")

    # ── Shared SparkSession ───────────────────────────────────────────────
    yield ""
    yield "─" * 50
    yield "Initialising SparkSession (may take 1–2 min in Docker) …"
    try:
        from src.utils.spark_session import get_spark_session

        spark = get_spark_session("AmazonReviews-Refresh")
        yield "SparkSession ready."
    except Exception as exc:
        raise RuntimeError(f"Failed to create SparkSession: {exc}") from exc

    # ── Step 1: Bronze ───────────────────────────────────────────────────
    yield ""
    yield "─" * 50
    yield f"[1/3] Amazon Bronze ingestion for {ingest_date} …"
    yield "─" * 50
    try:
        from src.jobs.amazon_bronze_ingestion import run as bronze_run

        bronze_run(spark=spark, ingestion_date=ingest_date)
        yield "[1/3] Bronze complete."
    except Exception as exc:
        raise RuntimeError(f"Bronze stage failed: {exc}") from exc

    # ── Step 2: Silver ───────────────────────────────────────────────────
    yield ""
    yield "─" * 50
    yield f"[2/3] Amazon Silver transformation for {ingest_date} …"
    yield "─" * 50
    try:
        from src.jobs.amazon_silver_transformation import run as silver_run

        silver_run(spark=spark, ingestion_date=ingest_date)
        yield "[2/3] Silver complete."
    except Exception as exc:
        raise RuntimeError(f"Silver stage failed: {exc}") from exc

    # ── Step 3: Gold ─────────────────────────────────────────────────────
    yield ""
    yield "─" * 50
    yield f"[3/3] Amazon Gold analytics for {ingest_date} …"
    yield "─" * 50
    try:
        from src.jobs.amazon_gold_analytics import run as gold_run

        gold_run(spark=spark, ingestion_date=ingest_date)
        yield "[3/3] Gold complete."
    except Exception as exc:
        raise RuntimeError(f"Gold stage failed: {exc}") from exc

    yield ""
    yield "─" * 50
    yield f"[Orchestrator] Amazon refresh complete for {ingest_date}."
    yield "─" * 50
