"""
Debug script: quantitative comparison across Raw, Bronze, Silver, and Gold.
Investigates data loss between pipeline stages. Supports per-category audit.

Usage (Docker - recommended if local Spark fails):
  docker compose -f docker/docker-compose.yml run --rm dashboard python3 scripts/debug_layer_counts.py --category Gift_Cards
  docker compose -f docker/docker-compose.yml run --rm dashboard python3 scripts/debug_layer_counts.py

Usage (local):
  PYTHONPATH=. python scripts/debug_layer_counts.py --category Gift_Cards   # bash
  $env:PYTHONPATH="."; python scripts/debug_layer_counts.py --category Gift_Cards   # PowerShell
"""

import argparse
import sys
from pathlib import Path

# Ensure project root is on path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from pyspark.sql import functions as F


def _raw_count(spark, raw_path: Path, category: str = None) -> dict:
    """Step 1: Count lines in raw .jsonl.gz files (reviews only)."""
    reviews_path = raw_path / "amazon" / "reviews"
    if not reviews_path.exists():
        return {"total": 0, "by_category": [], "note": "Path does not exist"}

    text_df = (
        spark.read
        .option("recursiveFileLookup", "true")
        .option("pathGlobFilter", "*.jsonl.gz")
        .text(str(reviews_path))
    )
    # Extract category from filename: .../Gift_Cards.jsonl.gz -> Gift_Cards
    text_df = text_df.withColumn(
        "category",
        F.regexp_extract(F.input_file_name(), r"([^/\\]+)\.jsonl\.gz$", 1),
    )
    if category:
        text_df = text_df.filter(F.col("category") == category)

    by_cat = text_df.groupBy("category").count().orderBy(F.desc("count")).collect()
    total = sum(r["count"] for r in by_cat)
    breakdown = [(r["category"], r["count"]) for r in by_cat]

    return {"total": total, "by_category": breakdown}


def _bronze_count(spark, bronze_path: Path, category: str = None) -> dict:
    """Step 2: Count Bronze Delta table, group by category and ingestion_date."""
    reviews_path = bronze_path / "amazon_reviews" / "reviews"
    if not (reviews_path / "_delta_log").exists():
        return {"total": 0, "by_category_date": [], "note": "Delta table not found"}

    df = spark.read.format("delta").load(str(reviews_path))
    if category:
        df = df.filter(F.col("category") == category)

    total = df.count()
    by_cat_date = (
        df.groupBy("category", "ingestion_date")
        .count()
        .orderBy(F.desc("count"))
        .collect()
    )
    breakdown = [(r["category"], str(r["ingestion_date"]), r["count"]) for r in by_cat_date]

    return {"total": total, "by_category_date": breakdown}


def _silver_count(spark, silver_path: Path, category: str = None) -> dict:
    """Step 3: Count Silver Delta table, group by category."""
    silver_table = silver_path / "amazon_reviews"
    if not (silver_table / "_delta_log").exists():
        return {"total": 0, "by_category": [], "note": "Delta table not found"}

    df = spark.read.format("delta").load(str(silver_table))
    if category:
        df = df.filter(F.col("category") == category)

    total = df.count()
    by_cat = (
        df.groupBy("category")
        .count()
        .orderBy(F.desc("count"))
        .collect()
    )
    breakdown = [(r["category"], r["count"]) for r in by_cat]

    return {"total": total, "by_category": breakdown}


def _gold_count(spark, gold_path: Path, category: str = None) -> dict:
    """Step 4: Count Gold analytics tables (aggregated, not 1:1 with reviews)."""
    root = gold_path / "amazon_reviews"
    result = {}

    for name in ["product_metrics", "category_trends", "verified_purchase_impact"]:
        path = root / name
        if (path / "_delta_log").exists():
            df = spark.read.format("delta").load(str(path))
            if category:
                df = df.filter(F.col("category") == category)
            result[name] = df.count()
        else:
            result[name] = 0

    return result


def _investigate_filters():
    """Check fetch.max_rows_per_category and filter logic."""
    from src.utils.config_loader import get_fetch_config

    fetch = get_fetch_config()
    max_rows = fetch.get("max_rows_per_category")
    return {
        "max_rows_per_category": max_rows,
        "effect": (
            f"Raw files are truncated to {max_rows} rows per category at fetch time"
            if max_rows
            else "No row limit (full download)"
        ),
    }


def _get_categories_to_audit(spark, bronze_path: Path, category_arg: str = None) -> list:
    """Get list of categories to audit: from Bronze if no --category, else [category_arg]."""
    if category_arg:
        return [category_arg]

    reviews_path = bronze_path / "amazon_reviews" / "reviews"
    if not (reviews_path / "_delta_log").exists():
        return []

    df = spark.read.format("delta").load(str(reviews_path))
    return [r["category"] for r in df.select("category").distinct().collect() if r["category"]]


def _print_summary(raw: dict, bronze: dict, silver: dict, gold: dict, category: str = None):
    """Print summary table for one or all categories."""
    label = f" [{category}]" if category else ""
    print(f"\n| Stage  | Count     | Notes{label:30} |")
    print(f"|--------|-----------|--------------------------------|")
    print(f"| Raw    | {raw['total']:>9,} | Lines in reviews/*.jsonl.gz     |")
    print(f"| Bronze | {bronze['total']:>9,} | Delta reviews table              |")
    print(f"| Silver | {silver['total']:>9,} | Delta amazon_reviews             |")
    print(f"| Gold   | (see below) | Aggregated metrics, not 1:1   |")


def _print_dropoff(raw_total: int, bronze_total: int, silver_total: int):
    """Print drop-off analysis."""
    if raw_total > 0:
        drop = raw_total - bronze_total
        print(f"\nRaw → Bronze drop: {drop:,} rows")
        if drop > 0:
            print("  Possible causes: category regex (category != ''), schema parse failures")
    if bronze_total > 0:
        drop = bronze_total - silver_total
        print(f"Bronze → Silver drop: {drop:,} rows")
        if drop > 0:
            print("  Possible causes: dropDuplicates(review_id), _apply_filters (category/ingestion_date)")


def main():
    parser = argparse.ArgumentParser(
        description="Debug layer counts: Raw, Bronze, Silver, Gold (optionally per category)"
    )
    parser.add_argument(
        "--category",
        type=str,
        default=None,
        help="Run for a single category (e.g. Gift_Cards). Omit for all categories.",
    )
    args = parser.parse_args()

    from src.utils.config_loader import get_paths
    from src.utils.spark_session import get_spark_session

    paths = get_paths()
    raw_root = Path(paths["raw"])
    bronze_root = Path(paths["bronze"])
    silver_root = Path(paths["silver"])
    gold_root = Path(paths["gold"])

    spark = get_spark_session("DebugLayerCounts")

    categories = _get_categories_to_audit(spark, bronze_root, args.category)
    if args.category:
        bronze_cats = _get_categories_to_audit(spark, bronze_root, None)
        if bronze_cats and args.category not in bronze_cats:
            print(f"Warning: '{args.category}' not in Bronze. Will still count raw/silver/gold.")

    # Fetch config (printed once)
    fetch_info = _investigate_filters()

    print("\n" + "=" * 60)
    print("LAYER ROW COUNT AUDIT")
    print("=" * 60)
    print(f"\nfetch.max_rows_per_category: {fetch_info['max_rows_per_category']}")
    print(f"  → {fetch_info['effect']}")

    for cat in categories:
        raw = _raw_count(spark, raw_root, cat)
        bronze = _bronze_count(spark, bronze_root, cat)
        silver = _silver_count(spark, silver_root, cat)
        gold = _gold_count(spark, gold_root, cat)

        print("\n" + "-" * 60)
        print(f"CATEGORY: {cat}")
        print("-" * 60)
        _print_summary(raw, bronze, silver, gold, cat)

        print("\nGold tables:")
        for name, cnt in gold.items():
            print(f"  - {name}: {cnt:,}")

        _print_dropoff(raw["total"], bronze["total"], silver["total"])

        if bronze.get("by_category_date"):
            print("\nBronze by (category, ingestion_date):")
            for r in bronze["by_category_date"][:10]:
                print(f"  {r[0]} | {r[1]}: {r[2]:,}")
            if len(bronze["by_category_date"]) > 10:
                print(f"  ... and {len(bronze['by_category_date']) - 10} more")

    # If no categories (e.g. Bronze empty), still run totals
    if not categories:
        raw = _raw_count(spark, raw_root)
        bronze = _bronze_count(spark, bronze_root)
        silver = _silver_count(spark, silver_root)
        gold = _gold_count(spark, gold_root)
        print("\n" + "-" * 60)
        print("ALL (no category filter)")
        print("-" * 60)
        _print_summary(raw, bronze, silver, gold)
        print("\nGold tables:")
        for name, cnt in gold.items():
            print(f"  - {name}: {cnt:,}")
        _print_dropoff(raw["total"], bronze["total"], silver["total"])

    print("\n" + "=" * 60)


if __name__ == "__main__":
    main()
