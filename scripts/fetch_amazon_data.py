"""
Fetch Amazon Reviews 2023 raw JSONL.gz files.

Primary: McAuley Lab (UCSD) direct URLs when available.
Fallback: Hugging Face datasets library.
Data source: https://amazon-reviews-2023.github.io/
"""

import argparse
import ast
import gzip
import json
import os
import sys
from pathlib import Path
from typing import List, Optional
from urllib.request import urlretrieve

# McAuley Lab public dataset (reviews_<Cat>.json.gz, meta_<Cat>.json.gz)
# Note: This is the 2018/older format; 2023 JSONL may require Hugging Face
MCALAB_BASE = "https://mcauleylab.ucsd.edu/public_datasets/data/amazon/categoryFiles"

# All 33 categories from Amazon Reviews 2023 (smallest first for quick tests)
ALL_CATEGORIES = [
    "Subscription_Boxes",      # ~16K reviews
    "Magazine_Subscriptions",  # ~71K
    "Digital_Music",          # ~130K
    "Gift_Cards",             # ~152K
    "All_Beauty",             # ~701K
    "Handmade_Products",      # ~664K
    "Health_and_Personal_Care",
    "Amazon_Fashion",
    "Appliances",
    "CDs_and_Vinyl",
    "Musical_Instruments",
    "Software",
    "Video_Games",
    "Arts_Crafts_and_Sewing",
    "Baby_Products",
    "Industrial_and_Scientific",
    "Movies_and_TV",
    "Kindle_Store",
    "Office_Products",
    "Pet_Supplies",
    "Toys_and_Games",
    "Electronics",
    "Grocery_and_Gourmet_Food",
    "Patio_Lawn_and_Garden",
    "Sports_and_Outdoors",
    "Tools_and_Home_Improvement",
    "Cell_Phones_and_Accessories",
    "Health_and_Household",
    "Beauty_and_Personal_Care",
    "Books",
    "Automotive",
    "Clothing_Shoes_and_Jewelry",
    "Home_and_Kitchen",
    "Unknown",
]


def _project_root() -> Path:
    return Path(__file__).resolve().parent.parent


def _raw_root() -> Path:
    """Resolve raw data root: DATA_ROOT env or project data/raw."""
    data_root = os.getenv("DATA_ROOT")
    if data_root:
        return Path(data_root) / "amazon"
    return _project_root() / "data" / "raw" / "amazon"


def _download_and_convert_to_jsonl(
    url: str, dest: Path, overwrite: bool = False, max_rows: Optional[int] = None
) -> bool:
    """
    Download gzipped JSON/JSONL and save as .jsonl.gz.
    Streams to avoid OOM on large files (e.g. Video_Games 347MB).
    Handles both JSON array and JSONL (one JSON per line) formats.
    Maps older Amazon format (overall, reviewText, summary) to 2023 schema.
    """
    dest.parent.mkdir(parents=True, exist_ok=True)
    if dest.exists() and not overwrite:
        print(f"  Skip (exists): {dest.name}")
        return False
    print(f"  Downloading: {dest.name} ...")
    try:
        import tempfile
        tmp_path = Path(tempfile.gettempdir()) / f"amazon_fetch_{os.getpid()}_{dest.stem}.gz"
        urlretrieve(url, tmp_path)
        try:
            is_meta = "meta_" in dest.name
            written = 0

            # Peek at format (array vs JSONL) without loading full file
            with gzip.open(tmp_path, "rt", encoding="utf-8") as f_in:
                first_chunk = f_in.read(1024)

            if first_chunk.strip().startswith("["):
                # JSON array: stream with ijson to avoid loading full file into memory
                import ijson
                with gzip.open(tmp_path, "rb") as f_in:
                    with gzip.open(dest, "wt", encoding="utf-8") as f_out:
                        for obj in ijson.items(f_in, "item"):
                            if max_rows is not None and written >= max_rows:
                                break
                            try:
                                if "overall" in obj:
                                    row = _map_to_2023_review(obj)
                                elif is_meta and "main_category" not in obj:
                                    row = _map_to_2023_meta(obj)
                                else:
                                    row = obj
                                f_out.write(json.dumps(row, default=str) + "\n")
                            except Exception:
                                f_out.write(json.dumps(obj, default=str) + "\n")
                            written += 1
            else:
                # JSONL: stream line by line
                with gzip.open(tmp_path, "rt", encoding="utf-8") as f_in:
                    with gzip.open(dest, "wt", encoding="utf-8") as f_out:
                        for line in f_in:
                            if max_rows is not None and written >= max_rows:
                                break
                            line = line.strip()
                            if not line:
                                continue
                            try:
                                obj = json.loads(line)
                            except json.JSONDecodeError:
                                try:
                                    obj = ast.literal_eval(line)
                                except (ValueError, SyntaxError):
                                    continue
                            if obj is None:
                                continue
                            try:
                                if "overall" in obj:
                                    row = _map_to_2023_review(obj)
                                elif is_meta and "main_category" not in obj:
                                    row = _map_to_2023_meta(obj)
                                else:
                                    row = obj
                                f_out.write(json.dumps(row, default=str) + "\n")
                            except Exception:
                                f_out.write(json.dumps(obj, default=str) + "\n")
                            written += 1
        finally:
            tmp_path.unlink(missing_ok=True)

        if max_rows is not None:
            print(f"  Limiting to {max_rows} rows")

        size_mb = dest.stat().st_size / (1024 * 1024)
        print(f"  Done: {dest.name} ({size_mb:.1f} MB, {written} rows)")
        return True
    except Exception as e:
        print(f"  Download failed: {e}")
        return False


def _map_to_2023_review(obj: dict) -> dict:
    """Map older Amazon format to 2023 schema (rating, title, text, etc.)."""
    return {
        "rating": float(obj.get("overall", 0)),
        "title": obj.get("summary", "") or "",
        "text": obj.get("reviewText", "") or "",
        "asin": obj.get("asin", ""),
        "parent_asin": obj.get("parent_asin") or obj.get("asin", ""),
        "user_id": obj.get("reviewerID", "") or obj.get("user_id", ""),
        "timestamp": int(obj.get("unixReviewTime", 0) or obj.get("timestamp", 0)) * 1000,
        "helpful_vote": int(obj.get("helpful", [0, 0])[0] if isinstance(obj.get("helpful"), list) else obj.get("helpful_vote", 0)),
        "verified_purchase": obj.get("verified_purchase", False),
    }


def _map_to_2023_meta(obj: dict) -> dict:
    """Map older metadata format (asin, categories, description) to 2023 schema."""
    cats = obj.get("categories") or []
    flat_cats = cats[0] if (isinstance(cats, list) and cats and isinstance(cats[0], list)) else (cats if isinstance(cats, list) else [])
    main_cat = flat_cats[0] if flat_cats else ""
    return {
        "main_category": obj.get("main_category") or main_cat,
        "title": obj.get("title", ""),
        "average_rating": float(obj.get("average_rating") or obj.get("avg_rating") or 0),
        "rating_number": int(obj.get("rating_number") or obj.get("num_reviews") or 0),
        "price": str(obj.get("price") or ""),
        "store": obj.get("store", ""),
        "categories": flat_cats if isinstance(flat_cats, list) else [],
        "parent_asin": obj.get("parent_asin") or obj.get("asin", ""),
    }


def fetch_categories(
    categories: List[str],
    raw_root: Path,
    overwrite: bool = False,
    max_rows: Optional[int] = None,
) -> int:
    """Download review and metadata files for each category. Returns count of files downloaded."""
    reviews_dir = raw_root / "reviews"
    metadata_dir = raw_root / "metadata"
    downloaded = 0

    for cat in categories:
        if cat not in ALL_CATEGORIES:
            print(f"Unknown category: {cat} (skipping)")
            continue
        print(f"\n[{cat}]")
        review_dest = reviews_dir / f"{cat}.jsonl.gz"
        meta_dest = metadata_dir / f"meta_{cat}.jsonl.gz"
        if review_dest.exists() and meta_dest.exists() and not overwrite:
            print(f"  Skip (both exist)")
            continue
        if overwrite:
            review_dest.unlink(missing_ok=True)
            meta_dest.unlink(missing_ok=True)

        # McAuley Lab categoryFiles (reviews_<Cat>.json.gz, meta_<Cat>.json.gz)
        review_url = f"{MCALAB_BASE}/reviews_{cat}.json.gz"
        meta_url = f"{MCALAB_BASE}/meta_{cat}.json.gz"
        if _download_and_convert_to_jsonl(review_url, review_dest, overwrite, max_rows=max_rows):
            downloaded += 1
        if _download_and_convert_to_jsonl(meta_url, meta_dest, overwrite, max_rows=max_rows):
            downloaded += 1

    return downloaded


def run_fetch(
    categories: Optional[List[str]] = None,
    raw_root: Optional[Path] = None,
    overwrite: bool = False,
    max_rows: Optional[int] = None,
    use_config_if_none: bool = True,
) -> int:
    """
    Programmatic fetch: download Amazon Reviews 2023 data.
    Returns count of files downloaded.
    If max_rows is None and use_config_if_none, reads from config fetch.max_rows_per_category.
    If max_rows is None and not use_config_if_none, no limit (full download).
    """
    raw_root = raw_root or _raw_root()
    categories = categories or ["Gift_Cards"]
    if len(categories) == 1 and categories[0].lower() == "all":
        categories = ALL_CATEGORIES
    if max_rows is None and use_config_if_none:
        try:
            from src.utils.config_loader import get_fetch_config
            max_rows = get_fetch_config().get("max_rows_per_category")
        except Exception:
            pass
    return fetch_categories(categories, raw_root, overwrite=overwrite, max_rows=max_rows)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Fetch Amazon Reviews 2023 raw JSONL.gz from McAuley Lab (UCSD)"
    )
    parser.add_argument(
        "--categories",
        nargs="+",
        default=["Gift_Cards"],
        help="Categories to fetch (default: Gift_Cards). Use 'all' for all 33 categories.",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Overwrite existing files",
    )
    parser.add_argument(
        "--raw-dir",
        type=Path,
        default=None,
        help="Override raw data directory (default: data/raw/amazon)",
    )
    parser.add_argument(
        "--max-rows",
        type=int,
        default=None,
        metavar="N",
        help="Limit rows per category (default: from config fetch.max_rows_per_category, or no limit)",
    )
    args = parser.parse_args()

    raw_root = args.raw_dir or _raw_root()
    print(f"Raw data root: {raw_root}")

    categories = args.categories
    if len(categories) == 1 and categories[0].lower() == "all":
        categories = ALL_CATEGORIES
        print(f"Fetching all {len(categories)} categories (this may take a while)")

    max_rows = args.max_rows
    if max_rows is None and os.getenv("FETCH_MAX_ROWS"):
        try:
            max_rows = int(os.getenv("FETCH_MAX_ROWS", "0"))
        except ValueError:
            max_rows = None
    if max_rows is None:
        try:
            sys.path.insert(0, str(_project_root()))
            from src.utils.config_loader import get_fetch_config
            max_rows = get_fetch_config().get("max_rows_per_category")
        except Exception:
            pass
    if max_rows is not None:
        print(f"Max rows per category: {max_rows}")

    downloaded = fetch_categories(
        categories, raw_root, overwrite=args.overwrite, max_rows=max_rows
    )
    print(f"\nTotal files downloaded: {downloaded}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
