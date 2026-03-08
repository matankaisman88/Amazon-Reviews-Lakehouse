"""Create minimal synthetic Amazon Reviews 2023 sample for Bronze ingestion tests."""

import gzip
import json
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
RAW_REVIEWS = PROJECT_ROOT / "data" / "raw" / "amazon" / "reviews"
RAW_METADATA = PROJECT_ROOT / "data" / "raw" / "amazon" / "metadata"


def main():
    RAW_REVIEWS.mkdir(parents=True, exist_ok=True)
    RAW_METADATA.mkdir(parents=True, exist_ok=True)

    reviews = [
        {
            "rating": 5.0,
            "title": "Great product",
            "text": "Works as expected.",
            "asin": "B001",
            "parent_asin": "B001",
            "user_id": "USER1",
            "timestamp": 1588687728923,
            "helpful_vote": 2,
            "verified_purchase": True,
        },
        {
            "rating": 4.0,
            "title": "Good value",
            "text": "Solid build.",
            "asin": "B002",
            "parent_asin": "B002",
            "user_id": "USER2",
            "timestamp": 1588690000000,
            "helpful_vote": 0,
            "verified_purchase": False,
        },
    ]

    metadata = [
        {
            "main_category": "Gift_Cards",
            "title": "Test Gift Card",
            "average_rating": 4.5,
            "rating_number": 100,
            "price": "25.00",
            "store": "TestStore",
            "categories": ["Gift Cards", "Prepaid"],
            "parent_asin": "B001",
        },
        {
            "main_category": "Gift_Cards",
            "title": "Another Card",
            "average_rating": 4.0,
            "rating_number": 50,
            "price": "50.00",
            "store": "TestStore",
            "categories": ["Gift Cards"],
            "parent_asin": "B002",
        },
    ]

    with gzip.open(RAW_REVIEWS / "Gift_Cards.jsonl.gz", "wt", encoding="utf-8") as f:
        for r in reviews:
            f.write(json.dumps(r) + "\n")

    with gzip.open(RAW_METADATA / "meta_Gift_Cards.jsonl.gz", "wt", encoding="utf-8") as f:
        for m in metadata:
            f.write(json.dumps(m) + "\n")

    print(f"Created {RAW_REVIEWS / 'Gift_Cards.jsonl.gz'}")
    print(f"Created {RAW_METADATA / 'meta_Gift_Cards.jsonl.gz'}")


if __name__ == "__main__":
    main()
