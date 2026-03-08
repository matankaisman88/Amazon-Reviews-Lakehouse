"""
Natural Language to Spark SQL query helper for the Amazon Reviews lakehouse.

Translates user questions into Spark SQL, executes them against the Amazon
Gold Delta tables via spark.sql(), and returns the generated SQL, results,
and explanation in one round trip.
"""

import json
import os
import re
from pathlib import Path
from datetime import date, timedelta
from typing import Any, Dict, List, Optional

import pandas as pd

_PRODUCT_METRICS_DDL = """\
CREATE TABLE product_metrics (
    parent_asin            STRING  NOT NULL
   ,category               STRING  NOT NULL
   ,review_date            DATE    NOT NULL
   ,total_reviews          BIGINT  NOT NULL
   ,average_rating         DOUBLE  NOT NULL
   ,rolling_30d_avg_rating DOUBLE  NOT NULL
   ,avg_price              DOUBLE
) USING DELTA
PARTITIONED BY (category, review_date);"""

_CATEGORY_TRENDS_DDL = """\
CREATE TABLE category_trends (
    category            STRING  NOT NULL
   ,review_date         DATE    NOT NULL
   ,daily_review_count  BIGINT  NOT NULL
   ,daily_avg_rating    DOUBLE  NOT NULL
   ,count_1_star        BIGINT
   ,count_2_star        BIGINT
   ,count_3_star        BIGINT
   ,count_4_star        BIGINT
   ,count_5_star        BIGINT
) USING DELTA
PARTITIONED BY (category, review_date);"""

_VERIFIED_IMPACT_DDL = """\
CREATE TABLE verified_purchase_impact (
    category             STRING   NOT NULL
   ,review_date          DATE     NOT NULL
   ,verified_purchase    BOOLEAN  NOT NULL
   ,daily_review_count   BIGINT   NOT NULL
   ,avg_rating           DOUBLE   NOT NULL
) USING DELTA
PARTITIONED BY (category, review_date);"""

_SYSTEM_PROMPT = f"""You are a Spark SQL expert embedded in an Amazon Reviews analytics lakehouse.

## Available Tables

### product_metrics — Product-level daily performance with rolling ratings
{_PRODUCT_METRICS_DDL}

### category_trends — Daily category-level review trends
{_CATEGORY_TRENDS_DDL}

### verified_purchase_impact — Daily trust analysis by verified purchase flag
{_VERIFIED_IMPACT_DDL}

## Partitioning And Performance

All three tables are partitioned by `category` and `review_date`.

You MUST:
1. Always end every query with `LIMIT 100`.
2. Filter by `category` and a concrete `review_date` or date range whenever the question allows it.
3. Avoid unbounded scans, cross joins, and unnecessary self joins.
4. Prefer Gold tables only; do not invent Silver or Bronze table names.

## Semantics

- `review_date` is already a DATE column. Do not perform timestamp conversion.
- Use `product_metrics` for product-level questions, cumulative totals, and 30-day rolling rating analysis.
- Use `category_trends` for daily category review counts and category average ratings.
- Use `verified_purchase_impact` for comparing verified vs. non-verified review behavior.
- `verified_purchase` is a BOOLEAN column.

## Clarification Rules

- If a product-specific question needs a product identifier and none is provided, ask the user for `parent_asin`.
- If a category-level question does not specify a category, ask the user to provide one unless the question explicitly asks for all categories.
- If the time period is missing, ask for a date or date range unless the question explicitly asks for an all-time result.
- Resolve relative dates like yesterday, today, and last 7 days using the temporal context below.

## Example Requests

- Show me top categories by average rating over the last 7 days.
- Find products with declining 30-day rolling averages in Electronics during the last month.
- Compare verified vs non-verified ratings for Gift_Cards last week.
- Which products had the most reviews in Books on 2020-05-05?

## Safety Rules

NEVER generate DROP, DELETE, UPDATE, INSERT, CREATE, ALTER, TRUNCATE, or MERGE.
Only read-only SELECT queries are allowed. CTEs using `WITH` are allowed if the final statement is a SELECT.

## Response Format

Reply with ONLY a valid JSON object — no markdown fences, no extra keys:
{{
  "sql":         "<complete Spark SQL read-only query>",
  "explanation": "<1–2 sentences describing what the query does and what its results reveal>",
  "error":       "<optional clarification message if required inputs are missing>"
}}
"""

_FORBIDDEN_PATTERN = re.compile(
    r"\b(DROP|DELETE|UPDATE|INSERT|CREATE|ALTER|TRUNCATE|MERGE)\b",
    re.IGNORECASE,
)
_LIMIT_PATTERN = re.compile(r"\bLIMIT\s+(\d+)", re.IGNORECASE)
_QUERY_HARD_CAP = 100


def _build_temporal_context() -> str:
    """Build the temporal context block with today's date for resolving relative terms."""
    today = date.today()
    yesterday = (today - timedelta(days=1)).strftime("%Y-%m-%d")
    today_str = today.strftime("%Y-%m-%d")
    week_ago = (today - timedelta(days=7)).strftime("%Y-%m-%d")
    month_ago = (today - timedelta(days=30)).strftime("%Y-%m-%d")
    return f"""

## Temporal Context

**Current date:** {today_str}

| User says      | Use in SQL (YYYY-MM-DD)                                  |
|----------------|-----------------------------------------------------------|
| yesterday      | {yesterday}                                               |
| today          | {today_str}                                               |
| last 7 days    | review_date BETWEEN '{week_ago}' AND '{yesterday}'        |
| last month     | review_date BETWEEN '{month_ago}' AND '{yesterday}'       |
"""


def _enforce_limit(sql: str, cap: int = _QUERY_HARD_CAP) -> str:
    """Ensure a LIMIT clause is present and does not exceed *cap*."""
    stripped = sql.strip().rstrip(";")
    match = _LIMIT_PATTERN.search(stripped)
    if match:
        if int(match.group(1)) > cap:
            stripped = _LIMIT_PATTERN.sub(f"LIMIT {cap}", stripped)
    else:
        stripped += f"\nLIMIT {cap}"
    return stripped


def _validate_sql(sql: str) -> None:
    """Raise ValueError if the query is not a read-only SELECT/CTE SELECT."""
    normalized = sql.strip()
    if _FORBIDDEN_PATTERN.search(normalized):
        raise ValueError(
            "Generated SQL contains a forbidden statement (DROP / DELETE / UPDATE / etc.). "
            "Only read-only queries are permitted."
        )

    upper = normalized.upper()
    if not (upper.startswith("SELECT") or upper.startswith("WITH")):
        raise ValueError("Generated SQL must start with SELECT or WITH.")


def _register_delta_views(spark) -> None:  # type: ignore[no-untyped-def]
    """Register the Amazon Gold Delta tables as temp views."""
    from src.utils.config_loader import get_paths

    paths = get_paths()
    gold_root = paths.get("gold")
    if not gold_root:
        return

    amazon_gold_root = Path(gold_root) / "amazon_reviews"
    table_paths = {
        "product_metrics": amazon_gold_root / "product_metrics",
        "category_trends": amazon_gold_root / "category_trends",
        "verified_purchase_impact": amazon_gold_root / "verified_purchase_impact",
    }

    for view_name, table_path in table_paths.items():
        if table_path.exists():
            spark.read.format("delta").load(str(table_path)).createOrReplaceTempView(view_name)


class AIQueryHelper:
    """
    Translate a natural-language Amazon analytics question to Spark SQL and execute it.

    Usage::

        helper = AIQueryHelper()
        result = helper.query("Show me top categories by average rating last week")
        print(result["sql"])
        print(result["dataframe"])
    """

    def __init__(self, api_key: Optional[str] = None) -> None:
        from openai import OpenAI

        key = api_key or os.getenv("OPENAI_API_KEY")
        if not key:
            raise ValueError(
                "OPENAI_API_KEY is not set. Add it to your .env file or export it "
                "in the shell before starting the dashboard."
            )
        self._client = OpenAI(api_key=key)
        self._model = os.getenv("OPENAI_MODEL", "gpt-4o-mini")

    def query(
        self,
        question: str,
        conversation_history: Optional[List[Dict[str, str]]] = None,
    ) -> Dict[str, Any]:
        """Full round-trip: NL question -> LLM -> SQL -> Spark execution -> result."""
        sql = ""
        explanation = ""

        system_content = _SYSTEM_PROMPT + _build_temporal_context()
        messages = [{"role": "system", "content": system_content}]
        if conversation_history:
            for msg in conversation_history:
                role = msg.get("role")
                content = msg.get("content", "")
                if role in ("user", "assistant") and content:
                    messages.append({"role": role, "content": content})
        messages.append({"role": "user", "content": question})

        try:
            response = self._client.chat.completions.create(
                model=self._model,
                messages=messages,
                temperature=0,
                response_format={"type": "json_object"},
            )
            raw_content = response.choices[0].message.content
            parsed = json.loads(raw_content)

            sql = parsed.get("sql", "").strip()
            explanation = parsed.get("explanation", "")
            llm_error = parsed.get("error", "").strip()

            if llm_error:
                return {
                    "sql": "",
                    "explanation": explanation,
                    "dataframe": pd.DataFrame(),
                    "error": llm_error,
                }

            _validate_sql(sql)
            sql = _enforce_limit(sql)

            from src.utils.spark_session import get_spark_session

            spark = get_spark_session(app_name="AmazonReviews-AIQuery")
            _register_delta_views(spark)
            result_pdf = spark.sql(sql).toPandas()

            return {
                "sql": sql,
                "explanation": explanation,
                "dataframe": result_pdf,
                "error": None,
            }

        except Exception as exc:  # noqa: BLE001
            return {
                "sql": sql,
                "explanation": explanation,
                "dataframe": pd.DataFrame(),
                "error": str(exc),
            }

    def summarize_declining_product(
        self,
        parent_asin: str,
        category: str,
        max_reviews: int = 50,
        max_chars_per_review: int = 500,
    ) -> Dict[str, Any]:
        """
        Fetch recent review texts from Silver for a product and use LLM to summarize
        potential root causes for rating decline.

        Returns dict with keys: summary, error (or None).
        """
        from src.utils.config_loader import get_paths

        paths = get_paths()
        silver_root = paths.get("silver")
        if not silver_root:
            return {"summary": "", "error": "Silver path not configured."}

        silver_path = Path(silver_root) / "amazon_reviews"
        if not silver_path.exists():
            return {"summary": "", "error": "Silver table not found."}

        try:
            from pyspark.sql import functions as F
            from src.utils.spark_session import get_spark_session

            spark = get_spark_session(app_name="AmazonReviews-AIRootCause")
            silver_df = spark.read.format("delta").load(str(silver_path))
            reviews_df = (
                silver_df.filter(
                    (F.col("parent_asin") == parent_asin) & (F.col("category") == category)
                )
                .select("review_text", "rating")
                .orderBy(F.col("review_timestamp").desc())
                .limit(max_reviews)
            )
            rows = reviews_df.collect()
        except Exception as exc:  # noqa: BLE001
            return {"summary": "", "error": str(exc)}

        if not rows:
            return {"summary": "", "error": "No reviews found for this product."}

        # Build context for LLM (truncate long reviews)
        review_texts = []
        for i, row in enumerate(rows):
            text = (row.review_text or "").strip()
            if len(text) > max_chars_per_review:
                text = text[:max_chars_per_review] + "..."
            if text:
                review_texts.append(f"[Rating: {row.rating}] {text}")

        if not review_texts:
            return {"summary": "", "error": "No review text content available."}

        reviews_block = "\n\n---\n\n".join(review_texts[:30])  # Limit to 30 for token budget

        prompt = f"""A product (ASIN: {parent_asin}) in category "{category}" has shown a declining average rating.

Below are recent customer reviews (rating and text). Analyze them and provide a concise root-cause summary:

1. What common complaints or themes appear?
2. What might explain the rating decline?
3. Any actionable insights for the seller?

Keep the response to 3-5 bullet points, under 300 words.

REVIEWS:
{reviews_block}
"""

        try:
            response = self._client.chat.completions.create(
                model=self._model,
                messages=[{"role": "user", "content": prompt}],
                temperature=0.3,
            )
            summary = response.choices[0].message.content or ""
            return {"summary": summary, "error": None}
        except Exception as exc:  # noqa: BLE001
            return {"summary": "", "error": str(exc)}
