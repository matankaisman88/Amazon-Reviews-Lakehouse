"""
Amazon Reviews BI Dashboard - High-value analytics with cross-category comparisons,
anomaly detection, value-for-money analysis, and AI-powered root-cause insights.
"""

import html
import os
from datetime import date, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
from deltalake import DeltaTable

from src.utils.config_loader import get_paths

AMAZON_ROOT = "amazon_reviews"

_CSS = """
<style>
header[data-testid="stHeader"] { display: none; }
#MainMenu { visibility: hidden; }
footer { visibility: hidden; }
.block-container { padding-top: 1.25rem; }
.user-msg-bubble {
    background: #334155;
    color: #f8fafc;
    padding: 0.75rem 1rem;
    border-radius: 12px;
    display: inline-block;
    max-width: 85%;
}
</style>
"""


def _gold_table_path(table_name: str) -> Path:
    """Return the nested Amazon Gold table path for the requested table."""
    paths = get_paths()
    gold_root = paths.get("gold")
    if not gold_root:
        raise RuntimeError("Gold path not found in config paths.")
    return Path(gold_root) / AMAZON_ROOT / table_name


def _date_range(start: date, end: date) -> List[str]:
    """Generate date strings from start to end inclusive."""
    dates = []
    current = start
    while current <= end:
        dates.append(current.strftime("%Y-%m-%d"))
        current += timedelta(days=1)
    return dates


def _normalize_review_date(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize review_date columns to Python date values."""
    if "review_date" in df.columns:
        df = df.copy()
        df["review_date"] = pd.to_datetime(df["review_date"]).dt.date
    return df


@st.cache_data(show_spinner=False)
def load_dashboard_metadata() -> Tuple[List[str], Optional[date], Optional[date]]:
    """Load category list and date bounds from the Amazon category trends table (no Spark)."""
    category_trends_path = _gold_table_path("category_trends")
    if not category_trends_path.exists():
        return [], None, None

    table = DeltaTable(str(category_trends_path))
    df = table.to_pandas()
    if df.empty:
        return [], None, None

    min_date = pd.Timestamp(df["review_date"].min()).date()
    max_date = pd.Timestamp(df["review_date"].max()).date()
    categories = sorted(df["category"].dropna().unique().tolist())
    return categories, min_date, max_date


@st.cache_data(show_spinner=False)
def load_global_category_leaderboard(start_date: date, end_date: date) -> pd.DataFrame:
    """Load aggregated category leaderboard (total reviews, weighted avg rating) via Spark."""
    from pyspark.sql import functions as F
    from src.utils.spark_session import get_spark_session

    category_trends_path = _gold_table_path("category_trends")
    if not category_trends_path.exists():
        return pd.DataFrame()

    spark = get_spark_session("AmazonDashboardLeaderboard")
    trends = spark.read.format("delta").load(str(category_trends_path))
    trends = trends.filter(
        (F.col("review_date") >= F.lit(start_date)) & (F.col("review_date") <= F.lit(end_date))
    )

    sum_count = F.sum("daily_review_count")
    sum_weighted = F.sum(F.col("daily_review_count") * F.col("daily_avg_rating"))
    leaderboard = (
        trends.groupBy("category")
        .agg(
            sum_count.alias("total_reviews"),
            F.when(sum_count > 0, sum_weighted / sum_count).otherwise(None).alias("weighted_avg_rating"),
        )
        .orderBy(F.desc("total_reviews"))
    )
    return leaderboard.toPandas()


@st.cache_data(show_spinner=False)
def load_category_trends(category: str, start_date: date, end_date: date) -> pd.DataFrame:
    """Load category trend metrics with partition pruning."""
    table = DeltaTable(str(_gold_table_path("category_trends")))
    df = table.to_pandas(
        partitions=[
            ("category", "=", category),
            ("review_date", "in", _date_range(start_date, end_date)),
        ]
    )
    return _normalize_review_date(df).sort_values("review_date")


@st.cache_data(show_spinner=False)
def load_product_metrics(category: str, start_date: date, end_date: date) -> pd.DataFrame:
    """Load product metrics with partition pruning."""
    table = DeltaTable(str(_gold_table_path("product_metrics")))
    df = table.to_pandas(
        partitions=[
            ("category", "=", category),
            ("review_date", "in", _date_range(start_date, end_date)),
        ]
    )
    return _normalize_review_date(df).sort_values(["parent_asin", "review_date"])


@st.cache_data(show_spinner=False)
def load_verified_purchase_impact(category: str, start_date: date, end_date: date) -> pd.DataFrame:
    """Load verified purchase metrics with partition pruning."""
    table = DeltaTable(str(_gold_table_path("verified_purchase_impact")))
    df = table.to_pandas(
        partitions=[
            ("category", "=", category),
            ("review_date", "in", _date_range(start_date, end_date)),
        ]
    )
    return _normalize_review_date(df).sort_values(["review_date", "verified_purchase"])


@st.cache_resource(show_spinner=False)
def _get_ai_helper():
    """Instantiate AIQueryHelper once per Streamlit session."""
    from src.utils.ai_query_helper import AIQueryHelper

    return AIQueryHelper()


def _format_rating(value: float) -> str:
    """Format rating values consistently."""
    return f"{value:.2f}"


def _run_refresh_flow(
    category: str,
    max_rows: Optional[int] = None,
    overwrite_raw: bool = False,
    skip_optimize: bool = False,
) -> None:
    """Execute the Amazon pipeline for category in a subprocess."""
    import subprocess
    import sys

    log_lines: List[str] = []
    status_placeholder = st.sidebar.empty()
    failed = False

    script_path = Path(__file__).resolve().parent.parent.parent / "scripts" / "run_refresh_standalone.py"
    if not script_path.exists():
        st.sidebar.error(f"Refresh script not found: {script_path}")
        return

    st.sidebar.caption(
        "Spark startup can take 1–2 minutes in Docker. The page will update when the pipeline finishes."
    )

    cmd = [sys.executable, str(script_path), f"--category={category}"]
    if max_rows is not None:
        cmd.append(f"--max-rows={max_rows}")
    else:
        # Unlimited: pass 0 so orchestrator uses no limit (not config default)
        cmd.append("--max-rows=0")
    if overwrite_raw:
        cmd.append("--overwrite-raw")
    if skip_optimize:
        cmd.append("--skip-optimize")

    try:
        with st.spinner(f"Running pipeline for {category}..."):
            proc = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                bufsize=1,
                cwd=str(script_path.parent.parent),
                env={**os.environ, "PYTHONPATH": str(script_path.parent.parent)},
            )
            for line in iter(proc.stdout.readline, ""):
                line = line.rstrip()
                if line:
                    log_lines.append(line)
                    status_placeholder.caption(line)
            proc.wait()
            if proc.returncode != 0:
                raise RuntimeError(f"Pipeline exited with code {proc.returncode}")

        status_placeholder.empty()
        st.sidebar.success(f"Pipeline finished for {category}.")
        st.cache_data.clear()
        st.cache_resource.clear()
        st.rerun()

    except RuntimeError as exc:
        status_placeholder.empty()
        st.sidebar.error(f"Pipeline failed: {exc}")
        if log_lines:
            log_lines.append(f"Error: {exc}")
        failed = True
    except Exception as exc:
        status_placeholder.empty()
        st.sidebar.error(f"Pipeline failed: {exc}")
        if log_lines:
            log_lines.append(f"Error: {exc}")
        failed = True

    if log_lines:
        with st.sidebar.expander("View pipeline log", expanded=failed):
            st.code("\n".join(log_lines), language=None)


def _pipeline_categories() -> List[str]:
    """Categories available for pipeline (from fetch script)."""
    import sys
    root = Path(__file__).resolve().parent.parent.parent
    sys.path.insert(0, str(root / "scripts"))
    try:
        from fetch_amazon_data import ALL_CATEGORIES
        return ALL_CATEGORIES
    except Exception:
        return ["Gift_Cards", "All_Beauty", "Books", "Electronics", "Digital_Music"]


def _render_refresh_button() -> None:
    """Render the Amazon Bronze -> Silver -> Gold pipeline controls."""
    st.sidebar.header("DATA MANAGEMENT")
    st.sidebar.caption(
        "Run the Medallion pipeline for a category. Fetches raw data if needed. "
        "Source data is static (up to 2023)."
    )

    pipeline_cats = _pipeline_categories()
    pipeline_category = st.sidebar.selectbox(
        "Category to process",
        options=pipeline_cats,
        index=pipeline_cats.index("Gift_Cards") if "Gift_Cards" in pipeline_cats else 0,
        key="pipeline_category",
    )

    with st.sidebar.expander("Fetch options", expanded=True):
        unlimited_rows = st.checkbox("Unlimited rows", value=False, key="fetch_unlimited")
        if unlimited_rows:
            fetch_max_rows: Optional[int] = None
        else:
            fetch_max_rows = st.number_input(
                "Max rows per category",
                min_value=1,
                value=10000,
                step=1000,
                key="fetch_max_rows",
            )
        overwrite_raw = st.checkbox(
            "Force re-fetch (overwrite existing raw files)",
            value=False,
            key="fetch_overwrite",
            help="Re-download even if raw files exist. Use with Unlimited to replace a limited fetch.",
        )
        skip_optimize = st.checkbox(
            "Skip Gold OPTIMIZE (faster runs)",
            value=False,
            key="skip_optimize",
            help="Skip Gold OPTIMIZE/Z-ORDER. Use for faster backfills; small tables are often skipped by config anyway.",
        )

    if st.sidebar.button("Run Pipeline", type="primary", key="run_pipeline", use_container_width=True):
        _run_refresh_flow(
            pipeline_category,
            max_rows=fetch_max_rows,
            overwrite_raw=overwrite_raw,
            skip_optimize=skip_optimize,
        )

    st.sidebar.divider()


def _render_sidebar_filters(categories: List[str], min_date: date, max_date: date) -> tuple:
    """Render category and date filters."""
    st.sidebar.header("FILTERS")
    category = st.sidebar.selectbox("Category", categories, index=0)
    default_start = max(min_date, max_date - timedelta(days=30))
    start_date = st.sidebar.date_input(
        "Start date",
        value=default_start,
        min_value=min_date,
        max_value=max_date,
    )
    end_date = st.sidebar.date_input(
        "End date",
        value=max_date,
        min_value=min_date,
        max_value=max_date,
    )
    return category, start_date, end_date


def _render_global_overview_tab(start_date: date, end_date: date) -> None:
    """Render Global Overview: category leaderboard comparing all categories."""
    leaderboard = load_global_category_leaderboard(start_date, end_date)
    if leaderboard.empty:
        st.warning("No category data available for the selected date range.")
        return

    leaderboard["rank"] = range(1, len(leaderboard) + 1)
    leaderboard["weighted_avg_rating"] = leaderboard["weighted_avg_rating"].round(2)

    # Summary metrics
    total_reviews = int(leaderboard["total_reviews"].sum())
    top_cat = leaderboard.iloc[0]
    avg_rating = leaderboard["weighted_avg_rating"].mean()

    m1, m2, m3, m4 = st.columns(4)
    m1.metric("Categories", len(leaderboard))
    m2.metric("Total Reviews", f"{total_reviews:,}")
    m3.metric("Avg Rating (all)", f"{avg_rating:.2f}")
    m4.metric("Top Category", top_cat["category"], delta=f"★ {top_cat['weighted_avg_rating']:.2f}")

    st.divider()

    # Horizontal bar chart: Reviews by Category
    chart_df = leaderboard.sort_values("total_reviews", ascending=True)
    fig_reviews = px.bar(
        chart_df,
        x="total_reviews",
        y="category",
        orientation="h",
        color="weighted_avg_rating",
        color_continuous_scale="Viridis",
        title="Reviews by Category",
        labels={"total_reviews": "Total Reviews", "weighted_avg_rating": "Avg Rating"},
    )
    fig_reviews.update_layout(
        height=max(300, len(leaderboard) * 28),
        margin=dict(l=20, r=20, t=40, b=20),
        yaxis=dict(autorange="reversed"),
        coloraxis_colorbar=dict(title="Rating"),
    )
    st.plotly_chart(fig_reviews, use_container_width=True)

    # Rating vs Reviews scatter
    fig_scatter = px.scatter(
        leaderboard,
        x="total_reviews",
        y="weighted_avg_rating",
        size="total_reviews",
        color="category",
        hover_data=["rank", "category"],
        title="Rating vs. Review Volume",
    )
    fig_scatter.update_layout(
        xaxis_title="Total Reviews",
        yaxis_title="Weighted Avg Rating",
        height=350,
        showlegend=len(leaderboard) <= 15,
    )
    st.plotly_chart(fig_scatter, use_container_width=True)

    # Ranked table with styling
    st.subheader("Full Leaderboard")
    display_df = leaderboard[["rank", "category", "total_reviews", "weighted_avg_rating"]].rename(
        columns={
            "weighted_avg_rating": "★ Rating",
            "total_reviews": "Reviews",
        }
    )
    st.dataframe(display_df, use_container_width=True, hide_index=True, column_config={
        "rank": st.column_config.NumberColumn("Rank", format="%d", width="small"),
        "category": st.column_config.TextColumn("Category"),
        "Reviews": st.column_config.NumberColumn("Reviews", format="%d"),
        "★ Rating": st.column_config.NumberColumn("Rating", format="%.2f"),
    })


def _render_declining_products(product_metrics: pd.DataFrame, category: str) -> None:
    """Identify products whose rolling_30d_avg_rating dropped > 0.5 vs previous week."""
    st.subheader("Product Red Flags: Declining Products")
    st.caption(
        "Products whose 30-day rolling average rating dropped by more than 0.5 points "
        "compared to the previous week."
    )

    if product_metrics.empty or len(product_metrics) < 2:
        st.info("Insufficient product metrics to detect declining products.")
        return

    pm = product_metrics.sort_values("review_date")
    latest = pm.groupby("parent_asin").tail(1)[["parent_asin", "review_date", "rolling_30d_avg_rating"]]
    latest = latest.rename(columns={"review_date": "latest_date", "rolling_30d_avg_rating": "current_rating"})
    latest["prev_week_date"] = (pd.to_datetime(latest["latest_date"]) - pd.Timedelta(days=7)).dt.date

    prev_rows = pm[["parent_asin", "review_date", "rolling_30d_avg_rating"]].rename(
        columns={"review_date": "prev_week_date", "rolling_30d_avg_rating": "prev_rating"}
    )
    merged = latest.merge(prev_rows, on=["parent_asin", "prev_week_date"], how="inner")
    merged["rating_decline"] = merged["prev_rating"] - merged["current_rating"]
    declining = merged[merged["rating_decline"] > 0.5].sort_values("rating_decline", ascending=False)

    if declining.empty:
        st.success("No declining products detected in this category.")
        return

    display_df = declining[["parent_asin", "current_rating", "prev_rating", "rating_decline"]].rename(
        columns={
            "prev_rating": "Prev Week Rating",
            "current_rating": "Current Rating",
            "rating_decline": "Decline (pts)",
        }
    ).drop_duplicates("parent_asin")
    st.dataframe(display_df, use_container_width=True, hide_index=True)

    selected_asin = st.selectbox(
        "Select a declining product for AI Root-Cause Analysis",
        options=declining["parent_asin"].unique().tolist(),
        key="declining_product_select",
    )
    _render_ai_root_cause_button(selected_asin, category)


def _render_ai_root_cause_button(parent_asin: str, category: str) -> None:
    """Render 'Generate AI Root-Cause Analysis' button using ai_query_helper."""
    if not os.getenv("OPENAI_API_KEY"):
        st.caption("Set OPENAI_API_KEY to enable AI Root-Cause Analysis.")
        return

    if st.button("Generate AI Root-Cause Analysis", key="ai_root_cause_btn"):
        with st.spinner("Analyzing recent reviews..."):
            try:
                helper = _get_ai_helper()
                result = helper.summarize_declining_product(parent_asin=parent_asin, category=category)
            except ValueError as exc:
                result = {"summary": "", "error": str(exc)}

        if result.get("error"):
            st.error(result["error"])
        elif result.get("summary"):
            with st.expander("AI Root-Cause Summary", expanded=True):
                st.markdown(result["summary"])


def _render_value_for_money(product_metrics: pd.DataFrame) -> None:
    """Scatter plot: Price vs Rating for top 50 products. Highlight 'Top Value' outliers."""
    st.subheader("Value for Money Analysis")
    st.caption("Price vs. Rating for top 50 products. Top Value = low price, high rating.")

    if "avg_price" not in product_metrics.columns:
        st.warning("Price data not available. Re-run the Gold pipeline to include avg_price.")
        return

    latest = (
        product_metrics.sort_values("review_date")
        .groupby("parent_asin")
        .tail(1)
        .dropna(subset=["avg_price", "rolling_30d_avg_rating"])
    )
    if latest.empty:
        st.info("No products with price and rating data.")
        return

    # Top 50 by total_reviews
    top50 = latest.nlargest(50, "total_reviews")
    if top50.empty:
        return

    # Top Value: low price, high rating (normalize and score)
    price_pct = top50["avg_price"].rank(pct=True)
    rating_pct = top50["rolling_30d_avg_rating"].rank(pct=True)
    top50 = top50.copy()
    top50["value_score"] = rating_pct - price_pct  # High rating + low price = high score
    top_value = top50.nlargest(5, "value_score")

    fig = px.scatter(
        top50,
        x="avg_price",
        y="rolling_30d_avg_rating",
        size="total_reviews",
        hover_data=["parent_asin", "total_reviews"],
        title="Price vs. Rating (Top 50 Products by Review Count)",
    )
    if not top_value.empty:
        fig.add_trace(
            go.Scatter(
                x=top_value["avg_price"].tolist(),
                y=top_value["rolling_30d_avg_rating"].tolist(),
                mode="markers",
                marker=dict(symbol="star", size=16, color="gold", line=dict(width=2, color="darkorange")),
                name="Top Value",
                text=top_value["parent_asin"].tolist(),
            )
        )
    fig.update_layout(xaxis_title="Avg Price ($)", yaxis_title="30-Day Avg Rating")
    st.plotly_chart(fig, use_container_width=True)


def _render_sentiment_breakdown(category_trends: pd.DataFrame) -> None:
    """Stacked bar chart: percentage of 1-5 star ratings over time."""
    st.subheader("Sentiment Breakdown")
    st.caption("Distribution of star ratings over time (stacked percentage).")

    star_cols = ["count_1_star", "count_2_star", "count_3_star", "count_4_star", "count_5_star"]
    if not all(c in category_trends.columns for c in star_cols):
        st.warning(
            "Rating distribution not available. Re-run the Gold pipeline to include star counts."
        )
        return

    df = category_trends.copy()
    df["total"] = df[star_cols].sum(axis=1)
    df = df[df["total"] > 0]
    if df.empty:
        st.info("No rating distribution data.")
        return

    for c in star_cols:
        df[c + "_pct"] = 100 * df[c] / df["total"]

    fig = go.Figure()
    colors = ["#dc2626", "#ea580c", "#ca8a04", "#65a30d", "#16a34a"]
    for i, (col, label) in enumerate(zip(star_cols, ["1 Star", "2 Star", "3 Star", "4 Star", "5 Star"])):
        fig.add_trace(
            go.Bar(
                x=df["review_date"],
                y=df[col + "_pct"],
                name=label,
                marker_color=colors[i],
            )
        )
    fig.update_layout(
        barmode="stack",
        title="Star Rating Distribution Over Time",
        xaxis_title="Date",
        yaxis_title="Percentage (%)",
        legend=dict(orientation="h", yanchor="bottom", y=1.02),
    )
    st.plotly_chart(fig, use_container_width=True)


def _render_overview_metrics(
    category_trends: pd.DataFrame,
    product_metrics: pd.DataFrame,
    verified_impact: pd.DataFrame,
) -> None:
    """Render high-level business metrics for the selected slice."""
    latest_category = category_trends.sort_values("review_date").iloc[-1]
    latest_products = product_metrics.sort_values("review_date").groupby("parent_asin").tail(1)
    top_product = latest_products.sort_values("rolling_30d_avg_rating", ascending=False).iloc[0]

    verified_summary = (
        verified_impact.groupby("verified_purchase")
        .agg({"avg_rating": "mean"})
        .reset_index()
    )
    verified_avg = float(
        verified_summary.loc[verified_summary["verified_purchase"] == True, "avg_rating"].mean()
    )
    non_verified_avg = float(
        verified_summary.loc[verified_summary["verified_purchase"] == False, "avg_rating"].mean()
    )
    trust_uplift = verified_avg - non_verified_avg

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Latest Daily Avg Rating", _format_rating(float(latest_category["daily_avg_rating"])))
    c2.metric("Reviews In Range", f"{int(category_trends['daily_review_count'].sum()):,}")
    c3.metric(
        "Top Product",
        str(top_product["parent_asin"]),
        delta=_format_rating(float(top_product["rolling_30d_avg_rating"])),
    )
    c4.metric("Verified Rating Uplift", f"{trust_uplift:+.2f}")


def _render_rating_trends(category_trends: pd.DataFrame) -> None:
    """Render category rating and review-volume trend charts."""
    st.subheader("Rating Trends")
    line_fig = px.line(
        category_trends,
        x="review_date",
        y="daily_avg_rating",
        markers=True,
        title="Daily Average Rating",
    )
    bar_fig = px.bar(
        category_trends,
        x="review_date",
        y="daily_review_count",
        title="Daily Review Count",
    )
    col1, col2 = st.columns(2)
    col1.plotly_chart(line_fig, use_container_width=True)
    col2.plotly_chart(bar_fig, use_container_width=True)


def _render_product_performance(product_metrics: pd.DataFrame) -> None:
    """Render product performance from rolling 30-day ratings."""
    st.subheader("Product Performance")
    latest_products = (
        product_metrics.sort_values("review_date")
        .groupby("parent_asin")
        .tail(1)
        .sort_values("rolling_30d_avg_rating", ascending=False)
    )

    display_cols = ["parent_asin", "review_date", "total_reviews", "average_rating", "rolling_30d_avg_rating"]
    if "avg_price" in product_metrics.columns:
        display_cols.append("avg_price")
    chart_df = latest_products.head(10)[display_cols].copy()

    bar_fig = px.bar(
        chart_df,
        x="parent_asin",
        y="rolling_30d_avg_rating",
        color="total_reviews",
        title="Top Products by Latest 30-Day Rolling Average Rating",
    )
    st.plotly_chart(bar_fig, use_container_width=True)

    display_df = latest_products[display_cols].rename(
        columns={
            "review_date": "latest_review_date",
            "average_rating": "cumulative_avg_rating",
        }
    )
    st.dataframe(display_df, use_container_width=True)


def _render_trust_analysis(verified_impact: pd.DataFrame) -> None:
    """Render verified vs non-verified rating comparisons."""
    st.subheader("Trust Analysis")
    verified_impact = verified_impact.copy()
    verified_impact["purchase_type"] = verified_impact["verified_purchase"].map(
        {True: "Verified", False: "Non-Verified"}
    )

    summary_df = (
        verified_impact.groupby("purchase_type", as_index=False)
        .agg(
            avg_rating=("avg_rating", "mean"),
            daily_review_count=("daily_review_count", "sum"),
        )
    )
    summary_fig = px.bar(
        summary_df,
        x="purchase_type",
        y="avg_rating",
        color="purchase_type",
        title="Average Rating by Purchase Trust Segment",
    )
    trend_fig = px.line(
        verified_impact,
        x="review_date",
        y="avg_rating",
        color="purchase_type",
        markers=True,
        title="Verified vs Non-Verified Rating Trend",
    )
    col1, col2 = st.columns(2)
    col1.plotly_chart(summary_fig, use_container_width=True)
    col2.plotly_chart(trend_fig, use_container_width=True)


def _render_category_analytics_tab(
    category: str, start_date: date, end_date: date
) -> None:
    """Render the category-specific analytics dashboard."""
    if start_date > end_date:
        st.error("Start date must be before or equal to end date.")
        return

    st.title("Amazon Reviews & Product Insights")
    st.caption(
        "Analyze category trends, product performance, value-for-money, and anomaly detection "
        "from the Amazon Reviews lakehouse."
    )

    with st.spinner(f"Loading Amazon Gold data for {category} ({start_date} to {end_date})..."):
        try:
            category_trends = load_category_trends(category, start_date, end_date)
            product_metrics = load_product_metrics(category, start_date, end_date)
            verified_impact = load_verified_purchase_impact(category, start_date, end_date)
        except Exception as exc:
            st.error(f"Failed to load Amazon Gold tables: {exc}")
            return

    if category_trends.empty or product_metrics.empty or verified_impact.empty:
        st.warning(
            "No Amazon Gold data was found for the selected filters. "
            "Run the Amazon pipeline first or widen the date range."
        )
        return

    _render_overview_metrics(category_trends, product_metrics, verified_impact)
    st.divider()

    _render_sentiment_breakdown(category_trends)
    st.divider()

    _render_rating_trends(category_trends)
    st.divider()

    _render_declining_products(product_metrics, category)
    st.divider()

    _render_value_for_money(product_metrics)
    st.divider()

    _render_product_performance(product_metrics)
    st.divider()

    _render_trust_analysis(verified_impact)


def _render_ai_chat_tab() -> None:
    """Render the Amazon Reviews NL-to-SQL chat experience."""
    st.markdown(
        '<h1 style="text-align: center; font-size: 2rem; font-weight: 700; margin-bottom: 0.25rem;">Amazon Reviews AI Query</h1>'
        '<p style="text-align: center; color: #64748b; font-size: 0.95rem; margin-bottom: 1.5rem;">'
        "Ask questions about product performance, category trends, and verified-purchase trust metrics. "
        "The assistant translates your question into Spark SQL, runs it, and explains the results.</p>",
        unsafe_allow_html=True,
    )

    if not os.getenv("OPENAI_API_KEY"):
        st.warning(
            "**OPENAI_API_KEY** is not set. Add it to your `.env` file and restart the dashboard "
            "to enable Ask AI.",
            icon="🔑",
        )
        return

    with st.expander("Example questions", expanded=False):
        st.markdown("- *Show me top categories by average rating over the last 7 days.*")
        st.markdown("- *Find products with declining 30-day rolling averages in Electronics during the last month.*")
        st.markdown("- *Compare verified vs non-verified ratings for Gift_Cards on 2020-05-05.*")
        st.markdown("- *Which products had the most reviews in Books last week?*")

    if "ai_chat_messages" not in st.session_state:
        st.session_state.ai_chat_messages = []

    for msg in st.session_state.ai_chat_messages:
        with st.chat_message(msg["role"]):
            if msg["role"] == "user":
                escaped = html.escape(msg["content"])
                st.markdown(f'<span class="user-msg-bubble">{escaped}</span>', unsafe_allow_html=True)
            else:
                if msg.get("error"):
                    st.error(msg["error"])
                if msg.get("sql"):
                    with st.expander("Generated SQL", expanded=False):
                        st.code(msg["sql"], language="sql")
                if msg.get("dataframe") is not None and not msg["dataframe"].empty:
                    st.dataframe(msg["dataframe"], use_container_width=True)
                    st.caption(f"{len(msg['dataframe']):,} row(s) returned.")
                if not msg.get("error") and msg.get("explanation"):
                    st.markdown(msg["explanation"])

    if st.session_state.ai_chat_messages:
        if st.button("Clear conversation", key="clear_ai_chat"):
            st.session_state.ai_chat_messages = []
            st.rerun()

    user_input = st.chat_input(
        "Ask about Amazon product trends, ratings, or trust analysis..."
    )
    if not user_input:
        return

    st.session_state.ai_chat_messages.append({"role": "user", "content": user_input})
    with st.chat_message("user"):
        escaped = html.escape(user_input)
        st.markdown(f'<span class="user-msg-bubble">{escaped}</span>', unsafe_allow_html=True)

    history: List[Dict[str, str]] = []
    for msg in st.session_state.ai_chat_messages[:-1]:
        if msg["role"] == "user":
            history.append({"role": "user", "content": msg["content"]})
        else:
            content = msg.get("error") or msg.get("explanation", "")
            if content:
                history.append({"role": "assistant", "content": content})

    with st.chat_message("assistant"):
        with st.spinner("Thinking..."):
            try:
                helper = _get_ai_helper()
                result = helper.query(user_input, conversation_history=history)
            except ValueError as exc:
                result = {
                    "sql": "",
                    "explanation": "",
                    "dataframe": pd.DataFrame(),
                    "error": str(exc),
                }

        if result.get("error"):
            st.error(result["error"])
        if result.get("sql"):
            with st.expander("Generated SQL", expanded=True):
                st.code(result["sql"], language="sql")
        if result.get("dataframe") is not None and not result["dataframe"].empty:
            st.dataframe(result["dataframe"], use_container_width=True)
            st.caption(f"{len(result['dataframe']):,} row(s) returned.")
        if not result.get("error") and result.get("explanation"):
            st.markdown(result["explanation"])

    st.session_state.ai_chat_messages.append(
        {
            "role": "assistant",
            "content": result.get("explanation", ""),
            "sql": result.get("sql", ""),
            "dataframe": result.get("dataframe"),
            "error": result.get("error"),
        }
    )


def main() -> None:
    st.set_page_config(
        page_title="Amazon Reviews & Product Insights",
        layout="wide",
        initial_sidebar_state="expanded",
    )
    st.markdown(_CSS, unsafe_allow_html=True)

    _render_refresh_button()

    categories, min_date, max_date = load_dashboard_metadata()
    if not categories or min_date is None or max_date is None:
        st.warning(
            "No Amazon Gold data was found under `data/gold/amazon_reviews`. "
            "Run the Amazon pipeline first to populate the dashboard."
        )
        return

    category, start_date, end_date = _render_sidebar_filters(categories, min_date, max_date)

    tab_global, tab_category, tab_ai = st.tabs(["Global Overview", "Category Analytics", "Ask AI"])
    with tab_global:
        _render_global_overview_tab(start_date, end_date)
    with tab_category:
        _render_category_analytics_tab(category, start_date, end_date)
    with tab_ai:
        _render_ai_chat_tab()


if __name__ == "__main__":
    main()
