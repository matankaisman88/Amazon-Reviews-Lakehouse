import html
import os
from datetime import date, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pandas as pd
import plotly.express as px
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
    """Load category list and date bounds from the Amazon category trends table."""
    from src.utils.spark_session import get_spark_session

    category_trends_path = _gold_table_path("category_trends")
    if not category_trends_path.exists():
        return [], None, None

    spark = get_spark_session("AmazonDashboardMetadata")
    trends = spark.read.format("delta").load(str(category_trends_path))
    if trends.isEmpty():
        return [], None, None

    from pyspark.sql import functions as F

    summary = trends.agg(
        F.min("review_date").alias("min_review_date"),
        F.max("review_date").alias("max_review_date"),
    ).collect()[0]
    categories = [
        row.category
        for row in trends.select("category").distinct().orderBy("category").collect()
        if row.category
    ]

    return categories, summary.min_review_date, summary.max_review_date


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


def _run_refresh_flow(target_date: str) -> None:
    """Execute the Amazon pipeline refresh for target_date."""
    from src.utils.pipeline_orchestrator import run_refresh

    log_lines: List[str] = []
    status_placeholder = st.sidebar.empty()

    try:
        with st.spinner(f"Running Amazon pipeline for {target_date}..."):
            for line in run_refresh(target_date):
                log_lines.append(line)
                status_placeholder.caption(line or "...")

        status_placeholder.empty()
        st.sidebar.success(f"Amazon pipeline finished for {target_date}.")
        st.cache_data.clear()
        st.cache_resource.clear()

    except RuntimeError as exc:
        status_placeholder.empty()
        st.sidebar.error(f"Pipeline failed: {exc}")

    if log_lines:
        with st.sidebar.expander("View pipeline log", expanded=False):
            st.code("\n".join(log_lines), language=None)


def _render_refresh_button() -> None:
    """Render the Amazon Bronze -> Silver -> Gold refresh controls."""
    from src.utils.pipeline_orchestrator import yesterday

    st.sidebar.header("DATA MANAGEMENT")
    st.sidebar.caption(
        "Run the Amazon Medallion pipeline against staged raw Amazon review files. "
        "This executes Bronze -> Silver -> Gold."
    )

    target_yesterday = yesterday()
    if st.sidebar.button(
        f"Run Daily Refresh ({target_yesterday})",
        type="primary",
        key="refresh_yesterday",
        use_container_width=True,
    ):
        _run_refresh_flow(target_yesterday)

    with st.sidebar.expander("Advanced: Manual Backfill", expanded=False):
        backfill_date = st.date_input(
            "Select ingestion date",
            value=date.today() - timedelta(days=1),
            min_value=date(2020, 1, 1),
            max_value=date.today(),
            key="backfill_date",
        )
        if st.button("Run Backfill", key="run_backfill", use_container_width=True):
            _run_refresh_flow(backfill_date.strftime("%Y-%m-%d"))

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
    c3.metric("Top Product", str(top_product["parent_asin"]), delta=_format_rating(float(top_product["rolling_30d_avg_rating"])))
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

    chart_df = latest_products.head(10).copy()
    bar_fig = px.bar(
        chart_df,
        x="parent_asin",
        y="rolling_30d_avg_rating",
        color="total_reviews",
        title="Top Products by Latest 30-Day Rolling Average Rating",
    )
    st.plotly_chart(bar_fig, use_container_width=True)

    display_df = latest_products[
        ["parent_asin", "review_date", "total_reviews", "average_rating", "rolling_30d_avg_rating"]
    ].rename(
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


def _render_dashboard_tab(category: str, start_date: date, end_date: date) -> None:
    """Render the Amazon analytics dashboard."""
    if start_date > end_date:
        st.error("Start date must be before or equal to end date.")
        return

    st.title("Amazon Reviews & Product Insights")
    st.caption(
        "Analyze category trends, product performance, and verified-purchase trust signals "
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
    _render_rating_trends(category_trends)
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

    tab_dashboard, tab_ai = st.tabs(["Dashboard", "Ask AI"])
    with tab_dashboard:
        _render_dashboard_tab(category, start_date, end_date)
    with tab_ai:
        _render_ai_chat_tab()


if __name__ == "__main__":
    main()
