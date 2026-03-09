# Amazon Reviews Lakehouse

Production-grade Medallion Architecture data lakehouse for the **Amazon Reviews 2023** dataset (McAuley Lab). Built with Spark 3.5, Delta Lake, and Great Expectations, designed to run under modest resource constraints (default 1GB executor; tunable via config).

## Architecture

```
Fetch (McAuley Lab UCSD) → Raw JSONL.gz
    → Bronze (Delta, partition by category, ingestion_date)
    → Silver (Normalize, metadata join, MERGE on review_id)
    → Gold (product_metrics, category_trends, verified_purchase_impact)
```

### Medallion Layers

| Layer | Purpose | Partitioning |
|-------|---------|--------------|
| Bronze | Raw ingestion, schema enforcement, category extraction from path | `category`, `ingestion_date` |
| Silver | Deduplication, metadata enrichment, price parsing, MERGE | `category`, `year` |
| Gold | Business analytics, rolling metrics, trust analysis | `category`, `review_date` |

**Quality checks** — Silver data is validated (non-null identifiers, valid ratings, non-negative helpful votes) before Gold aggregation; no separate pipeline step.

### Key Design Decisions

**Partitioning (category + date)**

- Aligns with query patterns (filter by category, date range)
- Keeps partition count manageable; avoids small-file problem

**Z-Order (non-partition columns)**

- `OPTIMIZE ... ZORDER BY` improves predicate pushdown on Gold tables
- Run only on newly merged partitions to avoid full-table rewrites

**MERGE Idempotency**

- Silver: match on `review_id` — re-runs do not create duplicates
- Gold: incremental MERGE preserves target Z-ORDER

**Amazon Reviews format**

- Raw JSONL.gz from McAuley Lab; timestamps in epoch milliseconds
- Category inferred from file path (e.g. `reviews/Gift_Cards/*.jsonl.gz`)

**History Server**

- Event logs: `http://localhost:18080`
- Debug slow stages, skew, spills via Spark UI

### Schema Evolution & Enforcement

- **Strict Schema Ingestion**: Explicit `StructType` definitions in `src/utils/amazon_schemas.py` for all layers.
- **Resilience to Source Changes**: Schemas enforce data contracts across the Medallion layers.
- **Manual Evolution**: To support new columns, update `amazon_schemas.py` and downstream jobs.

## Gold Tables

| Table | Purpose |
|------|---------|
| **product_metrics** | Per-product daily aggregates: `total_reviews`, `average_rating`, `rolling_30d_avg_rating`, `avg_price` |
| **category_trends** | Category-level daily: `daily_review_count`, `daily_avg_rating`, star distribution (`count_1_star` … `count_5_star`) |
| **verified_purchase_impact** | Trust analysis: avg rating by `verified_purchase` (true/false) |

## Run Modes

Two ways to process data:

| Mode | Use case | How |
|------|----------|-----|
| **1. Manual Pipeline** | Scripts, CI/CD, by category | Run `run_pipeline.sh` (auto-fetches raw data if needed) |
| **2. Dashboard** | Interactive UI, by category | Start dashboard → Select category → **Run Pipeline** (auto-fetches if needed) |

---

## Mode 1: Manual Pipeline

For running the pipeline via scripts (e.g. cron, CI, or one-off backfills).

### Prerequisites

- Docker & Docker Compose
- Python 3.9+ (for local dev; Docker uses Python 3.x from the Spark base image)

### Step 1: Fetch Raw Data (or use sample)

Raw data is **auto-fetched** from [McAuley Lab (UCSD)](https://mcauleylab.ucsd.edu/public_datasets/data/amazon/) when you run the pipeline and none is staged. When you pass `--category=X`, the pipeline fetches that category if its raw files are missing (even when other categories exist). To fetch manually:

```bash
# Fetch Gift_Cards (small, ~3.6MB) — default for quick start
python scripts/fetch_amazon_data.py --categories Gift_Cards

# Limit rows per category (controls raw data size)
python scripts/fetch_amazon_data.py --categories Gift_Cards --max-rows 5000

# Fetch multiple categories
python scripts/fetch_amazon_data.py --categories Gift_Cards All_Beauty

# Fetch all 33 categories (large, ~50GB+)
python scripts/fetch_amazon_data.py --categories all
```

**Size control:** Set `fetch.max_rows_per_category` in `config/config.yaml` (default: 10000) to limit auto-fetch size. Override with `--max-rows N` or `FETCH_MAX_ROWS=N` env var. Use `null` for no limit. Full-category downloads (e.g. Video_Games ~347MB) use streaming to avoid OOM.

Or generate synthetic sample data:

```bash
python scripts/create_amazon_sample.py
```

### Step 2: Start Spark Cluster

```bash
docker compose -f docker/docker-compose.yml up -d spark-master spark-worker history-server
```

### Step 3: Run Pipeline

#### Git Bash / WSL / Linux / macOS

```bash
# Full pipeline (single Spark session — avoids 3× startup; recommended)
./scripts/run_pipeline.sh                     # Bronze -> Silver -> Gold (all staged categories)
./scripts/run_pipeline.sh --category=Gift_Cards  # Scope to one category; auto-fetches if missing
./scripts/run_pipeline.sh --category=All_Beauty --skip-optimize  # One category, skip Gold OPTIMIZE

# Per-stage scripts (separate Spark sessions; use for debugging)
./scripts/run_amazon_bronze.sh [YYYY-MM-DD]   # Bronze only (optional ingestion_date; defaults to today)
./scripts/run_amazon_silver.sh [YYYY-MM-DD]   # Silver only (optional ingestion_date)
./scripts/run_amazon_gold.sh   [YYYY-MM-DD] [--skip-optimize]  # Gold only; --skip-optimize skips OPTIMIZE
```

#### PowerShell

```powershell
# Recommended: call the bash scripts via Git Bash:
#   & "C:\Program Files\Git\bin\bash.exe" ./scripts/run_pipeline.sh
```

Or run Bronze locally (without Docker):

```bash
PYTHONPATH=. python -m src.jobs.amazon_bronze_ingestion [YYYY-MM-DD]
```

### Step 4: History Server

Open `http://localhost:18080` after job completion.

### Debug: Layer Count Audit

To investigate data loss between stages (Raw → Bronze → Silver → Gold):

```bash
# Via Docker (recommended)
./scripts/run_debug_layer_counts.sh --category Gift_Cards
./scripts/run_debug_layer_counts.sh   # All categories

# Or directly
docker compose -f docker/docker-compose.yml run --rm dashboard python3 scripts/debug_layer_counts.py --category Gift_Cards
```

---

## Mode 2: Dashboard

For interactive exploration and pipeline runs by category.

### Start the Dashboard

```bash
docker compose -f docker/docker-compose.yml up -d dashboard
```

This starts the dashboard and its dependencies (`spark-master`, `spark-worker`). Access at `http://localhost:8501`. The dashboard runs Bronze → Silver → Gold via a **subprocess** (isolated from the UI). **Start the Spark cluster first** (`spark-master`, `spark-worker`) so the dashboard can submit jobs to it; the dashboard is on `spark-net` and uses `SPARK_MASTER=spark://spark-master:7077`.

### Run Pipeline (sidebar)

**Category** — Select a category (e.g. Gift_Cards, All_Beauty) and click **Run Pipeline** to execute Bronze → Silver → Gold for that category. Runs **Fetch (if needed) → Bronze → Silver → Gold**. Raw data is auto-fetched from McAuley Lab when the category is not staged.

**Fetch options** (expander) — Set **Max rows per category** when fetching (default: 10,000), or check **Unlimited rows** for full download. Check **Force re-fetch** to overwrite existing raw files (e.g. to replace a limited fetch with unlimited). Fetch streams large files to avoid OOM.

Cache is cleared on success. Pipeline logs in "View pipeline log" expander.

*Source data is static (up to 2023); processing is scoped by category, not date.*

**Note:** Spark startup can take 1–2 minutes in Docker. The page will update when the pipeline finishes. Progress is streamed to the sidebar.

### Dashboard UI

- **Global Overview**: Category leaderboard with metrics, bar chart, and rating vs. volume scatter
- **Category Analytics**: Drill-down with filters (category, date range)
  - **Sentiment Breakdown**: Stacked bar chart of 1–5 star rating distribution over time
  - **Rating Trends**: Daily review counts and average ratings
  - **Product Red Flags**: Declining products (rating drop > 0.5 vs previous week) with AI Root-Cause Analysis
  - **Value for Money**: Price vs. rating scatter for top 50 products; highlights “Top Value” outliers
  - **Product Performance**: Top products by rolling 30-day rating
  - **Trust Analysis**: Verified vs non-verified purchase impact

### AI Query (Natural Language to SQL)

Ask questions in plain English; an LLM translates to Spark SQL and explains results.

- **Requirements**: `OPENAI_API_KEY` in `.env`. Optional: `OPENAI_MODEL` (default `gpt-4o-mini`).
- **Tables**: `product_metrics` (incl. `avg_price`), `category_trends` (incl. star counts), `verified_purchase_impact`
- **Safety**: Only `SELECT`; `LIMIT 100`; read-only analytical queries
- **CTE Support**: `WITH ... SELECT` queries allowed for complex analytics

### AI Root-Cause Analysis

For declining products (rating drop > 0.5 vs previous week), use **Generate AI Root-Cause Analysis** to summarize recent review text from Silver and identify common themes. Requires `OPENAI_API_KEY`.

---

## Configuration

- **config/config.yaml** — Paths, Spark settings, dynamic config, AQE, GX checkpoint
- **.env** — Overrides (copy from `.env.example`)
- **Dashboard vs CLI**: Both use the Spark cluster when `spark-master` and `spark-worker` are running. The dashboard is on `spark-net` with `SPARK_MASTER=spark://spark-master:7077`; start the cluster before running the pipeline from the UI.
- **Dashboard metadata**: Sidebar (categories, date range) loads via `deltalake` (no Spark) for fast startup.
- **.streamlit/config.toml** — Dashboard theme (dark sidebar, light main, dark code blocks)
- **Debug**: Set `SPARK_DEBUG=1` or `debug.explain_enabled: true` in config to log `df.explain()` plans to `data/debug/`

### Spark Tuning (AQE & Memory)

- `spark.sql.adaptive.enabled=true` — AQE coalesces shuffle partitions at runtime.
- `spark.sql.shuffle.partitions=8` — Tuned for small batches (3MB–100K rows); increase to 200 for large datasets.
- `spark.memory.fraction=0.5` — Leaves headroom for JVM on constrained env (1–2GB RAM).
- `spark.max_partition_bytes` — Max bytes per partition for file scans (default `64m`).
- `dynamic_config.enabled` — When true, shuffle partitions are computed from input size (~128MB per partition; min 8, max 400).
- `dynamic_config.small_input_threshold_bytes` — Below 10MB input, use 1–2 partitions (Gift_Cards etc.).
- `dynamic_config.small_input_partitions` — Partitions when input < threshold (default 2).
- `spark.advisory_partition_size` — AQE target partition size (128MB) after coalesce.
- `spark.coalesce_partitions_enabled` — AQE coalesce for scalable shuffle.
- `gold.optimize_threshold_rows` — Min rows in batch to run OPTIMIZE (default 100000).
- `gold.optimize_threshold_size_mb` — Min table size (MB) to run OPTIMIZE; skips tiny tables to save DBUs (default 100).
- `gold.small_input_threshold_bytes` — Below 10MB Silver size, use fewer partitions (default 10MB) for faster Gold.
- `gold.small_output_coalesce` — Coalesce to N partitions before MERGE for small output (default 2).
- `gold.small_output_threshold_rows` — Coalesce when output rows < this (default 10000), even if input was large.
- **Gold parallel writes** — product_metrics, category_trends, and verified_purchase_impact are cached and written in parallel; Silver is read once (avoids 2× re-reads).
- **Gold filter pushdown** — category_trends and verified_impact filter by date before groupBy (no post-aggregate filter).
- **Dynamic by input size** — Bronze, Silver, Gold all adapt: small input (<10MB) → 2 partitions; large → 8–400. Silver coalesces before MERGE when Bronze is small.
- Default executor memory is `1g`; increase `spark.executor.memory` (e.g. `2g`) when RAM permits.
- **Full-category datasets** (e.g. Video_Games ~347MB): config uses `driver_memory: 2g` and `max_partition_bytes: 64m`; dashboard has `mem_limit: 4g`. Shuffle partitions scale automatically with input size when `dynamic_config.enabled=true`.
- Run Silver/Gold incrementally by passing an `ingestion_date` or `--category=` to scope work.
- **Silver partition change** (v2): Silver now partitions by `category`, `year` (not `review_date`) to reduce task count. If you have existing Silver data with the old schema, delete `data/silver/amazon_reviews` and re-run the pipeline to recreate with the new partitioning.

## Project Structure

```
.
├── .env.example             # Env template; copy to .env and set OPENAI_API_KEY for AI Query
├── config/
│   └── config.yaml          # Paths and Spark tunings
│
├── data/
│   ├── raw/amazon/          # Landing zone for JSONL.gz (reviews/, metadata/)
│   ├── bronze/amazon_reviews/  # Raw data in Delta (reviews, metadata)
│   ├── silver/amazon_reviews/  # Cleaned, enriched data
│   ├── gold/amazon_reviews/    # product_metrics, category_trends, verified_purchase_impact
│   └── metadata/            # Reference data (optional)
│
├── docker/
│   ├── Dockerfile           # Spark 3.5 + Delta + GX + Streamlit
│   └── docker-compose.yml   # Spark Master/Worker, History Server, Dashboard
│
├── great_expectations/
│   ├── expectations/       # GX suites (amazon_silver_suite.json)
│   └── checkpoints/         # Checkpoint configs
│
├── src/
│   ├── dashboard/
│   │   └── app.py           # Streamlit BI: Global Overview, Category Analytics, AI Query
│   │
│   ├── jobs/
│   │   ├── amazon_bronze_ingestion.py   # Raw JSONL.gz → Delta (reviews, metadata)
│   │   ├── amazon_silver_transformation.py  # Normalize, metadata join, MERGE
│   │   └── amazon_gold_analytics.py     # product_metrics, category_trends, verified_purchase_impact
│   │
│   ├── quality/
│   │   └── quality_checks.py  # Rule-based validation (fail-fast) before Gold write
│   │
│   └── utils/
│       ├── ai_query_helper.py       # NL-to-SQL via LLM against Gold tables
│       ├── amazon_schemas.py        # StructTypes for Bronze, Silver, Gold
│       ├── config_loader.py         # Loads config.yaml; dynamic shuffle partitions
│       ├── debug_explain.py         # Log df.explain() to data/debug/ when SPARK_DEBUG=1
│       ├── input_size_estimator.py  # Estimate input size for dynamic config
│       ├── pipeline_orchestrator.py  # Bronze → Silver → Gold for dashboard refresh
│       └── spark_session.py         # SparkSession builder; AQE; dynamic config
│
├── scripts/
│   ├── fetch_amazon_data.py # Download raw JSONL.gz from McAuley Lab (streaming; no OOM on large files)
│   ├── run_refresh_standalone.py  # Pipeline for dashboard refresh (subprocess; prints progress to stdout)
│   ├── run_pipeline.sh      # Full Medallion (auto-fetches if needed): Bronze → Silver → Gold
│   ├── run_pipeline_unified.py    # Bronze → Silver → Gold in single Spark session
│   ├── run_pipeline_spark_unified.sh  # Single spark-submit for unified pipeline
│   ├── run_amazon_bronze.sh # Bronze stage only
│   ├── run_amazon_silver.sh # Silver stage only
│   ├── run_amazon_gold.sh   # Gold stage only
│   ├── debug_layer_counts.py   # Audit Raw/Bronze/Silver/Gold row counts (per category)
│   ├── run_debug_layer_counts.sh   # Run debug via Docker (bash)
│   ├── run_debug_layer_counts.ps1  # Run debug via Docker (PowerShell)
│   ├── create_amazon_sample.py  # Generate synthetic sample JSONL.gz
│   └── drop_raw.sh          # Remove raw files after pipeline (optional)
│
└── tests/
    ├── conftest.py          # Pytest fixtures (SparkSession)
    └── test_transformations.py  # Amazon Silver/Gold transformation tests
```

## Testing

```bash
pip install -r requirements.txt
pytest tests/ -v
```

## CI

`.github/workflows/basic_ci.yml` — Lint (ruff) and tests (pytest).
