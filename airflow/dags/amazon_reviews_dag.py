from datetime import datetime, timedelta

from airflow import DAG
from airflow.models.param import Param
from airflow.operators.bash import BashOperator

PROJECT_ROOT = "/opt/airflow/project"

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="amazon_reviews",
    default_args=default_args,
    schedule="@daily",
    catchup=False,
    start_date=datetime(2025, 1, 1),
    params={
        "category": Param("Gift_Cards", type="string", description="Amazon category (e.g. Gift_Cards, All_Beauty)"),
        "skip_optimize": Param(False, type="boolean", description="Skip Delta OPTIMIZE/Z-ORDER in Gold"),
        "overwrite": Param(False, type="boolean", description="Overwrite existing raw files on fetch"),
        "max_rows": Param(None, type=["integer", "null"], description="Max rows per category (empty = use config)"),
    },
    tags=["amazon", "reviews"],
) as dag:
    ensure_dirs = BashOperator(
        task_id="ensure_dirs",
        bash_command=f"mkdir -p {PROJECT_ROOT}/data/bronze {PROJECT_ROOT}/data/silver {PROJECT_ROOT}/data/gold {PROJECT_ROOT}/data/raw",
    )

    fetch_data = BashOperator(
        task_id="fetch_data",
        bash_command=f"cd {PROJECT_ROOT} && python scripts/fetch_amazon_data.py --categories={{{{ params.category }}}} {{% if params.overwrite %}}--overwrite{{% endif %}} {{% if params.max_rows %}}--max-rows {{{{ params.max_rows }}}}{{% endif %}}",
    )

    run_pipeline = BashOperator(
        task_id="run_pipeline",
        bash_command=f"cd {PROJECT_ROOT} && python scripts/run_pipeline_unified.py {{{{ ds }}}} --category={{{{ params.category }}}} {{% if params.skip_optimize %}}--skip-optimize{{% endif %}}",
    )

    quality_checks = BashOperator(
        task_id="quality_checks",
        bash_command=(
            f"cd {PROJECT_ROOT} && python -c \""
            "from src.utils.spark_session import get_spark_session; "
            "from src.quality.quality_checks import validate_silver; "
            "spark = get_spark_session('QualityChecks'); "
            "df = spark.read.format('delta').load('data/silver/amazon_reviews'); "
            "validate_silver(df); "
            "print('Quality checks passed'); "
            "spark.stop()\""
        ),
    )

    ensure_dirs >> fetch_data >> run_pipeline >> quality_checks
