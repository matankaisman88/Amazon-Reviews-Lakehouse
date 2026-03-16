import os
from datetime import timedelta

import subprocess

from airflow import DAG
from airflow.models.param import Param
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

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
    start_date=days_ago(1),
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
        bash_command=f"cd {PROJECT_ROOT} && python src/quality/quality_checks.py",
    )

    def _start_dashboard():
        subprocess.run(["pkill", "-f", "streamlit run src/dashboard"], capture_output=True, check=False)
        env = {**os.environ, "PYTHONPATH": PROJECT_ROOT}
        with open("/tmp/dashboard.log", "a") as log:
            subprocess.Popen(
                ["streamlit", "run", "src/dashboard/app.py", "--server.port=8502", "--server.address=0.0.0.0", "--server.headless=true"],
                cwd=PROJECT_ROOT,
                env=env,
                stdout=log,
                stderr=subprocess.STDOUT,
                start_new_session=True,
            )

    run_dashboard = PythonOperator(
        task_id="run_dashboard",
        python_callable=_start_dashboard,
    )

    ensure_dirs >> fetch_data >> run_pipeline >> quality_checks >> run_dashboard
