from datetime import datetime, timedelta
import os

from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator

PROJECT_ROOT = os.getenv("PROJECT_ROOT", "/opt/airflow")

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2025, 12, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    "crypto_realtime_transform",
    default_args=default_args,
    description="Incremental real-time dbt transformations (staging + daily)",
    schedule_interval="*/5 * * * *",  # All 5 minutes
    catchup=False,
    is_paused_upon_creation=True,
    tags=["crypto", "realtime", "dbt"],
) as dag:
    run_dbt_incremental = DockerOperator(
        task_id="dbt_run_incremental",
        image="fishtownanalytics/dbt:1.0.0",
        api_version="auto",
        auto_remove=True,
        working_dir="/usr/app/dbt",
        environment={"DBT_PROFILES_DIR": "/usr/app/dbt"},
        network_mode="cookiecutterproject_slug_data-platform",
        command=["run", "--select", "stg_ohlc", "daily_ohlc"],
        # command=["run", "--exclude", "raw_ohlc"], # Launch everything except raw_ohlc
        mounts=[{"source": f"{PROJECT_ROOT}/dbt", "target": "/usr/app/dbt", "type": "bind"}],
        mount_tmp_dir=False,
    )

    run_dbt_incremental
