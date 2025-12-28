from datetime import datetime, timedelta
import os

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.docker.operators.docker import DockerOperator

import redis
import psycopg2
from psycopg2.extras import execute_batch


REDIS_HOST = os.getenv("REDIS_HOST", "data-platform-redis")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))

PROJECT_ROOT = os.getenv("PROJECT_ROOT", "/opt/airflow")

POSTGRES_HOST = os.getenv("POSTGRES_DATA_HOST", "data-platform-postgres")
POSTGRES_PORT = int(os.getenv("POSTGRES_DATA_PORT", "5432"))
POSTGRES_DB = os.getenv("POSTGRES_DATA_DB", "admin")
POSTGRES_USER = os.getenv("POSTGRES_DATA_USER", "admin")
POSTGRES_PASSWORD = os.getenv("POSTGRES_DATA_PASSWORD", "admin")

DATA_SOURCE_TYPE = os.getenv("DATA_SOURCE_TYPE", "local_csv")
CSV_PATH = os.getenv("CSV_PATH", "/data/")
S3_URI = os.getenv("S3_URI", "s3://my-bucket/crypto_data.csv")
PROVIDER_COIN = os.getenv("PROVIDER_COIN", "BTCUSDT")
PROVIDER_INTERVAL = os.getenv("PROVIDER_INTERVAL", "1m")

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2025, 11, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


def redis_to_postgres():
    """
    redis_to_postgres function
    """
    r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
    conn = psycopg2.connect(
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        database=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
    )
    cur = conn.cursor()

    keys = r.keys("*USDT-*")
    print(f"Found {len(keys)} keys in Redis")

    if not keys:
        print("No data in Redis")
        conn.close()
        return

    records = []
    for key in keys:
        parts = key.split("-", 1)
        if len(parts) != 2:
            continue
        symbol, timestamp_str = parts
        data = r.hgetall(key)
        if not data:
            continue
        timestamp_pg = timestamp_str.replace("T", " ") + ":00"
        records.append(
            (
                symbol,
                timestamp_pg,
                float(data.get("open", 0)),
                float(data.get("high", 0)),
                float(data.get("low", 0)),
                float(data.get("close", 0)),
                float(data.get("volume", 0)),
            )
        )

    if records:
        execute_batch(
            cur,
            """
            INSERT INTO raw_ohlc (symbol, timestamp, open, high, low, close, volume)
            VALUES (%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (symbol, timestamp) DO UPDATE SET
                open = EXCLUDED.open,
                high = EXCLUDED.high,
                low = EXCLUDED.low,
                close = EXCLUDED.close,
                volume = EXCLUDED.volume
        """,
            records,
        )
        conn.commit()
        print(f"✅ Inserted {len(records)} records into PostgreSQL")
    else:
        print("⚠️ No valid records to insert")

    cur.close()
    conn.close()


with DAG(
    "crypto_ingestion_pipeline",
    default_args=default_args,
    description="Pipeline crypto RAW → Staging → Analytics avec DBT",
    schedule_interval=None,  # Manual
    # schedule_interval=timedelta(minutes=5),
    catchup=False,
    # is_paused_upon_creation=False,
    is_paused_upon_creation=True,  # On pause
    tags=["crypto", "ingestion"],
) as dag:
    run_dbt_raw = DockerOperator(
        task_id="dbt_run_raw",
        image="fishtownanalytics/dbt:1.0.0",
        api_version="auto",
        auto_remove=True,
        network_mode="cookiecutterproject_slug_data-platform",
        working_dir="/usr/app/dbt",
        environment={"DBT_PROFILES_DIR": "/usr/app/dbt"},
        command=["run", "--select", "raw_ohlc"],
        mounts=[{"source": f"{PROJECT_ROOT}/dbt", "target": "/usr/app/dbt", "type": "bind"}],
        mount_tmp_dir=False,
    )

    run_rust_ingestion = DockerOperator(
        task_id="run_rust_ingestion",
        image="crypto-ingestion:latest",
        api_version="auto",
        auto_remove=True,
        network_mode="cookiecutterproject_slug_data-platform",
        docker_url="unix://var/run/docker.sock",
        mount_tmp_dir=False,
        environment={
            "DATASOURCE__SOURCE_TYPE": DATA_SOURCE_TYPE,
            "DATASOURCE__CSV_PATH": CSV_PATH,
            "DATASOURCE__S3_URI": S3_URI,
            "PROVIDER__COIN": PROVIDER_COIN,
            "PROVIDER__INTERVAL": PROVIDER_INTERVAL,
            "PROVIDER__ENABLED": "false",
            "PROVIDER__API_SECRET": "",
            "PROVIDER__API_KEY": "",
            "STORAGE__ENABLED": "true",
            "STORAGE__URI": f"redis://{REDIS_HOST}:{REDIS_PORT}",
            "STORAGE__CHANNEL": "channel",
        },
        mounts=[{"source": f"{PROJECT_ROOT}/data", "target": "/data", "type": "bind"}],
    )

    load_to_postgres = PythonOperator(
        task_id="redis_to_postgres",
        python_callable=redis_to_postgres,
    )

    run_dbt_transform = DockerOperator(
        task_id="dbt_run_staging_analytics",
        image="fishtownanalytics/dbt:1.0.0",
        api_version="auto",
        auto_remove=True,
        working_dir="/usr/app/dbt",
        environment={"DBT_PROFILES_DIR": "/usr/app/dbt"},
        network_mode="cookiecutterproject_slug_data-platform",
        command=["run", "--exclude", "raw_ohlc"],
        mounts=[{"source": f"{PROJECT_ROOT}/dbt", "target": "/usr/app/dbt", "type": "bind"}],
        mount_tmp_dir=False,
    )

    # Set after with cookicutter conf
    # run_dbt_raw  >> run_dbt_transform # real time ingestion with Kafka
    run_dbt_raw >> run_rust_ingestion >> load_to_postgres >> run_dbt_transform  #  Normal ingestion
