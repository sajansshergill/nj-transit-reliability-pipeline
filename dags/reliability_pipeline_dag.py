"""Airflow DAG: extract -> validate -> transform -> validate -> load -> model.

Validation tasks quarantine a bad batch and raise, so failures stop the run
before anything reaches the warehouse. Ingestion/validation/load run as
Python callables; Spark and dbt run as their own processes.
"""
from __future__ import annotations

import os
import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.exceptions import AirflowFailException
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

# Make the project importable when the DAG folder is mounted into Airflow.
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from ingestion import gtfs_realtime, gtfs_static, weather  # noqa: E402
from ingestion.storage import quarantine, read_ndjson_df  # noqa: E402
from quality import validate  # noqa: E402

DEFAULT_ARGS = {
    "owner": "data-eng",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


def _ingest_realtime(ds, ti, **_):
    ti.xcom_push(key="rt_key", value=gtfs_realtime.ingest(ds))


def _ingest_static(ds, **_):
    gtfs_static.ingest(ds)


def _ingest_weather(ds, **_):
    weather.ingest(ds)


def _validate_bronze(ds, ti, **_):
    key = ti.xcom_pull(task_ids="ingest_realtime", key="rt_key")
    df = read_ndjson_df(key)
    result = validate.validate_dataframe(
        df, "bronze_realtime", validate.bronze_realtime_expectations
    )
    if not result.success:
        dest = quarantine(key, result.summary())
        raise AirflowFailException(
            f"bronze validation failed: {result.summary()} (quarantined -> {dest})"
        )


def _validate_silver(ds, **_):
    # In prod this reads the silver parquet via pandas/pyarrow from S3.
    # Left as a hook so the gate is explicit in the DAG graph.
    import pandas as pd
    from common.config import settings

    path = f"s3://{settings.s3_bucket}/silver/stop_performance/dt={ds}"
    df = pd.read_parquet(path)
    result = validate.validate_dataframe(
        df, "silver", validate.silver_expectations
    )
    if not result.success:
        raise AirflowFailException(f"silver validation failed: {result.summary()}")


with DAG(
    dag_id="reliability_pipeline_dag",
    description="NJ Transit reliability ETL/ELT with data-quality gates",
    default_args=DEFAULT_ARGS,
    schedule="@hourly",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["data-engineering", "gtfs", "quality"],
) as dag:

    ingest_realtime = PythonOperator(
        task_id="ingest_realtime", python_callable=_ingest_realtime
    )
    ingest_static = PythonOperator(
        task_id="ingest_static", python_callable=_ingest_static
    )
    ingest_weather = PythonOperator(
        task_id="ingest_weather", python_callable=_ingest_weather
    )

    validate_bronze = PythonOperator(
        task_id="validate_bronze", python_callable=_validate_bronze
    )

    build_silver = BashOperator(
        task_id="build_silver",
        bash_command=(
            "spark-submit --master ${SPARK_MASTER:-local[*]} "
            f"{PROJECT_ROOT}/spark/build_silver.py --ds {{{{ ds }}}}"
        ),
    )

    validate_silver = PythonOperator(
        task_id="validate_silver", python_callable=_validate_silver
    )

    load_warehouse = BashOperator(
        task_id="load_to_warehouse",
        bash_command=f"python {PROJECT_ROOT}/load/load_to_warehouse.py --ds {{{{ ds }}}}",
    )

    dbt_build = BashOperator(
        task_id="dbt_build",
        bash_command=(
            f"cd {PROJECT_ROOT}/dbt && dbt build --target ${{DBT_TARGET:-snowflake}}"
        ),
    )

    [ingest_realtime, ingest_static, ingest_weather] >> validate_bronze
    validate_bronze >> build_silver >> validate_silver >> load_warehouse >> dbt_build