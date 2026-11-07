"""Batch pipeline: Postgres -> Bronze (watermark) -> dbt (Silver, Gold, tests) -> DQ summary.

Scheduled only when INGESTION_MODE=batch (the default); with INGESTION_MODE=cdc the
olist_cdc_pipeline DAG runs instead.
"""

import os
from datetime import datetime, timedelta

from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import dag
from alerts import notify_failure, send_dq_summary

PROJECT = "/opt/project"
VENV = "/opt/project-venv/bin"
BATCH_MODE = os.getenv("INGESTION_MODE", "batch") == "batch"


@dag(
    dag_id="olist_batch_pipeline",
    schedule="*/15 * * * *" if BATCH_MODE else None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,  # DuckDB allows a single writer
    default_args={
        "retries": 1,
        "retry_delay": timedelta(minutes=2),
        "on_failure_callback": notify_failure,
    },
    tags=["olist", "batch"],
)
def olist_batch_pipeline():
    ingest = BashOperator(
        task_id="ingest_bronze",
        bash_command=f"cd {PROJECT} && PYTHONPATH={PROJECT}/src {VENV}/python -m olist.ingest_batch",
    )
    dbt_build = BashOperator(
        task_id="dbt_build",
        bash_command=f"cd {PROJECT}/dbt/olist && {VENV}/dbt deps --quiet && {VENV}/dbt build",
        retries=0,  # a failed data test will not pass on retry
    )
    dq_summary = PythonOperator(task_id="dq_summary", python_callable=send_dq_summary)

    ingest >> dbt_build >> dq_summary


olist_batch_pipeline()
