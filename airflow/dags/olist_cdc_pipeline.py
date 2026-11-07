"""CDC pipeline: Bronze is fed continuously by Debezium -> Kafka -> Spark.

This DAG checks the CDC path and turns Bronze into Silver and Gold:
    cdc_health   connector and its task RUNNING, no events quarantined lately
    dbt_build    the same dbt project the batch DAG runs, unchanged
    dq_summary   dbt warnings to Slack

Scheduled only when INGESTION_MODE=cdc; the batch DAG is scheduled otherwise, so
exactly one of them writes to DuckDB.
"""

import os
from datetime import datetime, timedelta

from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import dag
from alerts import notify_failure, send_dq_summary

PROJECT = "/opt/project"
VENV = "/opt/project-venv/bin"
CDC_MODE = os.getenv("INGESTION_MODE", "batch") == "cdc"


@dag(
    dag_id="olist_cdc_pipeline",
    schedule="*/15 * * * *" if CDC_MODE else None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,  # DuckDB allows a single writer
    default_args={
        "retries": 1,
        "retry_delay": timedelta(minutes=2),
        "on_failure_callback": notify_failure,
    },
    tags=["olist", "cdc"],
)
def olist_cdc_pipeline():
    # No downstream task on purpose: a broken connector must not stop Gold from
    # building on what already reached Bronze, and each failure alerts on its own.
    BashOperator(
        task_id="cdc_health",
        bash_command=f"cd {PROJECT} && PYTHONPATH={PROJECT}/src {VENV}/python -m olist.cdc_health",
        retries=0,
    )
    dbt_build = BashOperator(
        task_id="dbt_build",
        bash_command=f"cd {PROJECT}/dbt/olist && {VENV}/dbt deps --quiet && {VENV}/dbt build",
        retries=0,  # a failed data test will not pass on retry
    )
    dq_summary = PythonOperator(task_id="dq_summary", python_callable=send_dq_summary)

    dbt_build >> dq_summary


olist_cdc_pipeline()
