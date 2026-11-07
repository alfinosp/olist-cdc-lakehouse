FROM apache/airflow:3.3.2-python3.12

# Project code (ingestion, dbt) runs from its own virtualenv so its dependencies
# never clash with Airflow's pinned constraints.
USER root
RUN python -m venv /opt/project-venv && chown -R airflow:0 /opt/project-venv

USER airflow
COPY requirements.txt /tmp/requirements.txt
RUN /opt/project-venv/bin/pip install --no-cache-dir -r /tmp/requirements.txt \
 && /opt/project-venv/bin/python -c "import duckdb; duckdb.sql('install delta'); duckdb.sql('install postgres')"

# The container runs as your WSL user (AIRFLOW_UID), who must be able to read the
# DuckDB extensions installed above.
USER root
RUN chmod -R a+rX /home/airflow/.duckdb
USER airflow
