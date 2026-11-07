-- Runs once, on the first start of an empty Postgres volume.
-- Airflow keeps its metadata in its own database on the same server.
create database airflow;
