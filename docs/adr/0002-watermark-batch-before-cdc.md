# ADR 0002: Watermark batch ingestion as the fallback path

**Status:** Accepted

## Context

The main ingestion path is change data capture (Debezium, Kafka, Spark). That stack needs about 4 GB of extra memory and has more parts that can fail. A lighter path is useful for development, for machines that cannot run the full stack, and as a fallback when CDC is down.

## Decision

A batch job reads changed rows by watermark on `updated_at`:

- The window is `(last_watermark, now() - 10 seconds]`; the lag covers transactions that commit just after the run starts.
- The watermark is saved only after the Bronze write succeeds (at-least-once). Duplicates are removed in Silver.
- Deletes are soft (`deleted_at`), so they bump `updated_at` and are captured.

Both paths write the same Bronze contract: source columns as text plus `_op`, `_source`, `_source_ts`, `_lsn`, `_ingested_at`, `_ingest_date`. `INGESTION_MODE` decides which Airflow DAG is scheduled.

## Consequences

- Hard deletes are invisible to the batch path; CDC sees them.
- Every run queries the source tables (an index on `updated_at` keeps it cheap); CDC reads the WAL instead.
- Latency is bounded by the schedule (15 minutes) instead of the streaming trigger (30 seconds).
- Switching paths needs no Bronze migration and no dbt change. Rows already in Bronze from the other path are deduplicated in Silver by `_source_ts`.
