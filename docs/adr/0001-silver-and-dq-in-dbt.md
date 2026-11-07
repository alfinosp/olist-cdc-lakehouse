# ADR 0001: Silver, Gold and data quality in dbt

**Status:** Accepted

## Context

Bronze is written by two ingestion paths: a CDC stream (Debezium, Kafka, Spark) and a Python batch job. Deduplication, delete handling, validation and data quality checks could live in each writer, in a separate validation library, or in dbt.

## Decision

Writers only land data in Bronze. Everything after Bronze is dbt:

- Silver models are incremental (`delete+insert` on a primary-key hash) and keep the newest version per key by `_source_ts`, never replacing a newer row with an older one.
- Rows that break validation rules are flagged in `_dq_errors`, exposed in `quarantine_rows`, and excluded from staging and Gold.
- Data tests carry a severity: `error` stops the pipeline, `warn` is reported.
- An `on-run-end` hook writes every test result to `audit.dq_audit_log`.

## Consequences

- One place for transformation and quality rules, visible in dbt lineage and docs.
- Switching Bronze from batch to CDC needs no change to any dbt model.
- Rules are SQL, so they are easy to review but limited to what SQL expresses well.
- CDC events that cannot even be decoded are handled before dbt, in `bronze/_quarantine` (ADR 0004).
