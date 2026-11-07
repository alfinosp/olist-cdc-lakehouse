# ADR 0004: Spark owns offsets, a shared writer owns Bronze

**Status:** Accepted

## Context

Debezium events have to land in Bronze in exactly the form the batch path writes, or every dbt model would need a CDC branch. Two options:

1. Parse the Debezium envelope with Spark SQL (`from_json` with a schema per table) and write Delta with Spark's Delta writer.
2. Let Spark handle Kafka (offsets, checkpoint, triggers, restarts) and pass each micro-batch to the Python Bronze writer the batch path already uses.

## Decision

Option 2. `streaming/cdc_to_bronze.py` reads the CDC topics with Structured Streaming and calls `olist.cdc.write_events` in `foreachBatch`.

- Field types come from the schema embedded in each event (`value.converter.schemas.enable=true`), so there is no per-table schema to maintain and a new source column flows through (`schema_mode="merge"` on write).
- Values are rendered as Postgres text (`MicroTimestamp` -> `2018-03-01 10:00:00`, `ZonedTimestamp` -> `2026-01-01 00:00:00+00`, numerics as exact strings), the same as `column::text` in the batch query.
- Events that cannot be decoded go to `bronze/_quarantine` instead of failing the stream.
- The decoder is plain Python, so it is unit-tested without Spark or Kafka, and CI runs dbt on Bronze produced by it.

## Consequences

- One Bronze writer, one contract, one set of tests. dbt does not change between paths.
- Delivery is at-least-once: a micro-batch written before its offsets were committed is written again after a restart. Replays are harmless because Silver keeps one row per key by `_source_ts`, then `_lsn`.
- Each micro-batch is collected to the driver. `maxOffsetsPerTrigger=20000` bounds memory, which is plenty for this workload; at real volume the decoder would move to Spark SQL and Spark's Delta writer, with `txnAppId`/`txnVersion` for idempotent writes.
- The JSON envelope with schemas is larger than Avro with a schema registry. Avro is the production choice; JSON keeps the stack smaller and the events readable.
