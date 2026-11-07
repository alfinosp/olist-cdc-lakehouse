"""Spark Structured Streaming: Debezium topics in Kafka -> Bronze Delta.

Spark owns the Kafka side: offsets, the checkpoint, micro-batch triggers and restarts.
Every micro-batch is decoded and appended by olist.cdc.write_events, the same Bronze
contract and writer the batch path uses (ADR 0004).

Delivery is at-least-once. If the job dies after writing a micro-batch but before
committing its offsets, the batch is written again on restart. The duplicates carry
the same _source_ts and _lsn, and Silver keeps one row per key, so Silver and Gold
stay exact.

Run inside the spark container (see docker-compose.cdc.yml):
    spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 streaming/cdc_to_bronze.py
"""

import logging
import os
import sys
from datetime import UTC, datetime

from pyspark.sql import DataFrame, SparkSession

from olist.cdc import KafkaRecord, write_events
from olist.config import LAKE_ROOT

log = logging.getLogger("cdc_to_bronze")

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
# olist_app.* carries the data. ops.debezium_heartbeat gets an event every 10 s even when
# the source is idle, so a micro-batch commits every trigger and cdc_health can tell a
# live pipeline from a dead one by the checkpoint's age.
TOPIC_PATTERN = os.getenv("CDC_TOPIC_PATTERN", r"olist\.(olist_app|ops)\..+")
CHECKPOINT = os.getenv("CDC_CHECKPOINT_DIR", str(LAKE_ROOT / "_checkpoints" / "cdc_to_bronze"))
TRIGGER = os.getenv("CDC_TRIGGER", "30 seconds")
MAX_OFFSETS_PER_TRIGGER = os.getenv("CDC_MAX_OFFSETS_PER_TRIGGER", "20000")


def handle_batch(batch: DataFrame, batch_id: int) -> None:
    records = [KafkaRecord(r.topic, r.partition, r.offset, r.value) for r in batch.collect()]
    if not records:
        return
    written = write_events(records, LAKE_ROOT, datetime.now(UTC))
    log.info("batch %d: %d records -> %s", batch_id, len(records), written)


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    spark = SparkSession.builder.appName("cdc_to_bronze").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    events = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", BOOTSTRAP)
        .option("subscribePattern", TOPIC_PATTERN)
        .option("startingOffsets", "earliest")
        .option("maxOffsetsPerTrigger", MAX_OFFSETS_PER_TRIGGER)
        .load()
        .selectExpr("topic", "partition", "offset", "cast(value as string) as value")
    )
    query = (
        events.writeStream.queryName("cdc_to_bronze")
        .foreachBatch(handle_batch)
        .option("checkpointLocation", CHECKPOINT)
        .trigger(processingTime=TRIGGER)
        .start()
    )
    log.info("Streaming %s from %s into %s/bronze", TOPIC_PATTERN, BOOTSTRAP, LAKE_ROOT)
    query.awaitTermination()
    return 0


if __name__ == "__main__":
    sys.exit(main())
