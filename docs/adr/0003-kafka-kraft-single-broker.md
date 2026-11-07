# ADR 0003: Kafka in KRaft mode with a single broker

**Status:** Accepted

## Context

Debezium needs Kafka (through Kafka Connect). The whole stack has to run on a 16 GB laptop next to Postgres, Airflow and Spark.

## Decision

- One Kafka 3.8 node in KRaft mode acting as both broker and controller. No ZooKeeper.
- Replication factor 1 for every topic, including the Connect internal topics.
- One partition per CDC topic. Debezium keys events by primary key, and a single partition keeps every table's changes in commit order, which the Silver reconciliation test relies on.
- 7-day retention on CDC topics. Bronze is the system of record; Kafka is a buffer.
- Kafka state lives in the container, not a volume. `make down` stops containers and keeps it; `make cdc-reset` deletes it on purpose.

## Consequences

- About 1 GB of RAM for Kafka instead of three brokers plus ZooKeeper.
- No fault tolerance: losing the broker loses unconsumed events. Recovery is `make cdc-reset && make cdc-up`, which re-snapshots the source; Silver absorbs the repeated rows.
- One partition caps throughput per table at what a single consumer can read, far above what the simulator produces. A production setup would use 3 brokers, replication factor 3, and more partitions keyed by primary key.
