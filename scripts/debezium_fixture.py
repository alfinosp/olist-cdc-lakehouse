"""Build Debezium-style change events from Bronze fixture rows.

The inverse of olist.cdc: it encodes a row of Postgres text values the way the
Debezium Postgres connector would (JSON converter with schemas, decimal.handling.mode
=string, time.precision.mode=adaptive). Used by the CI fixture build and the unit
tests, so the CDC decoder is exercised without Kafka.
"""

import json
from datetime import UTC, datetime

from olist.cdc import KafkaRecord

TIMESTAMPTZ = {"created_at", "updated_at", "deleted_at"}
TIMESTAMP = {
    "order_purchase_timestamp",
    "order_approved_at",
    "order_delivered_carrier_date",
    "order_delivered_customer_date",
    "order_estimated_delivery_date",
    "shipping_limit_date",
    "review_creation_date",
    "review_answer_timestamp",
}
INTEGER = {"order_item_id", "payment_sequential", "payment_installments", "review_score"}

EPOCH = datetime(1970, 1, 1)


def field_schema(column: str) -> dict:
    field = {"field": column, "optional": True}
    if column in TIMESTAMPTZ:
        return field | {"type": "string", "name": "io.debezium.time.ZonedTimestamp"}
    if column in TIMESTAMP:
        return field | {"type": "int64", "name": "io.debezium.time.MicroTimestamp"}
    if column in INTEGER:
        return field | {"type": "int32"}
    return field | {"type": "string"}


def encode_value(column: str, text: str | None) -> object:
    if text is None:
        return None
    if column in TIMESTAMPTZ:
        # Postgres "2026-01-01 00:00:00+00" -> Debezium "2026-01-01T00:00:00Z"
        dt = datetime.fromisoformat(text).astimezone(UTC)
        return dt.strftime("%Y-%m-%dT%H:%M:%S.%f").rstrip("0").rstrip(".") + "Z"
    if column in TIMESTAMP:
        delta = datetime.fromisoformat(text) - EPOCH
        return (delta.days * 86_400 + delta.seconds) * 1_000_000 + delta.microseconds
    if column in INTEGER:
        return int(text)
    return text


def event(table: str, row: dict[str, str | None], op: str, ts_us: int, lsn: int) -> dict:
    """One Debezium envelope. `row` holds Postgres text values; deletes carry it as `before`."""
    columns = list(row)
    row_schema = {"type": "struct", "optional": True, "fields": [field_schema(c) for c in columns]}
    encoded = {c: encode_value(c, row[c]) for c in columns}
    return {
        "schema": {
            "type": "struct",
            "name": f"olist.olist_app.{table}.Envelope",
            "fields": [
                {**row_schema, "field": "before"},
                {**row_schema, "field": "after"},
                {"field": "source", "type": "struct", "optional": False, "fields": []},
                {"field": "op", "type": "string", "optional": False},
                {"field": "ts_ms", "type": "int64", "optional": True},
            ],
        },
        "payload": {
            "before": encoded if op in ("u", "d") else None,
            "after": None if op == "d" else encoded,
            "source": {
                "connector": "postgresql",
                "name": "olist",
                "db": "olist",
                "schema": "olist_app",
                "table": table,
                "ts_ms": ts_us // 1000,
                "ts_us": ts_us,
                "lsn": lsn,
                "snapshot": "true" if op == "r" else "false",
            },
            "op": op,
            "ts_ms": ts_us // 1000,
        },
    }


def record(table: str, payload: dict, offset: int) -> KafkaRecord:
    return KafkaRecord(f"olist.olist_app.{table}", 0, offset, json.dumps(payload))
