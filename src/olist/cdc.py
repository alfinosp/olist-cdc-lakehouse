"""Decode Debezium change events into the Bronze contract.

Plain Python, no Spark. The streaming job (streaming/cdc_to_bronze.py) hands every
micro-batch of Kafka records to `write_events`; the unit tests and the CI fixture
build call the same function.

Each source value is rendered as the text Postgres prints for `column::text` in a UTC
session, which is what the batch path stores. A CDC row and a batch row of the same
version are therefore identical in Bronze, and dbt cannot tell the paths apart.

Connector settings this module relies on (infra/debezium/olist-postgres-connector.json):
    value.converter.schemas.enable=true   field types travel with each event
    decimal.handling.mode=string          numerics arrive as exact strings
    time.precision.mode=adaptive          timestamp -> MicroTimestamp, timestamptz -> ZonedTimestamp
    REPLICA IDENTITY FULL on every table  delete events carry the whole row

An event that cannot be decoded is written to bronze/_quarantine with the error and
its Kafka coordinates instead of stopping the stream.
"""

import json
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import polars as pl

from olist.bronze import UTC_TS, add_metadata, write_bronze
from olist.tables import TABLES

QUARANTINE_TABLE = "_quarantine"
OPS = {"r", "c", "u", "d"}

_EPOCH_UTC = datetime(1970, 1, 1, tzinfo=UTC)
_EPOCH_NAIVE = datetime(1970, 1, 1)
_EPOCH_DATE = date(1970, 1, 1)


class ParseError(ValueError):
    """The event does not fit the Bronze contract."""


@dataclass(frozen=True)
class KafkaRecord:
    topic: str
    partition: int
    offset: int
    value: str | None


def _pg_timestamp(dt: datetime) -> str:
    """Postgres text form: no trailing zeros in the fraction, no fraction when zero."""
    text = dt.strftime("%Y-%m-%d %H:%M:%S")
    if dt.microsecond:
        text += "." + f"{dt.microsecond:06d}".rstrip("0")
    return text


def render(value: object, field: dict) -> str | None:
    """Render one Debezium value as the text Postgres would print for it."""
    if value is None:
        return None
    name = field.get("name")
    if name == "io.debezium.time.MicroTimestamp":
        return _pg_timestamp(_EPOCH_NAIVE + timedelta(microseconds=value))
    if name in ("io.debezium.time.Timestamp", "org.apache.kafka.connect.data.Timestamp"):
        return _pg_timestamp(_EPOCH_NAIVE + timedelta(milliseconds=value))
    if name == "io.debezium.time.NanoTimestamp":
        return _pg_timestamp(_EPOCH_NAIVE + timedelta(microseconds=value // 1000))
    if name == "io.debezium.time.ZonedTimestamp":
        return _pg_timestamp(datetime.fromisoformat(value).astimezone(UTC)) + "+00"
    if name in ("io.debezium.time.Date", "org.apache.kafka.connect.data.Date"):
        return (_EPOCH_DATE + timedelta(days=value)).isoformat()
    if name == "org.apache.kafka.connect.data.Decimal":
        raise ParseError("binary decimal received; set decimal.handling.mode=string on the connector")
    if field.get("type") == "boolean":
        return "true" if value else "false"
    if isinstance(value, dict | list):
        return json.dumps(value, separators=(",", ":"))
    return str(value)


def _image_fields(schema: dict, image: str) -> list[dict]:
    for field in schema.get("fields", []):
        if field.get("field") == image:
            return field.get("fields", [])
    raise ParseError(f"schema has no '{image}' field")


def parse_event(record: KafkaRecord) -> tuple[str, dict] | None:
    """Decode one Kafka record into (table, Bronze row). None for a tombstone or heartbeat.

    Raises ParseError when the record does not fit the Bronze contract.
    """
    if record.value is None:
        return None
    try:
        message = json.loads(record.value)
    except json.JSONDecodeError as exc:
        raise ParseError(f"invalid JSON: {exc.msg}") from None
    if not isinstance(message, dict) or "payload" not in message or "schema" not in message:
        raise ParseError("expected a schema + payload envelope (value.converter.schemas.enable=true)")

    payload = message["payload"]
    if payload is None:
        return None
    source = payload.get("source") or {}
    if source.get("schema") == "ops":
        return None  # ops.debezium_heartbeat: keeps the stream and its checkpoint moving
    op = payload.get("op")
    if op not in OPS:
        raise ParseError(f"unknown op {op!r}")
    table = source.get("table")
    if table not in TABLES:
        raise ParseError(f"table {table!r} is not a pipeline table")

    image = "before" if op == "d" else "after"
    row = payload.get(image)
    if not isinstance(row, dict):
        raise ParseError(f"op {op!r} without a '{image}' row")
    if op == "d" and row.get("created_at") is None:
        raise ParseError("delete without the full row; set REPLICA IDENTITY FULL on the source table")

    ts_us = source.get("ts_us")
    if ts_us is None and source.get("ts_ms") is not None:
        ts_us = source["ts_ms"] * 1000
    if ts_us is None:
        raise ParseError("source.ts_ms is missing")

    values: dict[str, object] = {}
    for field in _image_fields(message["schema"], image):
        column = field["field"]
        try:
            values[column] = render(row.get(column), field)
        except ParseError:
            raise
        except (TypeError, ValueError, OverflowError) as exc:
            raise ParseError(f"column {column}: {exc}") from None
    values["_op"] = op
    values["_source_ts"] = _EPOCH_UTC + timedelta(microseconds=ts_us)
    values["_lsn"] = source.get("lsn")
    return table, values


def to_frame(rows: list[dict]) -> pl.DataFrame:
    """Rows of one table -> frame with source columns as strings (union of all rows' columns)."""
    columns: dict[str, None] = {}
    for row in rows:
        columns.update(dict.fromkeys(c for c in row if not c.startswith("_")))
    schema = {c: pl.Utf8 for c in columns} | {"_op": pl.Utf8, "_source_ts": UTC_TS, "_lsn": pl.Int64}
    return pl.DataFrame(rows, schema=schema)


def _quarantine_frame(bad: list[dict]) -> pl.DataFrame:
    return pl.DataFrame(
        bad,
        schema={
            "topic": pl.Utf8,
            "partition": pl.Int64,
            "offset": pl.Int64,
            "value": pl.Utf8,
            "error": pl.Utf8,
            "_op": pl.Utf8,
            "_source_ts": UTC_TS,
        },
    )


def write_events(records: Iterable[KafkaRecord], lake_root: Path, ingested_at: datetime) -> dict[str, int]:
    """Decode records and append them to Bronze. Returns rows written per table.

    Undecodable records go to bronze/_quarantine; tombstones are skipped.
    """
    rows: dict[str, list[dict]] = {}
    bad: list[dict] = []
    for record in records:
        try:
            parsed = parse_event(record)
        except ParseError as exc:
            bad.append(
                {
                    "topic": record.topic,
                    "partition": record.partition,
                    "offset": record.offset,
                    "value": record.value,
                    "error": str(exc),
                    "_op": None,
                    "_source_ts": None,
                }
            )
            continue
        if parsed is not None:
            table, values = parsed
            rows.setdefault(table, []).append(values)

    written = {}
    for table, table_rows in rows.items():
        frame = add_metadata(to_frame(table_rows), "cdc", ingested_at)
        written[table] = write_bronze(frame, lake_root, table)
    if bad:
        frame = add_metadata(_quarantine_frame(bad), "cdc", ingested_at)
        written[QUARANTINE_TABLE] = write_bronze(frame, lake_root, QUARANTINE_TABLE)
    return written
