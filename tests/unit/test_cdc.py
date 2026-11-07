import json
from datetime import UTC, datetime

import debezium_fixture
import polars as pl
import pytest
from deltalake import DeltaTable
from make_fixture_bronze import fixture_events, fixture_frame, read_fixture

from olist.bronze import META_SCHEMA, add_metadata, bronze_path
from olist.cdc import QUARANTINE_TABLE, KafkaRecord, ParseError, parse_event, render, write_events
from olist.tables import TABLES

INGESTED_AT = datetime(2026, 11, 5, 8, 30, tzinfo=UTC)
TS_US = 1_767_225_600_000_000  # 2026-01-01 00:00:00 UTC
MICRO_TS = {"type": "int64", "name": "io.debezium.time.MicroTimestamp"}
MILLI_TS = {"type": "int64", "name": "io.debezium.time.Timestamp"}
ZONED_TS = {"type": "string", "name": "io.debezium.time.ZonedTimestamp"}
DATE = {"type": "int32", "name": "io.debezium.time.Date"}

ORDER = {
    "order_id": "o1",
    "customer_id": "c1",
    "order_status": "shipped",
    "order_purchase_timestamp": "2018-03-01 10:00:00.25",
    "order_approved_at": None,
    "created_at": "2026-01-01 00:00:00+00",
    "updated_at": "2026-01-02 08:00:00.5+00",
    "deleted_at": None,
}


def order_record(op: str = "u", row: dict = ORDER, offset: int = 0) -> KafkaRecord:
    return debezium_fixture.record("orders", debezium_fixture.event("orders", row, op, TS_US, 42), offset)


def with_source_table(envelope: dict, table: str) -> dict:
    payload = envelope["payload"]
    return {**envelope, "payload": {**payload, "source": {**payload["source"], "table": table}}}


def read_bronze(lake, table) -> pl.DataFrame:
    return pl.from_arrow(DeltaTable(str(bronze_path(lake, table))).to_pyarrow_table())


@pytest.mark.parametrize(
    ("value", "field", "expected"),
    [
        (None, {"type": "string"}, None),
        ("abc", {"type": "string"}, "abc"),
        (7, {"type": "int32"}, "7"),
        ("59.90", {"type": "string"}, "59.90"),  # decimal.handling.mode=string
        (True, {"type": "boolean"}, "true"),
        (1_519_898_400_000_000, MICRO_TS, "2018-03-01 10:00:00"),
        (1_519_898_400_123_400, MICRO_TS, "2018-03-01 10:00:00.1234"),
        (1_519_898_400_000, MILLI_TS, "2018-03-01 10:00:00"),
        ("2026-01-01T07:00:00+07:00", ZONED_TS, "2026-01-01 00:00:00+00"),
        ("2026-01-01T00:00:00.5Z", ZONED_TS, "2026-01-01 00:00:00.5+00"),
        (17_591, DATE, "2018-03-01"),
    ],
)
def test_render_matches_postgres_text(value, field, expected):
    assert render(value, field) == expected


def test_render_rejects_binary_decimal():
    with pytest.raises(ParseError, match="decimal.handling.mode"):
        render("AV8=", {"type": "bytes", "name": "org.apache.kafka.connect.data.Decimal"})


def test_parse_update_uses_after_row_and_source_metadata():
    table, row = parse_event(order_record("u"))
    assert table == "orders"
    assert {k: row[k] for k in ORDER} == ORDER  # round trip back to Postgres text
    assert row["_op"] == "u"
    assert row["_lsn"] == 42
    assert row["_source_ts"] == datetime(2026, 1, 1, tzinfo=UTC)


def test_parse_delete_uses_before_row():
    table, row = parse_event(order_record("d"))
    assert row["_op"] == "d"
    assert row["order_status"] == "shipped"


def test_parse_skips_tombstone():
    assert parse_event(KafkaRecord("olist.olist_app.orders", 0, 0, None)) is None


def test_parse_falls_back_to_ts_ms():
    payload = debezium_fixture.event("orders", ORDER, "c", TS_US, 1)
    del payload["payload"]["source"]["ts_us"]
    _, row = parse_event(debezium_fixture.record("orders", payload, 0))
    assert row["_source_ts"] == datetime(2026, 1, 1, tzinfo=UTC)


@pytest.mark.parametrize(
    ("mutate", "message"),
    [
        (lambda p: "{not json", "invalid JSON"),
        (lambda p: json.dumps(p["payload"]), "schema \\+ payload"),
        (lambda p: json.dumps({**p, "payload": {**p["payload"], "op": "x"}}), "unknown op"),
        (lambda p: json.dumps(with_source_table(p, "geolocation")), "not a pipeline table"),
        (lambda p: json.dumps({**p, "payload": {**p["payload"], "after": None}}), "without a 'after' row"),
    ],
)
def test_parse_rejects_events_outside_the_contract(mutate, message):
    payload = debezium_fixture.event("orders", ORDER, "u", TS_US, 1)
    with pytest.raises(ParseError, match=message):
        parse_event(KafkaRecord("olist.olist_app.orders", 0, 0, mutate(payload)))


def test_parse_rejects_delete_without_replica_identity_full():
    keys_only = {"order_id": "o1"} | {k: None for k in ORDER if k != "order_id"}
    with pytest.raises(ParseError, match="REPLICA IDENTITY FULL"):
        parse_event(order_record("d", keys_only))


def test_write_events_appends_bronze_and_quarantines_bad_events(tmp_path):
    records = [
        order_record("r", offset=0),
        order_record("u", {**ORDER, "order_status": "delivered"}, offset=1),
        KafkaRecord("olist.olist_app.orders", 0, 2, None),
        KafkaRecord("olist.olist_app.orders", 0, 3, "{broken"),
    ]
    assert write_events(records, tmp_path, INGESTED_AT) == {"orders": 2, QUARANTINE_TABLE: 1}

    orders = read_bronze(tmp_path, "orders")
    for column, dtype in META_SCHEMA.items():
        assert orders.schema[column] == dtype, column
    assert orders["_source"].unique().to_list() == ["cdc"]
    assert orders["_lsn"].to_list() == [42, 42]
    assert orders["order_status"].to_list() == ["shipped", "delivered"]

    quarantine = read_bronze(tmp_path, QUARANTINE_TABLE)
    assert quarantine.select("offset", "value").row(0) == (3, "{broken")
    assert "invalid JSON" in quarantine["error"][0]


def test_write_events_merges_new_source_columns(tmp_path):
    write_events([order_record("r")], tmp_path, INGESTED_AT)
    write_events([order_record("u", {**ORDER, "order_channel": "app"})], tmp_path, INGESTED_AT)
    orders = read_bronze(tmp_path, "orders").sort("_op")
    assert orders["order_channel"].to_list() == [None, "app"]


@pytest.mark.parametrize("table", list(TABLES))
def test_cdc_and_batch_paths_write_identical_source_columns(table, tmp_path):
    df = read_fixture(table)
    write_events(fixture_events(table, df, lsn_start=1), tmp_path, INGESTED_AT)
    via_cdc = read_bronze(tmp_path, table)
    via_batch = add_metadata(fixture_frame(df), "fixture", INGESTED_AT)

    # Batch only knows "row" or "deleted"; CDC also says insert/update. Silver only reads 'd'.
    is_delete = (pl.col("_op") == "d").alias("_is_delete")
    columns = [*df.columns, "_source_ts", is_delete]
    sort = TABLES[table]["pk"] + ["_source_ts"]
    assert via_cdc.select(columns).sort(sort).equals(via_batch.select(columns).sort(sort))
