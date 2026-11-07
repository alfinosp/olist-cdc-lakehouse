from datetime import UTC, datetime

import polars as pl
from deltalake import DeltaTable

from olist.bronze import META_SCHEMA, UTC_TS, add_metadata, bronze_path, write_bronze
from olist.ingest_batch import to_frame

INGESTED_AT = datetime(2026, 11, 5, 8, 30, tzinfo=UTC)


def sample_frame() -> pl.DataFrame:
    rows = [
        ("o1", "delivered", None, "r", datetime(2026, 11, 5, 8, 0, tzinfo=UTC)),
        ("o2", "created", "2026-11-05 08:10:00+00", "d", datetime(2026, 11, 5, 8, 10, tzinfo=UTC)),
    ]
    return to_frame(rows, ["order_id", "order_status", "deleted_at"])


def test_to_frame_keeps_source_columns_as_strings():
    df = sample_frame()
    assert df.schema["order_id"] == pl.Utf8
    assert df.schema["deleted_at"] == pl.Utf8  # all-null column must not become Null type
    assert df.schema["_source_ts"] == UTC_TS


def test_add_metadata_matches_bronze_contract():
    df = add_metadata(sample_frame(), "batch", INGESTED_AT)
    for column, dtype in META_SCHEMA.items():
        assert df.schema[column] == dtype, column
    assert df["_ingest_date"].unique().to_list() == ["2026-11-05"]
    assert df["_lsn"].null_count() == df.height


def test_write_bronze_appends_and_partitions(tmp_path):
    df = add_metadata(sample_frame(), "batch", INGESTED_AT)
    assert write_bronze(df, tmp_path, "orders") == 2
    assert write_bronze(df, tmp_path, "orders") == 2  # append-only: a rerun adds rows

    table = DeltaTable(str(bronze_path(tmp_path, "orders")))
    assert table.metadata().partition_columns == ["_ingest_date"]
    assert pl.from_arrow(table.to_pyarrow_table()).height == 4


def test_write_bronze_skips_empty_frame(tmp_path):
    empty = add_metadata(to_frame([], ["order_id"]), "batch", INGESTED_AT)
    assert write_bronze(empty, tmp_path, "orders") == 0
    assert not bronze_path(tmp_path, "orders").exists()
