"""Build a tiny Bronze layer from tests/fixtures/bronze/*.csv.

CI has no Postgres or Kafka, so this writes the Bronze contract from a handful of
hand-made rows, and dbt builds on top of it. Two paths, same rows:

    --via batch   rows written the way ingest_batch.py writes them
    --via cdc     rows encoded as Debezium events and decoded by olist.cdc, the code
                  the streaming job runs

CI runs dbt on both, unchanged, which is the proof that dbt does not care how a row
reached Bronze.

It writes to data/ci/lake (or data/ci/lake-cdc; override with FIXTURE_LAKE_ROOT),
never to the real lake, because it deletes the Bronze tables it is about to rebuild.

Usage:
    python scripts/make_fixture_bronze.py
    python scripts/make_fixture_bronze.py --via cdc
"""

import argparse
import os
import shutil
import sys
from datetime import UTC, datetime
from pathlib import Path

import polars as pl

from olist.bronze import UTC_TS, add_metadata, bronze_path, write_bronze
from olist.cdc import QUARANTINE_TABLE, write_events
from olist.config import PROJECT_ROOT
from olist.tables import TABLES

sys.path.insert(0, str(Path(__file__).resolve().parent))
import debezium_fixture  # noqa: E402

FIXTURES = PROJECT_ROOT / "tests" / "fixtures" / "bronze"
DEFAULT_LAKE = {
    "batch": PROJECT_ROOT / "data" / "ci" / "lake",
    "cdc": PROJECT_ROOT / "data" / "ci" / "lake-cdc",
}


def read_fixture(table: str) -> pl.DataFrame:
    return pl.read_csv(FIXTURES / f"{table}.csv", infer_schema=False)  # every column as string


def fixture_frame(df: pl.DataFrame) -> pl.DataFrame:
    return df.with_columns(
        pl.when(pl.col("deleted_at").is_not_null()).then(pl.lit("d")).otherwise(pl.lit("r"))
        .alias("_op"),
        # Postgres renders timestamptz as text like "2026-01-01 00:00:00+00"
        pl.col("updated_at")
        .str.to_datetime("%Y-%m-%d %H:%M:%S%#z", time_zone="UTC")
        .cast(UTC_TS)
        .alias("_source_ts"),
    )


def fixture_events(table: str, df: pl.DataFrame, lsn_start: int) -> list:
    """Snapshot read for a key's first row, update for later rows, delete when deleted_at is set."""
    pk = TABLES[table]["pk"]
    seen: set[tuple] = set()
    records = []
    for i, row in enumerate(df.iter_rows(named=True)):
        key = tuple(row[c] for c in pk)
        if row["deleted_at"] is not None:
            op = "d"
        elif key in seen:
            op = "u"
        else:
            op = "r"
        seen.add(key)
        updated = datetime.fromisoformat(row["updated_at"]).astimezone(UTC)
        ts_us = int(updated.timestamp()) * 1_000_000 + updated.microsecond
        payload = debezium_fixture.event(table, row, op, ts_us, lsn_start + i)
        records.append(debezium_fixture.record(table, payload, i))
    return records


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--via", choices=["batch", "cdc"], default="batch")
    args = parser.parse_args()
    lake = Path(os.getenv("FIXTURE_LAKE_ROOT", DEFAULT_LAKE[args.via]))

    ingested_at = datetime.now(UTC)
    for table in [*TABLES, QUARANTINE_TABLE]:
        target = bronze_path(lake, table)
        if target.exists():
            shutil.rmtree(target)

    for n, table in enumerate(TABLES):
        df = read_fixture(table)
        if args.via == "batch":
            written = write_bronze(add_metadata(fixture_frame(df), "fixture", ingested_at), lake, table)
        else:
            records = fixture_events(table, df, lsn_start=(n + 1) * 1_000)
            written = write_events(records, lake, ingested_at).get(table, 0)
        print(f"{table:<36} {written:>4} rows  via {args.via}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
