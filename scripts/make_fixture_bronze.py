"""Build a tiny Bronze layer from tests/fixtures/bronze/*.csv.

CI has no Postgres, so this writes the same Bronze contract the batch path writes,
from a handful of hand-made rows, and dbt builds on top of it.

It writes to data/ci/lake (override with FIXTURE_LAKE_ROOT), never to the real lake,
because it deletes the Bronze tables it is about to rebuild.

Usage:
    python scripts/make_fixture_bronze.py
"""

import os
import shutil
import sys
from datetime import UTC, datetime
from pathlib import Path

import polars as pl

from olist.bronze import UTC_TS, add_metadata, bronze_path, write_bronze
from olist.config import PROJECT_ROOT
from olist.tables import TABLES

FIXTURES = PROJECT_ROOT / "tests" / "fixtures" / "bronze"
FIXTURE_LAKE = Path(os.getenv("FIXTURE_LAKE_ROOT", PROJECT_ROOT / "data" / "ci" / "lake"))


def fixture_frame(csv_path: Path) -> pl.DataFrame:
    df = pl.read_csv(csv_path, infer_schema=False)  # every column as string
    return df.with_columns(
        pl.when(pl.col("deleted_at").is_not_null()).then(pl.lit("d")).otherwise(pl.lit("r"))
        .alias("_op"),
        # Postgres renders timestamptz as text like "2026-01-01 00:00:00+00"
        pl.col("updated_at")
        .str.to_datetime("%Y-%m-%d %H:%M:%S%#z", time_zone="UTC")
        .cast(UTC_TS)
        .alias("_source_ts"),
    )


def main() -> int:
    ingested_at = datetime.now(UTC)
    for table in TABLES:
        target = bronze_path(FIXTURE_LAKE, table)
        if target.exists():
            shutil.rmtree(target)
        df = add_metadata(fixture_frame(FIXTURES / f"{table}.csv"), "fixture", ingested_at)
        print(f"{table:<36} {write_bronze(df, FIXTURE_LAKE, table):>4} rows")
    return 0


if __name__ == "__main__":
    sys.exit(main())
