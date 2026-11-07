"""Bronze contract shared by every ingestion path (batch watermark and CDC).

Bronze is append-only. Source columns are stored as strings exactly as the source
rendered them; typing happens in Silver (dbt). Metadata columns:

    _op           'r' (row read), 'c' / 'u' (insert / update, CDC), 'd' (delete)
    _source       'batch' | 'cdc' | 'fixture'
    _source_ts    when the row changed at the source (UTC)
    _lsn          Postgres log sequence number (CDC only, null for batch)
    _ingested_at  when the row landed in Bronze (UTC)
    _ingest_date  partition column, yyyy-mm-dd of _ingested_at
"""

from datetime import datetime
from pathlib import Path

import polars as pl
from deltalake import write_deltalake

UTC_TS = pl.Datetime("us", "UTC")

META_SCHEMA: dict[str, pl.DataType] = {
    "_op": pl.Utf8,
    "_source": pl.Utf8,
    "_source_ts": UTC_TS,
    "_lsn": pl.Int64,
    "_ingested_at": UTC_TS,
    "_ingest_date": pl.Utf8,
}


def bronze_path(lake_root: Path, table: str) -> Path:
    return lake_root / "bronze" / table


def add_metadata(df: pl.DataFrame, source: str, ingested_at: datetime) -> pl.DataFrame:
    """Add the metadata columns that are the same for every row of one ingestion run.

    `df` must already hold `_op` and `_source_ts`. A `_lsn` column, when present (CDC),
    is kept; otherwise it is added as null.
    """
    lsn = pl.col("_lsn").cast(pl.Int64) if "_lsn" in df.columns else pl.lit(None, dtype=pl.Int64)
    return df.with_columns(
        pl.lit(source, dtype=pl.Utf8).alias("_source"),
        lsn.alias("_lsn"),
        pl.lit(ingested_at, dtype=UTC_TS).alias("_ingested_at"),
        pl.lit(ingested_at.strftime("%Y-%m-%d"), dtype=pl.Utf8).alias("_ingest_date"),
    )


def write_bronze(df: pl.DataFrame, lake_root: Path, table: str) -> int:
    """Append a frame to the table's Bronze Delta table. Returns rows written."""
    if df.is_empty():
        return 0
    path = bronze_path(lake_root, table)
    path.parent.mkdir(parents=True, exist_ok=True)
    write_deltalake(
        str(path),
        df.to_arrow(),
        mode="append",
        partition_by=["_ingest_date"],
        schema_mode="merge",
    )
    return df.height
