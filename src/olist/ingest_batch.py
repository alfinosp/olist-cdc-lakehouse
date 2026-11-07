"""Batch ingestion: copy rows changed since the last watermark from Postgres to Bronze.

The window is (last_watermark, now - 10 s]. The 10-second lag leaves room for
transactions that started before the run but commit just after it.
The watermark moves only after the Delta write succeeds, so a crash between the
two re-reads the same window on the next run (at-least-once). Silver dedupes.

Usage:
    python -m olist.ingest_batch                    # all tables
    python -m olist.ingest_batch --tables orders    # one or more tables
"""

import argparse
import logging
import sys
from datetime import UTC, datetime

import polars as pl
import psycopg
from psycopg import sql

from olist.bronze import UTC_TS, add_metadata, write_bronze
from olist.config import LAKE_ROOT, pg_conninfo
from olist.tables import TABLES

log = logging.getLogger("ingest_batch")

EPOCH = datetime(1970, 1, 1, tzinfo=UTC)
SAFETY_LAG = "10 seconds"


def source_columns(cur: psycopg.Cursor, table: str) -> list[str]:
    cur.execute(
        """
        select column_name
        from information_schema.columns
        where table_schema = 'olist_app' and table_name = %s
        order by ordinal_position
        """,
        (table,),
    )
    return [r[0] for r in cur.fetchall()]


def read_watermark(cur: psycopg.Cursor, table: str) -> datetime:
    cur.execute("select last_watermark from ops.ingest_watermark where table_name = %s", (table,))
    row = cur.fetchone()
    return row[0] if row else EPOCH


def save_watermark(cur: psycopg.Cursor, table: str, watermark: datetime, rows: int) -> None:
    cur.execute(
        """
        insert into ops.ingest_watermark (table_name, last_watermark, rows_last_run, updated_at)
        values (%s, %s, %s, now())
        on conflict (table_name) do update
            set last_watermark = excluded.last_watermark,
                rows_last_run  = excluded.rows_last_run,
                updated_at     = now()
        """,
        (table, watermark, rows),
    )


def changed_rows_query(table: str, columns: list[str]) -> sql.Composed:
    """Every source column cast to text, plus _op and _source_ts."""
    casts = sql.SQL(", ").join(
        sql.SQL("{col}::text as {col}").format(col=sql.Identifier(c)) for c in columns
    )
    return sql.SQL(
        """
        select {casts},
               case when deleted_at is not null then 'd' else 'r' end as _op,
               updated_at as _source_ts
        from olist_app.{table}
        where updated_at > %s and updated_at <= %s
        order by updated_at
        """
    ).format(casts=casts, table=sql.Identifier(table))


def to_frame(rows: list[tuple], columns: list[str]) -> pl.DataFrame:
    schema = {c: pl.Utf8 for c in columns} | {"_op": pl.Utf8, "_source_ts": UTC_TS}
    return pl.DataFrame(rows, schema=schema, orient="row")


def ingest_table(conn: psycopg.Connection, table: str) -> int:
    with conn.cursor() as cur:
        low = read_watermark(cur, table)
        cur.execute(f"select clock_timestamp() - interval '{SAFETY_LAG}'")
        high = cur.fetchone()[0]
        columns = source_columns(cur, table)

        cur.execute(changed_rows_query(table, columns), (low, high))
        df = to_frame(cur.fetchall(), columns)

    written = write_bronze(add_metadata(df, "batch", datetime.now(UTC)), LAKE_ROOT, table)

    with conn.cursor() as cur:
        save_watermark(cur, table, high, written)
    conn.commit()
    log.info("%-36s %8d rows  window (%s, %s]", table, written, low, high)
    return written


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--tables", nargs="*", default=list(TABLES), choices=list(TABLES))
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    with psycopg.connect(pg_conninfo()) as conn:
        conn.execute("set timezone to 'UTC'")
        total = sum(ingest_table(conn, t) for t in args.tables)
    log.info("Done, %d rows written to Bronze", total)
    return 0


if __name__ == "__main__":
    sys.exit(main())
