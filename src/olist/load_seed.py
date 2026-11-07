"""Load the Olist CSV files into PostgreSQL (schema olist_app).

Usage:
    python -m olist.load_seed            # refuses to load into non-empty tables
    python -m olist.load_seed --force    # truncates tables and resets watermarks first
"""

import argparse
import logging
import sys
from pathlib import Path

import psycopg
from psycopg import sql

from olist.config import RAW_DIR, pg_conninfo
from olist.tables import TABLES

log = logging.getLogger("load_seed")


def csv_columns(csv_path: Path) -> list[str]:
    with csv_path.open("r", encoding="utf-8-sig") as f:
        header = f.readline().strip()
    return [c.strip().strip('"') for c in header.split(",")]


def table_has_rows(cur: psycopg.Cursor, table: str) -> bool:
    cur.execute(sql.SQL("select exists (select 1 from olist_app.{})").format(sql.Identifier(table)))
    return cur.fetchone()[0]


def load_table(cur: psycopg.Cursor, table: str, csv_path: Path) -> int:
    columns = csv_columns(csv_path)
    copy_stmt = sql.SQL(
        "copy olist_app.{table} ({cols}) from stdin with (format csv, header true)"
    ).format(
        table=sql.Identifier(table),
        cols=sql.SQL(", ").join(sql.Identifier(c) for c in columns),
    )
    with csv_path.open("rb") as f, cur.copy(copy_stmt) as copy:
        while chunk := f.read(1 << 20):
            copy.write(chunk)
    cur.execute(sql.SQL("select count(*) from olist_app.{}").format(sql.Identifier(table)))
    return cur.fetchone()[0]


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--force", action="store_true", help="truncate tables before loading")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    missing = [m["csv"] for m in TABLES.values() if not (RAW_DIR / m["csv"]).exists()]
    if missing:
        log.error("CSV not found in %s: %s. Run `make download` first.", RAW_DIR, missing)
        return 1

    with psycopg.connect(pg_conninfo()) as conn, conn.cursor() as cur:
        non_empty = [t for t in TABLES if table_has_rows(cur, t)]
        if non_empty and not args.force:
            log.error("Tables already contain data: %s. Use --force to reload.", non_empty)
            return 1
        if args.force:
            for table in TABLES:
                cur.execute(sql.SQL("truncate table olist_app.{}").format(sql.Identifier(table)))
            cur.execute("truncate table ops.ingest_watermark")
            log.info("Truncated all olist_app tables and watermarks")

        for table, meta in TABLES.items():
            rows = load_table(cur, table, RAW_DIR / meta["csv"])
            log.info("Loaded %-36s %8d rows", table, rows)
        conn.commit()
    return 0


if __name__ == "__main__":
    sys.exit(main())
