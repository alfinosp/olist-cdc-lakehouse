"""Prepare Postgres for Debezium and register (or update) the connector.

Idempotent, safe to run any number of times:
1. ops.debezium_heartbeat exists (the connector writes to it so the replication slot
   keeps advancing while the olist tables are quiet).
2. Every pipeline table has REPLICA IDENTITY FULL, so delete events carry the whole row.
3. PUT /connectors/<name>/config creates the connector or updates its config.

Usage:
    python scripts/register_connector.py
    python scripts/register_connector.py --connect-url http://localhost:8083 --wait 120
"""

import argparse
import json
import logging
import os
import sys
import time
from pathlib import Path
from string import Template

import psycopg
import requests
from psycopg import sql

from olist.config import PROJECT_ROOT, pg_conninfo
from olist.tables import TABLES

log = logging.getLogger("register_connector")

CONNECTOR = "olist-postgres"
CONFIG_FILE = PROJECT_ROOT / "infra" / "debezium" / "olist-postgres-connector.json"


def connector_config(env: dict[str, str], path: Path = CONFIG_FILE) -> dict:
    """Connector config with ${PG_*} filled from the environment (defaults match .env.example)."""
    values = {"PG_USER": "olist", "PG_PASSWORD": "olist", "PG_DB": "olist"} | {
        k: v for k, v in env.items() if k in ("PG_USER", "PG_PASSWORD", "PG_DB")
    }
    return json.loads(Template(path.read_text()).substitute(values))


def prepare_source(conn: psycopg.Connection) -> None:
    with conn.cursor() as cur:
        cur.execute(
            "create table if not exists ops.debezium_heartbeat ("
            "id integer primary key, ts timestamptz not null)"
        )
        for table in TABLES:
            cur.execute(
                sql.SQL("alter table olist_app.{} replica identity full").format(sql.Identifier(table))
            )
    conn.commit()


def wait_for_connect(url: str, timeout_s: int) -> None:
    deadline = time.monotonic() + timeout_s
    while True:
        try:
            requests.get(f"{url}/connectors", timeout=5).raise_for_status()
            return
        except requests.RequestException:
            if time.monotonic() > deadline:
                raise
            log.info("Waiting for Kafka Connect at %s ...", url)
            time.sleep(5)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--connect-url", default=os.getenv("CONNECT_URL", "http://localhost:8083"))
    parser.add_argument("--wait", type=int, default=120, help="seconds to wait for Kafka Connect")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    with psycopg.connect(pg_conninfo()) as conn:
        prepare_source(conn)
    log.info("Postgres ready: heartbeat table and REPLICA IDENTITY FULL on %d tables", len(TABLES))

    wait_for_connect(args.connect_url, args.wait)
    response = requests.put(
        f"{args.connect_url}/connectors/{CONNECTOR}/config",
        json=connector_config(dict(os.environ)),
        timeout=30,
    )
    if not response.ok:
        log.error("Kafka Connect refused the config (%s): %s", response.status_code, response.text)
        return 1
    log.info("Connector %s %s", CONNECTOR, "created" if response.status_code == 201 else "updated")
    return 0


if __name__ == "__main__":
    sys.exit(main())
