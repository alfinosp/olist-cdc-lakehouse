"""Health check of the CDC path, run by the olist_cdc_pipeline DAG.

Fails (exit 1) when any of these holds:
- the Debezium connector or one of its tasks is not RUNNING;
- the streaming job has not committed a micro-batch recently. Debezium writes a
  heartbeat event every 10 s and the job consumes it, so on a healthy pipeline the
  checkpoint advances every trigger even when the source is idle;
- events landed in bronze/_quarantine during the look-back window.

Usage:
    python -m olist.cdc_health
    python -m olist.cdc_health --since-minutes 15 --max-stream-lag-minutes 5
"""

import argparse
import logging
import os
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path

import polars as pl
import requests

from olist.bronze import bronze_path
from olist.cdc import QUARANTINE_TABLE
from olist.config import LAKE_ROOT

log = logging.getLogger("cdc_health")

CONNECTOR = "olist-postgres"
CHECKPOINT = Path(os.getenv("CDC_CHECKPOINT_DIR", LAKE_ROOT / "_checkpoints" / "cdc_to_bronze"))


def connector_problems(status: dict) -> list[str]:
    """Problems found in a Kafka Connect /connectors/<name>/status response."""
    problems = []
    state = status.get("connector", {}).get("state")
    if state != "RUNNING":
        problems.append(f"connector state is {state}")
    tasks = status.get("tasks", [])
    if not tasks:
        problems.append("connector has no tasks")
    for task in tasks:
        if task.get("state") != "RUNNING":
            trace = (task.get("trace") or "").splitlines()
            reason = f": {trace[0]}" if trace else ""
            problems.append(f"task {task.get('id')} state is {task.get('state')}{reason}")
    return problems


def last_commit(checkpoint: Path) -> datetime | None:
    """When the streaming job last committed a micro-batch (newest file in commits/)."""
    commits = [p for p in (checkpoint / "commits").glob("*") if p.is_file() and p.name.isdigit()]
    if not commits:
        return None
    return datetime.fromtimestamp(max(p.stat().st_mtime for p in commits), tz=UTC)


def quarantined_since(lake_root: Path, since: datetime) -> int:
    path = bronze_path(lake_root, QUARANTINE_TABLE)
    if not path.exists():
        return 0
    return (
        pl.scan_delta(str(path))
        .filter(pl.col("_ingested_at") >= since)
        .select(pl.len())
        .collect()
        .item()
    )


def check(connect_url: str, since_minutes: int, max_stream_lag_minutes: int) -> list[str]:
    problems = []
    try:
        response = requests.get(f"{connect_url}/connectors/{CONNECTOR}/status", timeout=10)
        if response.status_code == 404:
            problems.append(f"connector {CONNECTOR} is not registered (make cdc-register)")
        else:
            response.raise_for_status()
            problems += connector_problems(response.json())
    except requests.RequestException as exc:
        problems.append(f"Kafka Connect unreachable at {connect_url}: {exc}")

    now = datetime.now(UTC)
    committed = last_commit(CHECKPOINT)
    if committed is None:
        problems.append(f"streaming job has never committed a micro-batch ({CHECKPOINT})")
    elif now - committed > timedelta(minutes=max_stream_lag_minutes):
        problems.append(f"streaming job last committed at {committed:%Y-%m-%d %H:%M} UTC (make cdc-logs)")

    since = now - timedelta(minutes=since_minutes)
    bad = quarantined_since(LAKE_ROOT, since)
    if bad:
        problems.append(f"{bad} event(s) quarantined in bronze/{QUARANTINE_TABLE} since {since:%H:%M} UTC")
    return problems


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--connect-url", default=os.getenv("CONNECT_URL", "http://localhost:8083"))
    parser.add_argument("--since-minutes", type=int, default=15, help="quarantine look-back")
    parser.add_argument("--max-stream-lag-minutes", type=int, default=5)
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    problems = check(args.connect_url, args.since_minutes, args.max_stream_lag_minutes)
    for problem in problems:
        log.error(problem)
    if not problems:
        log.info("CDC healthy: connector RUNNING, streaming job committing, nothing quarantined")
    return 1 if problems else 0


if __name__ == "__main__":
    sys.exit(main())
