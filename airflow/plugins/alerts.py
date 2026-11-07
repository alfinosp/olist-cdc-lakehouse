"""Slack alerts for the Olist DAGs.

dbt checks the data, Airflow tells people:
- notify_failure: on_failure_callback for every task; lists failed dbt tests when the
  failing task is dbt_build.
- send_dq_summary: last task of a pipeline; posts the dbt tests that ended in `warn`.

Without SLACK_WEBHOOK_URL the message goes to the task log only.
"""

import json
import logging
import os
from pathlib import Path

import requests

log = logging.getLogger(__name__)

RUN_RESULTS = Path(os.getenv("DBT_TARGET_DIR", "/opt/project/dbt/olist/target")) / "run_results.json"


def post_to_slack(text: str) -> bool:
    url = os.getenv("SLACK_WEBHOOK_URL", "").strip()
    if not url:
        log.warning("SLACK_WEBHOOK_URL is not set; alert written to log only:\n%s", text)
        return False
    try:
        requests.post(url, json={"text": text}, timeout=10).raise_for_status()
    except requests.RequestException:
        log.exception("Slack webhook call failed; alert was:\n%s", text)
        return False
    return True


def load_run_results(path: Path = RUN_RESULTS) -> dict:
    if not path.exists():
        return {"results": []}
    return json.loads(path.read_text())


def summarize(run_results: dict, statuses: tuple[str, ...]) -> list[str]:
    """One bullet per dbt node whose status is in `statuses`."""
    lines = []
    for r in run_results.get("results", []):
        if r.get("status") not in statuses:
            continue
        name = r["unique_id"].split(".")[2]
        failures = r.get("failures")
        detail = f"{failures} rows" if failures is not None else (r.get("message") or "")
        lines.append(f"• `{name}`: {detail} ({r['status']})")
    return lines


def notify_failure(context: dict) -> None:
    ti = context["ti"]
    lines = [
        f":red_circle: *{ti.dag_id}* failed at task `{ti.task_id}`",
        f"Run: `{context.get('run_id', '?')}`",
    ]
    if ti.task_id == "dbt_build":
        failed = summarize(load_run_results(), ("fail", "error"))
        if failed:
            lines.append("Failed dbt nodes:")
            lines.extend(failed)
    post_to_slack("\n".join(lines))


def send_dq_summary(**context) -> None:
    warned = summarize(load_run_results(), ("warn",))
    if not warned:
        log.info("No dbt warnings in this run")
        return
    lines = [
        f":large_yellow_circle: *{context['dag'].dag_id}*: {len(warned)} data quality warning(s)",
        f"Run: `{context.get('run_id', '?')}`",
        *warned,
    ]
    post_to_slack("\n".join(lines))
