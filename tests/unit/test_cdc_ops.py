import json
import os
from datetime import UTC, datetime, timedelta

import debezium_fixture
from register_connector import connector_config

from olist.cdc import KafkaRecord, parse_event, write_events
from olist.cdc_health import connector_problems, last_commit, quarantined_since, write_report
from olist.tables import TABLES

RUNNING = {"connector": {"state": "RUNNING"}, "tasks": [{"id": 0, "state": "RUNNING"}]}


def test_healthy_connector_has_no_problems():
    assert connector_problems(RUNNING) == []


def test_failed_task_reports_first_trace_line():
    error = "org.postgresql.util.PSQLException: slot gone"
    status = {
        "connector": {"state": "RUNNING"},
        "tasks": [{"id": 0, "state": "FAILED", "trace": f"{error}\n\tat ..."}],
    }
    assert connector_problems(status) == [f"task 0 state is FAILED: {error}"]


def test_connector_without_tasks_is_a_problem():
    problems = connector_problems({"connector": {"state": "PAUSED"}, "tasks": []})
    assert problems == ["connector state is PAUSED", "connector has no tasks"]


def test_last_commit_reads_newest_commit_file(tmp_path):
    assert last_commit(tmp_path) is None
    commits = tmp_path / "commits"
    commits.mkdir()
    (commits / ".0.crc").write_text("")  # Hadoop checksum files are ignored
    for batch_id, mtime in [("0", 1_700_000_000), ("1", 1_700_000_600)]:
        (commits / batch_id).write_text("v1")
        os.utime(commits / batch_id, (mtime, mtime))
    assert last_commit(tmp_path) == datetime.fromtimestamp(1_700_000_600, tz=UTC)


def test_heartbeat_events_are_skipped():
    payload = debezium_fixture.event("orders", {"id": "1", "ts": None}, "u", 1_000_000, 1)
    payload["payload"]["source"] |= {"schema": "ops", "table": "debezium_heartbeat"}
    assert parse_event(debezium_fixture.record("orders", payload, 0)) is None


def test_quarantined_since_counts_only_recent_events(tmp_path):
    now = datetime.now(UTC)
    assert quarantined_since(tmp_path, now) == 0  # no quarantine table yet
    write_events([KafkaRecord("t", 0, 1, "{bad")], tmp_path, now - timedelta(hours=2))
    write_events([KafkaRecord("t", 0, 2, "{bad")], tmp_path, now)
    assert quarantined_since(tmp_path, now - timedelta(minutes=15)) == 1


def test_write_report_records_problems_and_time(tmp_path):
    checked_at = datetime(2026, 1, 1, 12, 0, tzinfo=UTC)
    write_report(tmp_path / "health" / "cdc_health.json", ["connector state is FAILED"], checked_at)
    report = json.loads((tmp_path / "health" / "cdc_health.json").read_text())
    assert report == {"checked_at": "2026-01-01T12:00:00+00:00", "problems": ["connector state is FAILED"]}


def test_connector_config_fills_credentials_and_covers_every_table():
    config = connector_config({"PG_USER": "u", "PG_PASSWORD": "p", "PATH": "/bin"})
    credentials = (config["database.user"], config["database.password"], config["database.dbname"])
    assert credentials == ("u", "p", "olist")
    assert "${" not in str(config)
    captured = set(config["table.include.list"].split(","))
    assert captured == {f"olist_app.{t}" for t in TABLES} | {"ops.debezium_heartbeat"}
    # settings olist.cdc relies on
    assert config["decimal.handling.mode"] == "string"
    assert config["value.converter.schemas.enable"] == "true"
    assert config["tombstones.on.delete"] == "false"
