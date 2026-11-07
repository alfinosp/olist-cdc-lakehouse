import json
from datetime import UTC, datetime, timedelta

import alerts

RUN_RESULTS = {
    "results": [
        {"unique_id": "test.olist.unique_fct_orders_order_id.a1b2", "status": "fail", "failures": 12},
        {"unique_id": "test.olist.assert_quarantine_has_rows", "status": "warn", "failures": 3},
        {"unique_id": "model.olist.fct_orders", "status": "success", "failures": None},
        {"unique_id": "model.olist.dim_product", "status": "error", "failures": None,
         "message": "Binder Error"},
    ]
}


def test_summarize_failures():
    lines = alerts.summarize(RUN_RESULTS, ("fail", "error"))
    assert lines == [
        "• `unique_fct_orders_order_id`: 12 rows (fail)",
        "• `dim_product`: Binder Error (error)",
    ]


def test_summarize_warnings():
    assert alerts.summarize(RUN_RESULTS, ("warn",)) == ["• `assert_quarantine_has_rows`: 3 rows (warn)"]


def test_post_without_webhook_only_logs(monkeypatch):
    monkeypatch.delenv("SLACK_WEBHOOK_URL", raising=False)
    assert alerts.post_to_slack("hello") is False


def test_post_with_webhook(monkeypatch):
    sent = {}

    class Response:
        def raise_for_status(self):
            pass

    def fake_post(url, json, timeout):
        sent.update(url=url, json=json)
        return Response()

    monkeypatch.setenv("SLACK_WEBHOOK_URL", "https://hooks.slack.test/abc")
    monkeypatch.setattr(alerts.requests, "post", fake_post)
    assert alerts.post_to_slack("hello") is True
    assert sent == {"url": "https://hooks.slack.test/abc", "json": {"text": "hello"}}


def test_load_run_results_missing_file(tmp_path):
    assert alerts.load_run_results(tmp_path / "missing.json") == {"results": []}


def test_cdc_problems_from_fresh_report(tmp_path):
    now = datetime(2026, 1, 1, 12, 0, tzinfo=UTC)
    report = tmp_path / "cdc_health.json"
    report.write_text(json.dumps({"checked_at": now.isoformat(), "problems": ["1 event(s) quarantined"]}))
    assert alerts.cdc_problems(report, now + timedelta(seconds=30)) == ["• 1 event(s) quarantined"]
    # a report left by an earlier run is not presented as this run's reason
    stale = alerts.cdc_problems(report, now + timedelta(hours=1))
    assert stale == ["• last health report is from 12:00 UTC; see the task log"]
    assert alerts.cdc_problems(tmp_path / "missing.json") == ["• no health report found; see the task log"]


def test_cdc_health_failure_alert_includes_problems(tmp_path, monkeypatch):
    report = tmp_path / "cdc_health.json"
    checked_at = datetime.now(UTC).isoformat()
    report.write_text(json.dumps({"checked_at": checked_at, "problems": ["connector state is FAILED"]}))
    monkeypatch.setattr(alerts, "CDC_HEALTH_REPORT", report)
    sent = []
    monkeypatch.setattr(alerts, "post_to_slack", sent.append)

    class TaskInstance:
        dag_id, task_id = "olist_cdc_pipeline", "cdc_health"

    alerts.notify_failure({"ti": TaskInstance(), "run_id": "manual__1"})
    assert sent == [
        ":red_circle: *olist_cdc_pipeline* failed at task `cdc_health`\n"
        "Run: `manual__1`\n"
        "CDC problems:\n"
        "• connector state is FAILED"
    ]
