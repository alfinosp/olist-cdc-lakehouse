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
