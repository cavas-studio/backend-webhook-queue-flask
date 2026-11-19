import sqlite3
import pytest

from app import create_app, process_one_task


@pytest.fixture()
def app(tmp_path):
    db_path = tmp_path / "test.db"
    app = create_app({"TESTING": True, "DATABASE": str(db_path)})
    app.db_path = db_path
    return app


@pytest.fixture()
def client(app):
    return app.test_client()


def valid_event(event_id="abc-123", event_type="payment_succeeded", payload=None):
    return {
        "event_id": event_id,
        "type": event_type,
        "payload": payload or {},
    }


def test_duplicate_webhook_does_not_create_second_event_or_task(client, app):
    assert client.post("/webhook", json=valid_event()).status_code == 200
    response = client.post("/webhook", json=valid_event())
    assert response.status_code == 200
    assert response.get_json()["status"] == "duplicate"

    conn = sqlite3.connect(app.db_path)
    try:
        assert conn.execute("SELECT COUNT(*) FROM events").fetchone()[0] == 1
        assert conn.execute("SELECT COUNT(*) FROM tasks").fetchone()[0] == 1
    finally:
        conn.close()


def test_invalid_body_returns_4xx(client):
    response = client.post("/webhook", json={"event_id": "x"})
    assert response.status_code == 400
    assert "error" in response.get_json()


def test_successful_task_processing_and_stats(client, app):
    client.post("/webhook", json=valid_event())
    assert process_one_task(app.db_path) is True

    stats = client.get("/stats").get_json()
    assert stats["events_by_type"] == {"payment_succeeded": 1}
    assert stats["tasks_by_status"] == {"success": 1}


def test_failed_task_retries_three_times_then_stops(client, app, monkeypatch):
    import app as app_module

    monkeypatch.setattr(app_module, "BACKOFF_SECONDS", (0, 0, 0))
    client.post(
        "/webhook",
        json=valid_event(payload={"fail_first": 99}),
    )

    assert process_one_task(app.db_path) is True
    assert process_one_task(app.db_path) is True
    assert process_one_task(app.db_path) is True
    assert process_one_task(app.db_path) is False

    conn = sqlite3.connect(app.db_path)
    try:
        status, attempts = conn.execute(
            "SELECT status, attempts FROM tasks"
        ).fetchone()
        assert status == "failed"
        assert attempts == 3
    finally:
        conn.close()


def test_unknown_event_type_is_skipped(client, app):
    client.post("/webhook", json=valid_event(event_type="unknown_event"))
    assert process_one_task(app.db_path) is True

    conn = sqlite3.connect(app.db_path)
    try:
        status = conn.execute("SELECT status FROM tasks").fetchone()[0]
        assert status == "skipped"
    finally:
        conn.close()


def test_crashed_worker_attempts_are_bounded_by_stale_lock_recovery(client, app, monkeypatch):
    import app as app_module

    monkeypatch.setattr(app_module, "LOCK_TIMEOUT_SECONDS", 0)
    client.post("/webhook", json=valid_event())

    for expected_attempts in (1, 2, 3):
        task = app_module.claim_next_task(app.db_path)
        assert task["attempts"] == expected_attempts
        # Simulate a worker crash: leave its lease in place without finishing.

    assert app_module.claim_next_task(app.db_path) is None
    conn = sqlite3.connect(app.db_path)
    try:
        status, attempts = conn.execute("SELECT status, attempts FROM tasks").fetchone()
        assert status == "failed"
        assert attempts == 3
    finally:
        conn.close()


def test_worker_rejects_non_positive_poll_interval(app):
    result = app.test_cli_runner().invoke(args=["worker", "--poll", "0"])
    assert result.exit_code != 0
    assert "0" in result.output
