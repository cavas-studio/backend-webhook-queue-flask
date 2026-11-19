import json
import sqlite3
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import click
from flask import Flask, jsonify, request

DB_PATH = Path(__file__).with_name("app.db")
MAX_ATTEMPTS = 3
LOCK_TIMEOUT_SECONDS = 60
BACKOFF_SECONDS = (1, 2)


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def connect_db(path: Path | str = DB_PATH) -> sqlite3.Connection:
    conn = sqlite3.connect(path, timeout=10, isolation_level=None)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    conn.execute("PRAGMA journal_mode = WAL")
    return conn


def init_db(path: Path | str = DB_PATH) -> None:
    conn = connect_db(path)
    try:
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                event_id TEXT NOT NULL UNIQUE,
                type TEXT NOT NULL,
                payload TEXT NOT NULL,
                created_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS tasks (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                event_row_id INTEGER NOT NULL UNIQUE,
                status TEXT NOT NULL CHECK(status IN ('pending','success','failed','skipped')),
                attempts INTEGER NOT NULL DEFAULT 0,
                next_attempt_at TEXT NOT NULL,
                locked_at TEXT,
                last_error TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                FOREIGN KEY(event_row_id) REFERENCES events(id) ON DELETE CASCADE
            );
            """
        )
    finally:
        conn.close()


def validate_event(data):
    if not isinstance(data, dict):
        return "JSON body must be an object"
    event_id = data.get("event_id")
    event_type = data.get("type")
    payload = data.get("payload")
    if not isinstance(event_id, str) or not event_id.strip():
        return "event_id must be a non-empty string"
    if not isinstance(event_type, str) or not event_type.strip():
        return "type must be a non-empty string"
    if not isinstance(payload, dict):
        return "payload must be an object"
    return None


def enqueue_event(db_path, data):
    now = utc_now()
    conn = connect_db(db_path)
    try:
        conn.execute("BEGIN IMMEDIATE")
        existing = conn.execute(
            "SELECT id FROM events WHERE event_id = ?", (data["event_id"],)
        ).fetchone()
        if existing:
            conn.execute("COMMIT")
            return False

        cur = conn.execute(
            "INSERT INTO events(event_id, type, payload, created_at) VALUES (?, ?, ?, ?)",
            (data["event_id"], data["type"], json.dumps(data["payload"]), now),
        )
        event_row_id = cur.lastrowid
        conn.execute(
            """
            INSERT INTO tasks(event_row_id, status, attempts, next_attempt_at, created_at, updated_at)
            VALUES (?, 'pending', 0, ?, ?, ?)
            """,
            (event_row_id, now, now, now),
        )
        conn.execute("COMMIT")
        return True
    except Exception:
        if conn.in_transaction:
            conn.execute("ROLLBACK")
        raise
    finally:
        conn.close()


def claim_next_task(db_path):
    now_dt = datetime.now(timezone.utc)
    now = now_dt.isoformat()
    stale_before = (now_dt - timedelta(seconds=LOCK_TIMEOUT_SECONDS)).isoformat()
    conn = connect_db(db_path)
    try:
        conn.execute("BEGIN IMMEDIATE")
        row = conn.execute(
            """
            SELECT t.id, t.attempts, t.locked_at, e.type, e.payload
            FROM tasks t
            JOIN events e ON e.id = t.event_row_id
            WHERE t.status = 'pending'
              AND t.next_attempt_at <= ?
              AND (t.locked_at IS NULL OR t.locked_at <= ?)
            ORDER BY t.id
            LIMIT 1
            """,
            (now, stale_before),
        ).fetchone()
        if not row:
            conn.execute("COMMIT")
            return None
        if row["attempts"] >= MAX_ATTEMPTS:
            conn.execute(
                """
                UPDATE tasks
                SET status='failed', locked_at=NULL,
                    last_error=COALESCE(last_error, 'worker stopped during final attempt'),
                    updated_at=?
                WHERE id=?
                """,
                (now, row["id"]),
            )
            conn.execute("COMMIT")
            return None
        conn.execute(
            "UPDATE tasks SET attempts = attempts + 1, locked_at = ?, updated_at = ? WHERE id = ?",
            (now, now, row["id"]),
        )
        conn.execute("COMMIT")
        return {
            "id": row["id"],
            "attempts": row["attempts"] + 1,
            "type": row["type"],
            "payload": json.loads(row["payload"]),
        }
    except Exception:
        conn.execute("ROLLBACK")
        raise
    finally:
        conn.close()


def process_event(event_type, payload, attempt_number):
    """Small deterministic stub used by the worker and tests."""
    known_types = {"payment_succeeded", "payment_failed", "user_registered"}
    if event_type not in known_types:
        return "skipped"

    fail_first = payload.get("fail_first", 0)
    if isinstance(fail_first, int) and attempt_number <= fail_first:
        raise RuntimeError(f"simulated failure on attempt {attempt_number}")

    return "success"


def finish_task(db_path, task_id, final_status, attempts, error=None, delay_seconds=0):
    now_dt = datetime.now(timezone.utc)
    now = now_dt.isoformat()
    conn = connect_db(db_path)
    try:
        if final_status == "pending":
            next_attempt = (now_dt + timedelta(seconds=delay_seconds)).isoformat()
            conn.execute(
                """
                UPDATE tasks
                SET status='pending', attempts=?, next_attempt_at=?, locked_at=NULL,
                    last_error=?, updated_at=?
                WHERE id=?
                """,
                (attempts, next_attempt, error, now, task_id),
            )
        else:
            conn.execute(
                """
                UPDATE tasks
                SET status=?, attempts=?, locked_at=NULL, last_error=?, updated_at=?
                WHERE id=?
                """,
                (final_status, attempts, error, now, task_id),
            )
    finally:
        conn.close()


def process_one_task(db_path=DB_PATH):
    task = claim_next_task(db_path)
    if not task:
        return False

    attempt_number = task["attempts"]
    try:
        result = process_event(task["type"], task["payload"], attempt_number)
        finish_task(db_path, task["id"], result, attempt_number)
    except Exception as exc:
        if attempt_number >= MAX_ATTEMPTS:
            finish_task(db_path, task["id"], "failed", attempt_number, str(exc))
        else:
            delay = BACKOFF_SECONDS[attempt_number - 1]
            finish_task(
                db_path,
                task["id"],
                "pending",
                attempt_number,
                str(exc),
                delay_seconds=delay,
            )
    return True


def create_app(test_config=None):
    app = Flask(__name__)
    app.config.update(DATABASE=str(DB_PATH))
    if test_config:
        app.config.update(test_config)
    init_db(app.config["DATABASE"])

    @app.post("/webhook")
    def webhook():
        if not request.is_json:
            return jsonify(error="Content-Type must be application/json"), 415
        data = request.get_json(silent=True)
        error = validate_event(data)
        if error:
            return jsonify(error=error), 400
        created = enqueue_event(app.config["DATABASE"], data)
        return jsonify(status="queued" if created else "duplicate"), 200

    @app.get("/stats")
    def stats():
        conn = connect_db(app.config["DATABASE"])
        try:
            event_rows = conn.execute(
                "SELECT type, COUNT(*) AS count FROM events GROUP BY type ORDER BY type"
            ).fetchall()
            task_rows = conn.execute(
                "SELECT status, COUNT(*) AS count FROM tasks GROUP BY status ORDER BY status"
            ).fetchall()
        finally:
            conn.close()
        return jsonify(
            events_by_type={row["type"]: row["count"] for row in event_rows},
            tasks_by_status={row["status"]: row["count"] for row in task_rows},
        )

    @app.cli.command("worker")
    @click.option("--once", is_flag=True, help="Process at most one available task.")
    @click.option(
        "--poll",
        default=0.5,
        show_default=True,
        type=click.FloatRange(min=0, min_open=True),
        help="Polling interval in seconds.",
    )
    def worker(once, poll):
        """Run the SQLite-backed queue worker."""
        while True:
            did_work = process_one_task(app.config["DATABASE"])
            if once:
                break
            if not did_work:
                time.sleep(poll)

    return app


app = create_app()

if __name__ == "__main__":
    app.run(debug=True)

