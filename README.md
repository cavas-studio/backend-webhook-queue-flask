# Flask webhook receiver with SQLite queue

Small test project: a Flask service accepts webhook events, stores them idempotently in SQLite, creates queue tasks, retries failed processing up to three times, and exposes simple statistics.

## Requirements

- Python 3.11+
- Flask
- SQLite (built into Python)
- pytest

## Run locally

```bash
python -m venv .venv
# Windows PowerShell
.venv\Scripts\Activate.ps1
pip install -r requirements.txt
flask --app app run
```

The database file `app.db` is created automatically.

## Send a webhook

```bash
curl -X POST http://127.0.0.1:5000/webhook \
  -H "Content-Type: application/json" \
  -d '{"event_id":"abc-123","type":"payment_succeeded","payload":{}}'
```

A repeated request with the same `event_id` returns HTTP 200 with `{"status":"duplicate"}` and does not create a second event or queue task.

## Run the worker

Continuous mode:

```bash
flask --app app worker
```

Process at most one available task:

```bash
flask --app app worker --once
```

The processing function is intentionally a stub. Known event types finish successfully unless `payload.fail_first` asks it to simulate failures. Unknown event types are marked `skipped`.

Example that fails twice and succeeds on the third attempt:

```json
{
  "event_id": "retry-1",
  "type": "payment_succeeded",
  "payload": {"fail_first": 2}
}
```

Retry delays grow as 1 and 2 seconds between the three attempts.
After the third failed attempt the task is marked `failed` and will no longer be selected. The worker's `--poll` interval must be greater than zero.

## Statistics

```bash
curl http://127.0.0.1:5000/stats
```

Example response:

```json
{
  "events_by_type": {"payment_succeeded": 2},
  "tasks_by_status": {"failed": 1, "success": 1}
}
```

## Tests

```bash
pytest -q
```

The tests cover:

- duplicate `event_id` does not create duplicate event/task rows;
- invalid request bodies return a 4xx error;
- successful processing and `/stats`;
- three failed attempts lead to final `failed` state;
- worker crashes consume bounded attempts, and stale-lock recovery finalizes the task;
- non-positive worker polling intervals are rejected;
- unknown event types become `skipped`.

## Design decisions

- **SQLite + `sqlite3`** instead of an ORM to keep the solution small and explicit.
- **Idempotency** is enforced twice: `events.event_id` has a `UNIQUE` constraint, and the insert is executed inside an immediate transaction.
- **Queue is a database table** as requested. No Celery/RQ/Huey is used.
- **No separate `processing` status** was added because the task specification lists only `pending`, `success`, `failed`, and `skipped`. A `locked_at` lease is used while a worker owns a pending task.
- **Crash recovery:** if a worker dies after claiming a task, the row remains `pending`, but its lock becomes stale after 60 seconds and another worker can claim it. The attempt is counted at claim time. If the final permitted attempt also crashes, the next stale-lock recovery marks the task `failed`. Recovery therefore requires a worker to run again; if no worker runs, pending tasks remain pending.
- **Retries** store the attempt count and next eligible processing time in SQLite. Processing is attempted at most three times, including attempts interrupted by a worker crash. After the third exception, or recovery of a lock left by the third attempt, the task becomes `failed`.
- **Unknown event types** are treated as intentionally unsupported and marked `skipped`.

## Not implemented

The worker is an in-process polling command; there is no supervisor to restart it after a crash. A task whose final attempt was interrupted is marked failed when a worker next recovers its stale lock. Authentication, frontend, Docker, migrations and deployment were not added because they are outside the requested scope.


