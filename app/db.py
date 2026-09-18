"""SQLite persistence layer.

A single connection guarded by a lock serializes all access, which keeps the
claim/update sequences atomic for the in-process workers. WAL mode is enabled
so readers are not blocked while a worker writes attempt results.
"""
from __future__ import annotations

import json
import sqlite3
import threading
import time
import uuid

SCHEMA = """
CREATE TABLE IF NOT EXISTS subscriptions (
    id          TEXT PRIMARY KEY,
    target_url  TEXT NOT NULL,
    secret      TEXT NOT NULL,
    created_at  REAL NOT NULL,
    paused_at   REAL,                   -- epoch seconds while paused; NULL when active
    previous_secret            TEXT,    -- retired secret, dual-signed during the grace window
    previous_secret_expires_at REAL,    -- epoch seconds when the previous secret stops being sent
    breaker_state                TEXT NOT NULL DEFAULT 'closed'
                                 CHECK (breaker_state IN ('closed', 'open', 'half_open')),
    breaker_consecutive_failures INTEGER NOT NULL DEFAULT 0,
    breaker_next_probe_at        REAL,  -- epoch seconds when an open breaker allows its probe
    breaker_probe_task_id        INTEGER -- the single half-open probe task; NULL unless half_open
);

CREATE TABLE IF NOT EXISTS events (
    id          TEXT PRIMARY KEY,
    payload     TEXT NOT NULL,          -- raw JSON body delivered to targets
    created_at  REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS tasks (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    event_id         TEXT NOT NULL REFERENCES events(id),
    subscription_id  TEXT NOT NULL,
    status           TEXT NOT NULL DEFAULT 'pending'
                     CHECK (status IN
                       ('pending', 'in_progress', 'delivered', 'failed', 'cancelled')),
    attempts         INTEGER NOT NULL DEFAULT 0,
    max_attempts     INTEGER NOT NULL,
    deliver_at       REAL,              -- requested scheduled time (epoch seconds); NULL = immediate
    next_attempt_at  REAL,              -- epoch seconds; NULL once finished
    claimed_by       TEXT,
    last_status_code INTEGER,
    last_error       TEXT,
    result           TEXT CHECK (result IN ('delivered', 'failed')),
    created_at       REAL NOT NULL,
    updated_at       REAL NOT NULL,
    cancelled_at     REAL,              -- epoch seconds when an event cancel claimed the task
    UNIQUE (event_id, subscription_id)  -- one task per (event, subscription)
);

CREATE TABLE IF NOT EXISTS attempts (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    task_id     INTEGER NOT NULL REFERENCES tasks(id),
    attempt_no  INTEGER NOT NULL,
    status_code INTEGER,                -- NULL when the request never got a response
    error       TEXT,
    started_at  REAL NOT NULL,
    finished_at REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS ingested_events (
    id          TEXT PRIMARY KEY,       -- X-Webhook-Id of a verified inbound event
    raw_body    BLOB NOT NULL,          -- exact original JSON bytes, for conflict checks
    receipt     TEXT NOT NULL,          -- JSON acceptance response returned first time
    created_at  REAL NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_tasks_due ON tasks (status, next_attempt_at);
CREATE INDEX IF NOT EXISTS idx_tasks_sub_order ON tasks (subscription_id, id, status);
CREATE INDEX IF NOT EXISTS idx_attempts_task ON attempts (task_id);
"""

# Columns returned to the API layer for a subscription (never secret material).
_SUB_COLUMNS = (
    "id, target_url, created_at, paused_at, previous_secret_expires_at, "
    "breaker_state, breaker_consecutive_failures, breaker_next_probe_at"
)


class Database:
    def __init__(
        self,
        path: str,
        breaker_failure_threshold: int = 5,
        breaker_cooldown_seconds: float = 30.0,
    ):
        self._breaker_threshold = breaker_failure_threshold
        self._breaker_cooldown = breaker_cooldown_seconds
        self._conn = sqlite3.connect(path, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA foreign_keys=ON")
        self._lock = threading.Lock()
        with self._lock:
            self._conn.executescript(SCHEMA)
            self._migrate()
            self._conn.commit()

    def _migrate(self) -> None:
        """Bring databases created by older versions up to date.

        CREATE TABLE IF NOT EXISTS leaves pre-existing tables untouched, so
        columns added later are applied here via ALTER TABLE. The tasks
        table additionally needs a widened status CHECK to accept the
        'cancelled' terminal state; SQLite cannot ALTER a CHECK constraint,
        so an old tasks table is rebuilt (foreign_keys off, inside a single
        transaction, per the SQLite recommended table-rebuild procedure).
        """
        cols = {r["name"] for r in self._conn.execute("PRAGMA table_info(subscriptions)")}
        if "paused_at" not in cols:
            self._conn.execute("ALTER TABLE subscriptions ADD COLUMN paused_at REAL")
        if "previous_secret" not in cols:
            self._conn.execute("ALTER TABLE subscriptions ADD COLUMN previous_secret TEXT")
        if "previous_secret_expires_at" not in cols:
            self._conn.execute(
                "ALTER TABLE subscriptions ADD COLUMN previous_secret_expires_at REAL"
            )
        if "breaker_state" not in cols:
            self._conn.execute(
                "ALTER TABLE subscriptions ADD COLUMN breaker_state TEXT NOT NULL DEFAULT 'closed'"
            )
        if "breaker_consecutive_failures" not in cols:
            self._conn.execute(
                "ALTER TABLE subscriptions ADD COLUMN breaker_consecutive_failures "
                "INTEGER NOT NULL DEFAULT 0"
            )
        if "breaker_next_probe_at" not in cols:
            self._conn.execute(
                "ALTER TABLE subscriptions ADD COLUMN breaker_next_probe_at REAL"
            )
        if "breaker_probe_task_id" not in cols:
            self._conn.execute(
                "ALTER TABLE subscriptions ADD COLUMN breaker_probe_task_id INTEGER"
            )

        task_cols = {r["name"] for r in self._conn.execute("PRAGMA table_info(tasks)")}
        if "cancelled_at" not in task_cols:
            # Commit the subscription ALTERs first; the rebuild then runs as
            # its own explicit transaction. executescript() is avoided on
            # purpose: it silently COMMITs any open transaction first.
            self._conn.commit()
            self._conn.execute("PRAGMA foreign_keys=OFF")
            try:
                self._conn.execute("BEGIN")
                self._conn.execute(
                    """
                    CREATE TABLE tasks_new (
                        id               INTEGER PRIMARY KEY AUTOINCREMENT,
                        event_id         TEXT NOT NULL REFERENCES events(id),
                        subscription_id  TEXT NOT NULL,
                        status           TEXT NOT NULL DEFAULT 'pending'
                                         CHECK (status IN
                                           ('pending', 'in_progress', 'delivered',
                                            'failed', 'cancelled')),
                        attempts         INTEGER NOT NULL DEFAULT 0,
                        max_attempts     INTEGER NOT NULL,
                        deliver_at       REAL,
                        next_attempt_at  REAL,
                        claimed_by       TEXT,
                        last_status_code INTEGER,
                        last_error       TEXT,
                        result           TEXT CHECK (result IN ('delivered', 'failed')),
                        created_at       REAL NOT NULL,
                        updated_at       REAL NOT NULL,
                        cancelled_at     REAL,
                        UNIQUE (event_id, subscription_id)
                    )
                    """
                )
                self._conn.execute(
                    """
                    INSERT INTO tasks_new
                        (id, event_id, subscription_id, status, attempts, max_attempts,
                         deliver_at, next_attempt_at, claimed_by, last_status_code, last_error,
                         result, created_at, updated_at, cancelled_at)
                    SELECT id, event_id, subscription_id, status, attempts, max_attempts,
                           NULL, next_attempt_at, claimed_by, last_status_code, last_error,
                           result, created_at, updated_at, NULL
                    FROM tasks
                    """
                )
                self._conn.execute("DROP TABLE tasks")
                self._conn.execute("ALTER TABLE tasks_new RENAME TO tasks")
                self._conn.execute(
                    "CREATE INDEX idx_tasks_due ON tasks (status, next_attempt_at)"
                )
                self._conn.execute(
                    "CREATE INDEX idx_tasks_sub_order ON tasks (subscription_id, id, status)"
                )
                self._conn.execute("COMMIT")
            except Exception:
                self._conn.execute("ROLLBACK")
                raise
            finally:
                # Re-enable foreign keys while outside the transaction; the
                # constructor's ON setting is what every new connection gets.
                self._conn.execute("PRAGMA foreign_keys=ON")
            # The rebuild above already has the column; refresh the set so
            # the ALTER below is skipped for this database.
            task_cols = {r["name"] for r in self._conn.execute("PRAGMA table_info(tasks)")}

        if "deliver_at" not in task_cols:
            # A database created by a version that already had 'cancelled'
            # but predates scheduled delivery: just add the column. Existing
            # rows get NULL, i.e. their tasks were immediate submissions.
            self._conn.execute("ALTER TABLE tasks ADD COLUMN deliver_at REAL")

    def close(self) -> None:
        with self._lock:
            self._conn.close()

    # ------------------------------------------------------------------
    # subscriptions
    # ------------------------------------------------------------------
    def create_subscription(self, target_url: str, secret: str) -> dict:
        sub_id = uuid.uuid4().hex[:12]
        now = time.time()
        with self._lock:
            self._conn.execute(
                "INSERT INTO subscriptions (id, target_url, secret, created_at) VALUES (?, ?, ?, ?)",
                (sub_id, target_url, secret, now),
            )
            self._conn.commit()
        return {
            "id": sub_id,
            "target_url": target_url,
            "created_at": now,
            "paused_at": None,
            "previous_secret_expires_at": None,
            "breaker_state": "closed",
            "breaker_consecutive_failures": 0,
            "breaker_next_probe_at": None,
        }

    def list_subscriptions(self) -> list[dict]:
        with self._lock:
            rows = self._conn.execute(
                f"SELECT {_SUB_COLUMNS} FROM subscriptions ORDER BY created_at"
            ).fetchall()
        return [dict(r) for r in rows]

    def get_subscription(self, sub_id: str) -> dict | None:
        with self._lock:
            row = self._conn.execute(
                f"SELECT {_SUB_COLUMNS} FROM subscriptions WHERE id = ?",
                (sub_id,),
            ).fetchone()
        return dict(row) if row else None

    def set_subscription_paused(self, sub_id: str, paused: bool) -> dict | None:
        """Pause or resume a subscription, idempotently.

        Only the paused_at flag on the subscription row is flipped; task
        rows are never touched, so every pending task keeps its original
        next_attempt_at schedule and simply becomes claimable again once
        the subscription is resumed. A repeated pause keeps the original
        paused_at timestamp. Returns the updated row, or None when the
        subscription does not exist.
        """
        with self._lock:
            row = self._conn.execute(
                "SELECT paused_at FROM subscriptions WHERE id = ?", (sub_id,)
            ).fetchone()
            if row is None:
                return None
            if paused and row["paused_at"] is None:
                self._conn.execute(
                    "UPDATE subscriptions SET paused_at = ? WHERE id = ?",
                    (time.time(), sub_id),
                )
            elif not paused and row["paused_at"] is not None:
                self._conn.execute(
                    "UPDATE subscriptions SET paused_at = NULL WHERE id = ?", (sub_id,)
                )
            self._conn.commit()
            row = self._conn.execute(
                f"SELECT {_SUB_COLUMNS} FROM subscriptions WHERE id = ?",
                (sub_id,),
            ).fetchone()
        return dict(row)

    def rotate_secret(self, sub_id: str, new_secret: str, grace_seconds: float) -> tuple[str, dict | None]:
        """Rotate a subscription's delivery-signing secret, atomically.

        The current secret becomes the previous secret and keeps being sent
        (as X-Webhook-Signature-Previous) until now + grace_seconds; with
        grace_seconds == 0 the switch is immediate and no previous secret is
        kept. While an earlier rotation's grace window is still open the
        rotation is refused ('in_grace'); once that window has expired the
        retired secret is silently discarded by the next rotation.

        previous_secret and previous_secret_expires_at are always set or
        cleared together, so readers can rely on the timestamp alone.

        Returns ("rotated", row) | ("not_found", None) | ("in_grace", row);
        the row never contains secret material.
        """
        now = time.time()
        with self._lock:
            row = self._conn.execute(
                "SELECT secret, previous_secret_expires_at FROM subscriptions WHERE id = ?",
                (sub_id,),
            ).fetchone()
            if row is None:
                return "not_found", None
            expires = row["previous_secret_expires_at"]
            if expires is not None and expires > now:
                status = "in_grace"
            else:
                if grace_seconds > 0:
                    previous, previous_expires = row["secret"], now + grace_seconds
                else:
                    previous, previous_expires = None, None
                self._conn.execute(
                    """UPDATE subscriptions
                       SET secret = ?, previous_secret = ?, previous_secret_expires_at = ?
                       WHERE id = ?""",
                    (new_secret, previous, previous_expires, sub_id),
                )
                status = "rotated"
            self._conn.commit()
            row = self._conn.execute(
                f"SELECT {_SUB_COLUMNS} FROM subscriptions WHERE id = ?",
                (sub_id,),
            ).fetchone()
        return status, dict(row)

    def subscription_exists(self, sub_id: str) -> bool:
        with self._lock:
            row = self._conn.execute(
                "SELECT 1 FROM subscriptions WHERE id = ?", (sub_id,)
            ).fetchone()
        return row is not None

    def delete_subscription(self, sub_id: str) -> bool:
        with self._lock:
            cur = self._conn.execute("DELETE FROM subscriptions WHERE id = ?", (sub_id,))
            self._conn.commit()
        return cur.rowcount > 0

    # ------------------------------------------------------------------
    # events and tasks
    # ------------------------------------------------------------------
    def get_event_payload(self, event_id: str) -> str | None:
        with self._lock:
            row = self._conn.execute(
                "SELECT payload FROM events WHERE id = ?", (event_id,)
            ).fetchone()
        return row["payload"] if row else None

    def create_event(self, event_id: str, payload: str) -> None:
        with self._lock:
            self._conn.execute(
                "INSERT INTO events (id, payload, created_at) VALUES (?, ?, ?)",
                (event_id, payload, time.time()),
            )
            self._conn.commit()

    def create_task(
        self,
        event_id: str,
        subscription_id: str,
        max_attempts: int,
        deliver_at: float | None = None,
    ) -> tuple[dict, bool]:
        """Insert a task unless one already exists for (event_id, subscription_id).

        deliver_at is the requested scheduled delivery time (epoch seconds);
        None means the task is due immediately. It also seeds
        next_attempt_at, so the worker claim query does not pick the task up
        before the scheduled time. Scheduling on an already-existing
        (duplicate) task is never changed.

        Returns (task_row, created) so callers can report duplicates.
        """
        now = time.time()
        due = deliver_at if deliver_at is not None else now
        with self._lock:
            cur = self._conn.execute(
                """INSERT OR IGNORE INTO tasks
                       (event_id, subscription_id, status, attempts, max_attempts,
                        deliver_at, next_attempt_at, created_at, updated_at)
                   VALUES (?, ?, 'pending', 0, ?, ?, ?, ?, ?)""",
                (event_id, subscription_id, max_attempts, deliver_at, due, now, now),
            )
            created = cur.rowcount > 0
            row = self._conn.execute(
                "SELECT * FROM tasks WHERE event_id = ? AND subscription_id = ?",
                (event_id, subscription_id),
            ).fetchone()
            self._conn.commit()
        return dict(row), created

    def ingest_event(
        self,
        webhook_id: str,
        raw_body: bytes,
        subscription_ids: list[str],
        max_attempts: int,
    ) -> tuple[str, dict | list[str]]:
        """Atomically record a verified inbound event and enqueue its tasks.

        Runs in a single transaction so a concurrent replay with the same
        X-Webhook-Id can never slip past the idempotency check.

        Returns one of:
            ("duplicate", receipt_dict)  -- id already accepted, body identical;
                                            the original acceptance is returned
            ("conflict", None)           -- id already exists with a different body
            ("unknown", [ids])           -- unknown subscription id(s); nothing stored
            ("created", receipt_dict)    -- first acceptance; tasks were created
        """
        now = time.time()
        with self._lock:
            row = self._conn.execute(
                "SELECT raw_body, receipt FROM ingested_events WHERE id = ?",
                (webhook_id,),
            ).fetchone()
            if row is not None:
                if row["raw_body"] == raw_body:
                    return "duplicate", json.loads(row["receipt"])
                return "conflict", None

            # An event created via POST /events shares the events namespace.
            # Treat an incompatible row as a conflict as well.
            existing = self._conn.execute(
                "SELECT payload FROM events WHERE id = ?", (webhook_id,)
            ).fetchone()
            if existing is not None and existing["payload"].encode("utf-8") != raw_body:
                return "conflict", None

            unknown = [
                sid
                for sid in dict.fromkeys(subscription_ids)
                if self._conn.execute(
                    "SELECT 1 FROM subscriptions WHERE id = ?", (sid,)
                ).fetchone()
                is None
            ]
            if unknown:
                return "unknown", unknown

            if existing is None:
                self._conn.execute(
                    "INSERT INTO events (id, payload, created_at) VALUES (?, ?, ?)",
                    (webhook_id, raw_body.decode("utf-8"), now),
                )

            created, duplicates = [], []
            for sid in dict.fromkeys(subscription_ids):  # preserve order, drop repeats
                cur = self._conn.execute(
                    """INSERT OR IGNORE INTO tasks
                           (event_id, subscription_id, status, attempts, max_attempts,
                            next_attempt_at, created_at, updated_at)
                       VALUES (?, ?, 'pending', 0, ?, ?, ?, ?)""",
                    (webhook_id, sid, max_attempts, now, now, now),
                )
                task = self._conn.execute(
                    "SELECT id, status FROM tasks WHERE event_id = ? AND subscription_id = ?",
                    (webhook_id, sid),
                ).fetchone()
                ref = {
                    "task_id": task["id"],
                    "subscription_id": sid,
                    "status": task["status"],
                    # POST /ingest never schedules; the explicit key keeps
                    # the stored receipt current with the TaskRef schema.
                    "deliver_at": None,
                }
                (created if cur.rowcount > 0 else duplicates).append(ref)

            receipt = {"event_id": webhook_id, "created": created, "duplicates": duplicates}
            self._conn.execute(
                "INSERT INTO ingested_events (id, raw_body, receipt, created_at) VALUES (?, ?, ?, ?)",
                (webhook_id, raw_body, json.dumps(receipt), now),
            )
            self._conn.commit()
        return "created", receipt

    def claim_due_task(self, worker_id: str, now: float) -> dict | None:
        """Atomically claim the oldest due task for this worker.

        The SELECT-then-UPDATE runs under the connection lock, so two workers
        can never claim the same task row.

        Strict per-subscription ordering: a task is eligible only when no
        earlier task (smaller id) of the same subscription is still
        'pending' or 'in_progress' -- the next task is released once its
        predecessor reaches a terminal state
        ('delivered'/'failed'/'cancelled'). The rule is evaluated against
        persisted state at claim time, so it holds across retry backoff,
        concurrent workers, and restarts. Different subscriptions are
        independent and still claimed in parallel; a blocked task simply
        stays 'pending' and remains visible as such in status queries.

        Scheduled delivery: a task submitted with deliver_at in the future
        is stored with next_attempt_at seeded to that instant, so the
        t.next_attempt_at <= ? predicate makes every worker skip it until
        its time comes (no in-memory timers, so the guarantee survives
        restarts). Because it stays 'pending' while waiting, the ordering
        rule above also makes it block every later task of the same
        subscription -- including immediately-due ones -- until the
        scheduled task itself has been delivered, failed, or cancelled.

        Event-cancelled tasks ('cancelled') are terminal and never selected:
        they neither retry nor count towards a subscription's breaker, and
        because they leave the pending/in_progress set they never block a
        later task of the same subscription.

        Paused subscriptions (paused_at set) are skipped entirely: their
        tasks stay 'pending' with their original next_attempt_at and
        become claimable again, in the same order, once resumed. Tasks
        already 'in_progress' when the pause lands are unaffected and
        run to completion.

        Circuit breaker: tasks of a subscription whose breaker is 'open'
        are not claimed until the cooldown expires (breaker_next_probe_at
        <= now). The first claim after that moment belongs to the earliest
        pending task of the subscription (guaranteed by the ordering rule)
        and atomically flips the breaker to 'half_open' with that task
        recorded as the single probe. While 'half_open', only the probe
        task itself can be claimed (e.g. for its retries); every other
        task of the subscription waits for the probe's verdict.
        """
        with self._lock:
            row = self._conn.execute(
                """SELECT t.*, s.target_url, s.secret, s.previous_secret,
                          s.previous_secret_expires_at, s.breaker_state, e.payload
                   FROM tasks t
                   LEFT JOIN subscriptions s ON s.id = t.subscription_id
                   LEFT JOIN events e ON e.id = t.event_id
                   WHERE t.status = 'pending' AND t.next_attempt_at <= ?
                     AND s.paused_at IS NULL
                     AND (
                           s.id IS NULL              -- subscription deleted: claim and fail
                           OR s.breaker_state = 'closed'
                           OR (s.breaker_state = 'open' AND s.breaker_next_probe_at <= ?)
                           OR (s.breaker_state = 'half_open'
                               AND t.id = s.breaker_probe_task_id)
                         )
                     AND NOT EXISTS (
                           SELECT 1 FROM tasks earlier
                           WHERE earlier.subscription_id = t.subscription_id
                             AND earlier.id < t.id
                             AND earlier.status IN ('pending', 'in_progress')
                         )
                   ORDER BY t.next_attempt_at, t.id
                   LIMIT 1""",
                (now, now),
            ).fetchone()
            if row is None:
                return None
            self._conn.execute(
                "UPDATE tasks SET status = 'in_progress', claimed_by = ?, updated_at = ? WHERE id = ?",
                (worker_id, now, row["id"]),
            )
            if row["breaker_state"] == "open":
                # Cooldown elapsed: this claim is the subscription's single
                # half-open probe. The state flip is committed together with
                # the claim, so no second worker can also become the probe.
                self._conn.execute(
                    """UPDATE subscriptions
                       SET breaker_state = 'half_open', breaker_probe_task_id = ?,
                           breaker_next_probe_at = NULL
                       WHERE id = ? AND breaker_state = 'open'""",
                    (row["id"], row["subscription_id"]),
                )
            self._conn.commit()
        return dict(row)

    def finish_attempt(
        self,
        task_id: int,
        attempt_no: int,
        status_code: int | None,
        error: str | None,
        started_at: float,
        finished_at: float,
        outcome: str,  # 'delivered' | 'failed' | 'retry'
        next_attempt_at: float | None,
    ) -> None:
        """Record an attempt and move the task to its next state, in one commit.

        Terminal outcomes also update the subscription's circuit breaker in
        the same transaction, so the failure count and the state machine can
        never drift apart from the task history.
        """
        with self._lock:
            self._conn.execute(
                """INSERT INTO attempts (task_id, attempt_no, status_code, error, started_at, finished_at)
                   VALUES (?, ?, ?, ?, ?, ?)""",
                (task_id, attempt_no, status_code, error, started_at, finished_at),
            )
            if outcome == "retry":
                self._conn.execute(
                    """UPDATE tasks SET status = 'pending', attempts = ?, next_attempt_at = ?,
                           last_status_code = ?, last_error = ?, claimed_by = NULL, updated_at = ?
                       WHERE id = ?""",
                    (attempt_no, next_attempt_at, status_code, error, finished_at, task_id),
                )
            else:
                self._conn.execute(
                    """UPDATE tasks SET status = ?, result = ?, attempts = ?, next_attempt_at = NULL,
                           last_status_code = ?, last_error = ?, claimed_by = NULL, updated_at = ?
                       WHERE id = ?""",
                    (outcome, outcome, attempt_no, status_code, error, finished_at, task_id),
                )
                self._apply_breaker_outcome(task_id, outcome, finished_at)
            self._conn.commit()

    def _apply_breaker_outcome(self, task_id: int, outcome: str, now: float) -> None:
        """Advance the circuit breaker of the task's subscription.

        Must be called with the connection lock held, inside the transaction
        that terminates the task. 'outcome' is 'delivered' or 'failed'
        (final failure after the attempt budget is exhausted).

        - delivered: any success breaks the consecutive-failure streak; a
          successful half-open probe additionally closes the breaker.
        - failed: the consecutive-failure count grows; at the configured
          threshold a 'closed' breaker trips 'open' atomically, and a failed
          probe re-opens a 'half_open' breaker with a refreshed cooldown.
        """
        row = self._conn.execute(
            """SELECT s.id, s.breaker_state, s.breaker_consecutive_failures,
                      s.breaker_probe_task_id
               FROM tasks t JOIN subscriptions s ON s.id = t.subscription_id
               WHERE t.id = ?""",
            (task_id,),
        ).fetchone()
        if row is None:
            return  # subscription was deleted; no breaker to update

        sub_id = row["id"]
        state = row["breaker_state"]
        failures = row["breaker_consecutive_failures"]
        is_probe = state == "half_open" and row["breaker_probe_task_id"] == task_id

        if outcome == "delivered":
            if is_probe:
                # Probe succeeded: close the breaker and reset the count.
                self._conn.execute(
                    """UPDATE subscriptions
                       SET breaker_state = 'closed', breaker_consecutive_failures = 0,
                           breaker_next_probe_at = NULL, breaker_probe_task_id = NULL
                       WHERE id = ?""",
                    (sub_id,),
                )
            elif failures:
                self._conn.execute(
                    "UPDATE subscriptions SET breaker_consecutive_failures = 0 WHERE id = ?",
                    (sub_id,),
                )
            return

        # outcome == 'failed': a task reached its final failure.
        failures += 1
        if is_probe or (state == "closed" and failures >= self._breaker_threshold):
            # Probe failed, or the threshold was reached: (re-)open the
            # breaker and start a fresh cooldown from this failure.
            self._conn.execute(
                """UPDATE subscriptions
                   SET breaker_state = 'open', breaker_consecutive_failures = ?,
                       breaker_next_probe_at = ?, breaker_probe_task_id = NULL
                   WHERE id = ?""",
                (failures, now + self._breaker_cooldown, sub_id),
            )
        else:
            self._conn.execute(
                "UPDATE subscriptions SET breaker_consecutive_failures = ? WHERE id = ?",
                (failures, sub_id),
            )

    def requeue_in_progress(self) -> int:
        """Reset tasks left 'in_progress' by a previous run (crash/restart recovery).

        Such a task is again unclaimed ('pending'), so if its event is
        (re-)cancelled after this reset the task is eligible for cancellation
        like any other pending task; otherwise it is redelivered.
        """
        now = time.time()
        with self._lock:
            cur = self._conn.execute(
                """UPDATE tasks SET status = 'pending', next_attempt_at = ?, claimed_by = NULL, updated_at = ?
                   WHERE status = 'in_progress'""",
                (now, now),
            )
            self._conn.commit()
        return cur.rowcount

    def event_status(self, event_id: str) -> dict | None:
        """Full status of an event: its tasks plus per-task attempt history."""
        with self._lock:
            event = self._conn.execute(
                "SELECT id, created_at FROM events WHERE id = ?", (event_id,)
            ).fetchone()
            if event is None:
                return None
            tasks = self._conn.execute(
                """SELECT t.*, s.target_url
                   FROM tasks t LEFT JOIN subscriptions s ON s.id = t.subscription_id
                   WHERE t.event_id = ? ORDER BY t.id""",
                (event_id,),
            ).fetchall()
            attempts = self._conn.execute(
                """SELECT a.* FROM attempts a
                   JOIN tasks t ON t.id = a.task_id
                   WHERE t.event_id = ? ORDER BY a.task_id, a.attempt_no""",
                (event_id,),
            ).fetchall()

        history: dict[int, list[dict]] = {}
        for a in attempts:
            history.setdefault(a["task_id"], []).append(dict(a))
        return {
            "event": dict(event),
            "tasks": [dict(t) for t in tasks],
            "attempts_by_task": history,
        }

    def cancel_event(self, event_id: str) -> dict | None:
        """Atomically cancel every not-yet-claimed delivery task of an event.

        Runs under the connection lock in a single transaction, serialized
        against worker claims, so a task a worker has already claimed
        ('in_progress') -- or which has already finished
        ('delivered'/'failed') -- is never touched and keeps its recorded
        result; only rows still 'pending' are flipped to the terminal
        'cancelled' state. Cancelled tasks:

        - are never claimed again (the claim query only selects 'pending'),
          so they take part in no retry and never reach finish_attempt,
          which also means they never move the circuit-breaker counters;
        - are terminal for the per-subscription ordering rule (the claim
          query's blocking NOT EXISTS watches only 'pending'/'in_progress'),
          so later tasks of the same subscription are released immediately.

        One edge case is repaired in the same transaction: a half-open
        breaker records exactly one probe task. That task sits 'pending'
        while backing off between probe attempts and can therefore be
        cancelled; the breaker is then moved back to 'open' with the probe
        due at once so the next pending task becomes the replacement probe
        instead of the subscription being wedged in 'half_open' forever.

        Repeated cancellation is idempotent: the second call finds no
        'pending' rows, cancels zero, and reports the same totals. Returns
        None when the event does not exist.
        """
        now = time.time()
        with self._lock:
            exists = self._conn.execute(
                "SELECT 1 FROM events WHERE id = ?", (event_id,)
            ).fetchone()
            if exists is None:
                return None

            cur = self._conn.execute(
                """UPDATE tasks
                       SET status = 'cancelled', next_attempt_at = NULL,
                           claimed_by = NULL, cancelled_at = ?, updated_at = ?
                     WHERE event_id = ? AND status = 'pending'""",
                (now, now, event_id),
            )
            newly_cancelled = cur.rowcount

            if newly_cancelled:
                # Release any half-open breaker whose unique probe was just
                # cancelled; the next claim promotes the earliest remaining
                # pending task to a fresh, immediately-due probe.
                self._conn.execute(
                    """UPDATE subscriptions
                          SET breaker_state = 'open', breaker_next_probe_at = ?,
                              breaker_probe_task_id = NULL
                        WHERE breaker_state = 'half_open'
                          AND breaker_probe_task_id IN (
                              SELECT id FROM tasks
                               WHERE event_id = ? AND status = 'cancelled'
                          )""",
                    (now, event_id),
                )

            rows = self._conn.execute(
                "SELECT status, COUNT(*) AS n FROM tasks WHERE event_id = ? GROUP BY status",
                (event_id,),
            ).fetchall()
            counts = {
                "pending": 0,
                "in_progress": 0,
                "delivered": 0,
                "failed": 0,
                "cancelled": 0,
            }
            for r in rows:
                counts[r["status"]] = r["n"]
            self._conn.commit()

        return {
            "event_id": event_id,
            "cancelled": newly_cancelled,
            "cancelled_at": now,
            "counts": counts,
        }
