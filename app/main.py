"""FastAPI application: subscription management, event ingestion, status queries."""
from __future__ import annotations

import asyncio
import json
import logging
import time
from contextlib import asynccontextmanager
from datetime import datetime, timezone

import httpx
from fastapi import FastAPI, HTTPException, Request, Response
from pydantic import ValidationError

from . import config, signing
from .db import Database
from .schemas import (
    EventAccepted,
    EventCancelResult,
    EventCreate,
    EventStatus,
    IngestEvent,
    SecretRotationCreate,
    SubscriptionCreate,
    SubscriptionOut,
)
from .worker import worker_loop

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
log = logging.getLogger("webhook-relay")


def _iso(ts: float | None) -> str | None:
    if ts is None:
        return None
    return datetime.fromtimestamp(ts, tz=timezone.utc).isoformat()


def _sub_out(sub: dict) -> dict:
    """Serialize a subscription row for the API, deriving the paused flag.

    Secret material (current or previous) is never included; only the
    rotation state (flag + grace expiry) is exposed. The circuit-breaker
    state is reported as persisted: 'closed' | 'open' | 'half_open', with
    the consecutive-failure count and, while open, the earliest time the
    half-open probe may run.
    """
    grace_expires = sub.get("previous_secret_expires_at")
    rotating = grace_expires is not None and grace_expires > time.time()
    return {
        "id": sub["id"],
        "target_url": sub["target_url"],
        "created_at": _iso(sub["created_at"]),
        "paused": sub["paused_at"] is not None,
        "paused_at": _iso(sub["paused_at"]),
        "rotating": rotating,
        "previous_secret_expires_at": _iso(grace_expires) if rotating else None,
        "state": sub["breaker_state"],
        "consecutive_failures": sub["breaker_consecutive_failures"],
        "next_probe_at": _iso(sub["breaker_next_probe_at"]),
    }


@asynccontextmanager
async def lifespan(app: FastAPI):
    db = Database(
        config.DB_PATH,
        breaker_failure_threshold=config.BREAKER_FAILURE_THRESHOLD,
        breaker_cooldown_seconds=config.BREAKER_COOLDOWN_SECONDS,
    )
    app.state.db = db
    app.state.http = httpx.AsyncClient(timeout=config.HTTP_TIMEOUT)

    # Resume work interrupted by a previous shutdown.
    recovered = db.requeue_in_progress()
    if recovered:
        log.info("requeued %s task(s) interrupted by previous run", recovered)

    stop = asyncio.Event()
    app.state.stop = stop
    workers = [
        asyncio.create_task(worker_loop(f"worker-{i+1}", db, app.state.http, stop))
        for i in range(config.WORKER_COUNT)
    ]
    log.info("started %s delivery worker(s), db=%s", len(workers), config.DB_PATH)
    try:
        yield
    finally:
        stop.set()
        await asyncio.gather(*workers)
        await app.state.http.aclose()
        db.close()


app = FastAPI(title="Local Webhook Relay", version="1.0.0", lifespan=lifespan)


@app.get("/healthz")
def healthz() -> dict:
    return {"status": "ok"}


# ----------------------------------------------------------------------
# subscriptions
# ----------------------------------------------------------------------
@app.post("/subscriptions", response_model=SubscriptionOut, status_code=201)
def create_subscription(body: SubscriptionCreate, response: Response) -> dict:
    sub = app.state.db.create_subscription(body.target_url, body.secret)
    return _sub_out(sub)


@app.get("/subscriptions", response_model=list[SubscriptionOut])
def list_subscriptions() -> list[dict]:
    return [_sub_out(s) for s in app.state.db.list_subscriptions()]


@app.get("/subscriptions/{sub_id}", response_model=SubscriptionOut)
def get_subscription(sub_id: str) -> dict:
    sub = app.state.db.get_subscription(sub_id)
    if sub is None:
        raise HTTPException(404, f"subscription {sub_id!r} not found")
    return _sub_out(sub)


@app.delete("/subscriptions/{sub_id}", status_code=204)
def delete_subscription(sub_id: str) -> Response:
    if not app.state.db.delete_subscription(sub_id):
        raise HTTPException(404, f"subscription {sub_id!r} not found")
    return Response(status_code=204)


@app.post("/subscriptions/{sub_id}/pause", response_model=SubscriptionOut)
def pause_subscription(sub_id: str) -> dict:
    """Pause claiming of new deliveries for a subscription (idempotent).

    Tasks already in progress run to completion; tasks submitted while
    paused are stored and stay 'pending' until the subscription resumes.
    """
    sub = app.state.db.set_subscription_paused(sub_id, True)
    if sub is None:
        raise HTTPException(404, f"subscription {sub_id!r} not found")
    return _sub_out(sub)


@app.post("/subscriptions/{sub_id}/resume", response_model=SubscriptionOut)
def resume_subscription(sub_id: str) -> dict:
    """Resume a paused subscription (idempotent).

    Pending tasks keep their original next_attempt_at and are claimed in
    the usual per-subscription task_id order; nothing is rescheduled.
    """
    sub = app.state.db.set_subscription_paused(sub_id, False)
    if sub is None:
        raise HTTPException(404, f"subscription {sub_id!r} not found")
    return _sub_out(sub)


@app.post("/subscriptions/{sub_id}/rotate-secret", response_model=SubscriptionOut)
def rotate_subscription_secret(sub_id: str, body: SecretRotationCreate) -> dict:
    """Rotate the delivery-signing secret with a dual-sign grace window.

    The new secret takes effect immediately for the X-Webhook-Signature
    header; during grace_seconds the previous secret is additionally sent
    as X-Webhook-Signature-Previous so receivers can switch over safely.
    A second rotation while the grace window is still open is rejected.
    """
    status, sub = app.state.db.rotate_secret(sub_id, body.new_secret, body.grace_seconds)
    if status == "not_found":
        raise HTTPException(404, f"subscription {sub_id!r} not found")
    if status == "in_grace":
        until = _iso(sub["previous_secret_expires_at"])
        raise HTTPException(409, f"a secret rotation is already in progress until {until}")
    log.info("subscription %s rotated signing secret (grace %.0fs)", sub_id, body.grace_seconds)
    return _sub_out(sub)


# ----------------------------------------------------------------------
# events
# ----------------------------------------------------------------------
@app.post("/events", response_model=EventAccepted)
def submit_event(body: EventCreate, response: Response) -> dict:
    db: Database = app.state.db

    unknown = [sid for sid in body.subscription_ids if not db.subscription_exists(sid)]
    if unknown:
        raise HTTPException(400, f"unknown subscription id(s): {', '.join(unknown)}")

    # Store the event once; an identical re-submission is idempotent, a
    # conflicting payload for the same event_id is rejected.
    payload = json.dumps(body.payload, separators=(",", ":"), ensure_ascii=False)
    existing = db.get_event_payload(body.event_id)
    if existing is None:
        db.create_event(body.event_id, payload)
    elif existing != payload:
        raise HTTPException(409, f"event {body.event_id!r} already exists with a different payload")

    # Optional scheduled delivery. Stored as epoch seconds and seeded into
    # each task's next_attempt_at, so workers simply cannot claim the tasks
    # before this instant; None means the tasks are due right away.
    deliver_at_ts = (
        body.deliver_at.astimezone(timezone.utc).timestamp()
        if body.deliver_at is not None
        else None
    )

    created, duplicates = [], []
    for sid in dict.fromkeys(body.subscription_ids):  # preserve order, drop repeats
        task, was_created = db.create_task(
            body.event_id, sid, config.MAX_ATTEMPTS, deliver_at=deliver_at_ts
        )
        ref = {
            "task_id": task["id"],
            "subscription_id": sid,
            "status": task["status"],
            "deliver_at": _iso(task["deliver_at"]),
        }
        (created if was_created else duplicates).append(ref)

    response.status_code = 201 if created else 200
    return {"event_id": body.event_id, "created": created, "duplicates": duplicates}


# ----------------------------------------------------------------------
# HMAC-protected inbound ingest endpoint
# ----------------------------------------------------------------------
@app.post("/ingest", response_model=EventAccepted)
async def ingest_event(request: Request, response: Response) -> dict:
    if not config.INGEST_SECRET:
        # Fail closed: without a configured secret every signature is suspect.
        raise HTTPException(503, "ingest endpoint is not configured (set INGEST_SECRET)")

    timestamp_h = request.headers.get("x-webhook-timestamp")
    webhook_id = request.headers.get("x-webhook-id")
    signature_h = request.headers.get("x-webhook-signature")
    if not timestamp_h or not webhook_id or not signature_h:
        raise HTTPException(
            400,
            "missing required header(s): X-Webhook-Timestamp, X-Webhook-Id, X-Webhook-Signature",
        )
    if len(webhook_id) > 128:
        raise HTTPException(400, "X-Webhook-Id too long (max 128 characters)")

    timestamp_h = timestamp_h.strip()
    try:
        ts = float(timestamp_h)
    except ValueError:
        raise HTTPException(400, "X-Webhook-Timestamp must be a Unix timestamp in seconds")

    skew = abs(time.time() - ts)
    if skew > config.INGEST_TIMESTAMP_TOLERANCE:
        raise HTTPException(
            401,
            f"timestamp outside the allowed +/-{config.INGEST_TIMESTAMP_TOLERANCE:.0f}s window "
            f"(skew {skew:.0f}s)",
        )

    raw_body = await request.body()
    if not raw_body:
        raise HTTPException(400, "request body must not be empty")

    if not signing.verify_ingest(config.INGEST_SECRET, timestamp_h, webhook_id, raw_body, signature_h):
        raise HTTPException(401, "invalid X-Webhook-Signature")

    # Signature verified; now validate the body shape.
    try:
        parsed = json.loads(raw_body.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        raise HTTPException(400, "request body must be valid UTF-8 JSON")
    try:
        body = IngestEvent.model_validate(parsed)
    except ValidationError as exc:
        raise HTTPException(422, json.loads(exc.json()))

    status, result = app.state.db.ingest_event(
        webhook_id, raw_body, body.subscription_ids, config.MAX_ATTEMPTS
    )
    if status == "conflict":
        raise HTTPException(409, f"webhook id {webhook_id!r} was already received with a different body")
    if status == "unknown":
        raise HTTPException(400, f"unknown subscription id(s): {', '.join(result)}")

    # Replays with the same id and body return the original result and never
    # create additional delivery tasks (tasks are only created on 'created').
    response.status_code = 201 if status == "created" else 200
    return result


@app.post("/events/{event_id}/cancel", response_model=EventCancelResult)
def cancel_event(event_id: str) -> dict:
    """Cancel the still-pending delivery tasks of an event (idempotent).

    Only tasks no worker has claimed yet are cancelled; tasks already in
    progress run to completion and finished tasks keep their recorded
    result. Cancelled tasks are terminal: they are never retried, never
    counted by the per-subscription circuit breaker, and never block later
    tasks of the same subscription. Repeating the call is a no-op that
    returns the same per-status totals; an unknown event returns 404.
    """
    result = app.state.db.cancel_event(event_id)
    if result is None:
        raise HTTPException(404, f"event {event_id!r} not found")
    log.info(
        "event %s cancelled: %s task(s) newly cancelled, totals=%s",
        event_id,
        result["cancelled"],
        result["counts"],
    )
    return {
        "event_id": event_id,
        "cancelled": result["cancelled"],
        "cancelled_at": _iso(result["cancelled_at"]),
        "counts": result["counts"],
    }


@app.get("/events/{event_id}", response_model=EventStatus)
def get_event_status(event_id: str) -> dict:
    data = app.state.db.event_status(event_id)
    if data is None:
        raise HTTPException(404, f"event {event_id!r} not found")

    counts = {"pending": 0, "in_progress": 0, "delivered": 0, "failed": 0, "cancelled": 0}
    last_cancelled_at: float | None = None
    targets = []
    for t in data["tasks"]:
        counts[t["status"]] = counts.get(t["status"], 0) + 1
        if t["status"] == "cancelled" and t.get("cancelled_at") is not None:
            if last_cancelled_at is None or t["cancelled_at"] > last_cancelled_at:
                last_cancelled_at = t["cancelled_at"]
        history = [
            {
                "attempt_no": a["attempt_no"],
                "status_code": a["status_code"],
                "error": a["error"],
                "started_at": _iso(a["started_at"]),
                "finished_at": _iso(a["finished_at"]),
                "duration_ms": round((a["finished_at"] - a["started_at"]) * 1000),
            }
            for a in data["attempts_by_task"].get(t["id"], [])
        ]
        targets.append(
            {
                "task_id": t["id"],
                "subscription_id": t["subscription_id"],
                "target_url": t["target_url"],
                "status": t["status"],
                "attempts": t["attempts"],
                "max_attempts": t["max_attempts"],
                "deliver_at": _iso(t["deliver_at"]),
                "next_attempt_at": _iso(t["next_attempt_at"]) if t["status"] == "pending" else None,
                "last_status_code": t["last_status_code"],
                "last_error": t["last_error"],
                "result": t["result"],
                "cancelled_at": _iso(t.get("cancelled_at")),
                "history": history,
            }
        )
    return {
        "event_id": event_id,
        "created_at": _iso(data["event"]["created_at"]),
        "cancelled": counts["cancelled"] > 0,
        "cancelled_at": _iso(last_cancelled_at),
        "counts": counts,
        "targets": targets,
    }
