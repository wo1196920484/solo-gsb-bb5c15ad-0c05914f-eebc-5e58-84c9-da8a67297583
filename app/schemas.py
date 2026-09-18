"""Pydantic request/response models."""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from pydantic import BaseModel, Field, field_validator


class SubscriptionCreate(BaseModel):
    target_url: str = Field(min_length=1, max_length=2048)
    secret: str = Field(min_length=1, max_length=256)

    @field_validator("target_url")
    @classmethod
    def must_be_http(cls, v: str) -> str:
        v = v.strip()
        if not (v.startswith("http://") or v.startswith("https://")):
            raise ValueError("target_url must start with http:// or https://")
        return v


class SubscriptionOut(BaseModel):
    id: str
    target_url: str
    created_at: str
    paused: bool
    paused_at: str | None
    rotating: bool
    previous_secret_expires_at: str | None
    state: str  # circuit breaker: 'closed' | 'open' | 'half_open'
    consecutive_failures: int
    next_probe_at: str | None  # set only while the breaker is open


class SecretRotationCreate(BaseModel):
    """Body of POST /subscriptions/{id}/rotate-secret.

    grace_seconds bounds how long deliveries are dual-signed with both the
    new and the previous secret (0 = switch immediately, max 24h).
    """

    new_secret: str = Field(min_length=1, max_length=256)
    grace_seconds: float = Field(ge=0, le=86400)


class EventCreate(BaseModel):
    event_id: str = Field(min_length=1, max_length=128)
    subscription_ids: list[str] = Field(min_length=1, max_length=100)
    payload: dict[str, Any]
    # Optional scheduled delivery time. When omitted the event's tasks are
    # due immediately; when set it must be an explicit, timezone-aware
    # ISO-8601 timestamp in the future (a 'Z' suffix is accepted).
    deliver_at: datetime | None = Field(default=None)

    @field_validator("deliver_at", mode="before")
    @classmethod
    def _validate_deliver_at(cls, v: Any) -> datetime | None:
        if v is None:
            return None
        if isinstance(v, datetime):
            dt = v
        elif isinstance(v, str):
            text = v.strip()
            try:
                # fromisoformat accepts 'Z' on Python 3.11+.
                dt = datetime.fromisoformat(text)
            except ValueError:
                raise ValueError("deliver_at must be an ISO-8601 timestamp")
        else:
            raise ValueError("deliver_at must be an ISO-8601 timestamp string")
        # An ISO-8601 date without a time (e.g. "2026-09-20") parses to a
        # naive midnight datetime; without an explicit timezone/offset the
        # instant is ambiguous, so it is rejected.
        if dt.tzinfo is None or dt.tzinfo.utcoffset(dt) is None:
            raise ValueError("deliver_at must include an explicit timezone, e.g. +00:00 or 'Z'")
        if dt <= datetime.now(timezone.utc):
            raise ValueError("deliver_at must be a time in the future")
        return dt


class IngestEvent(BaseModel):
    """Body of an HMAC-protected inbound event on POST /ingest.

    The whole body (including any extra fields) is forwarded verbatim to
    targets; only subscription_ids is consumed by the relay itself.
    """

    model_config = {"extra": "allow"}

    subscription_ids: list[str] = Field(min_length=1, max_length=100)


class TaskRef(BaseModel):
    task_id: int
    subscription_id: str
    status: str
    # Scheduled delivery time (ISO time); null = immediate. Has a default so
    # receipts persisted by older versions (which lack the field) still
    # validate when an idempotent replay returns the stored receipt.
    deliver_at: str | None = None


class EventAccepted(BaseModel):
    event_id: str
    created: list[TaskRef]
    duplicates: list[TaskRef]


class AttemptOut(BaseModel):
    attempt_no: int
    status_code: int | None
    error: str | None
    started_at: str
    finished_at: str
    duration_ms: int


class TaskStatus(BaseModel):
    task_id: int
    subscription_id: str
    target_url: str | None
    status: str  # 'pending' | 'in_progress' | 'delivered' | 'failed' | 'cancelled'
    attempts: int
    max_attempts: int
    deliver_at: str | None  # scheduled delivery time requested at submit (null = immediate)
    next_attempt_at: str | None
    last_status_code: int | None
    last_error: str | None
    result: str | None
    cancelled_at: str | None  # set only when the task was cancelled with its event
    history: list[AttemptOut]


class EventStatusCounts(BaseModel):
    """Per-status task totals for an event (all counts always present)."""

    pending: int
    in_progress: int
    delivered: int
    failed: int
    cancelled: int


class EventStatus(BaseModel):
    event_id: str
    created_at: str
    cancelled: bool  # the event was cancelled at least once (one or more tasks are 'cancelled')
    cancelled_at: str | None  # when the event's last cancel ran (ISO time, null if never)
    counts: EventStatusCounts
    targets: list[TaskStatus]


class EventCancelResult(BaseModel):
    """Result of POST /events/{event_id}/cancel.

    'cancelled' is the number of tasks moved to 'cancelled' by this call
    (0 on a repeated, idempotent call); 'counts' reports the resulting
    per-status totals, so callers can see how many tasks were already in
    flight or finished at cancel time.
    """

    event_id: str
    cancelled: int
    cancelled_at: str
    counts: EventStatusCounts
