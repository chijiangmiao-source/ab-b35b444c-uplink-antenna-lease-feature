"""Pydantic models for the antenna control lease API."""

from __future__ import annotations

from datetime import datetime

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    StrictInt,
    field_serializer,
    field_validator,
)

from app.config import (
    MAX_LEASE_SECONDS,
    MAX_RENEW_SECONDS,
    MIN_LEASE_SECONDS,
    MIN_RENEW_SECONDS,
)


class AcquireRequest(BaseModel):
    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)

    antenna_id: str = Field(..., min_length=1, max_length=64)
    controller: str = Field(..., min_length=1, max_length=128)
    duration_seconds: StrictInt = Field(
        ...,
        ge=MIN_LEASE_SECONDS,
        le=MAX_LEASE_SECONDS,
        description=f"租约时长（必须是 JSON 整数，不接受文本数字），闭区间 [{MIN_LEASE_SECONDS}, {MAX_LEASE_SECONDS}] 秒。",
    )
    idempotency_key: str = Field(..., min_length=1, max_length=128)

    @field_validator("antenna_id", "controller", "idempotency_key")
    @classmethod
    def _reject_blank(cls, value: str) -> str:
        if not value or not value.strip():
            raise ValueError("不能为空或纯空白。")
        return value


class _LeaseBase(BaseModel):
    """Shared lease fields and timestamp serialisation.

    Pydantic renders a UTC datetime as ``...Z`` while FastAPI's plain-dict
    path (``jsonable_encoder``) renders it as ``...+00:00``; routing every
    lease response through one explicit serializer keeps the same
    ``expires_at`` byte-identical across acquisition and lookup.

    The serializer references ``acquired_at``/``expires_at``, so those fields
    MUST be declared on this same class: Pydantic v2 rejects a field
    serializer whose target fields are only defined in a subclass (it raises
    at import time, which would prevent the app from starting).
    """

    antenna_id: str
    controller: str
    lease_token: str
    acquired_at: datetime
    expires_at: datetime
    # Monotonically increasing per-antenna control generation, allocated by
    # the acquisition transaction and frozen onto the lease. An uplink
    # device uses it to reject instructions from a stale (partitioned)
    # controller: only the newest generation is authoritative.
    control_generation: int

    @field_serializer("acquired_at", "expires_at", when_used="always")
    def _serialize_iso8601(self, value: datetime) -> str:
        return value.isoformat()


class AcquireResponse(_LeaseBase):
    replay: bool = False


class LeaseStatusResponse(_LeaseBase):
    active: bool
    # Progress tracking. Both stay NULL for leases that have never reported
    # (including every lease that existed before the progress feature); they
    # are never backfilled.
    last_command_sequence: int | None = None
    last_progress_at: datetime | None = None
    # NULL while the holder has not released early.  This is independent of
    # the progress high-water mark above.
    released_at: datetime | None = None

    @field_serializer("last_progress_at", when_used="always")
    def _serialize_progress_at(self, value: datetime | None) -> str | None:
        return value.isoformat() if value is not None else None

    @field_serializer("released_at", when_used="always")
    def _serialize_released_at(self, value: datetime | None) -> str | None:
        return value.isoformat() if value is not None else None


class ProgressRequest(BaseModel):
    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)

    # JSON integer only (a textual "3" is rejected); ge=0 enforces the
    # non-negative bound at the HTTP boundary as well as in the service.
    # The upper bound matches the BIGINT column so an oversized integer gets
    # a stable 422 instead of a database error.
    sequence: StrictInt = Field(..., ge=0, le=2**63 - 1)


class ProgressResponse(BaseModel):
    lease_token: str
    last_command_sequence: int
    last_progress_at: datetime

    @field_serializer("last_progress_at", when_used="always")
    def _serialize_progress_at(self, value: datetime) -> str:
        return value.isoformat()


class RenewRequest(BaseModel):
    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)

    # Extra holding time added ON TOP of the current expiry. JSON integer
    # only; the closed [5, 120] bound matches a single acquisition.
    extra_seconds: StrictInt = Field(
        ...,
        ge=MIN_RENEW_SECONDS,
        le=MAX_RENEW_SECONDS,
        description=(
            f"追加秒数（JSON 整数），闭区间 [{MIN_RENEW_SECONDS}, "
            f"{MAX_RENEW_SECONDS}] 秒，从当前到期时间继续累加。"
        ),
    )
    idempotency_key: str = Field(..., min_length=1, max_length=128)

    @field_validator("idempotency_key")
    @classmethod
    def _reject_blank(cls, value: str) -> str:
        if not value or not value.strip():
            raise ValueError("不能为空或纯空白。")
        return value


class RenewResponse(BaseModel):
    lease_token: str
    previous_expires_at: datetime
    new_expires_at: datetime
    # False on the first accepted renewal; True when the same key + same
    # parameters replay a previously accepted renewal. Apart from this flag
    # the business fields are byte-identical to the first response.
    replay: bool = False

    @field_serializer(
        "previous_expires_at", "new_expires_at", when_used="always"
    )
    def _serialize_iso8601(self, value: datetime) -> str:
        return value.isoformat()
