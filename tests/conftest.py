"""Shared fixtures for the acceptance suite.

These tests require a REAL PostgreSQL and a REAL running API:

* ``API_BASE_URL`` points at the HTTP service (http://api:8000 under compose,
  http://localhost:${API_PORT:-8000} for local runs).
* ``DATABASE_URL`` points at the same PostgreSQL instance and is used to seed
  expired rows, to inspect committed state, and to prove rejected requests
  never wrote anything.

Nothing here mocks the service or hard-codes responses.
"""

from __future__ import annotations

import os
import uuid
from concurrent.futures import ThreadPoolExecutor
from typing import Any

import httpx
import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

API_BASE_URL = os.environ.get(
    "API_BASE_URL", f"http://localhost:{os.environ.get('API_PORT', '8000')}"
)
DATABASE_URL = os.environ.get(
    "DATABASE_URL",
    "postgresql+psycopg2://satctl:satctl@localhost:5432/satctl",
)

MIN_LEASE_SECONDS = 5
MAX_LEASE_SECONDS = 120
KNOWN_ANTENNA = "ANT-01"


@pytest.fixture(scope="session")
def http_client() -> httpx.Client:
    with httpx.Client(base_url=API_BASE_URL, timeout=30.0) as client:
        # Fail fast with a readable message if the stack is not up.
        try:
            resp = client.get("/health")
            resp.raise_for_status()
        except Exception as exc:  # pragma: no cover - environment failure
            pytest.exit(f"API at {API_BASE_URL} is not reachable: {exc}")
        yield client


@pytest.fixture(scope="session")
def db_engine() -> Engine:
    engine = create_engine(DATABASE_URL, future=True)
    yield engine
    engine.dispose()


@pytest.fixture(autouse=True)
def reset_lease_tables(db_engine: Engine):
    """Each test starts with a clean lease/idempotency state.

    The seeded antenna catalog is intentionally preserved, but every
    antenna's control-generation counter is rewound to 0 so it stays
    consistent with the truncated lease history: the first acquisition in
    any test is generation 1 again.
    """
    with db_engine.begin() as conn:
        conn.execute(
            text(
                "TRUNCATE TABLE renewal_idempotency_keys, idempotency_keys, "
                "lease_renewals, leases RESTART IDENTITY CASCADE"
            )
        )
        conn.execute(text("UPDATE antennas SET last_control_generation = 0"))
    yield


def make_key(prefix: str = "key") -> str:
    return f"{prefix}-{uuid.uuid4()}"


def acquire(
    client: httpx.Client,
    *,
    antenna_id: str = KNOWN_ANTENNA,
    controller: str = "gs-beijing-A",
    duration_seconds: int = 30,
    idempotency_key: str | None = None,
) -> httpx.Response:
    return client.post(
        "/leases",
        json={
            "antenna_id": antenna_id,
            "controller": controller,
            "duration_seconds": duration_seconds,
            "idempotency_key": idempotency_key or make_key(),
        },
    )


def release(client: httpx.Client, lease_token: str) -> httpx.Response:
    return client.post(f"/leases/{lease_token}/release")


def renew(
    client: httpx.Client,
    lease_token: str,
    extra_seconds: int,
    idempotency_key: str | None = None,
) -> httpx.Response:
    return client.post(
        f"/leases/{lease_token}/renew",
        json={
            "extra_seconds": extra_seconds,
            "idempotency_key": idempotency_key or make_key("renew"),
        },
    )


def count_rows(db_engine: Engine, sql: str, **params: Any) -> int:
    with db_engine.connect() as conn:
        return int(conn.execute(text(sql), params).scalar_one())


def active_lease_count(db_engine: Engine, antenna_id: str) -> int:
    # Active == still held (never released) AND not yet expired, both judged
    # against the database clock — the same predicate the service uses.
    return count_rows(
        db_engine,
        """
        SELECT count(*) FROM leases
        WHERE antenna_id = :antenna_id
          AND released_at IS NULL
          AND expires_at > clock_timestamp()
        """,
        antenna_id=antenna_id,
    )


def total_lease_count(db_engine: Engine, antenna_id: str) -> int:
    return count_rows(
        db_engine,
        "SELECT count(*) FROM leases WHERE antenna_id = :antenna_id",
        antenna_id=antenna_id,
    )


def insert_expired_lease(
    db_engine: Engine,
    *,
    antenna_id: str,
    age_seconds: float,
    ttl_seconds: int = 10,
    controller: str = "expired-ctrl",
    token: str | None = None,
) -> dict[str, Any]:
    """Insert a lease that expired ``age_seconds`` ago, directly via SQL.

    The row consumes a control generation from the antenna's counter exactly
    like a real acquisition, so the invariant "every lease owns the next
    generation of its antenna" holds for seeded history as well.
    """
    token = token or f"expired-{uuid.uuid4()}"
    with db_engine.begin() as conn:
        row = conn.execute(
            text(
                """
                WITH bumped AS (
                    UPDATE antennas
                    SET last_control_generation =
                            last_control_generation + 1
                    WHERE id = :antenna_id
                    RETURNING last_control_generation AS gen
                )
                INSERT INTO leases (antenna_id, controller, token,
                                    acquired_at, expires_at,
                                    control_generation)
                SELECT
                    :antenna_id, :controller, :token,
                    clock_timestamp() - make_interval(secs => :age + :ttl),
                    clock_timestamp() - make_interval(secs => :age),
                    bumped.gen
                FROM bumped
                RETURNING token, acquired_at, expires_at, control_generation
                """
            ),
            {
                "antenna_id": antenna_id,
                "controller": controller,
                "token": token,
                "age": age_seconds,
                "ttl": ttl_seconds,
            },
        ).mappings().one()
    return dict(row)


def parallel_acquire(
    client: httpx.Client,
    requests: list[dict[str, Any]],
    *,
    max_workers: int | None = None,
) -> list[httpx.Response]:
    """Fire acquisition requests at the server truly simultaneously.

    A barrier is used so every worker thread is parked when its HTTP request
    is about to be sent; all are released together to force real contention.
    """
    import threading

    barrier = threading.Barrier(len(requests))

    def one(payload: dict[str, Any]) -> httpx.Response:
        barrier.wait(timeout=10)
        return client.post("/leases", json=payload)

    with ThreadPoolExecutor(max_workers=max_workers or len(requests)) as pool:
        return list(pool.map(one, requests))
