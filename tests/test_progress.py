"""Acceptance tests for lease command-progress reporting.

Covers, against the REAL API and REAL PostgreSQL:

* GET returns the two nullable progress fields (``null`` before any report,
  for both new and historically created leases);
* acquire -> successive reports -> GET reflects the latest high-water mark,
  while ``expires_at`` never changes (reporting does not extend a lease);
* re-reporting the same sequence is a replay: the recorded sequence and the
  ORIGINAL ``last_progress_at`` come back byte-identical;
* a smaller sequence is rejected with ``PROGRESS_REGRESSION`` and writes
  nothing;
* unknown token -> ``LEASE_NOT_FOUND``, expired token -> ``LEASE_EXPIRED``,
  neither writes anything;
* concurrent reports serialise on the antenna row and the final committed
  sequence is the maximum of all accepted reports;
* malformed sequence payloads are 422 with no writes.
"""

from __future__ import annotations

import threading
import uuid
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime

import httpx
import pytest
from sqlalchemy import text
from sqlalchemy.engine import Engine

from conftest import KNOWN_ANTENNA, acquire, insert_expired_lease, make_key


def progress(client: httpx.Client, token: str, sequence: int) -> httpx.Response:
    return client.post(
        f"/leases/{token}/progress", json={"sequence": sequence}
    )


def _lease_row(db_engine: Engine, token: str) -> dict:
    with db_engine.connect() as conn:
        row = conn.execute(
            text(
                """
                SELECT token, acquired_at, expires_at,
                       last_command_sequence, last_progress_at
                FROM leases WHERE token = :token
                """
            ),
            {"token": token},
        ).mappings().one()
    return dict(row)


def test_new_lease_reports_null_progress_and_fields_are_nullable(http_client):
    resp = acquire(http_client, antenna_id="ANT-01")
    assert resp.status_code == 200
    token = resp.json()["lease_token"]

    body = http_client.get(f"/leases/{token}").json()
    # Original fields are all still there...
    assert body["lease_token"] == token
    assert body["antenna_id"] == "ANT-01"
    assert body["controller"] == "gs-beijing-A"
    assert body["active"] is True
    assert "acquired_at" in body and "expires_at" in body
    # ...and the two new fields are present but null.
    assert body["last_command_sequence"] is None
    assert body["last_progress_at"] is None


def test_historical_lease_without_progress_keeps_null_fields(
    http_client, db_engine
):
    # A lease planted directly via SQL (like every pre-migration row) has no
    # progress; the migration must not have backfilled anything.
    planted = insert_expired_lease(
        db_engine,
        antenna_id="ANT-02",
        # Still active right now so GET says active=True.
        age_seconds=-60,
        ttl_seconds=120,
    )
    token = planted["token"]

    body = http_client.get(f"/leases/{token}").json()
    assert body["active"] is True
    assert body["last_command_sequence"] is None
    assert body["last_progress_at"] is None

    row = _lease_row(db_engine, token)
    assert row["last_command_sequence"] is None
    assert row["last_progress_at"] is None


def test_successive_reports_advance_and_get_returns_latest_progress(
    http_client, db_engine
):
    token = acquire(http_client, antenna_id="ANT-03").json()["lease_token"]
    before = _lease_row(db_engine, token)

    # Non-negative boundary: sequence 0 is a legitimate first command.
    for seq in (0, 1, 2, 5):
        resp = progress(http_client, token, seq)
        assert resp.status_code == 200, resp.text
        body = resp.json()
        assert body["last_command_sequence"] == seq
        recorded = datetime.fromisoformat(body["last_progress_at"])
        assert body["last_progress_at"].endswith("+00:00")
        assert recorded == _lease_row(db_engine, token)["last_progress_at"]

    status = http_client.get(f"/leases/{token}").json()
    assert status["last_command_sequence"] == 5
    assert status["last_progress_at"] is not None
    assert status["active"] is True

    after = _lease_row(db_engine, token)
    # Reporting progress never changes how long the lease is held.
    assert after["expires_at"] == before["expires_at"]
    assert after["acquired_at"] == before["acquired_at"]


def test_same_sequence_replay_returns_original_recorded_time(http_client):
    token = acquire(http_client, antenna_id="ANT-04").json()["lease_token"]

    first = progress(http_client, token, 7)
    assert first.status_code == 200
    first_body = first.json()

    # Enough real time to pass that a fresh clock_timestamp() would differ.
    import time

    time.sleep(0.2)
    replay = progress(http_client, token, 7)
    assert replay.status_code == 200
    replay_body = replay.json()

    assert replay_body == first_body
    assert replay_body["last_command_sequence"] == 7
    assert replay_body["last_progress_at"] == first_body["last_progress_at"]


def test_smaller_sequence_is_regression_and_writes_nothing(http_client, db_engine):
    token = acquire(http_client, antenna_id="ANT-05").json()["lease_token"]
    assert progress(http_client, token, 10).status_code == 200
    before = _lease_row(db_engine, token)

    resp = progress(http_client, token, 9)
    assert resp.status_code == 409
    error = resp.json()["error"]
    assert error["code"] == "PROGRESS_REGRESSION"
    assert error["details"]["reported_sequence"] == 9
    assert error["details"]["last_command_sequence"] == 10
    assert error["details"]["lease_token"] == token

    after = _lease_row(db_engine, token)
    assert after["last_command_sequence"] == 10
    assert after["last_progress_at"] == before["last_progress_at"]

    # Advancing past the high-water mark works normally again.
    again = progress(http_client, token, 11)
    assert again.status_code == 200
    assert again.json()["last_command_sequence"] == 11


def test_unknown_token_is_rejected_and_writes_nothing(http_client, db_engine):
    with db_engine.connect() as conn:
        before = int(conn.execute(text("SELECT count(*) FROM leases")).scalar_one())

    resp = progress(http_client, f"unknown-{uuid.uuid4()}", 1)
    assert resp.status_code == 404
    assert resp.json()["error"]["code"] == "LEASE_NOT_FOUND"

    with db_engine.connect() as conn:
        after = int(conn.execute(text("SELECT count(*) FROM leases")).scalar_one())
    assert after == before


def test_expired_token_is_rejected_and_data_is_unchanged(http_client, db_engine):
    planted = insert_expired_lease(
        db_engine, antenna_id=KNOWN_ANTENNA, age_seconds=5, ttl_seconds=10
    )
    token = planted["token"]
    before = _lease_row(db_engine, token)

    resp = progress(http_client, token, 1)
    assert resp.status_code == 409
    error = resp.json()["error"]
    assert error["code"] == "LEASE_EXPIRED"
    assert error["details"]["lease_token"] == token
    assert error["details"]["expires_at"] == before["expires_at"].isoformat()

    after = _lease_row(db_engine, token)
    assert after == before
    assert after["last_command_sequence"] is None
    assert after["last_progress_at"] is None


def test_expiry_boundary_allows_handover_after_progress_reports(http_client):
    # Shortest allowed lease: report progress, wait it out, acquire again
    # with a new key; the new holder can report and the old token is refused.
    first = acquire(
        http_client,
        antenna_id="ANT-06",
        duration_seconds=5,
        idempotency_key=make_key("progress-expiry-1"),
    ).json()
    old_token = first["lease_token"]
    assert progress(http_client, old_token, 1).status_code == 200

    import time

    # Boundary belongs to the new request once expires_at <= now.
    deadline = time.time() + 15
    second = None
    while time.time() < deadline:
        second = acquire(
            http_client,
            antenna_id="ANT-06",
            duration_seconds=5,
            idempotency_key=make_key("progress-expiry-2"),
        )
        if second.status_code == 200:
            break
        assert second.json()["error"]["code"] == "ANTENNA_BUSY"
        time.sleep(0.5)
    assert second is not None and second.status_code == 200
    new_token = second.json()["lease_token"]
    assert new_token != old_token

    new_report = progress(http_client, new_token, 0)
    assert new_report.status_code == 200

    old_report = progress(http_client, old_token, 2)
    assert old_report.status_code == 409
    assert old_report.json()["error"]["code"] == "LEASE_EXPIRED"


@pytest.mark.parametrize("payload", [{"sequence": -1}, {"sequence": "3"}, {"sequence": 1.5}, {}])
def test_invalid_sequence_payloads_are_422(http_client, db_engine, payload):
    token = acquire(http_client).json()["lease_token"]
    with db_engine.connect() as conn:
        before = int(conn.execute(text("SELECT count(*) FROM leases")).scalar_one())

    resp = http_client.post(f"/leases/{token}/progress", json=payload)
    assert resp.status_code == 422
    assert resp.json()["error"]["code"] == "VALIDATION_ERROR"

    row = _lease_row(db_engine, token)
    assert row["last_command_sequence"] is None
    with db_engine.connect() as conn:
        after = int(conn.execute(text("SELECT count(*) FROM leases")).scalar_one())
    assert after == before


def test_extra_field_in_progress_body_is_rejected(http_client):
    token = acquire(http_client).json()["lease_token"]
    resp = http_client.post(
        f"/leases/{token}/progress", json={"sequence": 1, "extra": 2}
    )
    assert resp.status_code == 422
    assert resp.json()["error"]["code"] == "VALIDATION_ERROR"


def test_concurrent_reports_keep_the_maximum_sequence(http_client, db_engine):
    token = acquire(http_client, antenna_id="ANT-01").json()["lease_token"]
    sequences = list(range(1, 21))
    # Submit out of order so contenders don't naturally line up 1..20.
    sequences_shuffled = sequences[::2] + sequences[1::2]

    barrier = threading.Barrier(len(sequences_shuffled))

    # One HTTP client per thread (httpx clients are not shared across threads
    # by guarantee); build clients lazily inside the workers.
    base_url = str(http_client.base_url)

    def one_with_client(seq: int) -> tuple[int, int, dict]:
        barrier.wait(timeout=10)
        with httpx.Client(base_url=base_url, timeout=30.0) as client:
            resp = progress(client, token, seq)
            return seq, resp.status_code, resp.json()

    with ThreadPoolExecutor(max_workers=len(sequences_shuffled)) as pool:
        results = list(pool.map(one_with_client, sequences_shuffled))

    accepted = {seq: body for seq, status, body in results if status == 200}
    rejected = [(seq, body) for seq, status, body in results if status != 200]

    # Every rejected call must be a regression caused by a higher sequence
    # winning the race first.
    for seq, body in rejected:
        assert body["error"]["code"] == "PROGRESS_REGRESSION"
        assert body["error"]["details"]["reported_sequence"] == seq
        assert body["error"]["details"]["last_command_sequence"] > seq

    # The maximum sequence necessarily succeeded (nothing higher exists to
    # block it), and its recorded time is the final one.
    assert 20 in accepted
    max_body = accepted[20]

    row = _lease_row(db_engine, token)
    assert row["last_command_sequence"] == 20
    assert row["last_progress_at"] == datetime.fromisoformat(
        max_body["last_progress_at"]
    )

    status = http_client.get(f"/leases/{token}").json()
    assert status["last_command_sequence"] == 20
    assert status["last_progress_at"] == max_body["last_progress_at"]
