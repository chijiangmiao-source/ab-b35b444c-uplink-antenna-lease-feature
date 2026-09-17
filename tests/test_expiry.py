"""Expiry handover: the database clock is the only clock.

A lease is active while ``expires_at > clock_timestamp()``. At the boundary
(``expires_at == clock_timestamp()``) and afterwards the antenna is free, and
the next atomic acquisition takes over without overwriting history.
"""

from __future__ import annotations

from datetime import datetime, timezone

from conftest import (
    KNOWN_ANTENNA,
    active_lease_count,
    acquire,
    count_rows,
    insert_expired_lease,
    make_key,
)


def test_expired_lease_hands_over_atomically(http_client, db_engine):
    expired = insert_expired_lease(
        db_engine, antenna_id=KNOWN_ANTENNA, age_seconds=2
    )
    # Precondition: no active lease, but history is still there.
    assert active_lease_count(db_engine, KNOWN_ANTENNA) == 0
    assert count_rows(db_engine, "SELECT count(*) FROM leases") == 1

    resp = acquire(http_client, controller="successor", duration_seconds=30)
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["lease_token"] != expired["token"]
    assert body["replay"] is False

    new_expiry = datetime.fromisoformat(body["expires_at"])
    assert new_expiry > expired["expires_at"]
    assert active_lease_count(db_engine, KNOWN_ANTENNA) == 1
    # Old row is preserved (audit trail), exactly one is active now.
    assert count_rows(db_engine, "SELECT count(*) FROM leases") == 2


def test_boundary_belongs_to_new_request(http_client, db_engine):
    # Lease that expired ~1.5s ago: clock is strictly past expires_at,
    # exercising the expires_at <= clock_timestamp() branch including margin.
    insert_expired_lease(db_engine, antenna_id=KNOWN_ANTENNA, age_seconds=1.5)
    responses_ok = []
    resp = acquire(http_client, antenna_id=KNOWN_ANTENNA)
    assert resp.status_code == 200
    responses_ok.append(resp.json()["lease_token"])

    # The very next contender must now be rejected.
    contender = acquire(http_client, antenna_id=KNOWN_ANTENNA)
    assert contender.status_code == 409
    assert contender.json()["error"]["code"] == "ANTENNA_BUSY"
    assert contender.json()["error"]["details"]["held_by_lease"] == responses_ok[0]
    assert active_lease_count(db_engine, KNOWN_ANTENNA) == 1


def test_expired_with_replay_key_then_new_key_gets_fresh_lease(
    http_client, db_engine
):
    # Replaying an expired lease keeps returning the old token; a different
    # idempotency key can still take over the now-free antenna.
    from sqlalchemy import text

    expired = insert_expired_lease(
        db_engine, antenna_id="ANT-02", age_seconds=3, ttl_seconds=5
    )
    old_key = make_key("old")
    with db_engine.begin() as conn:
        conn.execute(
            text(
                """
                INSERT INTO idempotency_keys
                    (idempotency_key, lease_id, request_params)
                SELECT :key, id,
                       'antenna_id=' || antenna_id || E'\n'
                       || 'controller=' || controller || E'\n'
                       || 'duration_seconds=5'
                FROM leases WHERE token = :token
                """
            ),
            {"key": old_key, "token": expired["token"]},
        )

    replay = http_client.post(
        "/leases",
        json={
            "antenna_id": "ANT-02",
            "controller": "expired-ctrl",
            "duration_seconds": 5,
            "idempotency_key": old_key,
        },
    )
    assert replay.status_code == 200
    assert replay.json()["replay"] is True
    assert replay.json()["lease_token"] == expired["token"]
    assert active_lease_count(db_engine, "ANT-02") == 0

    fresh = acquire(http_client, antenna_id="ANT-02", duration_seconds=10)
    assert fresh.status_code == 200
    assert fresh.json()["replay"] is False
    assert fresh.json()["lease_token"] != expired["token"]
    assert active_lease_count(db_engine, "ANT-02") == 1


def test_end_to_end_minimum_lease_expires_and_hands_over(http_client, db_engine):
    """Full path through the API only, with the minimum legal 5s lease:
    acquire -> active -> wait for database-time expiry -> successor takes over,
    and replay of the original key still returns the original token."""
    key = make_key("e2e")
    first = http_client.post(
        "/leases",
        json={
            "antenna_id": "ANT-05",
            "controller": "first-up",
            "duration_seconds": 5,
            "idempotency_key": key,
        },
    )
    assert first.status_code == 200
    first_body = first.json()

    status = http_client.get(f"/leases/{first_body['lease_token']}")
    assert status.status_code == 200
    assert status.json()["active"] is True

    # Wait in wall-clock terms for the DB-computed deadline to pass. The
    # lease duration itself is measured by PostgreSQL.
    import time

    deadline = datetime.fromisoformat(first_body["expires_at"])
    while datetime.now(timezone.utc) <= deadline:
        time.sleep(0.1)
    time.sleep(0.3)  # boundary margin

    status = http_client.get(f"/leases/{first_body['lease_token']}")
    assert status.json()["active"] is False

    successor = acquire(http_client, antenna_id="ANT-05", duration_seconds=10)
    assert successor.status_code == 200
    assert successor.json()["lease_token"] != first_body["lease_token"]
    assert active_lease_count(db_engine, "ANT-05") == 1

    # Original key replays the original (now expired) lease forever.
    replay = http_client.post(
        "/leases",
        json={
            "antenna_id": "ANT-05",
            "controller": "first-up",
            "duration_seconds": 5,
            "idempotency_key": key,
        },
    )
    assert replay.status_code == 200
    assert replay.json()["replay"] is True
    assert replay.json()["lease_token"] == first_body["lease_token"]
    assert replay.json()["expires_at"] == first_body["expires_at"]
    # Replay must not disturb the new active holder.
    assert active_lease_count(db_engine, "ANT-05") == 1


def test_unknown_token_returns_404(http_client):
    resp = http_client.get("/leases/does-not-exist")
    assert resp.status_code == 404
    assert resp.json()["error"]["code"] == "LEASE_NOT_FOUND"
