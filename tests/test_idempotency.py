"""Idempotency: same key + same parameters replays the original lease;
same key + different parameters is a stable conflict."""

from __future__ import annotations

from sqlalchemy import text

from conftest import (
    KNOWN_ANTENNA,
    acquire,
    active_lease_count,
    count_rows,
    insert_expired_lease,
    make_key,
)


def _payload(key, **overrides):
    payload = {
        "antenna_id": KNOWN_ANTENNA,
        "controller": "gs-beijing-A",
        "duration_seconds": 30,
        "idempotency_key": key,
    }
    payload.update(overrides)
    return payload


def test_replay_returns_original_token_and_expiry(http_client, db_engine):
    key = make_key()
    first = http_client.post("/leases", json=_payload(key))
    assert first.status_code == 200
    original = first.json()
    assert original["replay"] is False
    assert len(original["lease_token"]) >= 40  # 32 random bytes, base64

    for _ in range(3):
        replay = http_client.post("/leases", json=_payload(key))
        assert replay.status_code == 200
        body = replay.json()
        assert body["replay"] is True
        assert body["lease_token"] == original["lease_token"]
        assert body["expires_at"] == original["expires_at"]
        assert body["acquired_at"] == original["acquired_at"]

    # One lease, one idempotency record, one active lease — no second grant.
    assert active_lease_count(db_engine, KNOWN_ANTENNA) == 1
    assert count_rows(db_engine, "SELECT count(*) FROM leases") == 1
    assert count_rows(db_engine, "SELECT count(*) FROM idempotency_keys") == 1


def test_tokens_are_unpredictable(http_client):
    tokens = set()
    # Distinct antennas: a second caller on a held antenna would be rejected,
    # which is irrelevant to token entropy.
    for i in range(1, 6):
        resp = acquire(http_client, antenna_id=f"ANT-0{i}")
        assert resp.status_code == 200
        tokens.add(resp.json()["lease_token"])
    assert len(tokens) == 5  # no repetition, no guessable sequence


def test_same_key_different_controller_is_stable_conflict(http_client, db_engine):
    key = make_key()
    first = http_client.post("/leases", json=_payload(key))
    assert first.status_code == 200

    conflict = http_client.post(
        "/leases", json=_payload(key, controller="gs-sanya-B")
    )
    assert conflict.status_code == 409
    body = conflict.json()
    assert body["error"]["code"] == "IDEMPOTENCY_CONFLICT"
    assert body["error"]["details"]["idempotency_key"] == key

    # Conflict is stable on repeat and the original lease stays untouched.
    again = http_client.post(
        "/leases", json=_payload(key, controller="gs-sanya-B")
    )
    assert again.status_code == 409
    assert again.json()["error"]["code"] == "IDEMPOTENCY_CONFLICT"

    # Replaying the original parameters still returns the original token.
    replay = http_client.post("/leases", json=_payload(key))
    assert replay.status_code == 200
    assert replay.json()["lease_token"] == first.json()["lease_token"]

    assert count_rows(db_engine, "SELECT count(*) FROM leases") == 1


def test_same_key_different_duration_is_stable_conflict(http_client, db_engine):
    key = make_key()
    first = http_client.post("/leases", json=_payload(key, duration_seconds=30))
    assert first.status_code == 200
    conflict = http_client.post(
        "/leases", json=_payload(key, duration_seconds=31)
    )
    assert conflict.status_code == 409
    assert conflict.json()["error"]["code"] == "IDEMPOTENCY_CONFLICT"
    assert count_rows(db_engine, "SELECT count(*) FROM leases") == 1


def test_same_key_different_antenna_is_stable_conflict(http_client, db_engine):
    key = make_key()
    first = http_client.post(
        "/leases", json=_payload(key, antenna_id=KNOWN_ANTENNA)
    )
    assert first.status_code == 200
    conflict = http_client.post(
        "/leases", json=_payload(key, antenna_id="ANT-05")
    )
    assert conflict.status_code == 409
    assert conflict.json()["error"]["code"] == "IDEMPOTENCY_CONFLICT"
    # The other antenna must never have received a lease.
    assert count_rows(
        db_engine,
        "SELECT count(*) FROM leases WHERE antenna_id = :a",
        a="ANT-05",
    ) == 0


def test_conflict_rejected_before_busy_check(http_client, db_engine):
    """Parameter mismatch must be reported as IDEMPOTENCY_CONFLICT even if the
    request would also collide with an unrelated holder — idempotency wins."""
    key = make_key()
    first = http_client.post("/leases", json=_payload(key))
    assert first.status_code == 200
    # Different controller *and* would contend; idempotency conflict is the
    # deterministic outcome.
    resp = http_client.post(
        "/leases", json=_payload(key, controller="intruder")
    )
    assert resp.status_code == 409
    assert resp.json()["error"]["code"] == "IDEMPOTENCY_CONFLICT"


def test_replay_after_expiry_still_returns_original_token(http_client, db_engine):
    # Seed a lease + matching idempotency record that has already expired.
    expired = insert_expired_lease(
        db_engine, antenna_id="ANT-06", age_seconds=2, ttl_seconds=10
    )
    key = make_key()
    with db_engine.begin() as conn:
        conn.execute(
            text(
                """
                INSERT INTO idempotency_keys
                    (idempotency_key, lease_id, request_params)
                SELECT :key, id,
                       'antenna_id=' || antenna_id || E'\n'
                       || 'controller=' || controller || E'\n'
                       || 'duration_seconds=10'
                FROM leases WHERE token = :token
                """
            ),
            {"key": key, "token": expired["token"]},
        )

    resp = http_client.post(
        "/leases",
        json={
            "antenna_id": "ANT-06",
            "controller": "expired-ctrl",
            "duration_seconds": 10,
            "idempotency_key": key,
        },
    )
    assert resp.status_code == 200
    body = resp.json()
    assert body["replay"] is True
    assert body["lease_token"] == expired["token"]
    assert body["expires_at"] == expired["expires_at"].isoformat()
