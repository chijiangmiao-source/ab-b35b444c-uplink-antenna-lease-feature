"""Early release: the holder yields the antenna before natural expiry.

``POST /leases/{lease_token}/release`` ends a pass early (or lets a control
program step down voluntarily): the antenna becomes immediately acquirable
through the ordinary ``POST /leases`` interface. Release is idempotent, is
serialised against acquisition by the same antenna row lock, and never
rewrites a recorded release timestamp. Everything runs against the real
API + PostgreSQL; no mocks, no fixed responses.
"""

from __future__ import annotations

import re
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone

from sqlalchemy import text

from conftest import (
    KNOWN_ANTENNA,
    acquire,
    active_lease_count,
    count_rows,
    insert_expired_lease,
    make_key,
    release,
)

# ISO-8601 with an explicit UTC offset (same contract as the other
# timestamps: ``+00:00``, never the bare "Z" shorthand).
ISO_OFFSET = re.compile(
    r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?[+-]\d{2}:\d{2}$"
)

# The release feature must not change the acquisition/replay response shape.
ACQUIRE_RESPONSE_FIELDS = {
    "antenna_id",
    "controller",
    "lease_token",
    "acquired_at",
    "expires_at",
    "replay",
}


def test_release_active_lease_hands_over_immediately(http_client, db_engine):
    held = acquire(http_client, antenna_id=KNOWN_ANTENNA, duration_seconds=60)
    assert held.status_code == 200
    token = held.json()["lease_token"]
    assert set(held.json()) == ACQUIRE_RESPONSE_FIELDS

    # Before release: active, released_at null.
    status = http_client.get(f"/leases/{token}")
    assert status.status_code == 200
    assert status.json()["active"] is True
    assert status.json()["released_at"] is None

    released = release(http_client, token)
    assert released.status_code == 200, released.text
    body = released.json()
    assert body["lease_token"] == token
    assert body["active"] is False
    released_at = body["released_at"]
    assert released_at is not None
    assert ISO_OFFSET.match(released_at), released_at
    # Stamped by the database clock between acquisition and the far-future
    # expiry of this 60s lease.
    assert (
        datetime.fromisoformat(body["acquired_at"])
        < datetime.fromisoformat(released_at)
        < datetime.fromisoformat(body["expires_at"])
    )

    # The status query reports the release with the identical timestamp.
    status = http_client.get(f"/leases/{token}")
    assert status.json()["active"] is False
    assert status.json()["released_at"] == released_at

    # The antenna is immediately acquirable through the original interface.
    successor = acquire(
        http_client,
        antenna_id=KNOWN_ANTENNA,
        controller="successor",
        duration_seconds=30,
    )
    assert successor.status_code == 200, successor.text
    assert successor.json()["replay"] is False
    assert successor.json()["lease_token"] != token
    assert active_lease_count(db_engine, KNOWN_ANTENNA) == 1
    # History preserved: the released row stays, exactly one lease is active.
    assert count_rows(db_engine, "SELECT count(*) FROM leases") == 2
    assert count_rows(
        db_engine,
        "SELECT count(*) FROM leases WHERE token = :t "
        "AND released_at IS NOT NULL",
        t=token,
    ) == 1


def test_release_is_idempotent_and_never_rewrites_released_at(
    http_client, db_engine
):
    held = acquire(http_client, antenna_id="ANT-02", duration_seconds=60)
    token = held.json()["lease_token"]

    first = release(http_client, token)
    assert first.status_code == 200
    first_body = first.json()

    time.sleep(0.05)  # a rewrite would move the DB-clock timestamp

    for _ in range(3):
        again = release(http_client, token)
        assert again.status_code == 200
        assert again.json() == first_body  # the whole body is stable

    # The stored timestamp is exactly the one from the first release.
    with db_engine.connect() as conn:
        stored = conn.execute(
            text("SELECT released_at FROM leases WHERE token = :t"),
            {"t": token},
        ).scalar_one()
    assert stored.isoformat() == first_body["released_at"]


def test_release_unknown_token_returns_lease_not_found(http_client):
    resp = release(http_client, "no-such-token")
    assert resp.status_code == 404
    assert resp.json()["error"]["code"] == "LEASE_NOT_FOUND"


def test_release_of_naturally_expired_lease_is_rejected_and_unrecorded(
    http_client, db_engine
):
    expired = insert_expired_lease(
        db_engine, antenna_id="ANT-03", age_seconds=2, ttl_seconds=10
    )
    resp = release(http_client, expired["token"])
    assert resp.status_code == 409
    body = resp.json()
    assert body["error"]["code"] == "LEASE_EXPIRED"
    assert body["error"]["details"]["lease_token"] == expired["token"]

    # The record is left untouched and the rejection is stable on repeat.
    for _ in range(2):
        assert count_rows(
            db_engine,
            "SELECT count(*) FROM leases WHERE released_at IS NOT NULL",
        ) == 0
        again = release(http_client, expired["token"])
        assert again.status_code == 409
        assert again.json()["error"]["code"] == "LEASE_EXPIRED"


def test_released_token_does_not_affect_later_leases(http_client, db_engine):
    first = acquire(http_client, antenna_id="ANT-04", duration_seconds=60)
    old_token = first.json()["lease_token"]
    assert release(http_client, old_token).status_code == 200

    second = acquire(
        http_client,
        antenna_id="ANT-04",
        controller="next-shift",
        duration_seconds=60,
    )
    assert second.status_code == 200
    new_token = second.json()["lease_token"]

    # Re-releasing the old token replays its own release and leaves the new
    # holder fully intact.
    replay = release(http_client, old_token)
    assert replay.status_code == 200
    assert replay.json()["active"] is False

    status_new = http_client.get(f"/leases/{new_token}")
    assert status_new.json()["active"] is True
    assert status_new.json()["released_at"] is None
    assert active_lease_count(db_engine, "ANT-04") == 1
    assert count_rows(
        db_engine,
        "SELECT count(*) FROM leases WHERE token = :t AND released_at IS NULL",
        t=new_token,
    ) == 1


def test_replay_of_released_lease_still_returns_original_grant(
    http_client, db_engine
):
    key = make_key()
    payload = {
        "antenna_id": "ANT-05",
        "controller": "early-finisher",
        "duration_seconds": 60,
        "idempotency_key": key,
    }
    granted = http_client.post("/leases", json=payload)
    assert granted.status_code == 200
    original = granted.json()
    assert release(http_client, original["lease_token"]).status_code == 200

    # The idempotency record survives the release: same key + same params
    # replays the original grant, with the original response shape.
    replay = http_client.post("/leases", json=payload)
    assert replay.status_code == 200
    body = replay.json()
    assert set(body) == ACQUIRE_RESPONSE_FIELDS
    assert body["replay"] is True
    assert body["lease_token"] == original["lease_token"]
    assert body["expires_at"] == original["expires_at"]
    assert body["acquired_at"] == original["acquired_at"]


def test_concurrent_releases_of_one_lease_all_agree(http_client, db_engine):
    held = acquire(http_client, antenna_id=KNOWN_ANTENNA, duration_seconds=60)
    token = held.json()["lease_token"]

    barrier = threading.Barrier(8)

    def one_release(_: int):
        barrier.wait(timeout=10)
        return release(http_client, token)

    with ThreadPoolExecutor(max_workers=8) as pool:
        responses = list(pool.map(one_release, range(8)))

    assert all(r.status_code == 200 for r in responses), [
        r.status_code for r in responses
    ]
    # Exactly one release timestamp exists and every caller saw the same body.
    assert len({r.json()["released_at"] for r in responses}) == 1
    assert len({r.text for r in responses}) == 1
    assert active_lease_count(db_engine, KNOWN_ANTENNA) == 0
    assert count_rows(db_engine, "SELECT count(*) FROM leases") == 1


def test_concurrent_release_and_contention_never_double_controls(
    http_client, db_engine
):
    held = acquire(http_client, antenna_id=KNOWN_ANTENNA, duration_seconds=60)
    token = held.json()["lease_token"]

    def contender_payload() -> dict:
        return {
            "antenna_id": KNOWN_ANTENNA,
            "controller": f"contender-{make_key()}",
            "duration_seconds": 60,
            "idempotency_key": make_key(),
        }

    # One release races six would-be acquirers, all released from a barrier.
    kinds = ["release"] + ["acquire"] * 6
    barrier = threading.Barrier(len(kinds))

    def run(kind: str):
        barrier.wait(timeout=10)
        if kind == "release":
            return release(http_client, token)
        return http_client.post("/leases", json=contender_payload())

    with ThreadPoolExecutor(max_workers=len(kinds)) as pool:
        responses = list(pool.map(run, kinds))

    release_resp, acquire_resps = responses[0], responses[1:]
    assert release_resp.status_code == 200, release_resp.text
    winners = [r for r in acquire_resps if r.status_code == 200]
    busy = [r for r in acquire_resps if r.status_code == 409]
    assert len(winners) + len(busy) == len(acquire_resps)  # never a 5xx
    for r in busy:
        assert r.json()["error"]["code"] == "ANTENNA_BUSY"

    # At most one successor took over: the antenna is never doubly
    # controlled, and no extra lease rows appear out of nowhere.
    assert len(winners) <= 1
    assert active_lease_count(db_engine, KNOWN_ANTENNA) == len(winners)
    assert count_rows(db_engine, "SELECT count(*) FROM leases") == 1 + len(
        winners
    )
    if winners:
        successor_token = winners[0].json()["lease_token"]
        assert successor_token != token
        status = http_client.get(f"/leases/{successor_token}")
        assert status.json()["active"] is True
        assert status.json()["released_at"] is None


def test_rerelease_after_natural_expiry_still_replays(http_client, db_engine):
    # Released while active, then outlived its own expires_at: a later
    # release call still replays the recorded release (it already happened)
    # instead of reporting LEASE_EXPIRED.
    held = acquire(http_client, antenna_id="ANT-05", duration_seconds=5)
    token = held.json()["lease_token"]
    first = release(http_client, token)
    assert first.status_code == 200

    deadline = datetime.fromisoformat(held.json()["expires_at"])
    while datetime.now(timezone.utc) <= deadline:
        time.sleep(0.1)
    time.sleep(0.3)  # boundary margin

    again = release(http_client, token)
    assert again.status_code == 200
    assert again.json()["released_at"] == first.json()["released_at"]
    assert again.json()["active"] is False


def test_unreleased_lease_still_expires_on_the_original_boundary(
    http_client, db_engine
):
    """Regression: a lease that is never released keeps the original
    expiry-only semantics — active until ``expires_at`` (database clock),
    then the antenna hands over; ``released_at`` stays NULL throughout."""
    first = acquire(http_client, antenna_id="ANT-06", duration_seconds=5)
    assert first.status_code == 200
    token = first.json()["lease_token"]

    status = http_client.get(f"/leases/{token}")
    assert status.json()["active"] is True
    assert status.json()["released_at"] is None

    deadline = datetime.fromisoformat(first.json()["expires_at"])
    while datetime.now(timezone.utc) <= deadline:
        time.sleep(0.1)
    time.sleep(0.3)  # boundary margin

    status = http_client.get(f"/leases/{token}")
    assert status.json()["active"] is False
    assert status.json()["released_at"] is None  # expired, never released

    successor = acquire(http_client, antenna_id="ANT-06", duration_seconds=10)
    assert successor.status_code == 200
    assert successor.json()["lease_token"] != token
    assert active_lease_count(db_engine, "ANT-06") == 1
