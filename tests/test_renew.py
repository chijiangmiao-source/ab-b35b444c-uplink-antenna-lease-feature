"""Lease renewal: extend a still-valid lease without releasing/re-contending.

``POST /leases/{lease_token}/renew`` lets the current holder append 5–120
seconds onto the CURRENT expiry while a pass window runs long. The response
gives the expiry before/after the extension plus a ``replay`` flag: the first
accepted call is ``false``; retrying the same idempotency key with the same
parameters replays the first renewal (``true``) with otherwise identical
business fields.

Everything here runs against the REAL API + REAL PostgreSQL:

* acquire -> renew -> hand-over happens at the NEW boundary, and the lease
  was still active at the OLD boundary;
* same-key retries (sequential and concurrent) extend exactly once;
* renewals stack on top of the current expiry when keys differ;
* renewal vs. acquisition contention at the expiry boundary never produces
  two controlling parties (exactly one side wins, both sides are exercised);
* unknown token -> LEASE_NOT_FOUND, expired/released token -> LEASE_EXPIRED,
  same key with changed parameters -> IDEMPOTENCY_CONFLICT, and no rejected
  request ever rewrites a lease, a renewal record or a later holder;
* the original acquire/status responses keep their exact field sets.
"""

from __future__ import annotations

import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone

from sqlalchemy import text

from conftest import (
    KNOWN_ANTENNA,
    MAX_LEASE_SECONDS,
    MIN_LEASE_SECONDS,
    acquire,
    active_lease_count,
    count_rows,
    make_key,
    renew,
)

RENEW_RESPONSE_FIELDS = {
    "lease_token",
    "previous_expires_at",
    "new_expires_at",
    "replay",
}

# The renewal feature must not reshape the original responses.
ACQUIRE_RESPONSE_FIELDS = {
    "antenna_id",
    "controller",
    "lease_token",
    "acquired_at",
    "expires_at",
    "control_generation",
    "replay",
}
STATUS_RESPONSE_FIELDS = {
    "antenna_id",
    "controller",
    "lease_token",
    "acquired_at",
    "expires_at",
    "control_generation",
    "active",
    "last_command_sequence",
    "last_progress_at",
    "released_at",
}


def _parse(ts: str) -> datetime:
    return datetime.fromisoformat(ts)


def _renewal_count(db_engine, token: str) -> int:
    return count_rows(
        db_engine,
        """
        SELECT count(*) FROM lease_renewals r
        JOIN leases l ON l.id = r.lease_id
        WHERE l.token = :t
        """,
        t=token,
    )


def _stored_expiry(db_engine, token: str) -> datetime:
    with db_engine.connect() as conn:
        return conn.execute(
            text("SELECT expires_at FROM leases WHERE token = :t"),
            {"t": token},
        ).scalar_one()


def _insert_near_future_lease(
    db_engine,
    *,
    antenna_id: str,
    remaining_seconds: float,
    ttl_seconds: int = 10,
    controller: str = "boundary-ctrl",
):
    """Insert a lease that expires ``remaining_seconds`` from the DB clock.

    Mirrors conftest.insert_expired_lease but lands the boundary close to the
    DB clock on demand: a positive value expires in the future (so a renew/
    acquire wave can be aimed at the hand-over edge), a negative value is
    already in the past.
    """
    token = f"near-{uuid.uuid4()}"
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
                    clock_timestamp() - make_interval(secs => :ttl - :left),
                    clock_timestamp() + make_interval(secs => :left),
                    bumped.gen
                FROM bumped
                RETURNING token, acquired_at, expires_at, control_generation
                """
            ),
            {
                "antenna_id": antenna_id,
                "controller": controller,
                "token": token,
                "ttl": ttl_seconds,
                "left": remaining_seconds,
            },
        ).mappings().one()
    return dict(row)


# --------------------------------------------------------------------------
# Basic extension arithmetic and the new expiry boundary
# --------------------------------------------------------------------------


def test_renew_extends_from_current_expiry(http_client, db_engine):
    held = acquire(
        http_client, antenna_id=KNOWN_ANTENNA, duration_seconds=30
    )
    assert held.status_code == 200
    original = held.json()
    assert set(original) == ACQUIRE_RESPONSE_FIELDS
    token = original["lease_token"]

    key = make_key()
    resp = renew(http_client, token, 25, key)
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert set(body) == RENEW_RESPONSE_FIELDS
    assert body["replay"] is False
    assert body["lease_token"] == token
    assert body["previous_expires_at"] == original["expires_at"]

    prev, new = _parse(body["previous_expires_at"]), _parse(
        body["new_expires_at"]
    )
    assert new - prev == timedelta(seconds=25)

    # The lease row carries the new boundary; the renewal history row exists
    # exactly once and stores the same before/after pair.
    assert _stored_expiry(db_engine, token) == new
    assert _renewal_count(db_engine, token) == 1
    with db_engine.connect() as conn:
        record = conn.execute(
            text(
                """
                SELECT r.previous_expires_at, r.new_expires_at,
                       r.extra_seconds, k.idempotency_key, k.request_params
                FROM lease_renewals r
                JOIN leases l ON l.id = r.lease_id
                JOIN renewal_idempotency_keys k ON k.renewal_id = r.id
                WHERE l.token = :t
                """
            ),
            {"t": token},
        ).mappings().one()
    assert record.previous_expires_at == prev
    assert record.new_expires_at == new
    assert record.extra_seconds == 25
    assert record.idempotency_key == key

    # The status query keeps its original shape and shows the new boundary.
    status = http_client.get(f"/leases/{token}")
    assert status.status_code == 200
    sbody = status.json()
    assert set(sbody) == STATUS_RESPONSE_FIELDS
    assert sbody["expires_at"] == body["new_expires_at"]
    assert sbody["active"] is True
    assert sbody["released_at"] is None

    # A contender is still rejected and is pointed at the NEW boundary.
    busy = acquire(
        http_client, antenna_id=KNOWN_ANTENNA, controller="late"
    )
    assert busy.status_code == 409
    assert busy.json()["error"]["code"] == "ANTENNA_BUSY"
    assert busy.json()["error"]["details"]["expires_at"] == body[
        "new_expires_at"
    ]


def test_renewals_with_distinct_keys_stack_on_current_expiry(
    http_client, db_engine
):
    held = acquire(http_client, antenna_id="ANT-02", duration_seconds=30)
    token = held.json()["lease_token"]
    original_expiry = _parse(held.json()["expires_at"])

    first = renew(http_client, token, 10, make_key())
    assert first.status_code == 200
    second = renew(http_client, token, 20, make_key())
    assert second.status_code == 200
    sb = second.json()
    assert sb["replay"] is False
    # The second extension starts where the first one ended.
    assert sb["previous_expires_at"] == first.json()["new_expires_at"]
    assert _parse(sb["new_expires_at"]) - _parse(
        sb["previous_expires_at"]
    ) == timedelta(seconds=20)
    assert _parse(sb["new_expires_at"]) == original_expiry + timedelta(
        seconds=30
    )
    assert _renewal_count(db_engine, token) == 2
    assert _stored_expiry(db_engine, token) == _parse(sb["new_expires_at"])


def test_handover_happens_at_the_renewed_boundary_not_the_original(
    http_client, db_engine
):
    # Minimum 5s lease extended by another 5s: the holder must survive the
    # original boundary and hand over at the renewed one.
    held = acquire(http_client, antenna_id="ANT-05", duration_seconds=5)
    token = held.json()["lease_token"]
    renewed = renew(http_client, token, 5, make_key())
    assert renewed.status_code == 200

    original_deadline = _parse(held.json()["expires_at"])
    new_deadline = _parse(renewed.json()["new_expires_at"])
    assert new_deadline - original_deadline == timedelta(seconds=5)

    # Past the ORIGINAL boundary but inside the extension: still held.
    now = datetime.now(timezone.utc)
    time.sleep(max(0.0, (original_deadline - now).total_seconds()) + 0.5)
    status = http_client.get(f"/leases/{token}")
    assert status.status_code == 200
    assert status.json()["active"] is True
    assert active_lease_count(db_engine, "ANT-05") == 1

    # At/after the NEW boundary the antenna hands over exactly like before.
    now = datetime.now(timezone.utc)
    time.sleep(max(0.0, (new_deadline - now).total_seconds()) + 0.4)
    assert http_client.get(f"/leases/{token}").json()["active"] is False
    successor = acquire(http_client, antenna_id="ANT-05", duration_seconds=10)
    assert successor.status_code == 200
    assert successor.json()["lease_token"] != token
    assert active_lease_count(db_engine, "ANT-05") == 1


# --------------------------------------------------------------------------
# Idempotency: same key replays, changed parameters conflict
# --------------------------------------------------------------------------


def test_same_key_retry_replays_and_extends_once(http_client, db_engine):
    held = acquire(http_client, antenna_id="ANT-03", duration_seconds=60)
    token = held.json()["lease_token"]
    key = make_key()

    first = renew(http_client, token, 40, key)
    assert first.status_code == 200
    first_body = first.json()
    assert first_body["replay"] is False

    time.sleep(0.05)  # a real second extension would move the boundary
    for _ in range(3):
        again = renew(http_client, token, 40, key)
        assert again.status_code == 200
        body = again.json()
        assert body["replay"] is True
        # Apart from the flag, the business fields are byte-identical.
        assert body["lease_token"] == first_body["lease_token"]
        assert body["previous_expires_at"] == first_body["previous_expires_at"]
        assert body["new_expires_at"] == first_body["new_expires_at"]

    # Exactly one extension happened and the stored boundary is the first's.
    assert _renewal_count(db_engine, token) == 1
    assert count_rows(
        db_engine, "SELECT count(*) FROM renewal_idempotency_keys"
    ) == 1
    assert _stored_expiry(db_engine, token).isoformat() == first_body[
        "new_expires_at"
    ]


def test_concurrent_same_key_renewals_extend_exactly_once(
    http_client, db_engine
):
    held = acquire(http_client, antenna_id="ANT-04", duration_seconds=60)
    token = held.json()["lease_token"]
    before = _stored_expiry(db_engine, token)
    key = make_key()

    barrier = threading.Barrier(8)

    def one_renew(_: int):
        barrier.wait(timeout=10)
        return renew(http_client, token, 30, key)

    with ThreadPoolExecutor(max_workers=8) as pool:
        responses = list(pool.map(one_renew, range(8)))

    assert all(r.status_code == 200 for r in responses), [
        r.status_code for r in responses
    ]
    flags = [r.json()["replay"] for r in responses]
    assert flags.count(False) == 1
    assert flags.count(True) == 7
    boundaries = {r.json()["new_expires_at"] for r in responses}
    assert len(boundaries) == 1
    # All callers observed the same before/after pair.
    assert len({r.json()["previous_expires_at"] for r in responses}) == 1

    assert _renewal_count(db_engine, token) == 1
    assert _stored_expiry(db_engine, token) == before + timedelta(seconds=30)


def test_same_key_with_changed_extra_is_conflict_and_preserves_renewal(
    http_client, db_engine
):
    held = acquire(http_client, antenna_id="ANT-06", duration_seconds=60)
    token = held.json()["lease_token"]
    key = make_key()

    first = renew(http_client, token, 30, key)
    assert first.status_code == 200
    expiry_after_first = _stored_expiry(db_engine, token)

    conflict = renew(http_client, token, 31, key)
    assert conflict.status_code == 409
    assert conflict.json()["error"]["code"] == "IDEMPOTENCY_CONFLICT"
    assert conflict.json()["error"]["details"]["idempotency_key"] == key

    # Stable conflict on repeat; nothing moves.
    again = renew(http_client, token, 10, key)
    assert again.status_code == 409
    assert again.json()["error"]["code"] == "IDEMPOTENCY_CONFLICT"
    assert _stored_expiry(db_engine, token) == expiry_after_first
    assert _renewal_count(db_engine, token) == 1

    # The original parameters still replay the original renewal.
    replay = renew(http_client, token, 30, key)
    assert replay.status_code == 200
    assert replay.json()["replay"] is True
    assert replay.json()["new_expires_at"] == first.json()["new_expires_at"]

    # A different key extends normally.
    other = renew(http_client, token, 10, make_key())
    assert other.status_code == 200
    assert other.json()["replay"] is False


def test_same_key_against_another_token_is_conflict(http_client, db_engine):
    first = acquire(http_client, antenna_id="ANT-02", duration_seconds=60)
    second = acquire(http_client, antenna_id="ANT-03", duration_seconds=60)
    token_a, token_b = (
        first.json()["lease_token"],
        second.json()["lease_token"],
    )
    key = make_key()
    ok = renew(http_client, token_a, 10, key)
    assert ok.status_code == 200

    reused = renew(http_client, token_b, 10, key)
    assert reused.status_code == 409
    assert reused.json()["error"]["code"] == "IDEMPOTENCY_CONFLICT"
    # Token B's lease is untouched (still on its original boundary).
    assert _stored_expiry(db_engine, token_b) == _parse(
        second.json()["expires_at"]
    )
    assert _renewal_count(db_engine, token_b) == 0


def test_replay_after_the_renewed_lease_expired_still_returns_first_boundary(
    http_client, db_engine
):
    # Like acquisition replay: the renewal record survives natural expiry, so
    # a late retry replays the original boundaries WITHOUT re-extending.
    held = acquire(http_client, antenna_id="ANT-06", duration_seconds=5)
    token = held.json()["lease_token"]
    key = make_key()
    first = renew(http_client, token, 5, key)
    assert first.status_code == 200

    deadline = _parse(first.json()["new_expires_at"])
    now = datetime.now(timezone.utc)
    time.sleep(max(0.0, (deadline - now).total_seconds()) + 0.4)
    assert http_client.get(f"/leases/{token}").json()["active"] is False

    replay = renew(http_client, token, 5, key)
    assert replay.status_code == 200
    body = replay.json()
    assert body["replay"] is True
    assert body["previous_expires_at"] == first.json()["previous_expires_at"]
    assert body["new_expires_at"] == first.json()["new_expires_at"]
    assert _renewal_count(db_engine, token) == 1
    # The expired lease is not resurrected.
    assert active_lease_count(db_engine, "ANT-06") == 0


# --------------------------------------------------------------------------
# Rejections: unknown / expired / released tokens, input validation
# --------------------------------------------------------------------------


def test_renew_unknown_token_is_lease_not_found_and_write_free(
    http_client, db_engine
):
    resp = renew(http_client, "no-such-token", 10, make_key())
    assert resp.status_code == 404
    body = resp.json()
    assert body["error"]["code"] == "LEASE_NOT_FOUND"
    assert body["error"]["details"]["lease_token"] == "no-such-token"
    assert count_rows(db_engine, "SELECT count(*) FROM lease_renewals") == 0
    assert count_rows(
        db_engine, "SELECT count(*) FROM renewal_idempotency_keys"
    ) == 0

    # Stable on repeat.
    again = renew(http_client, "no-such-token", 10, make_key())
    assert again.status_code == 404
    assert again.json()["error"]["code"] == "LEASE_NOT_FOUND"


def test_renew_expired_lease_is_rejected_without_touching_data(
    http_client, db_engine
):
    lease = _insert_near_future_lease(
        db_engine, antenna_id="ANT-02", remaining_seconds=-2, ttl_seconds=10
    )
    token = lease["token"]
    original_expiry = lease["expires_at"]

    key = make_key()
    resp = renew(http_client, token, 10, key)
    assert resp.status_code == 409
    body = resp.json()
    assert body["error"]["code"] == "LEASE_EXPIRED"
    assert body["error"]["details"]["lease_token"] == token
    assert body["error"]["details"]["expires_at"] == original_expiry.isoformat()

    # The rejection is stable and never writes; the boundary is not moved.
    for _ in range(2):
        again = renew(http_client, token, 10, key)
        assert again.status_code == 409
        assert again.json()["error"]["code"] == "LEASE_EXPIRED"
    assert _stored_expiry(db_engine, token) == original_expiry
    assert _renewal_count(db_engine, token) == 0
    assert count_rows(
        db_engine, "SELECT count(*) FROM renewal_idempotency_keys"
    ) == 0


def test_renew_released_lease_is_rejected_and_keeps_release_time(
    http_client, db_engine
):
    held = acquire(http_client, antenna_id="ANT-03", duration_seconds=60)
    token = held.json()["lease_token"]
    expiry = _parse(held.json()["expires_at"])
    released = http_client.post(f"/leases/{token}/release")
    assert released.status_code == 200
    released_at = released.json()["released_at"]

    resp = renew(http_client, token, 10, make_key())
    assert resp.status_code == 409
    assert resp.json()["error"]["code"] == "LEASE_EXPIRED"

    assert _stored_expiry(db_engine, token) == expiry
    assert _renewal_count(db_engine, token) == 0
    with db_engine.connect() as conn:
        stored_release = conn.execute(
            text("SELECT released_at FROM leases WHERE token = :t"),
            {"t": token},
        ).scalar_one()
    assert stored_release.isoformat() == released_at


def test_rejected_renewal_of_old_token_never_affects_later_holder(
    http_client, db_engine
):
    held = acquire(http_client, antenna_id="ANT-04", duration_seconds=5)
    old_token = held.json()["lease_token"]
    old_key = make_key()

    deadline = _parse(held.json()["expires_at"])
    now = datetime.now(timezone.utc)
    time.sleep(max(0.0, (deadline - now).total_seconds()) + 0.4)

    successor = acquire(
        http_client,
        antenna_id="ANT-04",
        controller="next-shift",
        duration_seconds=60,
    )
    assert successor.status_code == 200
    new_token = successor.json()["lease_token"]
    new_expiry = _parse(successor.json()["expires_at"])
    successor_before = _stored_expiry(db_engine, new_token)

    # Expired token, unknown token and malformed payloads all miss the
    # successor's state.
    rejected = renew(http_client, old_token, 30, old_key)
    assert rejected.status_code == 409
    assert rejected.json()["error"]["code"] == "LEASE_EXPIRED"
    assert renew(http_client, "still-unknown", 30, make_key()).status_code == 404
    bad = http_client.post(
        f"/leases/{old_token}/renew", json={"extra_seconds": 999}
    )
    assert bad.status_code == 422

    status = http_client.get(f"/leases/{new_token}")
    assert status.json()["active"] is True
    assert _stored_expiry(db_engine, new_token) == successor_before == new_expiry
    assert _renewal_count(db_engine, new_token) == 0
    assert active_lease_count(db_engine, "ANT-04") == 1

    # The new holder can itself renew; the stale old key has no power here.
    ok = renew(http_client, new_token, 30, make_key())
    assert ok.status_code == 200
    assert ok.json()["replay"] is False


def test_renew_input_validation(http_client, db_engine):
    held = acquire(http_client, antenna_id="ANT-05", duration_seconds=60)
    token = held.json()["lease_token"]
    url = f"/leases/{token}/renew"

    def post(raw):
        return http_client.post(url, json=raw)

    # Out-of-range and non-integer "seconds" are 422.
    for bad in (
        MIN_LEASE_SECONDS - 1,
        MAX_LEASE_SECONDS + 1,
        0,
        -5,
        7.5,
        "10",
        None,
        True,
    ):
        resp = post({"extra_seconds": bad, "idempotency_key": make_key()})
        assert resp.status_code == 422, bad
        assert resp.json()["error"]["code"] == "VALIDATION_ERROR"

    # Missing/blank key, unknown field.
    assert post({"extra_seconds": 10}).status_code == 422
    assert post(
        {"extra_seconds": 10, "idempotency_key": "   "}
    ).status_code == 422
    assert post(
        {
            "extra_seconds": 10,
            "idempotency_key": make_key(),
            "unexpected": 1,
        }
    ).status_code == 422

    # Boundary values are accepted.
    for value in (5, 120):
        resp = renew(http_client, token, value, make_key())
        assert resp.status_code == 200, (value, resp.text)
        assert resp.json()["replay"] is False
    assert _renewal_count(db_engine, token) == 2

    # Every rejected request was write-free.
    assert count_rows(
        db_engine, "SELECT count(*) FROM renewal_idempotency_keys"
    ) == 2
    assert _stored_expiry(db_engine, token) == _parse(
        held.json()["expires_at"]
    ) + timedelta(seconds=125)


# --------------------------------------------------------------------------
# Concurrency: renewal vs. acquisition around the expiry boundary
# --------------------------------------------------------------------------


def test_concurrent_renew_and_contention_while_valid_never_doubly_controls(
    http_client, db_engine
):
    # Well inside the validity window: the renewal must win and every
    # contender must see ANTENNA_BUSY pointed at the extended boundary.
    held = acquire(http_client, antenna_id=KNOWN_ANTENNA, duration_seconds=60)
    token = held.json()["lease_token"]
    renew_key = make_key()

    def contender_payload():
        return {
            "antenna_id": KNOWN_ANTENNA,
            "controller": f"contender-{make_key()}",
            "duration_seconds": 60,
            "idempotency_key": make_key(),
        }

    kinds = ["renew"] + ["acquire"] * 8
    barrier = threading.Barrier(len(kinds) + 1)

    def run(kind):
        barrier.wait(timeout=10)
        if kind == "renew":
            return renew(http_client, token, 60, renew_key)
        return http_client.post("/leases", json=contender_payload())

    with ThreadPoolExecutor(max_workers=len(kinds)) as pool:
        futures = [pool.submit(run, k) for k in kinds]
        barrier.wait(timeout=10)  # release all workers together
        responses = [f.result(timeout=30) for f in futures]

    renew_resp, acquire_resps = responses[0], responses[1:]
    assert renew_resp.status_code == 200, renew_resp.text
    assert renew_resp.json()["replay"] is False
    assert all(r.status_code == 409 for r in acquire_resps), [
        r.status_code for r in acquire_resps
    ]
    assert {r.json()["error"]["code"] for r in acquire_resps} == {
        "ANTENNA_BUSY"
    }
    # A contender serialised before the renewal on the antenna lock reads the
    # original boundary; one serialised after reads the renewed boundary.
    # Both orderings are valid, but every busy response must name one of the
    # two real boundaries — never a third value.
    expected_busy_expiries = {
        held.json()["expires_at"],
        renew_resp.json()["new_expires_at"],
    }
    for r in acquire_resps:
        assert (
            r.json()["error"]["details"]["expires_at"]
            in expected_busy_expiries
        )

    assert active_lease_count(db_engine, KNOWN_ANTENNA) == 1
    assert count_rows(db_engine, "SELECT count(*) FROM leases") == 1
    assert _renewal_count(db_engine, token) == 1


def test_renew_and_handoff_at_boundary_exactly_one_side_wins(
    http_client, db_engine
):
    # Three waves aimed just before, at, and just after the expiry boundary.
    # At -0.6s the renewal must still own the boundary; at +0.6s the lease is
    # gone so an acquirer must own it. The 0.0 wave may go either way, but in
    # every wave exactly one side succeeds: the antenna is never doubly
    # controlled and no error other than the documented 409s appears.
    waves = [
        ("ANT-02", -0.6, "renew"),
        ("ANT-03", 0.0, None),
        ("ANT-04", 0.6, "acquire"),
    ]

    for antenna_id, offset_seconds, expected in waves:
        lease = _insert_near_future_lease(
            db_engine,
            antenna_id=antenna_id,
            remaining_seconds=2.5,
            ttl_seconds=10,
        )
        token = lease["token"]
        original_expiry = lease["expires_at"]
        renew_key = make_key()

        def contender_payload():
            return {
                "antenna_id": antenna_id,
                "controller": f"wave-{make_key()}",
                "duration_seconds": 30,
                "idempotency_key": make_key(),
            }

        kinds = ["renew"] + ["acquire"] * 5
        # Main thread is the final barrier party, released on schedule so the
        # wave lands at deadline + offset.
        barrier = threading.Barrier(len(kinds) + 1)

        def run(kind):
            barrier.wait(timeout=15)
            if kind == "renew":
                return renew(http_client, token, 30, renew_key)
            return http_client.post("/leases", json=contender_payload())

        with ThreadPoolExecutor(max_workers=len(kinds)) as pool:
            futures = [pool.submit(run, k) for k in kinds]
            wait_for = (
                original_expiry - datetime.now(timezone.utc)
            ).total_seconds() + offset_seconds
            time.sleep(max(0.0, wait_for))
            barrier.wait(timeout=15)
            responses = [f.result(timeout=30) for f in futures]

        renew_resp, acquire_resps = responses[0], responses[1:]
        statuses = {r.status_code for r in responses}
        assert statuses <= {200, 409}, (
            antenna_id,
            [r.status_code for r in responses],
        )
        winners = [r for r in acquire_resps if r.status_code == 200]
        renew_won = renew_resp.status_code == 200

        # The core invariant: exactly one controlling party at the boundary.
        assert len(winners) + int(renew_won) == 1, (
            antenna_id,
            renew_resp.status_code,
            [r.status_code for r in acquire_resps],
        )
        busy = [r for r in acquire_resps if r.status_code == 409]
        for r in busy:
            assert r.json()["error"]["code"] == "ANTENNA_BUSY"
        assert active_lease_count(db_engine, antenna_id) == 1

        if renew_won:
            assert not winners
            assert renew_resp.json()["replay"] is False
            assert renew_resp.json()["previous_expires_at"] == (
                original_expiry.isoformat()
            )
            stored = _stored_expiry(db_engine, token)
            assert stored == _parse(renew_resp.json()["new_expires_at"])
            assert stored > original_expiry
            assert _renewal_count(db_engine, token) == 1
            assert count_rows(
                db_engine,
                "SELECT count(*) FROM leases WHERE antenna_id = :a",
                a=antenna_id,
            ) == 1
        else:
            assert renew_resp.status_code == 409
            assert renew_resp.json()["error"]["code"] == "LEASE_EXPIRED"
            assert len(winners) == 1
            # The rejected renewal never moved the old boundary and wrote
            # nothing; the successor is the sole active holder.
            assert _stored_expiry(db_engine, token) == original_expiry
            assert _renewal_count(db_engine, token) == 0
            assert count_rows(
                db_engine,
                """
                SELECT count(*) FROM renewal_idempotency_keys k
                JOIN lease_renewals r ON r.id = k.renewal_id
                JOIN leases l ON l.id = r.lease_id
                WHERE l.token = :t
                """,
                t=token,
            ) == 0
            successor_token = winners[0].json()["lease_token"]
            assert successor_token != token
            assert count_rows(
                db_engine,
                "SELECT count(*) FROM leases WHERE antenna_id = :a",
                a=antenna_id,
            ) == 2
            status = http_client.get(f"/leases/{successor_token}")
            assert status.json()["active"] is True

        if expected == "renew":
            assert renew_won, f"wave {antenna_id}: renewal should win"
        elif expected == "acquire":
            assert not renew_won and len(winners) == 1, (
                f"wave {antenna_id}: a contender should win"
            )
