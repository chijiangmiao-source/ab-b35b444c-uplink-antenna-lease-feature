"""Control generation: a monotonic per-antenna counter handed out with every
lease so uplink devices can reject instructions from stale (partitioned)
controllers.

The acquisition transaction increments ``antennas.last_control_generation``
while holding the antenna row lock and freezes the value onto the new lease;
the same value is returned by acquisition, replay and token lookup. Only a
committed acquisition consumes a generation — busy antennas, unknown
antennas, idempotency conflicts and validation rejections all roll back
without moving the counter, so the first success after any failures follows
the last committed lease exactly.

Everything runs against the REAL API + REAL PostgreSQL.
"""

from __future__ import annotations

import threading
from concurrent.futures import ThreadPoolExecutor

from sqlalchemy import text

from conftest import (
    KNOWN_ANTENNA,
    acquire,
    active_lease_count,
    count_rows,
    insert_expired_lease,
    make_key,
    parallel_acquire,
    release,
)


def _last_generation(db_engine, antenna_id: str) -> int:
    return count_rows(
        db_engine,
        "SELECT last_control_generation FROM antennas WHERE id = :a",
        a=antenna_id,
    )


def _snapshot_antennas(db_engine) -> list[tuple]:
    with db_engine.connect() as conn:
        return list(
            conn.execute(
                text(
                    "SELECT id, name, last_control_generation "
                    "FROM antennas ORDER BY id"
                )
            ).all()
        )


def _snapshot_leases(db_engine) -> list[tuple]:
    with db_engine.connect() as conn:
        return list(
            conn.execute(
                text(
                    "SELECT id, antenna_id, controller, token, acquired_at, "
                    "expires_at, released_at, last_command_sequence, "
                    "last_progress_at, control_generation "
                    "FROM leases ORDER BY id"
                )
            ).all()
        )


def _stored_generation(db_engine, token: str) -> int:
    return count_rows(
        db_engine,
        "SELECT control_generation FROM leases WHERE token = :t",
        t=token,
    )


def _req(antenna=KNOWN_ANTENNA, controller=None, duration=60, key=None):
    return {
        "antenna_id": antenna,
        "controller": controller or f"ctrl-{make_key()}",
        "duration_seconds": duration,
        "idempotency_key": key or make_key(),
    }


def test_first_lease_is_generation_one(http_client, db_engine):
    resp = acquire(http_client, antenna_id=KNOWN_ANTENNA, duration_seconds=30)
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["control_generation"] == 1
    # The frozen lease value and the antenna counter agree after commit.
    assert _last_generation(db_engine, KNOWN_ANTENNA) == 1
    assert _stored_generation(db_engine, body["lease_token"]) == 1


def test_consecutive_handovers_increment_strictly(http_client, db_engine):
    generations = []
    for round_no in range(3):
        resp = acquire(
            http_client,
            antenna_id=KNOWN_ANTENNA,
            controller=f"shift-{round_no}",
            duration_seconds=60,
        )
        assert resp.status_code == 200, resp.text
        generations.append(resp.json()["control_generation"])
        if round_no < 2:
            # Early release hands the antenna to the next controller without
            # waiting for natural expiry.
            assert (
                release(http_client, resp.json()["lease_token"]).status_code
                == 200
            )

    assert generations == [1, 2, 3]
    assert _last_generation(db_engine, KNOWN_ANTENNA) == 3
    # Every historical lease keeps its own frozen generation.
    with db_engine.connect() as conn:
        stored = list(
            conn.execute(
                text(
                    "SELECT control_generation FROM leases "
                    "WHERE antenna_id = :a ORDER BY id"
                ),
                {"a": KNOWN_ANTENNA},
            ).scalars()
        )
    assert stored == [1, 2, 3]


def test_expiry_handover_continues_from_last_committed_lease(
    http_client, db_engine
):
    # Seeded history consumes generation 1 exactly like a real acquisition.
    expired = insert_expired_lease(
        db_engine, antenna_id=KNOWN_ANTENNA, age_seconds=2, ttl_seconds=10
    )
    assert expired["control_generation"] == 1

    resp = acquire(http_client, antenna_id=KNOWN_ANTENNA, duration_seconds=30)
    assert resp.status_code == 200, resp.text
    assert resp.json()["control_generation"] == 2
    assert _last_generation(db_engine, KNOWN_ANTENNA) == 2

    # The expired lease keeps generation 1 in historical queries.
    old = http_client.get(f"/leases/{expired['token']}")
    assert old.status_code == 200
    assert old.json()["control_generation"] == 1
    assert old.json()["active"] is False


def test_generations_are_independent_per_antenna(http_client, db_engine):
    first = acquire(http_client, antenna_id="ANT-01", duration_seconds=60)
    other = acquire(http_client, antenna_id="ANT-02", duration_seconds=60)
    assert first.status_code == 200 and other.status_code == 200
    # Each antenna hands out its own generation 1.
    assert first.json()["control_generation"] == 1
    assert other.json()["control_generation"] == 1

    assert release(http_client, first.json()["lease_token"]).status_code == 200
    second = acquire(http_client, antenna_id="ANT-01", duration_seconds=60)
    assert second.status_code == 200
    assert second.json()["control_generation"] == 2
    # The other antenna's counter is untouched by ANT-01's handover.
    assert _last_generation(db_engine, "ANT-01") == 2
    assert _last_generation(db_engine, "ANT-02") == 1


def test_replay_and_lookup_return_the_original_generation(
    http_client, db_engine
):
    key = make_key()
    payload = _req(duration=60, key=key)
    first = http_client.post("/leases", json=payload)
    assert first.status_code == 200
    original = first.json()
    assert original["replay"] is False
    generation = original["control_generation"]

    # Same key + same params replays the original grant, generation included.
    for _ in range(3):
        replay = http_client.post("/leases", json=payload)
        assert replay.status_code == 200
        body = replay.json()
        assert body["replay"] is True
        assert body["control_generation"] == generation
        assert body["lease_token"] == original["lease_token"]

    # Token lookup reports the identical value.
    status = http_client.get(f"/leases/{original['lease_token']}")
    assert status.status_code == 200
    assert status.json()["control_generation"] == generation

    # Replays never consume generations even after the lease handed over.
    assert release(http_client, original["lease_token"]).status_code == 200
    successor = acquire(
        http_client, antenna_id=KNOWN_ANTENNA, duration_seconds=60
    )
    assert successor.status_code == 200
    assert successor.json()["control_generation"] == generation + 1

    replay = http_client.post("/leases", json=payload)
    assert replay.status_code == 200
    assert replay.json()["control_generation"] == generation
    assert _last_generation(db_engine, KNOWN_ANTENNA) == generation + 1


def test_generation_visible_in_status_and_release_responses(
    http_client, db_engine
):
    granted = acquire(http_client, antenna_id="ANT-03", duration_seconds=60)
    assert granted.status_code == 200
    token = granted.json()["lease_token"]
    generation = granted.json()["control_generation"]

    status = http_client.get(f"/leases/{token}")
    assert status.status_code == 200
    assert status.json()["control_generation"] == generation

    released = release(http_client, token)
    assert released.status_code == 200
    assert released.json()["control_generation"] == generation

    # Still queryable afterwards: the generation is frozen onto the lease.
    status = http_client.get(f"/leases/{token}")
    assert status.json()["control_generation"] == generation
    assert status.json()["active"] is False


def test_concurrent_contention_commits_exactly_one_new_generation(
    http_client, db_engine
):
    # Wave 1: 12 contenders race for a free antenna.
    responses = parallel_acquire(http_client, [_req() for _ in range(12)])
    winners = [r for r in responses if r.status_code == 200]
    losers = [r for r in responses if r.status_code == 409]
    assert len(winners) == 1, [r.status_code for r in responses]
    assert len(losers) == 11
    for r in losers:
        assert r.json()["error"]["code"] == "ANTENNA_BUSY"

    # Exactly one generation was handed out; the 11 losers consumed nothing.
    assert winners[0].json()["control_generation"] == 1
    assert _last_generation(db_engine, KNOWN_ANTENNA) == 1
    assert count_rows(db_engine, "SELECT count(*) FROM leases") == 1

    # Wave 2: release, then race again — the single winner gets exactly the
    # next generation, no gaps from the losing contenders.
    assert (
        release(http_client, winners[0].json()["lease_token"]).status_code
        == 200
    )
    responses = parallel_acquire(http_client, [_req() for _ in range(12)])
    winners = [r for r in responses if r.status_code == 200]
    assert len(winners) == 1, [r.status_code for r in responses]
    assert winners[0].json()["control_generation"] == 2
    assert _last_generation(db_engine, KNOWN_ANTENNA) == 2
    assert count_rows(db_engine, "SELECT count(*) FROM leases") == 2
    assert active_lease_count(db_engine, KNOWN_ANTENNA) == 1


def test_same_key_concurrent_retry_shares_one_generation(
    http_client, db_engine
):
    payload = _req()
    responses = parallel_acquire(
        http_client, [dict(payload) for _ in range(8)]
    )
    assert all(r.status_code == 200 for r in responses)
    generations = {r.json()["control_generation"] for r in responses}
    tokens = {r.json()["lease_token"] for r in responses}
    assert generations == {1}
    assert len(tokens) == 1
    assert _last_generation(db_engine, KNOWN_ANTENNA) == 1
    assert count_rows(db_engine, "SELECT count(*) FROM leases") == 1


def test_concurrent_release_and_contention_yields_single_next_generation(
    http_client, db_engine
):
    held = acquire(http_client, antenna_id=KNOWN_ANTENNA, duration_seconds=60)
    assert held.status_code == 200
    token = held.json()["lease_token"]
    assert held.json()["control_generation"] == 1

    # One release races six would-be acquirers, all released from a barrier.
    kinds = ["release"] + ["acquire"] * 6
    barrier = threading.Barrier(len(kinds))

    def run(kind: str):
        barrier.wait(timeout=10)
        if kind == "release":
            return release(http_client, token)
        return http_client.post("/leases", json=_req())

    with ThreadPoolExecutor(max_workers=len(kinds)) as pool:
        responses = list(pool.map(run, kinds))

    release_resp, acquire_resps = responses[0], responses[1:]
    assert release_resp.status_code == 200, release_resp.text
    winners = [r for r in acquire_resps if r.status_code == 200]
    busy = [r for r in acquire_resps if r.status_code == 409]
    assert len(winners) + len(busy) == len(acquire_resps)  # never a 5xx
    assert len(winners) <= 1

    if winners:
        # The handover winner — and only the winner — owns generation 2.
        assert winners[0].json()["control_generation"] == 2
        assert _last_generation(db_engine, KNOWN_ANTENNA) == 2
        assert count_rows(db_engine, "SELECT count(*) FROM leases") == 2
    else:
        # The release lost the race against nobody: still only generation 1.
        assert _last_generation(db_engine, KNOWN_ANTENNA) == 1
        assert count_rows(db_engine, "SELECT count(*) FROM leases") == 1


def test_rejections_never_consume_generations(http_client, db_engine):
    key = make_key()
    held = http_client.post("/leases", json=_req(duration=60, key=key))
    assert held.status_code == 200
    holder = held.json()
    assert holder["control_generation"] == 1

    antennas_before = _snapshot_antennas(db_engine)
    leases_before = _snapshot_leases(db_engine)

    # 1. Busy antenna.
    busy = acquire(http_client, antenna_id=KNOWN_ANTENNA, duration_seconds=30)
    assert busy.status_code == 409
    assert busy.json()["error"]["code"] == "ANTENNA_BUSY"

    # 2. Unknown antenna.
    unknown = acquire(http_client, antenna_id="ANT-999", duration_seconds=30)
    assert unknown.status_code == 404
    assert unknown.json()["error"]["code"] == "ANTENNA_NOT_FOUND"

    # 3. Same idempotency key with different parameters.
    conflict = http_client.post(
        "/leases", json=_req(duration=60, key=key, controller="intruder")
    )
    assert conflict.status_code == 409
    assert conflict.json()["error"]["code"] == "IDEMPOTENCY_CONFLICT"

    # 4. Input rejections (below/above the lease bounds, textual number).
    for bad_duration in (4, 121, "30"):
        rejected = http_client.post(
            "/leases",
            json=_req(duration=bad_duration),
        )
        assert rejected.status_code == 422, bad_duration
        assert rejected.json()["error"]["code"] == "VALIDATION_ERROR"

    # Neither the antenna catalog/counters nor any lease row changed.
    assert _snapshot_antennas(db_engine) == antennas_before
    assert _snapshot_leases(db_engine) == leases_before

    # The first success after the failures continues exactly where the last
    # committed lease left off — no generation was burned by the rejections.
    assert release(http_client, holder["lease_token"]).status_code == 200
    successor = acquire(
        http_client, antenna_id=KNOWN_ANTENNA, duration_seconds=30
    )
    assert successor.status_code == 200
    assert successor.json()["control_generation"] == 2
    assert _last_generation(db_engine, KNOWN_ANTENNA) == 2


def test_seeded_history_generations_follow_acquisition_order(
    http_client, db_engine
):
    """The ordering rule the migration uses for pre-existing leases — per
    antenna, earlier (acquired_at, id) means a smaller generation — is the
    same rule seeded history follows, so historical lookups stay stable."""
    older = insert_expired_lease(
        db_engine, antenna_id="ANT-04", age_seconds=30, ttl_seconds=10
    )
    newer = insert_expired_lease(
        db_engine, antenna_id="ANT-04", age_seconds=10, ttl_seconds=10
    )
    assert older["control_generation"] == 1
    assert newer["control_generation"] == 2
    assert older["acquired_at"] < newer["acquired_at"]

    for token, generation in (
        (older["token"], 1),
        (newer["token"], 2),
    ):
        status = http_client.get(f"/leases/{token}")
        assert status.status_code == 200
        assert status.json()["control_generation"] == generation

    # The next live acquisition continues the sequence.
    resp = acquire(http_client, antenna_id="ANT-04", duration_seconds=30)
    assert resp.status_code == 200
    assert resp.json()["control_generation"] == 3
    assert _last_generation(db_engine, "ANT-04") == 3


def test_unknown_token_lookup_does_not_touch_generations(
    http_client, db_engine
):
    before = _snapshot_antennas(db_engine)
    resp = http_client.get("/leases/no-such-token")
    assert resp.status_code == 404
    assert resp.json()["error"]["code"] == "LEASE_NOT_FOUND"
    assert _snapshot_antennas(db_engine) == before
    assert count_rows(db_engine, "SELECT count(*) FROM leases") == 0
