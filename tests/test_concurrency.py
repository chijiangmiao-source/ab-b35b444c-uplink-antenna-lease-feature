"""Real concurrency: multiple uplink control programs contend for one
antenna simultaneously against the live API + PostgreSQL.

Threads rendezvous at a barrier so requests genuinely race; the database
(row lock on antennas + clock-based predicate) is the arbiter.
"""

from __future__ import annotations

from conftest import (
    KNOWN_ANTENNA,
    active_lease_count,
    count_rows,
    make_key,
    parallel_acquire,
)


def _req(antenna=KNOWN_ANTENNA, controller=None, duration=30, key=None):
    return {
        "antenna_id": antenna,
        "controller": controller or f"ctrl-{make_key()}",
        "duration_seconds": duration,
        "idempotency_key": key or make_key(),
    }


def test_single_winner_when_many_contend_for_free_antenna(http_client, db_engine):
    # Give the holder a long lease so losers cannot accidentally slip in.
    requests = [_req(duration=60) for _ in range(12)]
    responses = parallel_acquire(http_client, requests)

    statuses = sorted(r.status_code for r in responses)
    assert statuses.count(200) == 1, statuses
    assert statuses.count(409) == 11, statuses

    for r in responses:
        if r.status_code == 409:
            assert r.json()["error"]["code"] == "ANTENNA_BUSY"

    winner = next(r for r in responses if r.status_code == 200)
    assert winner.json()["replay"] is False
    assert active_lease_count(db_engine, KNOWN_ANTENNA) == 1
    # Expired rows included: still exactly one lease was ever written.
    assert count_rows(db_engine, "SELECT count(*) FROM leases") == 1
    # Losers must not persist anything (no idempotency row for a busy miss).
    assert count_rows(db_engine, "SELECT count(*) FROM idempotency_keys") == 1


def test_loser_never_overwrites_holder(http_client, db_engine):
    held = http_client.post("/leases", json=_req(duration=60, controller="incumbent"))
    assert held.status_code == 200
    incumbent = held.json()

    responses = parallel_acquire(
        http_client, [_req(duration=60) for _ in range(10)]
    )
    assert all(r.status_code == 409 for r in responses)
    for r in responses:
        body = r.json()
        assert body["error"]["code"] == "ANTENNA_BUSY"
        # Busy response points at the still-incumbent lease.
        assert body["error"]["details"]["held_by_lease"] == incumbent["lease_token"]

    assert active_lease_count(db_engine, KNOWN_ANTENNA) == 1
    assert count_rows(db_engine, "SELECT count(*) FROM leases") == 1


def test_lost_response_retried_with_same_key_never_creates_second_lease(
    http_client, db_engine
):
    # Simulate the operator retrying the *same* request after the response was
    # lost in the link, concurrently with itself.
    key = make_key()
    # The exact same request fired concurrently with itself: this is the
    # "response was lost in the link, retry" case. Payloads must be byte-for-
    # byte identical, including the controller and duration.
    payload = _req(duration=60, controller="retrying-program", key=key)
    requests = [dict(payload) for _ in range(8)]
    responses = parallel_acquire(http_client, requests)

    assert all(r.status_code == 200 for r in responses), [
        r.status_code for r in responses
    ]
    tokens = {r.json()["lease_token"] for r in responses}
    expiries = {r.json()["expires_at"] for r in responses}
    assert len(tokens) == 1
    assert len(expiries) == 1
    assert active_lease_count(db_engine, KNOWN_ANTENNA) == 1
    assert count_rows(db_engine, "SELECT count(*) FROM leases") == 1
    assert count_rows(db_engine, "SELECT count(*) FROM idempotency_keys") == 1


def test_same_key_fanout_with_divergent_params_has_no_lease_for_conflicts(
    http_client, db_engine
):
    key = make_key()
    requests = [
        _req(duration=60, key=key),
        _req(duration=60, key=key, controller="other-program"),
        _req(duration=60, key=key, controller="third-program"),
    ]
    responses = parallel_acquire(http_client, requests)

    # Exactly one outcome established the canonical request; every other
    # outcome is a stable conflict. Which payload wins is nondeterministic.
    winners = [r for r in responses if r.status_code == 200]
    conflicts = [r for r in responses if r.status_code == 409]
    assert len(winners) == 1, [r.status_code for r in responses]
    winner = winners[0]
    for r in conflicts:
        assert r.json()["error"]["code"] == "IDEMPOTENCY_CONFLICT"
    assert active_lease_count(db_engine, KNOWN_ANTENNA) == 1
    assert count_rows(db_engine, "SELECT count(*) FROM leases") == 1
    # Replaying the winner's exact request afterwards is a stable replay.
    winning_payload = requests[responses.index(winner)]
    again = http_client.post("/leases", json=winning_payload)
    assert again.status_code == 200
    assert again.json()["replay"] is True
    assert again.json()["lease_token"] == winner.json()["lease_token"]
    # Any non-winning payload keeps conflicting.
    losing_payload = requests[
        next(i for i, r in enumerate(responses) if r.status_code == 409)
    ]
    yet_again = http_client.post("/leases", json=losing_payload)
    assert yet_again.status_code == 409
    assert yet_again.json()["error"]["code"] == "IDEMPOTENCY_CONFLICT"


def test_contention_on_different_antennas_is_independent(http_client, db_engine):
    requests = [_req(antenna=f"ANT-0{i}") for i in range(1, 7)]
    responses = parallel_acquire(http_client, requests)
    assert all(r.status_code == 200 for r in responses), [
        r.status_code for r in responses
    ]
    tokens = [r.json()["lease_token"] for r in responses]
    assert len(set(tokens)) == 6
    for i in range(1, 7):
        assert active_lease_count(db_engine, f"ANT-0{i}") == 1


def test_repeated_renewal_storm_while_held_is_all_busy(http_client, db_engine):
    first = http_client.post("/leases", json=_req(duration=120))
    assert first.status_code == 200
    # Two waves of contention, distinct keys each time.
    for wave in range(2):
        responses = parallel_acquire(
            http_client, [_req(duration=120) for _ in range(6)]
        )
        assert all(r.status_code == 409 for r in responses), wave
    assert active_lease_count(db_engine, KNOWN_ANTENNA) == 1
    assert count_rows(db_engine, "SELECT count(*) FROM leases") == 1
