"""Control generation: a per-antenna, strictly increasing fencing token.

Uplink devices may keep emitting commands from a stale lease after a network
partition heals. Every accepted acquisition therefore freezes a per-antenna,
monotonically increasing ``control_generation`` into the lease, and the
antenna row remembers the last allocated value. Devices only obey the holder
of the largest generation they have seen.

Everything runs against the REAL API + REAL PostgreSQL:

* consecutive hand-overs (release-based and expiry-based) increment the
  generation strictly, one step per committed lease;
* same-key replay and the token query return the frozen original value;
* barrier-synchronised contention commits exactly one new generation;
* busy / unknown-antenna / idempotency-conflict / validation rejections
  consume nothing — the first success afterwards continues the sequence
  with no gap;
* the 0004 migration backfills historical leases deterministically in
  (acquired_at, id) order per antenna, verified against a scratch database
  migrated from the pre-generation schema.
"""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.engine.url import make_url
from sqlalchemy.exc import IntegrityError

from conftest import (
    DATABASE_URL,
    KNOWN_ANTENNA,
    acquire,
    active_lease_count,
    antenna_generation,
    count_rows,
    insert_expired_lease,
    make_key,
    parallel_acquire,
    release,
)

PROJECT_ROOT = Path(__file__).resolve().parents[1]


def _payload(antenna_id: str, controller: str, duration: int, key: str) -> dict:
    return {
        "antenna_id": antenna_id,
        "controller": controller,
        "duration_seconds": duration,
        "idempotency_key": key,
    }


def _lease_rows(db_engine) -> list:
    """Full committed lease state, in record order."""
    with db_engine.connect() as conn:
        return conn.execute(
            text(
                """
                SELECT id, antenna_id, controller, token, acquired_at,
                       expires_at, released_at, last_command_sequence,
                       control_generation
                FROM leases
                ORDER BY id
                """
            )
        ).all()


def _antenna_counters(db_engine) -> dict:
    with db_engine.connect() as conn:
        return dict(
            conn.execute(
                text("SELECT id, last_control_generation FROM antennas")
            ).all()
        )


def _generations(db_engine, antenna_id: str) -> list[int]:
    with db_engine.connect() as conn:
        return [
            row[0]
            for row in conn.execute(
                text(
                    """
                    SELECT control_generation FROM leases
                    WHERE antenna_id = :antenna_id
                    ORDER BY control_generation
                    """
                ),
                {"antenna_id": antenna_id},
            )
        ]


def test_first_grant_starts_at_one_and_matches_everywhere(http_client, db_engine):
    resp = acquire(http_client, antenna_id=KNOWN_ANTENNA, duration_seconds=30)
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["replay"] is False
    assert body["control_generation"] == 1

    # The token query exposes the same frozen value.
    status = http_client.get(f"/leases/{body['lease_token']}")
    assert status.status_code == 200
    assert status.json()["control_generation"] == 1

    # The antenna high-water mark and the stored lease row agree.
    assert antenna_generation(db_engine, KNOWN_ANTENNA) == 1
    assert (
        count_rows(
            db_engine,
            "SELECT control_generation FROM leases WHERE token = :t",
            t=body["lease_token"],
        )
        == 1
    )


def test_consecutive_handovers_increment_strictly(http_client, db_engine):
    expected = 0
    # Two hand-overs via early release...
    for _ in range(2):
        expected += 1
        resp = acquire(http_client, antenna_id=KNOWN_ANTENNA, duration_seconds=60)
        assert resp.status_code == 200, resp.text
        assert resp.json()["control_generation"] == expected
        token = resp.json()["lease_token"]
        assert release(http_client, token).status_code == 200
        # The release itself must not move the counter.
        assert antenna_generation(db_engine, KNOWN_ANTENNA) == expected

    # ...then a natural-expiry hand-over seeded directly in SQL.
    seeded = insert_expired_lease(db_engine, antenna_id=KNOWN_ANTENNA, age_seconds=2)
    expected += 1
    assert seeded["control_generation"] == expected

    expected += 1
    successor = acquire(http_client, antenna_id=KNOWN_ANTENNA, duration_seconds=30)
    assert successor.status_code == 200, successor.text
    assert successor.json()["control_generation"] == expected

    # The committed history is a gapless 1..N ladder; the antenna counter is N.
    assert _generations(db_engine, KNOWN_ANTENNA) == [1, 2, 3, 4]
    assert antenna_generation(db_engine, KNOWN_ANTENNA) == 4
    # Every historical lease keeps its own frozen generation on query.
    with db_engine.connect() as conn:
        tokens = conn.execute(
            text(
                """
                SELECT token, control_generation FROM leases
                WHERE antenna_id = :antenna_id ORDER BY control_generation
                """
            ),
            {"antenna_id": KNOWN_ANTENNA},
        ).all()
    for token, generation in tokens:
        status = http_client.get(f"/leases/{token}")
        assert status.status_code == 200
        assert status.json()["control_generation"] == generation


def test_replay_and_query_return_the_frozen_generation(http_client, db_engine):
    key = make_key()
    payload = _payload(KNOWN_ANTENNA, "gs-beijing-A", 30, key)
    first = http_client.post("/leases", json=payload)
    assert first.status_code == 200
    original = first.json()
    assert original["control_generation"] == 1

    for _ in range(3):
        replay = http_client.post("/leases", json=payload)
        assert replay.status_code == 200
        body = replay.json()
        assert body["replay"] is True
        assert body["control_generation"] == original["control_generation"]
        assert body["lease_token"] == original["lease_token"]

    # Replays are reads: the antenna counter never moved.
    assert antenna_generation(db_engine, KNOWN_ANTENNA) == 1
    assert count_rows(db_engine, "SELECT count(*) FROM leases") == 1


def test_replay_of_expired_lease_keeps_its_frozen_generation(
    http_client, db_engine
):
    # Seed an already-expired lease plus its idempotency record, exactly like
    # a grant whose response was lost before the lease lapsed.
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

    replay = http_client.post(
        "/leases",
        json=_payload("ANT-06", "expired-ctrl", 10, key),
    )
    assert replay.status_code == 200
    assert replay.json()["replay"] is True
    assert replay.json()["control_generation"] == expired["control_generation"]

    status = http_client.get(f"/leases/{expired['token']}")
    assert status.status_code == 200
    assert status.json()["control_generation"] == expired["control_generation"]
    # Nothing was allocated by the replay or the query.
    assert antenna_generation(db_engine, "ANT-06") == expired["control_generation"]


def test_concurrent_same_key_retry_commits_exactly_one_generation(
    http_client, db_engine
):
    # The "response lost, client retried" storm: byte-identical payloads
    # racing under one idempotency key.
    payload = _payload(KNOWN_ANTENNA, "retrying-program", 60, make_key())
    responses = parallel_acquire(http_client, [dict(payload) for _ in range(8)])

    assert all(r.status_code == 200 for r in responses), [
        r.status_code for r in responses
    ]
    generations = {r.json()["control_generation"] for r in responses}
    assert generations == {1}
    assert antenna_generation(db_engine, KNOWN_ANTENNA) == 1
    assert count_rows(db_engine, "SELECT count(*) FROM leases") == 1


def test_concurrent_contention_commits_exactly_one_new_generation(
    http_client, db_engine
):
    # One committed historical grant, then a 12-way barrier race for the
    # now-free antenna: exactly one contender may land generation 2.
    seeded = insert_expired_lease(db_engine, antenna_id=KNOWN_ANTENNA, age_seconds=2)
    before = antenna_generation(db_engine, KNOWN_ANTENNA)
    assert before == seeded["control_generation"] == 1

    requests = [
        _payload(KNOWN_ANTENNA, f"contender-{i}", 60, make_key())
        for i in range(12)
    ]
    responses = parallel_acquire(http_client, requests)

    winners = [r for r in responses if r.status_code == 200]
    losers = [r for r in responses if r.status_code == 409]
    assert len(winners) == 1, [r.status_code for r in responses]
    assert len(losers) == 11
    for r in losers:
        assert r.json()["error"]["code"] == "ANTENNA_BUSY"

    winner = winners[0].json()
    assert winner["replay"] is False
    assert winner["control_generation"] == before + 1

    # Exactly one new generation was committed: the ladder is gapless and the
    # counter moved by one.
    assert _generations(db_engine, KNOWN_ANTENNA) == [1, 2]
    assert antenna_generation(db_engine, KNOWN_ANTENNA) == before + 1
    assert active_lease_count(db_engine, KNOWN_ANTENNA) == 1
    assert count_rows(db_engine, "SELECT count(*) FROM leases") == 2
    # The 11 losers left no idempotency residue.
    assert count_rows(db_engine, "SELECT count(*) FROM idempotency_keys") == 1


def test_no_rejection_path_consumes_a_generation(http_client, db_engine):
    held_key = make_key()
    held = http_client.post(
        "/leases", json=_payload(KNOWN_ANTENNA, "incumbent", 60, held_key)
    )
    assert held.status_code == 200
    assert held.json()["control_generation"] == 1
    token = held.json()["lease_token"]

    counters_before = _antenna_counters(db_engine)
    leases_before = _lease_rows(db_engine)
    idem_before = count_rows(db_engine, "SELECT count(*) FROM idempotency_keys")

    def assert_unchanged() -> None:
        assert _antenna_counters(db_engine) == counters_before
        assert _lease_rows(db_engine) == leases_before
        assert (
            count_rows(db_engine, "SELECT count(*) FROM idempotency_keys")
            == idem_before
        )

    # 1. ANTENNA_BUSY: a fresh contender on the held antenna.
    busy = http_client.post(
        "/leases", json=_payload(KNOWN_ANTENNA, "contender", 30, make_key())
    )
    assert busy.status_code == 409
    assert busy.json()["error"]["code"] == "ANTENNA_BUSY"
    assert_unchanged()

    # 2. ANTENNA_NOT_FOUND: unknown antenna id.
    missing = http_client.post(
        "/leases", json=_payload("ANT-99", "ghost", 30, make_key())
    )
    assert missing.status_code == 404
    assert missing.json()["error"]["code"] == "ANTENNA_NOT_FOUND"
    assert_unchanged()

    # 3. IDEMPOTENCY_CONFLICT: the holder's key with different parameters.
    conflict = http_client.post(
        "/leases", json=_payload(KNOWN_ANTENNA, "other-controller", 30, held_key)
    )
    assert conflict.status_code == 409
    assert conflict.json()["error"]["code"] == "IDEMPOTENCY_CONFLICT"
    assert_unchanged()

    # 4. VALIDATION_ERROR: every input-rejection flavour.
    bad_payloads = [
        _payload(KNOWN_ANTENNA, "c", 4, make_key()),          # below range
        _payload(KNOWN_ANTENNA, "c", 121, make_key()),        # above range
        {**_payload(KNOWN_ANTENNA, "c", 30, make_key()), "duration_seconds": "30"},
        _payload(KNOWN_ANTENNA, "   ", 30, make_key()),       # blank controller
        {**_payload(KNOWN_ANTENNA, "c", 30, make_key()), "surprise": 1},
    ]
    # Missing required field.
    missing_field = _payload(KNOWN_ANTENNA, "c", 30, make_key())
    del missing_field["duration_seconds"]
    bad_payloads.append(missing_field)

    for bad in bad_payloads:
        resp = http_client.post("/leases", json=bad)
        assert resp.status_code == 422, (bad, resp.text)
        assert resp.json()["error"]["code"] == "VALIDATION_ERROR"
        assert_unchanged()

    # The first success after the rejection storm continues the sequence with
    # no gap: exactly the last committed generation + 1.
    assert release(http_client, token).status_code == 200
    successor = http_client.post(
        "/leases", json=_payload(KNOWN_ANTENNA, "successor", 30, make_key())
    )
    assert successor.status_code == 200, successor.text
    assert successor.json()["control_generation"] == 2
    assert antenna_generation(db_engine, KNOWN_ANTENNA) == 2
    assert _generations(db_engine, KNOWN_ANTENNA) == [1, 2]


def test_generations_are_independent_per_antenna(http_client, db_engine):
    first = acquire(http_client, antenna_id="ANT-01", duration_seconds=60)
    second = acquire(http_client, antenna_id="ANT-02", duration_seconds=60)
    assert first.status_code == 200
    assert second.status_code == 200
    # Each antenna starts its own ladder at 1.
    assert first.json()["control_generation"] == 1
    assert second.json()["control_generation"] == 1

    assert release(http_client, first.json()["lease_token"]).status_code == 200
    third = acquire(http_client, antenna_id="ANT-01", duration_seconds=60)
    assert third.status_code == 200
    assert third.json()["control_generation"] == 2

    # The other antenna's ladder is untouched.
    assert antenna_generation(db_engine, "ANT-01") == 2
    assert antenna_generation(db_engine, "ANT-02") == 1
    status = http_client.get(f"/leases/{second.json()['lease_token']}")
    assert status.json()["control_generation"] == 1


# --------------------------------------------------------------------------
# Migration: historical leases are backfilled in (acquired_at, id) order
# --------------------------------------------------------------------------

MIGRATION_DB = "satctl_control_generation_migration"


def _alembic(url: str, *args: str) -> None:
    env = dict(os.environ, DATABASE_URL=url)
    proc = subprocess.run(
        [sys.executable, "-m", "alembic", *args],
        cwd=PROJECT_ROOT,
        env=env,
        capture_output=True,
        text=True,
    )
    assert proc.returncode == 0, (
        f"alembic {' '.join(args)} failed:\n{proc.stdout}\n{proc.stderr}"
    )


def _insert_legacy_lease(conn, antenna, token, acquired, expires) -> None:
    conn.execute(
        text(
            """
            INSERT INTO leases (antenna_id, controller, token,
                                acquired_at, expires_at)
            VALUES (:antenna, 'legacy-ctrl', :token, :acquired, :expires)
            """
        ),
        {
            "antenna": antenna,
            "token": token,
            "acquired": acquired,
            "expires": expires,
        },
    )


def test_migration_backfills_generations_in_acquire_order():
    """Upgrade a pre-generation database and verify the deterministic
    backfill: per antenna, generations follow (acquired_at, id); the antenna
    counter resumes at the backfilled maximum; re-running the backfill (via
    downgrade/upgrade) reproduces the identical assignment."""
    main_url = make_url(DATABASE_URL)
    scratch_url = str(main_url.set(database=MIGRATION_DB))
    admin_url = str(main_url.set(database="postgres"))

    admin = create_engine(admin_url, isolation_level="AUTOCOMMIT", future=True)
    with admin.connect() as conn:
        conn.execute(text(f"DROP DATABASE IF EXISTS {MIGRATION_DB} WITH (FORCE)"))
        conn.execute(text(f"CREATE DATABASE {MIGRATION_DB}"))
    admin.dispose()

    scratch = create_engine(scratch_url, future=True)
    try:
        # Schema as it was before control generations existed.
        _alembic(scratch_url, "upgrade", "0003_lease_renew")

        with scratch.begin() as conn:
            # ANT-01: inserted OUT of acquire order — the earlier grant gets
            # the larger record id, so acquired_at must dominate the id.
            _insert_legacy_lease(
                conn,
                "ANT-01",
                "legacy-ant01-late",
                "2026-09-01 10:05:00+00",
                "2026-09-01 10:05:30+00",
            )
            _insert_legacy_lease(
                conn,
                "ANT-01",
                "legacy-ant01-early",
                "2026-09-01 10:00:00+00",
                "2026-09-01 10:00:30+00",
            )
            # ANT-02: identical acquired_at — the record id breaks the tie.
            _insert_legacy_lease(
                conn,
                "ANT-02",
                "legacy-ant02-first",
                "2026-09-01 10:00:00+00",
                "2026-09-01 10:00:30+00",
            )
            _insert_legacy_lease(
                conn,
                "ANT-02",
                "legacy-ant02-second",
                "2026-09-01 10:00:00+00",
                "2026-09-01 10:00:30+00",
            )
            # ANT-03 … ANT-06 have no history and must start from zero.

        _alembic(scratch_url, "upgrade", "head")

        expected = {
            "legacy-ant01-early": 1,
            "legacy-ant01-late": 2,
            "legacy-ant02-first": 1,
            "legacy-ant02-second": 2,
        }
        with scratch.connect() as conn:
            rows = dict(
                conn.execute(
                    text("SELECT token, control_generation FROM leases")
                ).all()
            )
            assert rows == expected
            counters = dict(
                conn.execute(
                    text("SELECT id, last_control_generation FROM antennas")
                ).all()
            )
            assert counters["ANT-01"] == 2
            assert counters["ANT-02"] == 2
            for other in ("ANT-03", "ANT-04", "ANT-05", "ANT-06"):
                assert counters[other] == 0
            # Historical queries are stable: the same read returns the same
            # assignment again.
            again = dict(
                conn.execute(
                    text("SELECT token, control_generation FROM leases")
                ).all()
            )
            assert again == rows

        # The next allocation continues from the backfilled high-water mark,
        # so a post-upgrade grant can never collide with a historical one.
        with scratch.begin() as conn:
            nxt = conn.execute(
                text(
                    """
                    UPDATE antennas
                    SET last_control_generation = last_control_generation + 1
                    WHERE id = 'ANT-01'
                    RETURNING last_control_generation
                    """
                )
            ).scalar_one()
            assert nxt == 3
            conn.execute(
                text(
                    """
                    INSERT INTO leases (antenna_id, controller, token,
                                        acquired_at, expires_at,
                                        control_generation)
                    VALUES ('ANT-01', 'post-upgrade-ctrl', 'post-upgrade',
                            clock_timestamp(),
                            clock_timestamp() + make_interval(secs => 30),
                            :generation)
                    """
                ),
                {"generation": nxt},
            )

        # The database itself refuses to hand one generation to two leases.
        with scratch.connect() as conn:
            with pytest.raises(IntegrityError):
                conn.execute(
                    text(
                        """
                        INSERT INTO leases (antenna_id, controller, token,
                                            acquired_at, expires_at,
                                            control_generation)
                        VALUES ('ANT-01', 'dup-ctrl', 'dup-generation',
                                clock_timestamp(),
                                clock_timestamp() + make_interval(secs => 30),
                                3)
                        """
                    )
                )

        # The backfill is deterministic: a downgrade/upgrade round-trip
        # recomputes the identical assignment for the surviving rows.
        _alembic(scratch_url, "downgrade", "0003_lease_renew")
        _alembic(scratch_url, "upgrade", "head")
        with scratch.connect() as conn:
            recomputed = dict(
                conn.execute(
                    text("SELECT token, control_generation FROM leases")
                ).all()
            )
            assert recomputed == {**expected, "post-upgrade": 3}
    finally:
        scratch.dispose()
        admin = create_engine(
            admin_url, isolation_level="AUTOCOMMIT", future=True
        )
        with admin.connect() as conn:
            conn.execute(
                text(f"DROP DATABASE IF EXISTS {MIGRATION_DB} WITH (FORCE)")
            )
        admin.dispose()
