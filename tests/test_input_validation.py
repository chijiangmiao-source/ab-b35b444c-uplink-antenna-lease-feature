"""Input rejection: unknown antennas and out-of-range leases are stable
errors and must never touch the database."""

from __future__ import annotations

from conftest import (
    KNOWN_ANTENNA,
    MAX_LEASE_SECONDS,
    MIN_LEASE_SECONDS,
    acquire,
    count_rows,
    make_key,
)


def test_unknown_antenna_is_404_and_persists_nothing(http_client, db_engine):
    resp = acquire(http_client, antenna_id="ANT-999")
    assert resp.status_code == 404
    body = resp.json()
    assert body["error"]["code"] == "ANTENNA_NOT_FOUND"
    assert body["error"]["details"]["antenna_id"] == "ANT-999"

    assert count_rows(db_engine, "SELECT count(*) FROM leases") == 0
    assert count_rows(db_engine, "SELECT count(*) FROM idempotency_keys") == 0

    # Stable: repeated rejection is identical and still write-free.
    again = acquire(http_client, antenna_id="ANT-999")
    assert again.status_code == 404
    assert again.json()["error"]["code"] == "ANTENNA_NOT_FOUND"
    assert count_rows(db_engine, "SELECT count(*) FROM leases") == 0


def test_duration_below_minimum_rejected(http_client, db_engine):
    resp = acquire(
        http_client, duration_seconds=MIN_LEASE_SECONDS - 1
    )
    assert resp.status_code == 422
    assert resp.json()["error"]["code"] == "VALIDATION_ERROR"
    assert count_rows(db_engine, "SELECT count(*) FROM leases") == 0


def test_duration_above_maximum_rejected(http_client, db_engine):
    resp = acquire(
        http_client, duration_seconds=MAX_LEASE_SECONDS + 1
    )
    assert resp.status_code == 422
    assert resp.json()["error"]["code"] == "VALIDATION_ERROR"
    assert count_rows(db_engine, "SELECT count(*) FROM idempotency_keys") == 0


def test_duration_zero_negative_and_fractional_rejected(http_client, db_engine):
    base = {
        "antenna_id": KNOWN_ANTENNA,
        "controller": "ctrl",
        "duration_seconds": 30,
        "idempotency_key": make_key(),
    }
    for bad in (0, -5, 7.5, "30", None):
        payload = {**base, "duration_seconds": bad, "idempotency_key": make_key()}
        resp = http_client.post("/leases", json=payload)
        assert resp.status_code == 422, bad
        assert resp.json()["error"]["code"] == "VALIDATION_ERROR"
    assert count_rows(db_engine, "SELECT count(*) FROM leases") == 0


def test_boundary_durations_accepted(http_client, db_engine):
    # Two antennas so the second acquisition cannot be blocked by the first.
    for antenna, duration in (
        ("ANT-02", MIN_LEASE_SECONDS),
        ("ANT-03", MAX_LEASE_SECONDS),
    ):
        resp = acquire(
            http_client, antenna_id=antenna, duration_seconds=duration
        )
        assert resp.status_code == 200, (duration, resp.text)
        assert resp.json()["replay"] is False


def test_missing_and_blank_fields_rejected(http_client, db_engine):
    resp = http_client.post(
        "/leases",
        json={
            "antenna_id": KNOWN_ANTENNA,
            "controller": "ctrl",
            # idempotency_key missing
            "duration_seconds": 30,
        },
    )
    assert resp.status_code == 422

    for field in ("antenna_id", "controller", "idempotency_key"):
        payload = {
            "antenna_id": KNOWN_ANTENNA,
            "controller": "ctrl",
            "duration_seconds": 30,
            "idempotency_key": make_key(),
            field: "   ",
        }
        resp = http_client.post("/leases", json=payload)
        assert resp.status_code == 422, field

    assert count_rows(db_engine, "SELECT count(*) FROM leases") == 0


def test_unknown_antenna_with_bad_duration_does_not_write(http_client, db_engine):
    # Whichever validation wins, it must be a stable 4xx with zero writes.
    resp = acquire(
        http_client, antenna_id="ANT-404", duration_seconds=999
    )
    assert resp.status_code in (404, 422)
    assert count_rows(db_engine, "SELECT count(*) FROM leases") == 0
    assert count_rows(db_engine, "SELECT count(*) FROM idempotency_keys") == 0


def test_catalog_lists_provisioned_antennas(http_client):
    resp = http_client.get("/antennas")
    assert resp.status_code == 200
    ids = {a["id"] for a in resp.json()["antennas"]}
    assert {"ANT-01", "ANT-02", "ANT-03", "ANT-04", "ANT-05", "ANT-06"} <= ids
