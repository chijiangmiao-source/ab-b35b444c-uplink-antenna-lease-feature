"""Token safety and lease retrieval.

Regression coverage for two defects:

1. Tokens were standard base64 (alphabet ``+ / =``). A token containing ``/``
   broke the single-segment route ``/leases/{token}`` so a freshly acquired
   lease queried as "not found". Tokens must be URL-safe base64url.
2. ``duration_seconds`` sent as the string ``"30"`` was coerced by lax
   Pydantic typing and created a lease. It must be rejected with zero writes.
"""

from __future__ import annotations

import re
from urllib.parse import quote

from sqlalchemy import text

from conftest import acquire, count_rows, make_key

# 32 random bytes, base64url, padding stripped -> exactly 43 chars, alphabet
# restricted to unreserved URL characters.
URLSAFE_TOKEN = re.compile(r"^[A-Za-z0-9_-]{43}$")


def test_issued_tokens_are_url_safe_base64url(http_client):
    resp = acquire(http_client, antenna_id="ANT-01")
    assert resp.status_code == 200
    token = resp.json()["lease_token"]
    assert URLSAFE_TOKEN.match(token), token
    for unsafe in ("/", "+", "="):
        assert unsafe not in token


def test_database_token_generator_never_emits_unsafe_chars(db_engine):
    # Sample the exact SQL expression the application uses many times; with
    # standard base64 a slash would appear almost immediately.
    with db_engine.connect() as conn:
        rows = conn.execute(
            text(
                """
                SELECT rtrim(
                         replace(
                           replace(encode(gen_random_bytes(32), 'base64'), '+', '-'),
                           '/', '_'
                         ),
                         '='
                       ) AS token
                FROM generate_series(1, 2000)
                """
            )
        ).scalars().all()
    assert len(rows) == 2000
    for token in rows:
        assert URLSAFE_TOKEN.match(token), token


def test_freshly_acquired_lease_is_retrievable_by_token(http_client):
    acquired = acquire(http_client, antenna_id="ANT-02", duration_seconds=20)
    assert acquired.status_code == 200
    body = acquired.json()
    token = body["lease_token"]

    # Raw interpolation is safe here: token is restricted to [A-Za-z0-9_-];
    # still quote() it to exercise realistic client behaviour.
    resp = http_client.get(f"/leases/{quote(token, safe='')}")
    assert resp.status_code == 200, resp.text
    detail = resp.json()
    assert detail["lease_token"] == token
    assert detail["antenna_id"] == "ANT-02"
    assert detail["active"] is True
    assert detail["expires_at"] == body["expires_at"]
    assert detail["acquired_at"] == body["acquired_at"]


# ISO-8601 with an explicit UTC offset (e.g. 2026-09-12T04:00:30.123456+00:00).
# A bare "Z" on one endpoint and "+00:00" on the other would fail this.
ISO_OFFSET = re.compile(
    r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?[+-]\d{2}:\d{2}$"
)


def test_expires_at_format_is_identical_between_acquire_and_lookup(http_client):
    acquired = acquire(http_client, antenna_id="ANT-03", duration_seconds=25)
    assert acquired.status_code == 200

    token = acquired.json()["lease_token"]
    lookup = http_client.get(f"/leases/{token}")
    assert lookup.status_code == 200

    acquired_raw = acquired.json()
    lookup_raw = lookup.json()

    # Byte-identical timestamps across the two endpoints (same representation,
    # not merely the same parsed instant).
    for field in ("acquired_at", "expires_at"):
        assert acquired_raw[field] == lookup_raw[field], (
            field,
            acquired_raw[field],
            lookup_raw[field],
        )
        assert ISO_OFFSET.match(acquired_raw[field]), acquired_raw[field]
        assert ISO_OFFSET.match(lookup_raw[field]), lookup_raw[field]
        assert not acquired_raw[field].endswith("Z")
        assert not lookup_raw[field].endswith("Z")


def test_replay_and_acquire_share_identical_timestamp_format(http_client):
    key = make_key()
    payload = {
        "antenna_id": "ANT-04",
        "controller": "format-check",
        "duration_seconds": 15,
        "idempotency_key": key,
    }
    first = http_client.post("/leases", json=payload)
    assert first.status_code == 200
    replay = http_client.post("/leases", json=payload)
    assert replay.status_code == 200
    assert replay.json()["replay"] is True

    first_raw = first.json()
    replay_raw = replay.json()
    for field in ("acquired_at", "expires_at"):
        assert replay_raw[field] == first_raw[field]
        assert ISO_OFFSET.match(first_raw[field]), first_raw[field]


def test_retrieval_works_for_antennas_that_would_contain_slashes_under_base64(
    http_client, db_engine
):
    # Acquire on every provisioned antenna (fresh schema state) and retrieve
    # each one; over many runs a standard-base64 deployment would hit a slash.
    tokens = []
    for i in range(1, 7):
        resp = acquire(http_client, antenna_id=f"ANT-0{i}", duration_seconds=30)
        assert resp.status_code == 200, (i, resp.text)
        tokens.append(resp.json()["lease_token"])

    for token in tokens:
        resp = http_client.get(f"/leases/{token}")
        assert resp.status_code == 200, token
        assert resp.json()["lease_token"] == token


def test_textual_duration_30_is_rejected_and_writes_nothing(http_client, db_engine):
    payload = {
        "antenna_id": "ANT-01",
        "controller": "text-duration",
        "duration_seconds": "30",  # JSON string, not a number
        "idempotency_key": make_key(),
    }
    resp = http_client.post("/leases", json=payload)
    assert resp.status_code == 422, resp.text
    body = resp.json()
    assert body["error"]["code"] == "VALIDATION_ERROR"
    assert any(
        e["field"] == "duration_seconds" for e in body["error"]["details"]["errors"]
    )
    assert count_rows(db_engine, "SELECT count(*) FROM leases") == 0
    assert count_rows(db_engine, "SELECT count(*) FROM idempotency_keys") == 0

    # Repeated attempt is a stable rejection.
    resp2 = http_client.post("/leases", json=payload)
    assert resp2.status_code == 422
    assert count_rows(db_engine, "SELECT count(*) FROM leases") == 0


def test_stringly_out_of_range_duration_is_also_rejected(http_client, db_engine):
    # Even numeric-looking strings at the edges must not be parsed at all.
    for value in ("5", "120", "121", "4", " 30 "):
        payload = {
            "antenna_id": "ANT-03",
            "controller": "ctrl",
            "duration_seconds": value,
            "idempotency_key": make_key(),
        }
        resp = http_client.post("/leases", json=payload)
        assert resp.status_code == 422, value
    assert count_rows(db_engine, "SELECT count(*) FROM leases") == 0


def test_integer_json_number_still_accepted(http_client):
    resp = http_client.post(
        "/leases",
        json={
            "antenna_id": "ANT-04",
            "controller": "numeric",
            "duration_seconds": 30,
            "idempotency_key": make_key(),
        },
    )
    assert resp.status_code == 200, resp.text
    assert URLSAFE_TOKEN.match(resp.json()["lease_token"])
