"""initial schema: antennas, leases, idempotency_keys

Revision ID: 0001_initial
Revises:
Create Date: 2026-09-12
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0001_initial"
down_revision: str | None = None
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

# Provisioned antennas. The catalog is fixed on purpose: the service only
# grants control over known antennas.
SEED_ANTENNAS = [
    ("ANT-01", "Beijing uplink array"),
    ("ANT-02", "Sanya uplink array"),
    ("ANT-03", "Kashgar uplink array"),
    ("ANT-04", "Kunming uplink array"),
    ("ANT-05", "Urumqi uplink array"),
    ("ANT-06", "Harbin uplink array"),
]


def upgrade() -> None:
    # CSPRNG for unpredictable lease tokens (gen_random_bytes).
    op.execute("CREATE EXTENSION IF NOT EXISTS pgcrypto")

    antennas = op.create_table(
        "antennas",
        sa.Column("id", sa.String(length=64), primary_key=True),
        sa.Column("name", sa.String(length=256), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("clock_timestamp()"),
        ),
    )

    op.create_table(
        "leases",
        # autoincrement renders as BIGSERIAL / IDENTITY on PostgreSQL; a plain
        # BIGINT PRIMARY KEY has no sequence and inserts that omit id would
        # violate NOT NULL.
        sa.Column(
            "id",
            sa.BigInteger(),
            primary_key=True,
            autoincrement=True,
        ),
        sa.Column(
            "antenna_id",
            sa.String(length=64),
            sa.ForeignKey("antennas.id", ondelete="RESTRICT"),
            nullable=False,
        ),
        sa.Column("controller", sa.String(length=128), nullable=False),
        sa.Column("token", sa.String(length=64), nullable=False),
        sa.Column("acquired_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("expires_at", sa.DateTime(timezone=True), nullable=False),
        sa.CheckConstraint(
            "expires_at > acquired_at", name="leases_expires_after_acquire"
        ),
    )
    op.create_index("uq_leases_token", "leases", ["token"], unique=True)
    # Active-lease lookups during acquisition always filter by antenna and
    # compare expires_at against the database clock.
    op.create_index(
        "ix_leases_antenna_expires", "leases", ["antenna_id", "expires_at"]
    )

    op.create_table(
        "idempotency_keys",
        sa.Column("idempotency_key", sa.String(length=128), primary_key=True),
        sa.Column(
            "lease_id",
            sa.BigInteger,
            sa.ForeignKey("leases.id", ondelete="RESTRICT"),
            nullable=False,
        ),
        sa.Column("request_params", sa.Text, nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("clock_timestamp()"),
        ),
    )
    op.create_index(
        "ix_idempotency_lease", "idempotency_keys", ["lease_id"], unique=False
    )

    op.bulk_insert(
        antennas,
        [{"id": aid, "name": name} for aid, name in SEED_ANTENNAS],
    )


def downgrade() -> None:
    op.drop_table("idempotency_keys")
    op.drop_index("ix_leases_antenna_expires", table_name="leases")
    op.drop_index("uq_leases_token", table_name="leases")
    op.drop_table("leases")
    op.drop_table("antennas")
