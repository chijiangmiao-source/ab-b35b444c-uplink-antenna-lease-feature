"""lease renewal: lease_renewals, renewal_idempotency_keys

Revision ID: 0003_lease_renew
Revises: 0002_lease_release
Create Date: 2026-09-12
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0003_lease_renew"
down_revision: str | None = "0002_lease_release"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    # One row per accepted renewal. Timestamps are database-clock values by
    # construction: previous_expires_at is the lease's expires_at as found
    # inside the renewal transaction, new_expires_at is previous + extra
    # seconds; the CHECK makes that arithmetic a stored invariant instead of
    # a promise from the application.
    op.create_table(
        "lease_renewals",
        sa.Column(
            "id",
            sa.BigInteger(),
            primary_key=True,
            autoincrement=True,
        ),
        sa.Column(
            "lease_id",
            sa.BigInteger,
            sa.ForeignKey("leases.id", ondelete="RESTRICT"),
            nullable=False,
        ),
        sa.Column(
            "previous_expires_at", sa.DateTime(timezone=True), nullable=False
        ),
        sa.Column(
            "new_expires_at", sa.DateTime(timezone=True), nullable=False
        ),
        # Same inclusive [5, 120] bound as an acquisition, enforced in the
        # database as well as at the HTTP/service boundary.
        sa.Column("extra_seconds", sa.Integer(), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("clock_timestamp()"),
        ),
        sa.CheckConstraint(
            "extra_seconds >= 5 AND extra_seconds <= 120",
            name="lease_renewals_extra_seconds_range",
        ),
        sa.CheckConstraint(
            "new_expires_at = previous_expires_at "
            "+ make_interval(secs => extra_seconds)",
            name="lease_renewals_extension_matches",
        ),
    )
    op.create_index(
        "ix_lease_renewals_lease", "lease_renewals", ["lease_id"]
    )

    # Idempotency for renewal requests, kept in its own table so renewal keys
    # never collide with acquisition keys (a client may reuse a UUID
    # generator independently for the two operations).
    op.create_table(
        "renewal_idempotency_keys",
        sa.Column(
            "idempotency_key", sa.String(length=128), primary_key=True
        ),
        sa.Column(
            "renewal_id",
            sa.BigInteger,
            sa.ForeignKey("lease_renewals.id", ondelete="RESTRICT"),
            nullable=False,
        ),
        sa.Column("request_params", sa.Text(), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("clock_timestamp()"),
        ),
    )
    op.create_index(
        "ix_renewal_idempotency_renewal",
        "renewal_idempotency_keys",
        ["renewal_id"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index(
        "ix_renewal_idempotency_renewal",
        table_name="renewal_idempotency_keys",
    )
    op.drop_table("renewal_idempotency_keys")
    op.drop_index("ix_lease_renewals_lease", table_name="lease_renewals")
    op.drop_table("lease_renewals")
