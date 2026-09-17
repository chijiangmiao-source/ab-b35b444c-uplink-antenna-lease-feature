"""lease early release: leases.released_at

Revision ID: 0002_lease_release
Revises: 0002_lease_progress
Create Date: 2026-09-12
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0002_lease_release"
down_revision: str | None = "0002_lease_progress"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    # Nullable on purpose: NULL means "still held" and lets the active-lease
    # predicate stay a plain ``released_at IS NULL``. The value is written
    # exclusively by the database clock (clock_timestamp()) at release time;
    # no server_default — a lease is not born released.
    op.add_column(
        "leases",
        sa.Column("released_at", sa.DateTime(timezone=True), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("leases", "released_at")
