"""lease progress: last_command_sequence, last_progress_at

Revision ID: 0002_lease_progress
Revises: 0001_initial
Create Date: 2026-09-12
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0002_lease_progress"
down_revision: str | None = "0001_initial"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    # Both columns stay NULL for leases that have never reported progress:
    # existing rows are not backfilled, and freshly acquired leases start
    # with no progress either. Adding nullable columns without a default is a
    # metadata-only change on PostgreSQL.
    op.add_column(
        "leases",
        sa.Column("last_command_sequence", sa.BigInteger(), nullable=True),
    )
    # The recording time is produced by the database clock
    # (clock_timestamp()) at UPDATE time, never by the application host.
    op.add_column(
        "leases",
        sa.Column(
            "last_progress_at", sa.DateTime(timezone=True), nullable=True
        ),
    )
    op.create_check_constraint(
        "leases_progress_sequence_nonneg",
        "leases",
        "last_command_sequence IS NULL OR last_command_sequence >= 0",
    )
    # Sequence number and recording time always move together.
    op.create_check_constraint(
        "leases_progress_fields_together",
        "leases",
        "(last_command_sequence IS NULL) = (last_progress_at IS NULL)",
    )


def downgrade() -> None:
    op.drop_constraint("leases_progress_fields_together", "leases")
    op.drop_constraint("leases_progress_sequence_nonneg", "leases")
    op.drop_column("leases", "last_progress_at")
    op.drop_column("leases", "last_command_sequence")
