"""control generation: antennas.last_control_generation, leases.control_generation

Revision ID: 0004_control_generation
Revises: 0003_lease_renew
Create Date: 2026-09-17
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0004_control_generation"
down_revision: str | None = "0003_lease_renew"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    # Every antenna carries a monotonically increasing counter of the control
    # generations it has handed out. Acquisition increments it inside the
    # same transaction that creates the lease (while holding the antenna row
    # lock), so a committed lease always owns a unique, gap-free-next value
    # and rolled-back contenders never consume one. New antennas start at 0.
    op.add_column(
        "antennas",
        sa.Column(
            "last_control_generation",
            sa.BigInteger(),
            nullable=False,
            server_default=sa.text("0"),
        ),
    )
    op.create_check_constraint(
        "antennas_last_control_generation_nonneg",
        "antennas",
        "last_control_generation >= 0",
    )

    # Each lease freezes the generation it was granted with. Added nullable
    # first so existing rows can be backfilled deterministically: within one
    # antenna, leases are numbered 1..N by (acquired_at, id) — acquisition
    # time first, the record id as the tie-breaker for identical timestamps —
    # which makes historical token lookups stable after the upgrade.
    op.add_column(
        "leases",
        sa.Column("control_generation", sa.BigInteger(), nullable=True),
    )
    op.execute(
        """
        WITH ordered AS (
            SELECT id,
                   row_number() OVER (
                       PARTITION BY antenna_id
                       ORDER BY acquired_at, id
                   ) AS gen
            FROM leases
        )
        UPDATE leases
        SET control_generation = ordered.gen
        FROM ordered
        WHERE leases.id = ordered.id
        """
    )
    op.alter_column("leases", "control_generation", nullable=False)
    op.create_check_constraint(
        "leases_control_generation_positive",
        "leases",
        "control_generation >= 1",
    )
    # One generation is handed out at most once per antenna, ever.
    op.create_index(
        "uq_leases_antenna_generation",
        "leases",
        ["antenna_id", "control_generation"],
        unique=True,
    )

    # The counter continues from the highest generation already granted so
    # the first post-upgrade acquisition follows the last historical lease.
    op.execute(
        """
        UPDATE antennas a
        SET last_control_generation = COALESCE((
            SELECT max(l.control_generation)
            FROM leases l
            WHERE l.antenna_id = a.id
        ), 0)
        """
    )


def downgrade() -> None:
    op.drop_index("uq_leases_antenna_generation", table_name="leases")
    op.drop_constraint("leases_control_generation_positive", "leases")
    op.drop_column("leases", "control_generation")
    op.drop_constraint(
        "antennas_last_control_generation_nonneg", "antennas"
    )
    op.drop_column("antennas", "last_control_generation")
