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
    # Uplink devices may keep emitting commands from a stale lease after a
    # network partition heals. Every accepted acquisition therefore stamps the
    # lease with a per-antenna, strictly increasing ``control_generation`` so
    # a device can tell the current controller apart from any predecessor.
    #
    # ``antennas.last_control_generation`` is the high-water mark of allocated
    # generations; ``leases.control_generation`` freezes the value the lease
    # was granted with. Both start nullable so existing rows can be backfilled
    # before the NOT NULL constraint lands.
    op.add_column(
        "antennas",
        sa.Column("last_control_generation", sa.BigInteger(), nullable=True),
    )
    op.add_column(
        "leases",
        sa.Column("control_generation", sa.BigInteger(), nullable=True),
    )

    # Backfill historical leases deterministically: within one antenna the
    # generation order follows the order the leases were granted, i.e.
    # (acquired_at, id) — the record id breaks ties between rows committed
    # inside the same microsecond. ROW_NUMBER yields 1..N per antenna, so
    # historical queries return the same value on every read after the
    # upgrade.
    op.execute(
        """
        WITH ranked AS (
            SELECT id,
                   ROW_NUMBER() OVER (
                       PARTITION BY antenna_id
                       ORDER BY acquired_at, id
                   ) AS gen
            FROM leases
        )
        UPDATE leases
        SET control_generation = ranked.gen
        FROM ranked
        WHERE leases.id = ranked.id
        """
    )

    # The antenna counter resumes exactly where history left off: the next
    # acquisition continues at max(existing generations) + 1. An antenna that
    # never had a lease starts at 0 so its first grant is generation 1.
    op.execute(
        """
        UPDATE antennas
        SET last_control_generation = COALESCE(
            (SELECT MAX(control_generation) FROM leases
             WHERE leases.antenna_id = antennas.id),
            0
        )
        """
    )

    op.alter_column("leases", "control_generation", nullable=False)
    op.alter_column("antennas", "last_control_generation", nullable=False)

    op.create_check_constraint(
        "leases_control_generation_positive",
        "leases",
        "control_generation >= 1",
    )
    op.create_check_constraint(
        "antennas_last_control_generation_nonneg",
        "antennas",
        "last_control_generation >= 0",
    )
    # A generation is allocated at most once per antenna: even a buggy writer
    # cannot hand the same control generation to two leases.
    op.create_index(
        "uq_leases_antenna_generation",
        "leases",
        ["antenna_id", "control_generation"],
        unique=True,
    )


def downgrade() -> None:
    op.drop_index("uq_leases_antenna_generation", table_name="leases")
    op.drop_constraint(
        "antennas_last_control_generation_nonneg", "antennas"
    )
    op.drop_constraint("leases_control_generation_positive", "leases")
    op.drop_column("leases", "control_generation")
    op.drop_column("antennas", "last_control_generation")
