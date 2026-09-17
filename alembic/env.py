"""Alembic environment.

The target DB URL always comes from ``DATABASE_URL``; no host clock or
hard-coded credentials live here.
"""

from __future__ import annotations

import os
from logging.config import fileConfig

from alembic import context
from sqlalchemy import create_engine, pool

from app.config import DATABASE_URL

config = context.config

if config.config_file_name is not None:
    fileConfig(config.config_file_name)

# Migrations are written by hand; no ORM metadata is used for autogenerate.
target_metadata = None


def _render_url() -> str:
    # Allow overriding independently of the app config when desired.
    return os.environ.get("DATABASE_URL", DATABASE_URL)


def run_migrations_offline() -> None:
    context.configure(
        url=_render_url(),
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
        compare_type=True,
    )
    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    connectable = create_engine(
        _render_url(), poolclass=pool.NullPool, future=True
    )
    with connectable.connect() as connection:
        context.configure(
            connection=connection,
            compare_type=True,
            target_metadata=target_metadata,
        )
        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
