"""Database engine / session management.

The application never trusts the host clock: every decision that depends on
"now" is expressed with PostgreSQL ``clock_timestamp()`` inside SQL.
"""

from __future__ import annotations

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

from app.config import DATABASE_URL

engine: Engine = create_engine(
    DATABASE_URL,
    pool_pre_ping=True,
    pool_size=10,
    max_overflow=20,
    future=True,
)


def db_now_sql() -> str:
    """Return SQL fragment for the current database time."""
    return "clock_timestamp()"


def check_database() -> bool:
    """Lightweight liveness probe used by the health endpoint."""
    with engine.connect() as conn:
        conn.execute(text("SELECT 1"))
    return True
