"""Read-only catalog routes."""

from __future__ import annotations

from fastapi import APIRouter
from sqlalchemy import text

from app.db import engine

router = APIRouter(tags=["catalog"])


@router.get("/antennas", summary="列出全部预置天线")
def list_antennas():
    with engine.connect() as conn:
        rows = conn.execute(
            text("SELECT id, name FROM antennas ORDER BY id")
        ).mappings().all()
    return {"antennas": [dict(row) for row in rows]}
