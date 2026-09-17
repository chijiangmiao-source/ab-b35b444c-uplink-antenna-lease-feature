"""FastAPI application entrypoint.

Run with: uvicorn app.main:app --host 0.0.0.0 --port 8000
Migrations are applied separately via Alembic (see entrypoint.sh).
"""

from __future__ import annotations

from fastapi import FastAPI
from fastapi.exceptions import RequestValidationError
from sqlalchemy import text

from app.db import engine
from app.errors import api_error_handler, validation_error_handler, APIError
from app.routers import catalog, leases

app = FastAPI(
    title="卫星天线控制租约服务",
    description=(
        "为上行控制程序分配天线控制权。数据库时间是唯一时钟；"
        "每副天线任一时刻至多存在一个未到期租约。"
    ),
    version="1.0.0",
)

app.add_exception_handler(APIError, api_error_handler)
app.add_exception_handler(RequestValidationError, validation_error_handler)
app.include_router(leases.router)
app.include_router(catalog.router)


@app.get("/health", tags=["system"], summary="存活探针（含数据库时间）")
def health():
    with engine.connect() as conn:
        db_time = conn.execute(text("SELECT clock_timestamp()")).scalar_one()
    return {"status": "ok", "database_time": db_time}
