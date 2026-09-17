"""Stable, machine-readable application errors.

Every error uses the same JSON envelope::

    {"error": {"code": "...", "message": "...", "details": {...}}}

Codes never change so clients can branch on them; messages may.
"""

from __future__ import annotations

from typing import Any

from fastapi import Request
from fastapi.responses import JSONResponse


class APIError(Exception):
    def __init__(
        self,
        status_code: int,
        code: str,
        message: str,
        details: dict[str, Any] | None = None,
    ) -> None:
        super().__init__(message)
        self.status_code = status_code
        self.code = code
        self.message = message
        self.details = details or {}


def _envelope(code: str, message: str, details: dict[str, Any]) -> dict[str, Any]:
    return {"error": {"code": code, "message": message, "details": details}}


async def api_error_handler(request: Request, exc: APIError) -> JSONResponse:
    return JSONResponse(
        status_code=exc.status_code,
        content=_envelope(exc.code, exc.message, exc.details),
    )


async def validation_error_handler(
    request: Request, exc: Exception
) -> JSONResponse:
    # FastAPI RequestValidationError; serialised lazily to avoid importing it
    # outside the handler registration path.
    errors: list[dict[str, Any]] = []
    for err in exc.errors():  # type: ignore[attr-defined]
        ctx = err.get("ctx")
        errors.append(
            {
                "field": ".".join(str(p) for p in err.get("loc", []) if p != "body"),
                "message": err.get("msg", ""),
                "type": err.get("type", ""),
                "constraint": str(ctx.get("error")) if ctx and "error" in ctx else None,
            }
        )
    return JSONResponse(
        status_code=422,
        content=_envelope(
            "VALIDATION_ERROR", "请求参数未通过校验。", {"errors": errors}
        ),
    )
