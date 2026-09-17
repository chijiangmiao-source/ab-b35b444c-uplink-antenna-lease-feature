"""HTTP routes for antenna control leases."""

from __future__ import annotations

from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session

from app.db import engine
from app.errors import APIError
from app.schemas import (
    AcquireRequest,
    AcquireResponse,
    LeaseStatusResponse,
    ProgressRequest,
    ProgressResponse,
    RenewRequest,
    RenewResponse,
)
from app.services import (
    acquire_lease,
    get_lease_by_token,
    release_lease,
    renew_lease,
    report_progress,
)

router = APIRouter(tags=["leases"])


def get_session():
    # Plain transactional scope: the service layer decides commit/rollback
    # boundaries explicitly.
    with Session(engine, future=True) as session:
        yield session


def _to_status_response(result: dict) -> dict:
    # Normalise internal database names to the public response model.
    result.pop("lease_id", None)
    result["lease_token"] = result.pop("token")
    return result


@router.post(
    "/leases",
    response_model=AcquireResponse,
    status_code=200,
    summary="原子获取一副天线的控制租约",
)
def acquire(payload: AcquireRequest, session: Session = Depends(get_session)):
    try:
        result = acquire_lease(
            session.connection(),
            antenna_id=payload.antenna_id,
            controller=payload.controller,
            duration_seconds=payload.duration_seconds,
            idempotency_key=payload.idempotency_key,
        )
    except APIError:
        session.rollback()
        raise
    session.commit()
    return result


@router.get(
    "/leases/{lease_token}",
    response_model=LeaseStatusResponse,
    summary="按令牌查询租约状态",
)
def lease_status(lease_token: str, session: Session = Depends(get_session)):
    result = get_lease_by_token(session.connection(), lease_token)
    if result is None:
        session.rollback()
        raise APIError(
            404,
            "LEASE_NOT_FOUND",
            "未知租约令牌。",
            {"lease_token": lease_token},
        )
    session.commit()
    return _to_status_response(result)


@router.post(
    "/leases/{lease_token}/progress",
    response_model=ProgressResponse,
    status_code=200,
    summary="上报当前持有方已执行到的指令序号",
)
def report_lease_progress(
    lease_token: str,
    payload: ProgressRequest,
    session: Session = Depends(get_session),
):
    try:
        result = report_progress(
            session.connection(), lease_token, payload.sequence
        )
    except APIError:
        session.rollback()
        raise
    session.commit()
    return result


@router.post(
    "/leases/{lease_token}/release",
    response_model=LeaseStatusResponse,
    status_code=200,
    summary="持有方提前释放租约（过站提前结束/主动让权）",
)
def release(lease_token: str, session: Session = Depends(get_session)):
    try:
        result = release_lease(session.connection(), lease_token)
    except APIError:
        session.rollback()
        raise
    session.commit()
    return _to_status_response(result)


@router.post(
    "/leases/{lease_token}/renew",
    response_model=RenewResponse,
    status_code=200,
    summary="租约仍有效时凭令牌追加 5–120 秒（过站窗口临时延长）",
)
def renew(
    lease_token: str,
    payload: RenewRequest,
    session: Session = Depends(get_session),
):
    try:
        result = renew_lease(
            session.connection(),
            lease_token,
            extra_seconds=payload.extra_seconds,
            idempotency_key=payload.idempotency_key,
        )
    except APIError:
        session.rollback()
        raise
    session.commit()
    return result
