from __future__ import annotations

from fastapi import APIRouter, Depends, Query

from app.api.dependencies import require_session
from app.services.user_service import (
    get_dashboard,
    get_recent_operations,
    get_referrals_summary,
    get_user_profile,
)

router = APIRouter(prefix='/api/v1', tags=['user'])


@router.get('/me')
def me(session: dict = Depends(require_session)) -> dict:
    return get_user_profile(int(session['user_id']))


@router.get('/dashboard')
def dashboard(
    include_balance: bool = Query(default=False),
    session: dict = Depends(require_session),
) -> dict:
    return get_dashboard(int(session['user_id']), include_balance=include_balance)


@router.get('/operations')
def operations(
    limit: int = Query(default=20, ge=1, le=100),
    session: dict = Depends(require_session),
) -> dict:
    return get_recent_operations(int(session['user_id']), limit=limit)


@router.get('/referrals')
def referrals(session: dict = Depends(require_session)) -> dict:
    return get_referrals_summary(int(session['user_id']))
