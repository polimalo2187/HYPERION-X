from __future__ import annotations

from fastapi import APIRouter, Depends, Query

from app.api.dependencies import require_admin_session
from app.services.admin_service import (
    admin_activate_premium,
    admin_get_user_detail,
    admin_search_users,
    get_admin_overview,
)

router = APIRouter(prefix='/api/v1/admin', tags=['admin'])


@router.get('/overview')
def admin_overview(_: dict = Depends(require_admin_session)) -> dict:
    return get_admin_overview()


@router.get('/users/search')
def admin_search(
    q: str = Query(min_length=1),
    limit: int = Query(default=10, ge=1, le=25),
    _: dict = Depends(require_admin_session),
) -> dict:
    return admin_search_users(q, limit=limit)


@router.get('/users/{user_id}')
def admin_user_detail(user_id: int, _: dict = Depends(require_admin_session)) -> dict:
    return admin_get_user_detail(int(user_id))


@router.post('/users/{user_id}/plan/premium')
def admin_activate_user_premium(user_id: int, _: dict = Depends(require_admin_session)) -> dict:
    return admin_activate_premium(int(user_id))
