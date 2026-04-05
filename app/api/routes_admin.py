from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Query, status

from app.api.dependencies import require_admin_session
from app.services import admin_service

router = APIRouter(prefix='/api/v1/admin', tags=['admin'])


def _require_admin_attr(name: str):
    attr = getattr(admin_service, name, None)
    if attr is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=f'Admin service no disponible: {name}',
        )
    return attr


@router.get('/overview')
def admin_overview(_: dict = Depends(require_admin_session)) -> dict:
    return _require_admin_attr('get_admin_overview')()


@router.get('/users/search')
def admin_search(
    q: str = Query(min_length=1),
    limit: int = Query(default=10, ge=1, le=25),
    _: dict = Depends(require_admin_session),
) -> dict:
    return _require_admin_attr('admin_search_users')(q, limit=limit)


@router.get('/users/{user_id}')
def admin_user_detail(user_id: int, _: dict = Depends(require_admin_session)) -> dict:
    return _require_admin_attr('admin_get_user_detail')(int(user_id))


@router.post('/users/{user_id}/plan/premium')
def admin_activate_user_premium(user_id: int, _: dict = Depends(require_admin_session)) -> dict:
    return _require_admin_attr('admin_activate_premium')(int(user_id))
