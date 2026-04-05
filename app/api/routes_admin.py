from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Query, status
from pydantic import BaseModel, Field

from app.api.dependencies import require_admin_session
from app.services import admin_service

router = APIRouter(prefix='/api/v1/admin', tags=['admin'])


class ManualPremiumDaysPayload(BaseModel):
    days: int = Field(..., ge=1, le=3650)


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


@router.get('/users/{user_id}/plan/manual-days-preview')
def admin_preview_user_manual_premium_days(
    user_id: int,
    days: int = Query(..., ge=1, le=3650),
    _: dict = Depends(require_admin_session),
) -> dict:
    return _require_admin_attr('admin_preview_manual_premium_days')(int(user_id), int(days))


@router.post('/users/{user_id}/plan/premium')
def admin_activate_user_premium(user_id: int, _: dict = Depends(require_admin_session)) -> dict:
    return _require_admin_attr('admin_activate_premium')(int(user_id))


@router.post('/users/{user_id}/plan/manual-days')
def admin_grant_user_manual_premium_days(
    user_id: int,
    payload: ManualPremiumDaysPayload,
    _: dict = Depends(require_admin_session),
) -> dict:
    return _require_admin_attr('admin_grant_manual_premium_days')(int(user_id), int(payload.days))


@router.post('/users/{user_id}/trading/activate')
def admin_activate_user_trading(user_id: int, _: dict = Depends(require_admin_session)) -> dict:
    return _require_admin_attr('admin_activate_user_trading')(int(user_id))


@router.post('/users/{user_id}/trading/pause')
def admin_pause_user_trading(user_id: int, _: dict = Depends(require_admin_session)) -> dict:
    return _require_admin_attr('admin_pause_user_trading')(int(user_id))


@router.post('/users/{user_id}/stats/reset')
def admin_reset_stats(user_id: int, _: dict = Depends(require_admin_session)) -> dict:
    return _require_admin_attr('admin_reset_user_stats')(int(user_id))


@router.post('/users/{user_id}/security/migrate-key')
def admin_migrate_user_key(user_id: int, _: dict = Depends(require_admin_session)) -> dict:
    return _require_admin_attr('admin_migrate_user_private_key')(int(user_id))


@router.post('/security/migrate-legacy-keys')
def admin_bulk_migrate_keys(
    limit: int = Query(default=25, ge=1, le=100),
    _: dict = Depends(require_admin_session),
) -> dict:
    return _require_admin_attr('admin_bulk_migrate_legacy_keys')(limit=limit)
