from __future__ import annotations

from fastapi import APIRouter, Depends

from app.api.dependencies import require_admin_session
from app.services.admin_service import get_admin_overview

router = APIRouter(prefix='/api/v1/admin', tags=['admin'])


@router.get('/overview')
def admin_overview(_: dict = Depends(require_admin_session)) -> dict:
    return get_admin_overview()
