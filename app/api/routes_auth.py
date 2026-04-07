from __future__ import annotations

from pydantic import BaseModel
from fastapi import APIRouter

from app.api.security import create_session_token, verify_telegram_init_data
from app.config import WEBAPP_SESSION_TTL_SECONDS
from app.database import set_referrer
from app.services.user_service import ensure_user_exists, get_user_profile

router = APIRouter(prefix='/api/v1/auth', tags=['auth'])


class TelegramAuthRequest(BaseModel):
    init_data: str


@router.post('/telegram')
def auth_with_telegram(payload: TelegramAuthRequest) -> dict:
    verified = verify_telegram_init_data(payload.init_data)
    ensure_user_exists(verified['user_id'], verified.get('username'))

    start_param = (verified.get('start_param') or '').strip()
    if start_param.isdigit() and int(start_param) != int(verified['user_id']):
        set_referrer(int(verified['user_id']), int(start_param))

    token = create_session_token(verified)
    return {
        'access_token': token,
        'token_type': 'bearer',
        'expires_in': int(WEBAPP_SESSION_TTL_SECONDS),
        'is_admin': bool(verified.get('is_admin')),
        'user': get_user_profile(int(verified['user_id'])),
    }
