from __future__ import annotations

from datetime import datetime
from typing import Any

from app.database import (
    create_user,
    get_last_operation,
    get_referral_valid_count,
    get_user_public_snapshot,
    get_user_trades_limited,
)
from app.hyperliquid_client import get_balance


def _serialize_dt(value: Any) -> str | None:
    if isinstance(value, datetime):
        return value.isoformat()
    return None


def ensure_user_exists(user_id: int, username: str | None = None) -> dict:
    safe_username = (username or '').strip() or f'user_{int(user_id)}'
    create_user(int(user_id), safe_username)
    profile = get_user_public_snapshot(int(user_id))
    if not profile:
        raise RuntimeError(f'No se pudo cargar el perfil del usuario {user_id}')
    return profile


def get_user_profile(user_id: int) -> dict:
    profile = get_user_public_snapshot(int(user_id))
    if not profile:
        raise LookupError('Usuario no encontrado')

    return {
        'user_id': profile['user_id'],
        'username': profile.get('username'),
        'wallet': profile.get('wallet'),
        'wallet_configured': bool(profile.get('wallet_configured')),
        'private_key_configured': bool(profile.get('private_key_configured')),
        'trading_status': profile.get('trading_status', 'inactive'),
        'plan': profile.get('plan', 'none'),
        'plan_active': bool(profile.get('plan_active')),
        'plan_expires_at': _serialize_dt(profile.get('plan_expires_at')),
        'trial_used': bool(profile.get('trial_used')),
        'terms_accepted': bool(profile.get('terms_accepted')),
        'referral_valid_count': int(profile.get('referral_valid_count', 0) or 0),
        'last_open_at': _serialize_dt(profile.get('last_open_at')),
        'last_close_at': _serialize_dt(profile.get('last_close_at')),
    }


def get_dashboard(user_id: int, include_balance: bool = False) -> dict:
    profile = get_user_profile(int(user_id))
    payload = {
        **profile,
        'status_summary': 'ready' if (
            profile['wallet_configured'] and profile['private_key_configured'] and profile['trading_status'] == 'active' and profile['plan_active']
        ) else 'not_ready',
    }
    if include_balance:
        payload['exchange_balance'] = float(get_balance(int(user_id)) or 0.0)
    return payload


def get_recent_operations(user_id: int, limit: int = 20) -> dict:
    last_operation = get_last_operation(int(user_id)) or {}
    trades = get_user_trades_limited(int(user_id), limit=limit)

    normalized_trades = []
    for trade in trades:
        normalized = dict(trade)
        ts = normalized.get('timestamp')
        normalized['timestamp'] = _serialize_dt(ts)
        normalized_trades.append(normalized)

    return {
        'last_open': last_operation.get('last_open'),
        'last_close': last_operation.get('last_close'),
        'trades': normalized_trades,
        'count': len(normalized_trades),
    }


def get_referrals_summary(user_id: int) -> dict:
    return {
        'user_id': int(user_id),
        'referral_valid_count': int(get_referral_valid_count(int(user_id)) or 0),
    }
