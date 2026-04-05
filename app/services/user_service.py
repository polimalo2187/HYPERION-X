from __future__ import annotations

import re
from datetime import datetime
from typing import Any

from fastapi import HTTPException, status

from app.config import ADMIN_WHATSAPP_LINK
from app.database import (
    accept_terms,
    create_user,
    ensure_access_on_activate,
    get_last_operation,
    get_referral_valid_count,
    get_system_runtime_snapshot,
    get_user_public_snapshot,
    get_user_trade_stats,
    get_user_trades_limited,
    has_accepted_terms,
    save_user_private_key,
    save_user_wallet,
    set_trading_status,
)
from app.hyperliquid_client import get_balance

_ETH_ADDRESS_RE = re.compile(r"^0x[a-fA-F0-9]{40}$")
_PRIVATE_KEY_RE = re.compile(r"^(0x)?[a-fA-F0-9]{64}$")


def _serialize_dt(value: Any) -> str | None:
    if isinstance(value, datetime):
        return value.isoformat()
    return None


def _clean_wallet(wallet: str) -> str:
    value = (wallet or '').strip()
    if not value:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail='La wallet no puede estar vacía')
    if not _ETH_ADDRESS_RE.fullmatch(value):
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail='Wallet inválida. Debe ser una dirección 0x de 42 caracteres')
    return value


def _clean_private_key(private_key: str) -> str:
    value = (private_key or '').strip()
    if not value:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail='La private key no puede estar vacía')
    if not _PRIVATE_KEY_RE.fullmatch(value):
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail='Private key inválida. Debe ser hexadecimal de 64 caracteres')
    return value if value.startswith('0x') else f'0x{value}'


def _snapshot_or_raise(user_id: int) -> dict:
    profile = get_user_public_snapshot(int(user_id))
    if not profile:
        raise LookupError('Usuario no encontrado')
    return profile


def ensure_user_exists(user_id: int, username: str | None = None) -> dict:
    safe_username = (username or '').strip() or f'user_{int(user_id)}'
    create_user(int(user_id), safe_username)
    profile = get_user_public_snapshot(int(user_id))
    if not profile:
        raise RuntimeError(f'No se pudo cargar el perfil del usuario {user_id}')
    return profile


def get_user_profile(user_id: int) -> dict:
    profile = _snapshot_or_raise(int(user_id))

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
        'terms_timestamp': _serialize_dt(profile.get('terms_timestamp')),
        'private_key_storage': profile.get('private_key_storage', 'not_configured'),
        'referral_valid_count': int(profile.get('referral_valid_count', 0) or 0),
        'last_open_at': _serialize_dt(profile.get('last_open_at')),
        'last_close_at': _serialize_dt(profile.get('last_close_at')),
    }


def get_dashboard(user_id: int, include_balance: bool = False) -> dict:
    profile = get_user_profile(int(user_id))
    ready = (
        profile['wallet_configured']
        and profile['private_key_configured']
        and profile['trading_status'] == 'active'
        and profile['plan_active']
        and profile['terms_accepted']
    )
    payload = {
        **profile,
        'status_summary': 'ready' if ready else 'not_ready',
    }
    if include_balance:
        payload['exchange_balance'] = float(get_balance(int(user_id)) or 0.0)
    return payload


def get_control_summary(user_id: int) -> dict:
    profile = get_user_profile(int(user_id))
    terms_accepted = bool(profile['terms_accepted'])
    return {
        **profile,
        'wallet_masked': profile['wallet'],
        'private_key_masked': '******** configurada' if profile['private_key_configured'] else 'No configurada',
        'security_posture': ('encrypted_at_rest' if profile.get('private_key_storage') == 'encrypted' else ('legacy_plaintext' if profile.get('private_key_storage') == 'legacy_plaintext' else 'not_configured')), 
        'activation_ready': bool(
            profile['wallet_configured']
            and profile['private_key_configured']
            and terms_accepted
        ),
        'activation_blockers': [
            blocker
            for blocker, is_missing in (
                ('wallet_missing', not profile['wallet_configured']),
                ('private_key_missing', not profile['private_key_configured']),
                ('terms_missing', not terms_accepted),
            )
            if is_missing
        ],
        'support_contact': ADMIN_WHATSAPP_LINK or None,
    }


def update_user_configuration(
    user_id: int,
    *,
    wallet: str | None = None,
    private_key: str | None = None,
) -> dict:
    if wallet is None and private_key is None:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail='No se recibió ningún cambio para guardar')

    if wallet is not None:
        save_user_wallet(int(user_id), _clean_wallet(wallet))

    if private_key is not None:
        save_user_private_key(int(user_id), _clean_private_key(private_key))

    return get_control_summary(int(user_id))


def accept_user_terms(user_id: int) -> dict:
    if not accept_terms(int(user_id)):
        raise RuntimeError('No se pudieron aceptar los términos en este momento')
    return get_control_summary(int(user_id))


def activate_user_trading(user_id: int) -> dict:
    summary = get_control_summary(int(user_id))
    if not has_accepted_terms(int(user_id)):
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail='Debes aceptar los términos antes de activar el trading')

    access = ensure_access_on_activate(int(user_id))
    if not access.get('allowed', False):
        detail = access.get('message') or 'Tu acceso está bloqueado'
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail=detail)

    set_trading_status(int(user_id), 'active')
    summary = get_control_summary(int(user_id))
    return {
        'result': 'activated',
        'message': access.get('plan_message') or 'Trading activado',
        'control': summary,
    }


def pause_user_trading(user_id: int) -> dict:
    set_trading_status(int(user_id), 'inactive')
    return {
        'result': 'paused',
        'message': 'Trading pausado',
        'control': get_control_summary(int(user_id)),
    }


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


def get_performance_summary(user_id: int) -> dict:
    return {
        '24h': _normalize_trade_stats(get_user_trade_stats(int(user_id), 24) or {}),
        '7d': _normalize_trade_stats(get_user_trade_stats(int(user_id), 24 * 7) or {}),
        '30d': _normalize_trade_stats(get_user_trade_stats(int(user_id), 24 * 30) or {}),
    }


def _normalize_trade_stats(payload: dict) -> dict:
    return {
        'total': int(payload.get('total', 0) or 0),
        'wins': int(payload.get('wins', 0) or 0),
        'losses': int(payload.get('losses', 0) or 0),
        'break_evens': int(payload.get('break_evens', 0) or 0),
        'decisive_trades': int(payload.get('decisive_trades', 0) or 0),
        'win_rate': float(payload.get('win_rate', 0.0) or 0.0),
        'win_rate_decisive': float(payload.get('win_rate_decisive', 0.0) or 0.0),
        'pnl_total': float(payload.get('pnl_total', 0.0) or 0.0),
        'gross_profit': float(payload.get('gross_profit', 0.0) or 0.0),
        'gross_loss': float(payload.get('gross_loss', 0.0) or 0.0),
        'profit_factor': payload.get('profit_factor', 0.0),
        'since': _serialize_dt(payload.get('since')),
        'epoch': _serialize_dt(payload.get('epoch')),
    }


def get_referrals_summary(user_id: int) -> dict:
    return {
        'user_id': int(user_id),
        'referral_valid_count': int(get_referral_valid_count(int(user_id)) or 0),
    }


def get_system_runtime_summary(user_id: int) -> dict:
    profile = get_user_profile(int(user_id))
    snapshot = get_system_runtime_snapshot() or {}

    def _serialize_component(item: dict | None) -> dict:
        payload = dict(item or {})
        payload['last_seen_at'] = _serialize_dt(payload.get('last_seen_at'))
        return payload

    def _serialize_activity(item: dict | None) -> dict | None:
        if not item:
            return None
        payload = dict(item)
        payload['at'] = _serialize_dt(payload.get('at'))
        return payload

    latest_manager = (snapshot.get('runtime') or {}).get('latest_trade_manager') or {}
    latest_manager = dict(latest_manager) if latest_manager else None
    if latest_manager and latest_manager.get('manager_heartbeat_at'):
        latest_manager['manager_heartbeat_at'] = _serialize_dt(latest_manager.get('manager_heartbeat_at'))

    return {
        'viewer_user_id': int(profile['user_id']),
        'viewer_plan': profile.get('plan'),
        'overall_status': snapshot.get('overall_status') or 'unknown',
        'checked_at': _serialize_dt(snapshot.get('checked_at')),
        'backend': {
            'status': 'online',
            'message': 'El backend web respondió a esta solicitud.',
            'checked_at': _serialize_dt(snapshot.get('checked_at')),
        },
        'components': {
            'telegram_bot': _serialize_component((snapshot.get('components') or {}).get('telegram_bot')),
            'trading_loop': _serialize_component((snapshot.get('components') or {}).get('trading_loop')),
            'scanner': _serialize_component((snapshot.get('components') or {}).get('scanner')),
        },
        'runtime': {
            **(snapshot.get('runtime') or {}),
            'latest_open': _serialize_activity((snapshot.get('runtime') or {}).get('latest_open')),
            'latest_close': _serialize_activity((snapshot.get('runtime') or {}).get('latest_close')),
            'latest_trade_manager': latest_manager,
        },
        'issues': snapshot.get('issues') or [],
    }
