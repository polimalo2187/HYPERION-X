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
    get_user_active_trade_snapshot,
    get_referral_valid_count,
    get_system_runtime_snapshot,
    get_user_activity,
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

def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except Exception:
        return float(default)


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except Exception:
        return int(default)


def _plan_status_label(profile: dict) -> str:
    if bool(profile.get('plan_active')):
        return 'active'
    if str(profile.get('plan') or '').lower() in {'trial', 'premium'}:
        return 'expired'
    return 'none'


def _access_copy(profile: dict) -> dict:
    days_remaining = _safe_int(profile.get('plan_days_remaining'), 0)
    expires_at = profile.get('plan_expires_at')
    if bool(profile.get('plan_active')):
        if days_remaining > 0:
            detail = f'{days_remaining} día(s) restantes'
        elif expires_at:
            detail = 'Acceso vigente con vencimiento cercano'
        else:
            detail = 'Acceso operativo vigente'
        return {'label': 'Activo', 'detail': detail, 'tone': 'active'}

    if str(profile.get('plan') or '').lower() in {'trial', 'premium'}:
        return {'label': 'Vencido', 'detail': 'El acceso existe, pero ya no está vigente', 'tone': 'blocked'}

    return {'label': 'Sin acceso', 'detail': 'Todavía no hay un plan activo', 'tone': 'blocked'}


def _readiness_score(profile: dict) -> tuple[int, int]:
    checks = [
        bool(profile.get('wallet_configured')),
        bool(profile.get('private_key_configured')),
        bool(profile.get('terms_accepted')),
        bool(profile.get('plan_active')),
        str(profile.get('trading_status') or '').lower() == 'active',
    ]
    total = len(checks)
    completed = sum(1 for item in checks if item)
    return completed, total


def _trade_result_meta(profit: Any) -> dict:
    value = _safe_float(profit, 0.0)
    if value > 0:
        return {'label': 'Win', 'tone': 'success'}
    if value < 0:
        return {'label': 'Loss', 'tone': 'danger'}
    return {'label': 'Flat', 'tone': 'neutral'}


def _build_operation_snapshot(payload: Any, fallback_title: str) -> dict:
    data = dict(payload or {}) if isinstance(payload, dict) else {}
    symbol = data.get('symbol') or data.get('coin') or data.get('asset') or '—'
    side = data.get('side') or data.get('direction') or data.get('signal') or '—'
    price = data.get('entry_price') or data.get('exit_price') or data.get('price') or data.get('mark_price')
    quantity = data.get('qty') or data.get('size') or data.get('amount')
    reason = data.get('reason') or data.get('status') or data.get('result') or data.get('close_reason')
    summary_parts = []
    if price is not None:
        summary_parts.append(f'Precio {price}')
    if quantity is not None:
        summary_parts.append(f'Qty {quantity}')
    if reason:
        summary_parts.append(str(reason))
    return {
        'title': f"{symbol} · {str(side).upper() if side != '—' else fallback_title}",
        'detail': ' · '.join(summary_parts) if summary_parts else 'Sin detalle adicional disponible.',
    }


def _friendly_blockers(blockers: list[str] | None) -> list[str]:
    labels = {
        'wallet_missing': 'Falta configurar la wallet.',
        'private_key_missing': 'Falta configurar la private key.',
        'terms_missing': 'Debes aceptar los términos operativos.',
    }
    return [labels.get(str(item), str(item)) for item in (blockers or [])]


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
        'plan_days_remaining': _safe_int(profile.get('plan_days_remaining'), 0),
        'plan_expires_at': _serialize_dt(profile.get('plan_expires_at')),
        'access_state': _plan_status_label(profile),
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
    completed, total = _readiness_score(profile)
    access = _access_copy(profile)
    payload = {
        **profile,
        'status_summary': 'ready' if ready else 'not_ready',
        'readiness_completed': completed,
        'readiness_total': total,
        'access_label': access['label'],
        'access_detail': access['detail'],
        'access_tone': access['tone'],
    }
    if include_balance:
        payload['exchange_balance'] = float(get_balance(int(user_id)) or 0.0)
    return payload


def get_control_summary(user_id: int) -> dict:
    profile = get_user_profile(int(user_id))
    terms_accepted = bool(profile['terms_accepted'])
    completed, total = _readiness_score(profile)
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
        'activation_blockers_copy': _friendly_blockers([
            blocker
            for blocker, is_missing in (
                ('wallet_missing', not profile['wallet_configured']),
                ('private_key_missing', not profile['private_key_configured']),
                ('terms_missing', not terms_accepted),
            )
            if is_missing
        ]),
        'support_contact': ADMIN_WHATSAPP_LINK or None,
        'readiness_completed': completed,
        'readiness_total': total,
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




def _normalize_trade_row(trade: dict) -> dict:
    normalized = dict(trade or {})
    ts = normalized.get('timestamp')
    normalized['timestamp'] = _serialize_dt(ts)
    profit_value = _safe_float(normalized.get('profit'), 0.0)
    meta = _trade_result_meta(profit_value)
    normalized['profit'] = profit_value
    normalized['result_label'] = meta['label']
    normalized['result_tone'] = {'success': 'success', 'danger': 'danger', 'neutral': 'neutral'}.get(meta['tone'], 'neutral')
    return normalized


def _build_operation_summary(trades: list[dict]) -> dict:
    if not trades:
        return {
            'wins': 0,
            'losses': 0,
            'net_visible': 0.0,
            'best_trade_pnl': 0.0,
            'worst_trade_pnl': 0.0,
        }

    profits = [_safe_float(item.get('profit'), 0.0) for item in trades]
    wins = sum(1 for value in profits if value > 0)
    losses = sum(1 for value in profits if value < 0)
    net_visible = round(sum(profits), 4)
    return {
        'wins': wins,
        'losses': losses,
        'net_visible': net_visible,
        'best_trade_pnl': round(max(profits), 4),
        'worst_trade_pnl': round(min(profits), 4),
    }


def _build_last_operation_summary(payload: Any, fallback_title: str, empty_detail: str) -> dict | None:
    if not isinstance(payload, dict) or not payload:
        return None
    title = payload.get('symbol') or payload.get('coin') or fallback_title
    side = str(payload.get('side') or payload.get('direction') or '').upper()
    detail_parts = []
    if payload.get('entry_price') is not None:
        detail_parts.append(f"Entrada {payload.get('entry_price')}")
    if payload.get('exit_price') is not None:
        detail_parts.append(f"Salida {payload.get('exit_price')}")
    if payload.get('qty') is not None:
        detail_parts.append(f"Qty {payload.get('qty')}")
    if payload.get('profit') is not None:
        detail_parts.append(f"PnL {round(_safe_float(payload.get('profit'), 0.0), 4)}")
    if payload.get('message'):
        detail_parts.append(str(payload.get('message')))
    return {
        'title': f"{title} · {side}" if side else str(title),
        'detail': ' · '.join(detail_parts) if detail_parts else empty_detail,
    }


def _activity_family(event_type: str | None) -> str:
    normalized = str(event_type or '').strip().lower()
    if normalized in {'trade_opened', 'trade_closed'}:
        return 'trading'
    if normalized in {'wallet_updated', 'private_key_updated', 'terms_accepted', 'access_updated', 'private_key_hardened'}:
        return 'account'
    if normalized in {'trading_activated', 'trading_paused', 'stats_reset'}:
        return 'control'
    return 'info'


def _activity_badge(event_type: str | None, tone: str | None) -> str:
    normalized = str(event_type or '').strip().lower()
    if normalized == 'trade_opened':
        return 'OPEN'
    if normalized == 'trade_closed':
        return 'CLOSE'
    if normalized == 'trading_activated':
        return 'ON'
    if normalized == 'trading_paused':
        return 'PAUSE'
    if normalized == 'wallet_updated':
        return 'WALLET'
    if normalized == 'private_key_updated':
        return 'KEY'
    if normalized == 'terms_accepted':
        return 'TERMS'
    if normalized == 'access_updated':
        return 'PLAN'
    if normalized == 'private_key_hardened':
        return 'HARDEN'
    if normalized == 'stats_reset':
        return 'RESET'
    tone_normalized = str(tone or '').lower()
    if tone_normalized == 'success':
        return 'OK'
    if tone_normalized == 'danger':
        return 'ALERT'
    return 'INFO'


def _serialize_activity_rows(rows: list[dict]) -> list[dict]:
    serialized = []
    for row in rows or []:
        item = dict(row or {})
        event_type = item.get('event_type') or 'info'
        tone = item.get('tone') or 'info'
        item['at'] = _serialize_dt(item.get('created_at'))
        serialized.append({
            'title': item.get('title') or 'Actividad',
            'detail': item.get('detail') or 'Sin detalle adicional.',
            'tone': tone,
            'event_type': event_type,
            'family': _activity_family(event_type),
            'badge': _activity_badge(event_type, tone),
            'at': item.get('at'),
        })
    return serialized


def _build_active_trade_summary(payload: Any) -> dict | None:
    if not isinstance(payload, dict) or not payload:
        return None

    symbol = payload.get('symbol') or payload.get('coin') or 'Operación activa'
    side = str(payload.get('side') or payload.get('direction') or '').upper()
    entry = payload.get('entry_price')
    last_price = payload.get('last_price')
    pnl = payload.get('last_pnl_pct')
    qty = payload.get('qty_coin_for_log') or payload.get('qty') or payload.get('size')
    detail_parts = []
    if entry is not None:
        detail_parts.append(f'Entrada {entry}')
    if last_price is not None:
        detail_parts.append(f'Último precio {last_price}')
    if qty is not None:
        detail_parts.append(f'Qty {qty}')
    if pnl is not None:
        detail_parts.append(f'PnL vivo {round(_safe_float(pnl, 0.0), 4)}%')
    return {
        'title': f"{symbol} · {side}" if side else str(symbol),
        'detail': ' · '.join(detail_parts) if detail_parts else 'Hay una operación activa registrada en este momento.',
        'started_at': payload.get('started_at') or payload.get('persisted_at'),
        'symbol': symbol,
        'side': side,
    }


def _build_timeline_summary(activity: list[dict], trades: list[dict], active_trade: dict | None) -> dict:
    trading_events = sum(1 for item in activity if item.get('family') == 'trading')
    account_events = sum(1 for item in activity if item.get('family') == 'account')
    control_events = sum(1 for item in activity if item.get('family') == 'control')
    total_visible = len(activity)
    return {
        'total_visible_events': total_visible,
        'trading_events': trading_events,
        'account_events': account_events,
        'control_events': control_events,
        'recent_trades_visible': len(trades),
        'live_trade': bool(active_trade),
    }


def get_recent_operations(user_id: int, limit: int = 20) -> dict:
    last_operation = get_last_operation(int(user_id)) or {}
    active_trade = get_user_active_trade_snapshot(int(user_id)) or {}
    trades = get_user_trades_limited(int(user_id), limit=limit)
    normalized_trades = [_normalize_trade_row(trade) for trade in trades]
    activity = _serialize_activity_rows(get_user_activity(int(user_id), limit=max(min(limit, 20), 12)))
    active_trade_summary = _build_active_trade_summary(active_trade)

    return {
        'last_open': last_operation.get('last_open'),
        'last_close': last_operation.get('last_close'),
        'active_trade': active_trade if active_trade_summary else None,
        'active_trade_summary': active_trade_summary,
        'last_open_summary': _build_last_operation_summary(last_operation.get('last_open'), 'Última apertura', 'Sin aperturas registradas todavía.'),
        'last_close_summary': _build_last_operation_summary(last_operation.get('last_close'), 'Último cierre', 'Sin cierres registrados todavía.'),
        'activity': activity,
        'timeline_summary': _build_timeline_summary(activity, normalized_trades, active_trade_summary),
        'summary': _build_operation_summary(normalized_trades),
        'trades': normalized_trades,
        'count': len(normalized_trades),
    }


def get_performance_summary(user_id: int) -> dict:
    stats_24h = _normalize_trade_stats(get_user_trade_stats(int(user_id), 24) or {})
    stats_7d = _normalize_trade_stats(get_user_trade_stats(int(user_id), 24 * 7) or {})
    stats_30d = _normalize_trade_stats(get_user_trade_stats(int(user_id), 24 * 30) or {})

    windows = {'24h': stats_24h, '7d': stats_7d, '30d': stats_30d}
    candidates = [(label, payload) for label, payload in windows.items() if int(payload.get('total', 0) or 0) > 0]
    best_window = max(candidates, key=lambda item: _safe_float(item[1].get('pnl_total'), 0.0), default=None)

    edge_tone = 'warning'
    edge_label = 'Sin suficiente muestra'
    edge_detail = 'Todavía no hay suficientes cierres para leer una ventaja estable.'
    if stats_7d.get('total', 0) >= 3:
        pf = _safe_float(stats_7d.get('profit_factor'), 0.0)
        wr = _safe_float(stats_7d.get('win_rate'), 0.0)
        if pf >= 1.3 and wr >= 55:
            edge_tone = 'success'
            edge_label = 'Ventaja favorable'
            edge_detail = 'La ventana de 7d mantiene profit factor y win rate aceptables.'
        elif pf >= 1.0 and wr >= 45:
            edge_tone = 'warning'
            edge_label = 'Ventaja mixta'
            edge_detail = 'Hay señal operativa, pero todavía no es una lectura contundente.'
        else:
            edge_tone = 'danger'
            edge_label = 'Requiere ajuste'
            edge_detail = 'La ventana de 7d todavía no sostiene una ventaja operativa clara.'

    return {
        '24h': stats_24h,
        '7d': stats_7d,
        '30d': stats_30d,
        'executive': {
            'best_window': best_window[0] if best_window else None,
            'best_window_pnl': _safe_float(best_window[1].get('pnl_total'), 0.0) if best_window else 0.0,
            'edge_label': edge_label,
            'edge_tone': edge_tone,
            'edge_detail': edge_detail,
            'trades_30d': _safe_int(stats_30d.get('total'), 0),
            'decisive_30d': _safe_int(stats_30d.get('decisive_trades'), 0),
            'cadence_30d': round(_safe_int(stats_30d.get('total'), 0) / 30.0, 2),
        },
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


def get_system_runtime_summary(user_id: int, include_private_details: bool = False) -> dict:
    profile = get_user_profile(int(user_id))
    snapshot = get_system_runtime_snapshot() or {}
    runtime = dict((snapshot.get('runtime') or {}))

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

    latest_manager = runtime.get('latest_trade_manager') or {}
    latest_manager = dict(latest_manager) if latest_manager else None
    if latest_manager and latest_manager.get('manager_heartbeat_at'):
        latest_manager['manager_heartbeat_at'] = _serialize_dt(latest_manager.get('manager_heartbeat_at'))

    latest_open = _serialize_activity(runtime.get('latest_open'))
    latest_close = _serialize_activity(runtime.get('latest_close'))

    if latest_close:
        recent_activity_label = 'Cierres recientes'
        recent_activity_detail = 'Se registró actividad reciente de cierre en la plataforma.'
    elif latest_open:
        recent_activity_label = 'Actividad de trading'
        recent_activity_detail = 'Se registró actividad reciente de apertura en la plataforma.'
    elif runtime.get('scanner_last_event'):
        recent_activity_label = 'Mercado monitoreado'
        recent_activity_detail = 'La plataforma sigue procesando actividad de mercado.'
    else:
        recent_activity_label = 'Sin actividad reciente'
        recent_activity_detail = 'Todavía no hay eventos recientes para mostrar.'

    overall_status = snapshot.get('overall_status') or 'unknown'
    public_summary = {
        'last_update_at': _serialize_dt(snapshot.get('checked_at')),
        'connection_label': 'Sincronizada',
        'active_trades': int(runtime.get('active_trades', 0) or 0),
        'recent_activity_label': recent_activity_label,
        'recent_activity_detail': recent_activity_detail,
        'execution_label': 'Operativa' if overall_status == 'healthy' else ('Atención' if overall_status in {'warning', 'stale'} else 'Incidencia'),
        'execution_detail': 'La lectura mostrada aquí es una vista resumida para el usuario.',
        'plan_notice': 'Tu acceso vigente y configuración se muestran en esta sesión.' if profile.get('plan_active') else 'Tu acceso depende del plan activo y la configuración de tu cuenta.',
    }

    payload = {
        'viewer_user_id': int(profile['user_id']),
        'viewer_plan': profile.get('plan'),
        'overall_status': overall_status,
        'checked_at': _serialize_dt(snapshot.get('checked_at')),
        'public_summary': public_summary,
    }

    if not include_private_details:
        return payload

    payload.update({
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
            **runtime,
            'latest_open': latest_open,
            'latest_close': latest_close,
            'latest_trade_manager': latest_manager,
        },
        'issues': snapshot.get('issues') or [],
    })
    return payload
