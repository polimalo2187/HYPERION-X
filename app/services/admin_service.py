from __future__ import annotations

from fastapi import HTTPException, status

from app.database import (
    activate_premium_plan,
    grant_manual_plan_days,
    get_manual_plan_days_preview,
    admin_set_user_trading_status,
    ensure_access_on_activate,
    get_admin_trade_stats,
    get_admin_user_snapshot,
    get_admin_visual_stats,
    get_security_overview,
    get_user_trade_stats,
    migrate_legacy_private_keys,
    migrate_user_private_key_to_encrypted,
    reset_user_trade_stats_epoch,
    search_users_for_admin,
)


def _with_user_performance(detail: dict) -> dict:
    enriched = dict(detail)
    user_id = int(detail['user_id'])
    enriched['performance'] = {
        '24h': get_user_trade_stats(user_id, 24) or {},
        '7d': get_user_trade_stats(user_id, 24 * 7) or {},
        '30d': get_user_trade_stats(user_id, 24 * 30) or {},
    }
    return enriched


def get_admin_overview() -> dict:
    visual = get_admin_visual_stats() or {}
    trade_stats = get_admin_trade_stats(720) or {}
    security = get_security_overview() or {}
    return {
        'visual': visual,
        'trade_stats_30d': trade_stats,
        'security': security,
    }


def admin_search_users(query: str, limit: int = 10) -> dict:
    items = search_users_for_admin(query=query, limit=limit)
    return {
        'query': (query or '').strip(),
        'count': len(items),
        'items': items,
    }


def admin_get_user_detail(user_id: int) -> dict:
    detail = get_admin_user_snapshot(int(user_id))
    if not detail:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail='Usuario no encontrado')
    return _with_user_performance(detail)


def admin_activate_premium(user_id: int) -> dict:
    if not activate_premium_plan(int(user_id)):
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail='No se pudo activar premium para este usuario')
    detail = admin_get_user_detail(int(user_id))
    return {
        'result': 'premium_activated',
        'message': 'Premium activado manualmente',
        'user': detail,
    }


def admin_preview_manual_plan_days(user_id: int, plan: str, days: int) -> dict:
    if int(days) <= 0:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail='La cantidad de días debe ser mayor que cero')
    outcome = get_manual_plan_days_preview(int(user_id), str(plan or 'premium'), int(days))
    if not outcome.get('ok'):
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=outcome.get('message') or 'No se pudo calcular la previsualización')
    detail = admin_get_user_detail(int(user_id))
    return {
        'result': 'manual_plan_days_preview',
        'message': 'Previsualización calculada',
        'user': detail,
        'preview': outcome,
    }


def admin_grant_manual_plan_days(user_id: int, plan: str, days: int) -> dict:
    if int(days) <= 0:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail='La cantidad de días debe ser mayor que cero')
    outcome = grant_manual_plan_days(int(user_id), str(plan or 'premium'), int(days))
    if not outcome.get('ok'):
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=outcome.get('message') or 'No se pudo aplicar la extensión manual')
    detail = admin_get_user_detail(int(user_id))
    return {
        'result': 'manual_plan_days_granted',
        'message': outcome.get('message') or f'Plan actualizado manualmente por {int(days)} días',
        'user': detail,
        'plan': outcome.get('target_plan') or str(plan or 'premium'),
        'days': int(days),
    }


def admin_preview_manual_premium_days(user_id: int, days: int) -> dict:
    return admin_preview_manual_plan_days(user_id, 'premium', days)


def admin_grant_manual_premium_days(user_id: int, days: int) -> dict:
    return admin_grant_manual_plan_days(user_id, 'premium', days)


def admin_pause_user_trading(user_id: int) -> dict:
    if not admin_set_user_trading_status(int(user_id), 'inactive'):
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail='No se pudo pausar el trading del usuario')
    detail = admin_get_user_detail(int(user_id))
    return {
        'result': 'trading_paused',
        'message': 'Trading pausado por admin',
        'user': detail,
    }


def admin_activate_user_trading(user_id: int) -> dict:
    access = ensure_access_on_activate(int(user_id))
    if not access.get('allowed', False):
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=access.get('message') or 'El usuario no tiene acceso vigente para activar trading')
    if not admin_set_user_trading_status(int(user_id), 'active'):
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail='No se pudo activar el trading del usuario')
    detail = admin_get_user_detail(int(user_id))
    return {
        'result': 'trading_activated',
        'message': access.get('plan_message') or 'Trading activado por admin',
        'user': detail,
    }


def admin_reset_user_stats(user_id: int) -> dict:
    if not reset_user_trade_stats_epoch(int(user_id)):
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail='No se pudo resetear el rendimiento del usuario')
    detail = admin_get_user_detail(int(user_id))
    return {
        'result': 'stats_reset',
        'message': 'Rendimiento del usuario reseteado desde este momento',
        'user': detail,
    }


def admin_migrate_user_private_key(user_id: int) -> dict:
    outcome = migrate_user_private_key_to_encrypted(int(user_id))
    result = outcome.get('result')
    if result == 'user_not_found':
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail='Usuario no encontrado')
    if result == 'not_configured':
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail='El usuario no tiene private key configurada')
    detail = admin_get_user_detail(int(user_id))
    messages = {
        'already_encrypted': 'La private key del usuario ya estaba cifrada',
        'migrated': 'Private key legacy migrada a cifrado',
    }
    return {
        'result': result,
        'message': messages.get(result, 'Operación completada'),
        'user': detail,
    }


def admin_bulk_migrate_legacy_keys(limit: int = 25) -> dict:
    outcome = migrate_legacy_private_keys(limit=limit)
    outcome['result'] = 'bulk_migration_completed'
    outcome['message'] = f"Migración completada. {outcome.get('migrated_count', 0)} keys actualizadas."
    return outcome
