# ============================================================
# DATABASE – TRADING X HIPER PRO
# Sistema profesional MongoDB Atlas
# PRODUCCIÓN REAL – BLINDADO
# + INTERÉS COMPUESTO (capital Telegram se actualiza automático)
#
# PLANES (SIN FEES / SIN REFERIDOS):
#   - PRUEBA 5 días (vence a medianoche Cuba)
#   - PREMIUM 30 días (vence a medianoche Cuba)
# ============================================================

from datetime import datetime, timedelta
from pymongo import MongoClient
import os
import re
import sys
import pytz

from app.crypto_utils import decrypt_private_key, encrypt_private_key

# ============================================================
# CONEXIÓN MONGODB
# ============================================================

MONGO_URI = os.getenv("MONGO_URL")
DB_NAME = os.getenv("MONGO_DB_NAME", "TRADING_X_HIPER_PRO")

client = MongoClient(MONGO_URI)
db = client[DB_NAME]

users_col = db["users"]
trades_col = db["trades"]
settings_col = db["settings"]
admin_action_logs_col = db["admin_action_logs"]
runtime_status_col = db["runtime_status"]
user_activity_col = db["user_activity"]

# ============================================================
# ZONA HORARIA (CUBA) – vencimientos por medianoche
# ============================================================

CUBA_TZ = pytz.timezone("America/Havana")

def _now_utc() -> datetime:
    return datetime.utcnow()

def _now_cuba() -> datetime:
    return datetime.now(CUBA_TZ)

def _midnight_cuba_after_days(days: int) -> datetime:
    """
    Retorna la medianoche (00:00) de Cuba para (hoy + days), convertida a UTC naive.
    Ej:
      si hoy es 2026-02-04 (Cuba), days=5 => 2026-02-09 00:00 (Cuba).
    """
    now_cuba = _now_cuba()
    target_date = now_cuba.date() + timedelta(days=int(days))
    midnight_local = CUBA_TZ.localize(datetime(target_date.year, target_date.month, target_date.day, 0, 0, 0))
    # Guardamos en UTC (naive) para consistencia
    return midnight_local.astimezone(pytz.UTC).replace(tzinfo=None)

def _parse_dt(x):
    if not x:
        return None
    if isinstance(x, datetime):
        return x
    try:
        return datetime.fromisoformat(str(x))
    except Exception:
        return None

# ============================================================
# LOG EN VIVO (SERVIDOR)
# ============================================================

def db_log(msg: str):
    ts = _now_utc().isoformat()
    print(f"[DB {ts}] {msg}", file=sys.stdout, flush=True)

# ============================================================
# UTILIDADES (blindaje)
# ============================================================



def _sanitize_admin_reason(reason: str | None) -> str | None:
    value = str(reason or '').strip()
    if not value:
        return None
    return value[:300]


def _serialize_runtime_dt(value: datetime | None) -> str | None:
    if isinstance(value, datetime):
        return value.isoformat()
    return None


def touch_runtime_component(component: str, state: str = 'online', *, metadata: dict | None = None) -> bool:
    name = str(component or '').strip().lower()
    if not name:
        return False
    now = _now_utc()
    payload = {
        'component': name,
        'state': str(state or 'online').strip().lower() or 'online',
        'last_seen_at': now,
        'updated_at': now,
        'metadata': metadata or {},
    }
    try:
        runtime_status_col.update_one(
            {'component': name},
            {'$set': payload, '$setOnInsert': {'created_at': now}},
            upsert=True,
        )
        return True
    except Exception as e:
        try:
            db_log(f"⚠ touch_runtime_component error component={name}: {e}")
        except Exception:
            pass
        return False


def get_runtime_component(component: str) -> dict | None:
    name = str(component or '').strip().lower()
    if not name:
        return None
    doc = runtime_status_col.find_one({'component': name}, {'_id': 0})
    if not doc:
        return None
    last_seen = _parse_dt(doc.get('last_seen_at'))
    updated_at = _parse_dt(doc.get('updated_at'))
    return {
        'component': doc.get('component') or name,
        'state': doc.get('state') or 'unknown',
        'last_seen_at': last_seen,
        'updated_at': updated_at,
        'metadata': doc.get('metadata') or {},
        'created_at': _parse_dt(doc.get('created_at')),
    }


def _runtime_component_snapshot(component: str, *, warning_after_seconds: int, critical_after_seconds: int) -> dict:
    now = _now_utc()
    doc = get_runtime_component(component)
    if not doc:
        return {
            'component': component,
            'status': 'offline',
            'last_seen_at': None,
            'freshness_seconds': None,
            'state': 'missing',
            'metadata': {},
            'message': 'Sin heartbeat registrado todavía.',
        }

    last_seen = doc.get('last_seen_at')
    freshness = None
    if isinstance(last_seen, datetime):
        freshness = max(0, int((now - last_seen).total_seconds()))

    explicit_state = str(doc.get('state') or 'online').lower()
    if explicit_state in {'error', 'failed'}:
        status = 'error'
        message = 'El componente reportó error en el último heartbeat.'
    elif freshness is None:
        status = 'offline'
        message = 'Heartbeat inválido o sin timestamp.'
    elif freshness <= int(warning_after_seconds):
        status = 'online'
        message = 'Heartbeat reciente.'
    elif freshness <= int(critical_after_seconds):
        status = 'stale'
        message = 'Heartbeat atrasado; revisar proceso.'
    else:
        status = 'offline'
        message = 'Heartbeat demasiado antiguo o proceso detenido.'

    return {
        'component': component,
        'status': status,
        'last_seen_at': last_seen,
        'freshness_seconds': freshness,
        'state': explicit_state,
        'metadata': doc.get('metadata') or {},
        'message': message,
    }


def get_system_runtime_snapshot() -> dict:
    now = _now_utc()
    components = {
        'telegram_bot': _runtime_component_snapshot('telegram_bot', warning_after_seconds=90, critical_after_seconds=300),
        'trading_loop': _runtime_component_snapshot('trading_loop', warning_after_seconds=90, critical_after_seconds=300),
        'scanner': _runtime_component_snapshot('scanner', warning_after_seconds=120, critical_after_seconds=360),
    }

    plan_active_query = {
        'plan': {'$in': ['trial', 'premium']},
        'plan_expires_at': {'$gt': now},
    }
    users_with_active_plan = users_col.count_documents(plan_active_query)
    users_trading_active = users_col.count_documents({'trading_status': 'active'})

    latest_open_doc = users_col.find_one(
        {'last_open_at': {'$ne': None}},
        {'_id': 0, 'user_id': 1, 'username': 1, 'last_open_at': 1, 'last_open': 1},
        sort=[('last_open_at', -1)],
    )
    latest_close_doc = users_col.find_one(
        {'last_close_at': {'$ne': None}},
        {'_id': 0, 'user_id': 1, 'username': 1, 'last_close_at': 1, 'last_close': 1},
        sort=[('last_close_at', -1)],
    )

    active_trades_collection_name = os.getenv('ACTIVE_TRADES_COLLECTION', 'active_trades')
    active_trades_collection = db[active_trades_collection_name]
    try:
        active_trades_count = int(active_trades_collection.count_documents({}))
    except Exception:
        active_trades_count = 0

    latest_trade_manager = None
    try:
        latest_active_trade = active_trades_collection.find_one(
            {},
            {'_id': 0, 'user_id': 1, 'symbol': 1, 'direction': 1, 'manager_heartbeat_ts': 1},
            sort=[('manager_heartbeat_ts', -1)],
        )
        if latest_active_trade:
            ts = latest_active_trade.get('manager_heartbeat_ts')
            ts_dt = datetime.utcfromtimestamp(float(ts)) if ts else None
            latest_trade_manager = {
                'user_id': latest_active_trade.get('user_id'),
                'symbol': latest_active_trade.get('symbol'),
                'direction': latest_active_trade.get('direction'),
                'manager_heartbeat_at': ts_dt,
            }
    except Exception as e:
        latest_trade_manager = {'error': str(e)}

    scanner_meta = components['scanner'].get('metadata') or {}
    issues = []
    for name, snapshot in components.items():
        if snapshot.get('status') not in {'online'}:
            issues.append({
                'component': name,
                'status': snapshot.get('status'),
                'message': snapshot.get('message'),
            })

    statuses = [components['telegram_bot']['status'], components['trading_loop']['status'], components['scanner']['status']]
    if 'error' in statuses or 'offline' in statuses:
        overall_status = 'degraded'
    elif 'stale' in statuses:
        overall_status = 'warning'
    else:
        overall_status = 'healthy'

    def _activity_payload(doc: dict | None, key: str, ts_key: str) -> dict | None:
        if not doc:
            return None
        payload = doc.get(key) or {}
        return {
            'user_id': doc.get('user_id'),
            'username': doc.get('username'),
            'at': _parse_dt(doc.get(ts_key)),
            'symbol': payload.get('symbol') if isinstance(payload, dict) else None,
            'event': payload.get('message') if isinstance(payload, dict) else None,
            'payload': payload if isinstance(payload, dict) else None,
        }

    return {
        'overall_status': overall_status,
        'checked_at': now,
        'components': components,
        'runtime': {
            'users_with_active_plan': int(users_with_active_plan),
            'users_trading_active': int(users_trading_active),
            'active_trades': int(active_trades_count),
            'latest_trade_manager': latest_trade_manager,
            'latest_open': _activity_payload(latest_open_doc, 'last_open', 'last_open_at'),
            'latest_close': _activity_payload(latest_close_doc, 'last_close', 'last_close_at'),
            'scanner_last_event': scanner_meta.get('last_event'),
            'scanner_last_symbol': scanner_meta.get('symbol'),
            'scanner_last_user_id': scanner_meta.get('user_id'),
        },
        'issues': issues,
    }


def log_admin_action(
    actor_user_id: int,
    actor_username: str | None,
    action: str,
    *,
    target_user_id: int | None = None,
    target_username: str | None = None,
    reason: str | None = None,
    status: str = 'success',
    message: str | None = None,
    metadata: dict | None = None,
) -> bool:
    try:
        doc = {
            'created_at': _now_utc(),
            'actor_user_id': int(actor_user_id),
            'actor_username': str(actor_username or '').strip() or None,
            'action': str(action or '').strip() or 'unknown',
            'target_user_id': int(target_user_id) if target_user_id is not None else None,
            'target_username': str(target_username or '').strip() or None,
            'reason': _sanitize_admin_reason(reason),
            'status': str(status or 'success').strip() or 'success',
            'message': str(message or '').strip() or None,
            'metadata': metadata or {},
        }
        admin_action_logs_col.insert_one(doc)
        return True
    except Exception as e:
        db_log(f"⚠ log_admin_action error actor={actor_user_id} action={action}: {e}")
        return False


def get_admin_action_history(limit: int = 20, target_user_id: int | None = None) -> list[dict]:
    lim = max(1, min(int(limit), 100))
    query = {}
    if target_user_id is not None:
        query['target_user_id'] = int(target_user_id)

    cursor = admin_action_logs_col.find(query, {'_id': 0}).sort('created_at', -1).limit(lim)
    results = []
    for row in cursor:
        created_at = _parse_dt(row.get('created_at'))
        results.append({
            'created_at': created_at,
            'actor_user_id': row.get('actor_user_id'),
            'actor_username': row.get('actor_username'),
            'action': row.get('action') or 'unknown',
            'target_user_id': row.get('target_user_id'),
            'target_username': row.get('target_username'),
            'reason': row.get('reason'),
            'status': row.get('status') or 'success',
            'message': row.get('message'),
            'metadata': row.get('metadata') or {},
        })
    return results



def log_user_activity(
    user_id: int,
    title: str,
    detail: str | None = None,
    *,
    tone: str = 'info',
    event_type: str = 'info',
    metadata: dict | None = None,
    occurred_at: datetime | None = None,
) -> bool:
    try:
        uid = int(user_id)
        title_value = str(title or '').strip()[:120]
        if not title_value:
            return False
        detail_value = str(detail or '').strip()[:500] or None
        event_value = str(event_type or 'info').strip().lower()[:40] or 'info'
        tone_value = str(tone or 'info').strip().lower()[:20] or 'info'
        now = occurred_at if isinstance(occurred_at, datetime) else _now_utc()
        user_doc = users_col.find_one({'user_id': uid}, {'_id': 0, 'username': 1}) or {}
        user_activity_col.insert_one({
            'user_id': uid,
            'username': user_doc.get('username'),
            'title': title_value,
            'detail': detail_value,
            'tone': tone_value,
            'event_type': event_value,
            'metadata': metadata or {},
            'created_at': now,
        })
        return True
    except Exception as e:
        try:
            db_log(f"⚠ log_user_activity error user_id={user_id}: {e}")
        except Exception:
            pass
        return False


def get_user_activity(user_id: int, limit: int = 12):
    try:
        uid = int(user_id)
        lim = max(1, min(int(limit), 50))
    except Exception:
        return []

    try:
        return list(
            user_activity_col.find(
                {'user_id': uid},
                {'_id': 0},
            )
            .sort('created_at', -1)
            .limit(lim)
        )
    except Exception as e:
        try:
            db_log(f"⚠ get_user_activity error user_id={user_id}: {e}")
        except Exception:
            pass
        return []


def get_user_active_trade_snapshot(user_id: int) -> dict | None:
    try:
        uid = int(user_id)
    except Exception:
        return None

    try:
        active_trades_collection_name = os.getenv('ACTIVE_TRADES_COLLECTION', 'active_trades')
        active_trades_collection = db[active_trades_collection_name]
        doc = active_trades_collection.find_one({'user_id': uid})
        if not isinstance(doc, dict):
            return None
        doc.pop('_id', None)
        return doc
    except Exception as e:
        try:
            db_log(f"⚠ get_user_active_trade_snapshot error user_id={user_id}: {e}")
        except Exception:
            pass
        return None


def _safe_float(x, default: float = 0.0) -> float:
    try:
        if x is None:
            return float(default)
        return float(x)
    except Exception:
        return float(default)

def _clamp_non_negative(x: float) -> float:
    try:
        return x if x >= 0 else 0.0
    except Exception:
        return 0.0

# ============================================================
# USUARIOS
# ============================================================

def create_user(user_id: int, username: str):
    if not users_col.find_one({"user_id": int(user_id)}):
        users_col.insert_one({
            "user_id": int(user_id),
            "username": username,
            "wallet": None,
            "private_key": None,
            "private_key_encrypted": False,
            "private_key_version": None,
            "private_key_updated_at": None,
            "capital": 0.0,
            "trading_status": "inactive",

            # ✅ Planes (trial/premium)
            "plan": "none",                 # none | trial | premium
            "plan_expires_at": None,        # datetime UTC naive
            "trial_used": False,            # trial una sola vez
            "expiry_notified_on": None,     # YYYY-MM-DD (Cuba) para evitar spam

            # ✅ Referidos (solo conteo de válidos)
            "referrer": None,                # user_id del referidor
            "referral_valid_count": 0,       # contador en el referidor
            "referral_counted": False,       # marca si este usuario ya contó como válido
            "terms_accepted": False,
            "terms_timestamp": None,
        })
        db_log(f"👤 Usuario creado {user_id}")

def is_user_registered(user_id: int) -> bool:
    return users_col.find_one({"user_id": int(user_id)}, {"_id": 1}) is not None

def get_all_users():
    return list(users_col.find({}, {"_id": 0, "user_id": 1}))

def save_user_wallet(user_id: int, wallet: str):
    users_col.update_one({"user_id": int(user_id)}, {"$set": {"wallet": wallet}})
    log_user_activity(int(user_id), 'Wallet actualizada', 'La wallet operativa fue actualizada desde la plataforma.', tone='info', event_type='wallet_updated', metadata={'wallet_masked': str(wallet)[:8] + '...' + str(wallet)[-6:] if wallet else None})

def save_user_private_key(user_id: int, pk: str):
    encrypted_pk, version = encrypt_private_key(pk)
    users_col.update_one(
        {"user_id": int(user_id)},
        {
            "$set": {
                "private_key": encrypted_pk,
                "private_key_encrypted": True,
                "private_key_version": version,
                "private_key_updated_at": datetime.utcnow(),
            }
        },
    )
    log_user_activity(int(user_id), 'Clave operativa actualizada', 'La private key quedó registrada de forma segura.', tone='info', event_type='private_key_updated')

def get_raw_user_private_key_record(user_id: int) -> dict | None:
    return users_col.find_one(
        {"user_id": int(user_id)},
        {
            "_id": 0,
            "user_id": 1,
            "private_key": 1,
            "private_key_encrypted": 1,
            "private_key_version": 1,
            "private_key_updated_at": 1,
        },
    )


def migrate_user_private_key_to_encrypted(user_id: int) -> dict:
    doc = get_raw_user_private_key_record(int(user_id))
    if not doc:
        return {"result": "user_not_found", "changed": False}

    current_value = doc.get("private_key")
    if not current_value:
        return {"result": "not_configured", "changed": False}

    if bool(doc.get("private_key_encrypted", False)):
        return {
            "result": "already_encrypted",
            "changed": False,
            "version": doc.get("private_key_version"),
        }

    save_user_private_key(int(user_id), str(current_value))
    return {"result": "migrated", "changed": True, "version": "fernet-v1"}


def migrate_legacy_private_keys(limit: int = 25) -> dict:
    lim = max(1, min(int(limit), 100))
    cursor = users_col.find(
        {
            "private_key": {"$ne": None},
            "$or": [
                {"private_key_encrypted": {"$exists": False}},
                {"private_key_encrypted": False},
            ],
        },
        {"_id": 0, "user_id": 1},
    ).sort("user_id", 1).limit(lim)

    migrated_user_ids = []
    skipped_user_ids = []
    for row in cursor:
        uid = int(row.get("user_id"))
        outcome = migrate_user_private_key_to_encrypted(uid)
        if outcome.get("changed"):
            migrated_user_ids.append(uid)
        else:
            skipped_user_ids.append(uid)

    remaining_legacy = users_col.count_documents({
        "private_key": {"$ne": None},
        "$or": [
            {"private_key_encrypted": {"$exists": False}},
            {"private_key_encrypted": False},
        ],
    })

    return {
        "requested_limit": lim,
        "migrated_count": len(migrated_user_ids),
        "migrated_user_ids": migrated_user_ids,
        "skipped_user_ids": skipped_user_ids,
        "remaining_legacy_plaintext_keys": int(remaining_legacy),
    }

def save_user_capital(user_id: int, capital: float):
    capital = _clamp_non_negative(_safe_float(capital, 0.0))
    users_col.update_one({"user_id": int(user_id)}, {"$set": {"capital": capital}})

def set_trading_status(user_id: int, status: str):
    normalized = str(status or '').strip().lower() or 'inactive'
    users_col.update_one({"user_id": int(user_id)}, {"$set": {"trading_status": normalized}})
    if normalized == 'active':
        log_user_activity(int(user_id), 'Trading activado', 'El motor quedó habilitado para operar con tu configuración actual.', tone='success', event_type='trading_activated')
    else:
        log_user_activity(int(user_id), 'Trading pausado', 'El motor quedó pausado para esta cuenta.', tone='warning', event_type='trading_paused')


# ============================================================
# REFERIDOS (SOLO CONTEO DE VÁLIDOS)
# ============================================================

def set_referrer(user_id: int, referrer_id: int):
    """
    Asigna el referidor una sola vez (si no existe ya).
    No permite auto-referido.
    """
    try:
        user_id = int(user_id)
        referrer_id = int(referrer_id)
        if user_id == referrer_id:
            return

        users_col.update_one(
            {"user_id": user_id, "$or": [{"referrer": None}, {"referrer": {"$exists": False}}]},
            {"$set": {"referrer": referrer_id}}
        )
    except Exception as e:
        db_log(f"❌ Error set_referrer user={user_id}: {e}")

def get_user_referrer(user_id: int):
    u = users_col.find_one({"user_id": int(user_id)}, {"_id": 0, "referrer": 1})
    return (u or {}).get("referrer")

def get_referral_valid_count(referrer_id: int) -> int:
    u = users_col.find_one({"user_id": int(referrer_id)}, {"_id": 0, "referral_valid_count": 1})
    try:
        return int((u or {}).get("referral_valid_count", 0) or 0)
    except Exception:
        return 0

def _mark_referral_valid(target_user_id: int):
    """
    Cuando un usuario activa Premium por primera vez:
    - si tiene referrer y aún no fue contado, incrementa referral_valid_count en el referidor
    - marca referral_counted=True en el usuario
    """
    try:
        target_user_id = int(target_user_id)
        u = users_col.find_one(
            {"user_id": target_user_id},
            {"_id": 0, "referrer": 1, "referral_counted": 1}
        )
        if not u:
            return

        if bool(u.get("referral_counted", False)):
            return

        referrer_id = u.get("referrer")
        if not referrer_id:
            # igual marcamos counted para no reintentar en el futuro
            users_col.update_one({"user_id": target_user_id}, {"$set": {"referral_counted": True}})
            return

        # 1) marcar counted solo si aún es False (evita doble conteo)
        res = users_col.update_one(
            {"user_id": target_user_id, "$or": [{"referral_counted": False}, {"referral_counted": {"$exists": False}}]},
            {"$set": {"referral_counted": True}}
        )
        if res.modified_count != 1:
            return

        # 2) incrementar contador en el referidor
        users_col.update_one(
            {"user_id": int(referrer_id)},
            {"$inc": {"referral_valid_count": 1}}
        )
        db_log(f"👥 Referido válido contado: referrer={referrer_id} user={target_user_id}")

    except Exception as e:
        db_log(f"❌ Error _mark_referral_valid user={target_user_id}: {e}")


def get_user_wallet(user_id: int):
    u = users_col.find_one({"user_id": int(user_id)})
    return u.get("wallet") if u else None

def get_user_private_key(user_id: int):
    u = users_col.find_one(
        {"user_id": int(user_id)},
        {"_id": 0, "private_key": 1, "private_key_encrypted": 1, "private_key_version": 1},
    )
    if not u:
        return None
    value = u.get("private_key")
    if not value:
        return None
    return decrypt_private_key(
        value,
        encrypted=bool(u.get("private_key_encrypted", False)),
        version=u.get("private_key_version"),
    )

def get_user_capital(user_id: int):
    u = users_col.find_one({"user_id": int(user_id)})
    return _safe_float(u.get("capital", 0.0), 0.0) if u else 0.0

def _plan_is_active(u: dict) -> bool:
    plan = (u or {}).get("plan") or "none"
    exp = _parse_dt((u or {}).get("plan_expires_at"))
    if plan not in ("trial", "premium"):
        return False
    if not exp:
        return False
    return _now_utc() < exp


def _days_remaining_from_exp(exp: datetime | None) -> int:
    exp = _parse_dt(exp)
    if not exp:
        return 0
    remaining = exp - _now_utc()
    if remaining.total_seconds() <= 0:
        return 0
    return max(1, int((remaining.total_seconds() + 86399) // 86400))


MANUAL_PLAN_OPTIONS = {"trial", "premium"}


def _normalize_manual_plan(plan: str | None) -> str:
    normalized = str(plan or '').strip().lower()
    return normalized if normalized in MANUAL_PLAN_OPTIONS else 'premium'


def _manual_plan_label(plan: str | None) -> str:
    normalized = _normalize_manual_plan(plan)
    return 'PRUEBA' if normalized == 'trial' else 'PREMIUM'


def get_manual_plan_days_preview(target_user_id: int, target_plan: str, days: int) -> dict:
    """
    Previsualiza el efecto de una extensión manual sin persistir cambios.
    """
    try:
        target_user_id = int(target_user_id)
        days = int(days)
        target_plan = _normalize_manual_plan(target_plan)
        if days <= 0:
            return {"ok": False, "message": "La cantidad de días debe ser mayor que cero"}

        u = users_col.find_one({"user_id": target_user_id}, {"plan": 1, "plan_expires_at": 1, "trial_used": 1})
        if not u:
            return {"ok": False, "message": "Usuario no encontrado"}

        previous_plan = (u.get("plan") or "none")
        previous_exp = _parse_dt(u.get("plan_expires_at"))
        has_active_access = _plan_is_active(u)

        if target_plan == 'trial' and has_active_access and previous_plan == 'premium':
            return {
                "ok": False,
                "message": "No se puede aplicar PRUEBA mientras el usuario tenga PREMIUM activo",
            }

        base_type = "current_expiry" if has_active_access and previous_exp else "today"
        exp_utc = _midnight_cuba_after_days_from_base(previous_exp, days)

        return {
            "ok": True,
            "days": days,
            "target_plan": target_plan,
            "target_plan_label": _manual_plan_label(target_plan),
            "previous_plan": previous_plan,
            "previous_expires_at": previous_exp,
            "previous_days_remaining": _days_remaining_from_exp(previous_exp) if has_active_access else 0,
            "new_plan": target_plan,
            "new_plan_label": _manual_plan_label(target_plan),
            "new_expires_at": exp_utc,
            "new_days_remaining": _days_remaining_from_exp(exp_utc),
            "base_type": base_type,
        }
    except Exception as e:
        db_log(f"❌ Error previsualizando plan manual user={target_user_id} plan={target_plan}: {e}")
        return {"ok": False, "message": "Error interno al calcular la previsualización"}


def get_manual_premium_days_preview(target_user_id: int, days: int) -> dict:
    return get_manual_plan_days_preview(target_user_id, 'premium', days)


def user_is_ready(user_id: int) -> bool:
    u = users_col.find_one({"user_id": int(user_id)})
    if not u:
        return False

    # ✅ Debe tener plan activo (trial o premium)
    if not _plan_is_active(u):
        return False

    return bool(
        u.get("wallet") and
        u.get("private_key") and
        u.get("trading_status") == "active"
    )

# ============================================================
# PLANES (TRIAL / PREMIUM)
# ============================================================

def ensure_access_on_activate(user_id: int) -> dict:
    """
    Llamar cuando el usuario toca "Activar Trading".
    - Si tiene premium activo => allowed
    - Si tiene trial activo => allowed
    - Si no tiene plan activo y NO ha usado trial => inicia trial 5 días (vence medianoche Cuba)
    - Si ya usó trial y no tiene premium => bloqueado
    """
    u = users_col.find_one({"user_id": int(user_id)})
    if not u:
        return {"allowed": False, "message": "❌ Usuario no registrado."}

    # Premium activo
    if _plan_is_active(u) and (u.get("plan") == "premium"):
        return {"allowed": True, "plan_message": "🟢 *Trading ACTIVADO*\nPlan: *PREMIUM* ✅"}

    # Trial activo
    if _plan_is_active(u) and (u.get("plan") == "trial"):
        exp = _parse_dt(u.get("plan_expires_at"))
        if exp:
            exp_cuba = exp.replace(tzinfo=pytz.UTC).astimezone(CUBA_TZ)
            exp_str = exp_cuba.strftime("%Y-%m-%d 00:00 Cuba")
        else:
            exp_str = ""
        return {"allowed": True, "plan_message": f"🟢 *Trading ACTIVADO*\nPlan: *PRUEBA* ✅\nVence: *{exp_str}*"}

    # Iniciar trial (una sola vez)
    if not bool(u.get("trial_used", False)):
        exp_utc = _midnight_cuba_after_days(5)
        users_col.update_one(
            {"user_id": int(user_id)},
            {"$set": {"plan": "trial", "plan_expires_at": exp_utc, "trial_used": True}}
        )
        exp_cuba = exp_utc.replace(tzinfo=pytz.UTC).astimezone(CUBA_TZ)
        exp_str = exp_cuba.strftime("%Y-%m-%d 00:00 Cuba")
        db_log(f"✅ Trial iniciado user={user_id} exp={exp_utc.isoformat()}")
        return {"allowed": True, "plan_message": f"🟢 *Trading ACTIVADO*\nPlan: *PRUEBA* ✅\nVence: *{exp_str}*"}
    # Ya usó trial
    return {
        "allowed": False,
        "message": "⛔ Tu prueba terminó.\nPara seguir utilizando el bot, contacta al administrador."
    }

def _midnight_cuba_after_days_from_base(base_dt: datetime | None, days: int) -> datetime:
    """
    Calcula la próxima expiración a medianoche Cuba sumando días completos
    sobre una fecha base.

    - Si `base_dt` existe y sigue vigente, se usa como base para extender.
    - Si no existe o ya venció, el conteo parte desde hoy en Cuba.
    """
    base_utc = _parse_dt(base_dt)
    if base_utc and base_utc > _now_utc():
        base_local = base_utc.replace(tzinfo=pytz.UTC).astimezone(CUBA_TZ)
        base_date = base_local.date()
    else:
        base_date = _now_cuba().date()

    target_date = base_date + timedelta(days=int(days))
    midnight_local = CUBA_TZ.localize(datetime(target_date.year, target_date.month, target_date.day, 0, 0, 0))
    return midnight_local.astimezone(pytz.UTC).replace(tzinfo=None)


def grant_manual_plan_days(target_user_id: int, target_plan: str, days: int) -> dict:
    """
    Extensión manual de acceso por una cantidad exacta de días.

    Reglas:
    - Si el usuario tiene acceso vigente, suma desde el vencimiento actual.
    - Si no tiene acceso vigente, parte desde hoy (hora Cuba).
    - Si el plan destino es PREMIUM, el usuario queda actualizado inmediatamente a premium.
    - Si el plan destino es PRUEBA, no se permite degradar un PREMIUM activo.
    - No marca referidos válidos: esto es una extensión/admin reward, no una compra.
    """
    try:
        target_user_id = int(target_user_id)
        days = int(days)
        target_plan = _normalize_manual_plan(target_plan)
        if days <= 0:
            return {"ok": False, "message": "La cantidad de días debe ser mayor que cero"}

        u = users_col.find_one({"user_id": target_user_id}, {"plan": 1, "plan_expires_at": 1, "trial_used": 1})
        if not u:
            return {"ok": False, "message": "Usuario no encontrado"}

        previous_plan = (u.get("plan") or "none")
        previous_exp = _parse_dt(u.get("plan_expires_at"))
        has_active_access = _plan_is_active(u)

        if target_plan == 'trial' and has_active_access and previous_plan == 'premium':
            return {"ok": False, "message": "No se puede aplicar PRUEBA mientras el usuario tenga PREMIUM activo"}

        exp_utc = _midnight_cuba_after_days_from_base(previous_exp, days)
        set_fields = {
            "plan": target_plan,
            "plan_expires_at": exp_utc,
            "expiry_notified_on": None,
        }
        if target_plan == 'trial':
            set_fields['trial_used'] = True

        users_col.update_one({"user_id": target_user_id}, {"$set": set_fields})

        plan_label = _manual_plan_label(target_plan)
        db_log(
            f"🎁 Plan manual user={target_user_id} plan={target_plan} days={days} prev_plan={previous_plan} prev_exp={previous_exp.isoformat() if previous_exp else 'none'} new_exp={exp_utc.isoformat()}"
        )
        log_user_activity(
            int(target_user_id),
            f"Acceso actualizado · {plan_label}",
            f"Tu acceso fue extendido manualmente por {days} día(s). Nuevo vencimiento {exp_utc.strftime('%Y-%m-%d %H:%M:%S')} UTC.",
            tone='success',
            event_type='access_updated',
            occurred_at=_now_utc(),
            metadata={
                'plan': target_plan,
                'days': int(days),
                'new_expires_at': exp_utc.isoformat(),
            },
        )
        return {
            "ok": True,
            "message": f"{plan_label} actualizado manualmente por {days} días",
            "target_plan": target_plan,
            "target_plan_label": plan_label,
            "previous_plan": previous_plan,
            "previous_expires_at": previous_exp,
            "new_plan": target_plan,
            "new_plan_label": plan_label,
            "new_expires_at": exp_utc,
            "new_days_remaining": _days_remaining_from_exp(exp_utc),
            "days": days,
        }
    except Exception as e:
        db_log(f"❌ Error aplicando plan manual user={target_user_id} plan={target_plan}: {e}")
        return {"ok": False, "message": "Error interno al aplicar la extensión manual"}


def grant_manual_premium_days(target_user_id: int, days: int) -> dict:
    return grant_manual_plan_days(target_user_id, 'premium', days)


def activate_premium_plan(target_user_id: int) -> bool:
    """
    Compatibilidad legacy: activación manual premium de 30 días.

    Nota: no marca referidos válidos. Las compras automáticas futuras
    deben contabilizarse por su propio flujo de pagos.
    """
    outcome = grant_manual_premium_days(int(target_user_id), 30)
    return bool(outcome.get("ok"))

def is_plan_expired(user_id: int) -> bool:
    u = users_col.find_one({"user_id": int(user_id)}, {"_id": 0, "plan": 1, "plan_expires_at": 1})
    if not u:
        return False
    plan = u.get("plan") or "none"
    exp = _parse_dt(u.get("plan_expires_at"))
    if plan not in ("trial", "premium") or not exp:
        return False
    return _now_utc() >= exp

def should_notify_expired(user_id: int) -> bool:
    """
    True si está vencido y no se notificó hoy (hora Cuba).
    """
    u = users_col.find_one({"user_id": int(user_id)}, {"_id": 0, "expiry_notified_on": 1})
    if not u:
        return False
    today_cuba = _now_cuba().strftime("%Y-%m-%d")
    return u.get("expiry_notified_on") != today_cuba

def mark_expiry_notified(user_id: int):
    today_cuba = _now_cuba().strftime("%Y-%m-%d")
    users_col.update_one({"user_id": int(user_id)}, {"$set": {"expiry_notified_on": today_cuba}})

# ============================================================
# TRADES + INTERÉS COMPUESTO
# ============================================================

def register_trade(user_id, symbol, side, entry_price, exit_price, qty, profit, best_score, **extra_fields):
    """
    Registra el trade.

    Nota:
    - No aplica interés compuesto (capital configurado eliminado).
    - El sizing/uso de capital debe venir del balance real del exchange.
    - `profit` debe llegar NETO (realized pnl - fees).
    - `extra_fields` permite persistir metadatos de auditoría sin romper llamadas antiguas.
    """
    doc = {
        "user_id": int(user_id),
        "symbol": str(symbol),
        "side": str(side),
        "entry_price": _safe_float(entry_price, 0.0),
        "exit_price": _safe_float(exit_price, 0.0),
        "qty": _safe_float(qty, 0.0),
        "profit": _safe_float(profit, 0.0),
        "best_score": _safe_float(best_score, 0.0),
        "timestamp": datetime.utcnow(),
    }

    for key, value in (extra_fields or {}).items():
        if value is None:
            continue
        if key in {"fees", "gross_pnl", "realized_fills", "opened_at_ms"}:
            doc[key] = _safe_float(value, 0.0)
        elif key in {"pnl_source", "exit_reason", "close_source", "direction"}:
            doc[key] = str(value)
        elif key == "metadata" and isinstance(value, dict):
            doc[key] = value
        else:
            doc[key] = value

    trades_col.insert_one(doc)

def get_user_trades(user_id: int):
    return list(
        trades_col.find({"user_id": int(user_id)}, {"_id": 0})
        .sort("timestamp", -1)
        .limit(20)
    )

# ============================================================
# ADMIN – INFORMACIÓN VISUAL (STATS)
# ============================================================

def get_admin_visual_stats() -> dict:
    """
    Retorna métricas globales para el panel admin:
    - total_users
    - free_old
    - premium_active
    - premium_expired
    """
    total_users = users_col.count_documents({})

    now = datetime.utcnow()

    premium_active = users_col.count_documents({
        "plan": "premium",
        "plan_expires_at": {"$gt": now}
    })

    premium_expired = users_col.count_documents({
        "plan": "premium",
        "plan_expires_at": {"$lte": now}
    })

    free_old = users_col.count_documents({
        "$or": [
            {"plan": {"$exists": False}},
            {"plan": None},
            {"plan": "trial"}
        ],
        "trial_used": True
    })

    return {
        "total_users": int(total_users),
        "free_old": int(free_old),
        "premium_active": int(premium_active),
        "premium_expired": int(premium_expired),
    }

# ============================================================
# OPERACIÓN ACTUAL / ÚLTIMA (INFO PARA BOT)
# ============================================================

def save_last_open(user_id: int, open_data: dict):
    """
    Guarda la información de la última operación ABIERTA.
    Se sobreescribe siempre (solo informativo).
    """
    now = datetime.utcnow()
    payload = dict(open_data or {})
    users_col.update_one(
        {"user_id": int(user_id)},
        {"$set": {"last_open": payload, "last_open_at": now}}
    )
    symbol = payload.get('symbol') or payload.get('coin') or 'Operación'
    side = str(payload.get('side') or payload.get('direction') or '').upper()
    detail_parts = []
    if payload.get('entry_price') is not None:
        detail_parts.append(f"Entrada {payload.get('entry_price')}")
    if payload.get('qty') is not None:
        detail_parts.append(f"Qty {payload.get('qty')}")
    if payload.get('message'):
        detail_parts.append(str(payload.get('message')))
    detail = ' · '.join(detail_parts) if detail_parts else 'Se registró una nueva apertura en el bot.'
    title = f"Apertura {symbol}" if not side else f"Apertura {symbol} · {side}"
    log_user_activity(int(user_id), title, detail, tone='info', event_type='trade_opened', metadata={'symbol': symbol, 'side': side or None}, occurred_at=now)

def save_last_close(user_id: int, close_data: dict):
    """
    Guarda la información de la última operación CERRADA.
    Se sobreescribe siempre (solo informativo).
    """
    now = datetime.utcnow()
    payload = dict(close_data or {})
    users_col.update_one(
        {"user_id": int(user_id)},
        {"$set": {"last_close": payload, "last_close_at": now}}
    )
    symbol = payload.get('symbol') or payload.get('coin') or 'Operación'
    side = str(payload.get('side') or payload.get('direction') or '').upper()
    pnl = _safe_float(payload.get('profit'), 0.0) if payload.get('profit') is not None else None
    tone = 'success' if pnl is not None and pnl > 0 else ('danger' if pnl is not None and pnl < 0 else 'info')
    detail_parts = []
    if payload.get('entry_price') is not None:
        detail_parts.append(f"Entrada {payload.get('entry_price')}")
    if payload.get('exit_price') is not None:
        detail_parts.append(f"Salida {payload.get('exit_price')}")
    if pnl is not None:
        detail_parts.append(f"PnL {round(pnl, 4)}")
    if payload.get('message'):
        detail_parts.append(str(payload.get('message')))
    detail = ' · '.join(detail_parts) if detail_parts else 'Se registró un cierre en el bot.'
    title = f"Cierre {symbol}" if not side else f"Cierre {symbol} · {side}"
    log_user_activity(int(user_id), title, detail, tone=tone, event_type='trade_closed', metadata={'symbol': symbol, 'side': side or None, 'profit': pnl}, occurred_at=now)

def get_last_operation(user_id: int) -> dict:
    """
    Retorna last_open y last_close para mostrar en el botón Información.
    """
    u = users_col.find_one(
        {"user_id": int(user_id)},
        {"_id": 0, "last_open": 1, "last_close": 1}
    )
    return u or {}

def admin_set_user_trading_status(user_id: int, status: str) -> bool:
    normalized = str(status or '').strip().lower()
    if normalized not in {'active', 'inactive'}:
        return False
    doc = users_col.find_one({"user_id": int(user_id)}, {"_id": 1})
    if not doc:
        return False
    set_trading_status(int(user_id), normalized)
    return True

# ============================================================
# LEGACY – FEES (DESACTIVADO)
# Mantener SOLO para compatibilidad con imports en trading_engine.py
# NO afecta trading / estrategia.
# ============================================================

def add_daily_admin_fee(user_id: int, amount: float):
    """DEPRECATED: fees desactivadas. Se deja para no romper imports."""
    try:
        db_log(f"ℹ add_daily_admin_fee ignorado (fees desactivadas) user={user_id} amount={amount}")
    except Exception:
        pass
    return None

def add_weekly_ref_fee(referrer_id: int, amount: float):
    """DEPRECATED: fees desactivadas. Se deja para no romper imports."""
    try:
        db_log(f"ℹ add_weekly_ref_fee ignorado (fees desactivadas) referrer={referrer_id} amount={amount}")
    except Exception:
        pass
    return None



# ============================================================
# ADMIN – STATS EPOCH (RESET CONTADOR)
# ============================================================

def get_admin_trade_stats_epoch() -> datetime | None:
    """Devuelve el epoch (UTC) desde el cual se deben contar las estadísticas del admin."""
    try:
        doc = settings_col.find_one({"_id": "admin_stats_epoch"}, {"_id": 0, "epoch": 1})
        epoch = (doc or {}).get("epoch")
        if isinstance(epoch, datetime):
            return epoch
        # si por algún motivo quedó como string ISO
        try:
            return datetime.fromisoformat(str(epoch))
        except Exception:
            return None
    except Exception:
        return None


def reset_admin_trade_stats_epoch() -> datetime:
    """Resetea estadísticas: fija epoch=now (UTC). Devuelve el epoch guardado."""
    now = datetime.utcnow()
    try:
        settings_col.update_one(
            {"_id": "admin_stats_epoch"},
            {"$set": {"epoch": now}},
            upsert=True
        )
        db_log(f"♻️ admin_stats_epoch actualizado -> {now.isoformat()}")
    except Exception as e:
        try:
            db_log(f"⚠ reset_admin_trade_stats_epoch error: {e}")
        except Exception:
            pass
    return now


def get_user_trade_stats_epoch(user_id: int):
    """Retorna el epoch (datetime UTC) desde el cual se calculan las stats del usuario."""
    try:
        uid = int(user_id)
    except Exception:
        return None
    try:
        u = users_col.find_one({"user_id": uid}, {"user_trade_stats_epoch": 1})
        if not u:
            return None
        ep = u.get("user_trade_stats_epoch")
        return ep if isinstance(ep, datetime) else None
    except Exception:
        return None


def reset_user_trade_stats_epoch(user_id: int):
    """Resetea el conteo de stats para un usuario (no borra trades, solo mueve el epoch)."""
    try:
        uid = int(user_id)
    except Exception:
        return False
    try:
        now = datetime.utcnow()
        users_col.update_one({"user_id": uid}, {"$set": {"user_trade_stats_epoch": now}}, upsert=True)
        return True
    except Exception as e:
        try:
            db_log(f"⚠ reset_user_trade_stats_epoch error user_id={user_id}: {e}")
        except Exception:
            pass
        return False


# ============================================================
# ADMIN – ESTADÍSTICAS DE TRADING (24h / 7d / 30d)
# ============================================================

def _empty_trade_stats(*, real_since: datetime, epoch: datetime | None, user_id: int | None = None, error: str | None = None) -> dict:
    payload = {
        "total": 0,
        "wins": 0,
        "losses": 0,
        "break_evens": 0,
        "decisive_trades": 0,
        "win_rate": 0.0,
        "win_rate_decisive": 0.0,
        "pnl_total": 0.0,
        "gross_profit": 0.0,
        "gross_loss": 0.0,
        "profit_factor": 0.0,
        "since": real_since,
        "epoch": epoch,
    }
    if user_id is not None:
        payload["user_id"] = int(user_id)
    if error:
        payload["error"] = str(error)
    return payload


def get_admin_trade_stats(hours: int) -> dict:
    """
    Retorna estadísticas globales de trades cerrados en un período:
      - total, wins, losses, break_evens
      - win_rate sobre total y win_rate_decisive excluyendo breakevens
      - pnl_total (suma profits netos)
      - gross_profit (suma profits positivos)
      - gross_loss (suma pérdidas en valor absoluto)
      - profit_factor = gross_profit / gross_loss (si gross_loss==0 => 0 o inf)

    hours: ventana hacia atrás en horas (ej: 24, 168, 720)
    """
    try:
        hours = int(hours)
    except Exception:
        hours = 24

    if hours <= 0:
        hours = 24

    since = datetime.utcnow() - timedelta(hours=hours)

    epoch = get_admin_trade_stats_epoch()
    real_since = since
    if isinstance(epoch, datetime) and epoch > since:
        real_since = epoch

    try:
        pipeline = [
            {"$match": {"timestamp": {"$gte": real_since}}},
            {"$group": {
                "_id": None,
                "total": {"$sum": 1},
                "wins": {"$sum": {"$cond": [{"$gt": ["$profit", 0]}, 1, 0]}},
                "losses": {"$sum": {"$cond": [{"$lt": ["$profit", 0]}, 1, 0]}},
                "pnl_total": {"$sum": "$profit"},
                "gross_profit": {"$sum": {"$cond": [{"$gt": ["$profit", 0]}, "$profit", 0]}},
                "gross_loss": {"$sum": {"$cond": [{"$lt": ["$profit", 0]}, {"$abs": "$profit"}, 0]}},
            }}
        ]

        agg = list(trades_col.aggregate(pipeline, allowDiskUse=False))
        if not agg:
            return _empty_trade_stats(real_since=real_since, epoch=epoch)

        r = agg[0]
        total = int(r.get("total", 0) or 0)
        wins = int(r.get("wins", 0) or 0)
        losses = int(r.get("losses", 0) or 0)
        break_evens = max(0, total - wins - losses)
        decisive_trades = wins + losses
        pnl_total = float(r.get("pnl_total", 0.0) or 0.0)
        gross_profit = float(r.get("gross_profit", 0.0) or 0.0)
        gross_loss = float(r.get("gross_loss", 0.0) or 0.0)

        win_rate = (wins / total * 100.0) if total > 0 else 0.0
        win_rate_decisive = (wins / decisive_trades * 100.0) if decisive_trades > 0 else 0.0
        if gross_loss > 0:
            profit_factor = gross_profit / gross_loss
        else:
            profit_factor = float("inf") if gross_profit > 0 else 0.0

        return {
            "total": total,
            "wins": wins,
            "losses": losses,
            "break_evens": break_evens,
            "decisive_trades": decisive_trades,
            "win_rate": round(win_rate, 2),
            "win_rate_decisive": round(win_rate_decisive, 2),
            "pnl_total": round(pnl_total, 6),
            "gross_profit": round(gross_profit, 6),
            "gross_loss": round(gross_loss, 6),
            "profit_factor": round(profit_factor, 4) if profit_factor != float("inf") else float("inf"),
            "since": real_since,
            "epoch": epoch,
        }
    except Exception as e:
        try:
            db_log(f"⚠ get_admin_trade_stats error hours={hours}: {e}")
        except Exception:
            pass
        return _empty_trade_stats(real_since=real_since, epoch=epoch, error=str(e))


# ============================================================
# ESTADÍSTICAS POR USUARIO (24h / 7d / 30d)
# ============================================================

def get_user_trade_stats(user_id: int, hours: int) -> dict:
    """
    Retorna estadísticas de trades cerrados para un usuario en un período:
      - total, wins, losses, break_evens
      - win_rate sobre total y win_rate_decisive excluyendo breakevens
      - pnl_total (suma profits netos)
      - gross_profit (suma profits positivos)
      - gross_loss (suma pérdidas en valor absoluto)
      - profit_factor = gross_profit / gross_loss (si gross_loss==0 => 0 o inf)

    user_id: ID de Telegram del usuario
    hours: ventana hacia atrás en horas (ej: 24, 168, 720)
    """
    try:
        hours = int(hours)
    except Exception:
        hours = 24

    if hours <= 0:
        hours = 24

    try:
        uid = int(user_id)
    except Exception:
        uid = 0

    since = datetime.utcnow() - timedelta(hours=hours)

    # Respeta el reset global del admin y el reset específico del usuario
    epoch = get_admin_trade_stats_epoch()
    user_epoch = get_user_trade_stats_epoch(uid)
    real_since = since
    if isinstance(epoch, datetime) and epoch > real_since:
        real_since = epoch
    if isinstance(user_epoch, datetime) and user_epoch > real_since:
        real_since = user_epoch

    try:
        pipeline = [
            {"$match": {"user_id": uid, "timestamp": {"$gte": real_since}}},
            {"$group": {
                "_id": None,
                "total": {"$sum": 1},
                "wins": {"$sum": {"$cond": [{"$gt": ["$profit", 0]}, 1, 0]}},
                "losses": {"$sum": {"$cond": [{"$lt": ["$profit", 0]}, 1, 0]}},
                "pnl_total": {"$sum": "$profit"},
                "gross_profit": {"$sum": {"$cond": [{"$gt": ["$profit", 0]}, "$profit", 0]}},
                "gross_loss": {"$sum": {"$cond": [{"$lt": ["$profit", 0]}, {"$abs": "$profit"}, 0]}},
            }}
        ]

        agg = list(trades_col.aggregate(pipeline, allowDiskUse=False))
        if not agg:
            return _empty_trade_stats(real_since=real_since, epoch=epoch, user_id=uid)

        r = agg[0]
        total = int(r.get("total", 0) or 0)
        wins = int(r.get("wins", 0) or 0)
        losses = int(r.get("losses", 0) or 0)
        break_evens = max(0, total - wins - losses)
        decisive_trades = wins + losses
        pnl_total = float(r.get("pnl_total", 0.0) or 0.0)
        gross_profit = float(r.get("gross_profit", 0.0) or 0.0)
        gross_loss = float(r.get("gross_loss", 0.0) or 0.0)

        win_rate = (wins / total * 100.0) if total > 0 else 0.0
        win_rate_decisive = (wins / decisive_trades * 100.0) if decisive_trades > 0 else 0.0
        if gross_loss > 0:
            profit_factor = gross_profit / gross_loss
        else:
            profit_factor = float("inf") if gross_profit > 0 else 0.0

        return {
            "total": total,
            "wins": wins,
            "losses": losses,
            "break_evens": break_evens,
            "decisive_trades": decisive_trades,
            "win_rate": round(win_rate, 2),
            "win_rate_decisive": round(win_rate_decisive, 2),
            "pnl_total": round(pnl_total, 6),
            "gross_profit": round(gross_profit, 6),
            "gross_loss": round(gross_loss, 6),
            "profit_factor": round(profit_factor, 4) if profit_factor != float("inf") else float("inf"),
            "since": real_since,
            "epoch": epoch,
            "user_id": uid,
        }
    except Exception as e:
        try:
            db_log(f"⚠ get_user_trade_stats error user_id={user_id} hours={hours}: {e}")
        except Exception:
            pass
        return _empty_trade_stats(real_since=real_since, epoch=epoch, user_id=uid, error=str(e))



def get_user_track_record_summary(user_id: int, recent_form_limit: int = 12) -> dict:
    """
    Resume el track record completo del usuario respetando epochs de reset.
    Diseñado para MiniApp / panel de rendimiento.
    """
    try:
        uid = int(user_id)
    except Exception:
        uid = 0

    epoch = get_admin_trade_stats_epoch()
    user_epoch = get_user_trade_stats_epoch(uid)
    real_since = datetime.utcnow() - timedelta(days=3650)
    if isinstance(epoch, datetime) and epoch > real_since:
        real_since = epoch
    if isinstance(user_epoch, datetime) and user_epoch > real_since:
        real_since = user_epoch

    payload = {
        'total': 0,
        'wins': 0,
        'losses': 0,
        'break_evens': 0,
        'decisive_trades': 0,
        'net_pnl': 0.0,
        'gross_profit': 0.0,
        'gross_loss': 0.0,
        'avg_pnl': 0.0,
        'avg_win': 0.0,
        'avg_loss': 0.0,
        'expectancy': 0.0,
        'best_trade': 0.0,
        'worst_trade': 0.0,
        'profit_factor': 0.0,
        'win_rate': 0.0,
        'current_streak_type': 'none',
        'current_streak_count': 0,
        'best_win_streak': 0,
        'best_loss_streak': 0,
        'recent_form': [],
        'recent_form_compact': '—',
        'dominant_symbol': None,
        'dominant_symbol_count': 0,
        'dominant_symbol_pnl': 0.0,
        'first_trade_at': None,
        'last_trade_at': None,
        'since': real_since,
        'epoch': epoch,
        'user_id': uid,
    }

    try:
        cursor = trades_col.find(
            {'user_id': uid, 'timestamp': {'$gte': real_since}},
            {'_id': 0, 'symbol': 1, 'profit': 1, 'timestamp': 1},
        ).sort('timestamp', 1)

        prev_streak = None
        current_streak_count = 0
        form = []
        symbol_stats: dict[str, dict] = {}

        for trade in cursor:
            profit = _safe_float((trade or {}).get('profit'), 0.0)
            ts = _parse_dt((trade or {}).get('timestamp'))
            symbol = str((trade or {}).get('symbol') or '—')

            payload['total'] += 1
            payload['net_pnl'] += profit
            payload['best_trade'] = profit if payload['total'] == 1 else max(payload['best_trade'], profit)
            payload['worst_trade'] = profit if payload['total'] == 1 else min(payload['worst_trade'], profit)

            if payload['first_trade_at'] is None:
                payload['first_trade_at'] = ts
            payload['last_trade_at'] = ts

            symbol_row = symbol_stats.setdefault(symbol, {'count': 0, 'net_pnl': 0.0})
            symbol_row['count'] += 1
            symbol_row['net_pnl'] += profit

            if profit > 0:
                payload['wins'] += 1
                payload['gross_profit'] += profit
                result = 'win'
                form.append('W')
            elif profit < 0:
                payload['losses'] += 1
                payload['gross_loss'] += abs(profit)
                result = 'loss'
                form.append('L')
            else:
                payload['break_evens'] += 1
                result = 'flat'
                form.append('F')

            if result in {'win', 'loss'}:
                if prev_streak == result:
                    current_streak_count += 1
                else:
                    current_streak_count = 1
                    prev_streak = result
                if result == 'win':
                    payload['best_win_streak'] = max(payload['best_win_streak'], current_streak_count)
                else:
                    payload['best_loss_streak'] = max(payload['best_loss_streak'], current_streak_count)
            else:
                prev_streak = None
                current_streak_count = 0

        payload['decisive_trades'] = payload['wins'] + payload['losses']
        if payload['total'] > 0:
            payload['avg_pnl'] = payload['net_pnl'] / payload['total']
            payload['expectancy'] = payload['avg_pnl']
            payload['win_rate'] = (payload['wins'] / payload['total']) * 100.0
        if payload['wins'] > 0:
            payload['avg_win'] = payload['gross_profit'] / payload['wins']
        if payload['losses'] > 0:
            payload['avg_loss'] = -(payload['gross_loss'] / payload['losses'])
        if payload['gross_loss'] > 0:
            payload['profit_factor'] = payload['gross_profit'] / payload['gross_loss']
        else:
            payload['profit_factor'] = float('inf') if payload['gross_profit'] > 0 else 0.0

        if prev_streak is not None and current_streak_count > 0:
            payload['current_streak_type'] = prev_streak
            payload['current_streak_count'] = current_streak_count

        if symbol_stats:
            dominant_symbol, dominant_stats = sorted(
                symbol_stats.items(),
                key=lambda item: (item[1]['count'], item[1]['net_pnl']),
                reverse=True,
            )[0]
            payload['dominant_symbol'] = dominant_symbol
            payload['dominant_symbol_count'] = int(dominant_stats['count'])
            payload['dominant_symbol_pnl'] = float(dominant_stats['net_pnl'])

        recent = form[-max(1, int(recent_form_limit)):]
        recent.reverse()
        payload['recent_form'] = recent
        payload['recent_form_compact'] = ' '.join(recent) if recent else '—'

        payload['net_pnl'] = round(payload['net_pnl'], 6)
        payload['gross_profit'] = round(payload['gross_profit'], 6)
        payload['gross_loss'] = round(payload['gross_loss'], 6)
        payload['avg_pnl'] = round(payload['avg_pnl'], 6)
        payload['expectancy'] = round(payload['expectancy'], 6)
        payload['avg_win'] = round(payload['avg_win'], 6)
        payload['avg_loss'] = round(payload['avg_loss'], 6)
        payload['best_trade'] = round(payload['best_trade'], 6)
        payload['worst_trade'] = round(payload['worst_trade'], 6)
        payload['win_rate'] = round(payload['win_rate'], 2)
        payload['dominant_symbol_pnl'] = round(payload['dominant_symbol_pnl'], 6)
        if payload['profit_factor'] != float('inf'):
            payload['profit_factor'] = round(payload['profit_factor'], 4)
        return payload
    except Exception as e:
        try:
            db_log(f"⚠ get_user_track_record_summary error user_id={user_id}: {e}")
        except Exception:
            pass
        payload['error'] = str(e)
        return payload


# ============================================================
# TÉRMINOS Y CONDICIONES
# ============================================================

def accept_terms(user_id: int) -> bool:
    """Marca aceptación de términos y guarda timestamp UTC."""
    try:
        from datetime import datetime
        ts = datetime.utcnow()
        users_col.update_one(
            {"user_id": int(user_id)},
            {"$set": {"terms_accepted": True, "terms_timestamp": ts}},
            upsert=False,
        )
        log_user_activity(int(user_id), 'Términos aceptados', 'La cuenta ya puede habilitar el trading cuando la configuración esté completa.', tone='success', event_type='terms_accepted', occurred_at=ts)
        return True
    except Exception:
        return False


def has_accepted_terms(user_id: int) -> bool:
    """Retorna True si el usuario ya aceptó términos."""
    try:
        u = users_col.find_one({"user_id": int(user_id)}, {"_id": 0, "terms_accepted": 1})
        return bool((u or {}).get("terms_accepted", False))
    except Exception:
        return False


# ============================================================
# HELPERS PÚBLICOS PARA API / MINIAPP
# ============================================================

def get_user_public_snapshot(user_id: int) -> dict | None:
    """
    Snapshot seguro del usuario para API/MiniApp.
    Nunca expone la private key.
    """
    try:
        uid = int(user_id)
    except Exception:
        return None

    u = users_col.find_one(
        {"user_id": uid},
        {
            "_id": 0,
            "user_id": 1,
            "username": 1,
            "wallet": 1,
            "private_key": 1,
            "private_key_encrypted": 1,
            "private_key_version": 1,
            "terms_timestamp": 1,
            "trading_status": 1,
            "plan": 1,
            "plan_expires_at": 1,
            "trial_used": 1,
            "terms_accepted": 1,
            "referral_valid_count": 1,
            "last_open_at": 1,
            "last_close_at": 1,
        },
    )
    if not u:
        return None

    exp = _parse_dt(u.get("plan_expires_at"))
    return {
        "user_id": int(u.get("user_id")),
        "username": u.get("username"),
        "wallet": u.get("wallet"),
        "wallet_configured": bool(u.get("wallet")),
        "private_key_configured": bool(u.get("private_key")),
        "private_key_storage": ("encrypted" if bool(u.get("private_key")) and bool(u.get("private_key_encrypted", False)) else ("legacy_plaintext" if bool(u.get("private_key")) else "not_configured")),
        "trading_status": u.get("trading_status") or "inactive",
        "plan": u.get("plan") or "none",
        "plan_expires_at": exp,
        "plan_active": _plan_is_active(u),
        "plan_days_remaining": _days_remaining_from_exp(exp) if _plan_is_active(u) else 0,
        "trial_used": bool(u.get("trial_used", False)),
        "terms_accepted": bool(u.get("terms_accepted", False)),
        "terms_timestamp": _parse_dt(u.get("terms_timestamp")),
        "referral_valid_count": int(u.get("referral_valid_count", 0) or 0),
        "last_open_at": _parse_dt(u.get("last_open_at")),
        "last_close_at": _parse_dt(u.get("last_close_at")),
    }


def get_user_trades_limited(user_id: int, limit: int = 20):
    try:
        uid = int(user_id)
        lim = max(1, min(int(limit), 100))
    except Exception:
        return []

    return list(
        trades_col.find({"user_id": uid}, {"_id": 0})
        .sort("timestamp", -1)
        .limit(lim)
    )


def get_security_overview() -> dict:
    encrypted_keys = users_col.count_documents({"private_key": {"$ne": None}, "private_key_encrypted": True})
    legacy_plaintext_keys = users_col.count_documents({
        "private_key": {"$ne": None},
        "$or": [
            {"private_key_encrypted": {"$exists": False}},
            {"private_key_encrypted": False},
        ],
    })
    users_with_wallet = users_col.count_documents({"wallet": {"$ne": None}})
    return {
        "encrypted_private_keys": int(encrypted_keys),
        "legacy_plaintext_private_keys": int(legacy_plaintext_keys),
        "wallets_configured": int(users_with_wallet),
    }


def _build_admin_user_projection() -> dict:
    return {
        "_id": 0,
        "user_id": 1,
        "username": 1,
        "wallet": 1,
        "private_key": 1,
        "private_key_encrypted": 1,
        "private_key_version": 1,
        "private_key_updated_at": 1,
        "trading_status": 1,
        "plan": 1,
        "plan_expires_at": 1,
        "trial_used": 1,
        "terms_accepted": 1,
        "terms_timestamp": 1,
        "referral_valid_count": 1,
        "last_open_at": 1,
        "last_close_at": 1,
    }


def get_admin_user_snapshot(user_id: int) -> dict | None:
    doc = users_col.find_one({"user_id": int(user_id)}, _build_admin_user_projection())
    if not doc:
        return None
    snapshot = get_user_public_snapshot(int(user_id))
    if not snapshot:
        return None
    snapshot.update({
        "terms_timestamp": _parse_dt(doc.get("terms_timestamp")),
        "private_key_version": doc.get("private_key_version"),
        "private_key_updated_at": _parse_dt(doc.get("private_key_updated_at")),
        "recent_admin_actions": get_admin_action_history(limit=10, target_user_id=int(user_id)),
    })
    return snapshot


def search_users_for_admin(query: str, limit: int = 10) -> list[dict]:
    query = (query or '').strip()
    lim = max(1, min(int(limit), 25))
    if not query:
        return []

    clauses = []
    if query.isdigit():
        clauses.append({"user_id": int(query)})

    safe_pattern = re.escape(query)
    clauses.append({"username": {"$regex": safe_pattern, "$options": "i"}})

    cursor = users_col.find({"$or": clauses}, _build_admin_user_projection()).sort("user_id", 1).limit(lim)
    results = []
    for doc in cursor:
        snapshot = get_user_public_snapshot(int(doc.get("user_id")))
        if snapshot:
            results.append(snapshot)
    return results
