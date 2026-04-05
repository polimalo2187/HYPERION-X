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

def save_user_capital(user_id: int, capital: float):
    capital = _clamp_non_negative(_safe_float(capital, 0.0))
    users_col.update_one({"user_id": int(user_id)}, {"$set": {"capital": capital}})

def set_trading_status(user_id: int, status: str):
    users_col.update_one({"user_id": int(user_id)}, {"$set": {"trading_status": status}})


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

def activate_premium_plan(target_user_id: int) -> bool:
    """
    Activación manual por admin:
    - Premium 30 días (vence medianoche Cuba)
    """
    try:
        u = users_col.find_one({"user_id": int(target_user_id)})
        if not u:
            return False

        exp_utc = _midnight_cuba_after_days(30)
        users_col.update_one(
            {"user_id": int(target_user_id)},
            {"$set": {"plan": "premium", "plan_expires_at": exp_utc, "expiry_notified_on": None}}
        )
        db_log(f"💎 Premium activado user={target_user_id} exp={exp_utc.isoformat()}")
        # ✅ Marcar referido válido (una sola vez)
        _mark_referral_valid(int(target_user_id))
        return True
    except Exception as e:
        db_log(f"❌ Error activando premium user={target_user_id}: {e}")
        return False

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
    users_col.update_one(
        {"user_id": int(user_id)},
        {"$set": {"last_open": open_data, "last_open_at": datetime.utcnow()}}
    )

def save_last_close(user_id: int, close_data: dict):
    """
    Guarda la información de la última operación CERRADA.
    Se sobreescribe siempre (solo informativo).
    """
    users_col.update_one(
        {"user_id": int(user_id)},
        {"$set": {"last_close": close_data, "last_close_at": datetime.utcnow()}}
    )

def get_last_operation(user_id: int) -> dict:
    """
    Retorna last_open y last_close para mostrar en el botón Información.
    """
    u = users_col.find_one(
        {"user_id": int(user_id)},
        {"_id": 0, "last_open": 1, "last_close": 1}
    )
    return u or {}

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

# ============================================================
# TÉRMINOS Y CONDICIONES
# ============================================================

def accept_terms(user_id: int) -> bool:
    """Marca aceptación de términos y guarda timestamp UTC."""
    try:
        from datetime import datetime
        users_col.update_one(
            {"user_id": int(user_id)},
            {"$set": {"terms_accepted": True, "terms_timestamp": datetime.utcnow()}},
            upsert=False,
        )
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
