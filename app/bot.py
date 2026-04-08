# ============================================================
# BOT PRINCIPAL – TRADING X HYPER PRO
# Archivo 8/9 – Sistema de control vía Telegram (VERSIÓN FINAL)
# MOD: Capital manual eliminado. El bot opera usando el balance real del exchange.
# ============================================================

import asyncio
import logging
import os

from telegram import (
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    Update,
    WebAppInfo,
)
from telegram.ext import (
    ApplicationBuilder,
    CommandHandler,
    CallbackQueryHandler,
    MessageHandler,
    ContextTypes,
    filters,
)

from app.config import TELEGRAM_BOT_TOKEN, BOT_NAME, MINIAPP_URL

# ============================================================
# ENV (Admin / Bot)
# ============================================================
BOT_USERNAME = os.getenv("BOT_USERNAME", "TradingXHiperPro_bot")
ADMIN_TELEGRAM_ID = int(os.getenv("ADMIN_TELEGRAM_ID", "0") or "0")
ADMIN_WHATSAPP_LINK = os.getenv("ADMIN_WHATSAPP_LINK", "").strip()

from app.database import (
    has_accepted_terms,
    create_user,
    save_user_wallet,
    save_user_private_key,
    set_trading_status,
    get_user_wallet,
    get_user_private_key,
    user_is_ready,
    get_user_trades,
    # Referidos (solo conteo)
    set_referrer,
    get_referral_valid_count,
    # Planes
    ensure_access_on_activate,
    is_user_registered,
    activate_premium_plan,
    # Admin visual
    get_admin_visual_stats,
    get_last_operation,
    get_admin_trade_stats,
    reset_admin_trade_stats_epoch,
    # Stats por usuario (admin)
    get_user_trade_stats,
    reset_user_trade_stats_epoch,
    touch_runtime_component,
)

from app.hyperliquid_client import get_balance
from app.trading_loop import trading_loop


# ============================================================
# LOGGING
# ============================================================

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO
)


# ============================================================
# MENÚ PRINCIPAL
# ============================================================

def _launcher_menu(user_id: int | None = None):
    kb = []
    kb.append([InlineKeyboardButton("📜 Ver políticas", callback_data="policies")])
    if MINIAPP_URL:
        kb.append([InlineKeyboardButton("🚀 Abrir MiniApp", web_app=WebAppInfo(url=MINIAPP_URL))])
    return InlineKeyboardMarkup(kb)


def main_menu(user_id: int | None = None):
    return _launcher_menu(user_id)


def _miniapp_entry_text(*, terms_accepted: bool = False, is_admin: bool = False) -> str:
    base = (
        f"🚀 *Bienvenido a {BOT_NAME}*\n\n"
        "El bot automático diseñado para operar en *Hyperliquid* con una experiencia más seria, rápida y profesional.\n\n"
        "✅ Ejecuta trading automático sin depender de que estés conectado\n"
        "✅ Gestiona la operativa desde una *MiniApp* visual y centralizada\n"
        "✅ Controla wallet, clave, estado operativo, historial y rendimiento desde un solo panel\n"
        "✅ Mantén Telegram como canal de alertas, avisos y acceso rápido\n\n"
        "Para empezar correctamente:\n"
        "1. *Lee las Políticas* aquí en Telegram\n"
        "2. Entra en la *MiniApp* y *confirma su aceptación*\n"
        "3. Completa la configuración y activa la operativa"
    )
    if not terms_accepted:
        base += "\n\n⚠️ *Las Políticas son obligatorias.* Debes leerlas aquí y luego confirmar su aceptación dentro de la MiniApp para poder trabajar el bot."
    else:
        base += "\n\n✅ Tu confirmación de políticas ya fue registrada. Solo falta abrir la MiniApp para continuar."
    if MINIAPP_URL:
        base += "\n\n🚀 Pulsa *Abrir MiniApp* para entrar al panel operativo."
    if is_admin:
        base += "\n\n🛠 Tu acceso administrativo también se gestiona desde la MiniApp."
    return base


async def _send_miniapp_redirect_message(target, user_id: int, title: str, as_edit: bool = True):
    text = (
        f"{title}\n\n"
        "Esta acción ahora se gestiona desde la *MiniApp* para mantener una sola interfaz operativa y evitar configuraciones duplicadas.\n\n"
        "📜 Acepta las *Políticas* si aún no lo has hecho.\n"
        "🚀 Después entra en la *MiniApp* para continuar."
    )
    markup = _launcher_menu(user_id)
    if as_edit:
        await target.edit_message_text(text, parse_mode="Markdown", reply_markup=markup)
    else:
        await target.reply_text(text, parse_mode="Markdown", reply_markup=markup)


# ============================================================
# /START
# ============================================================

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):

    user = update.effective_user
    user_id = user.id
    create_user(user.id, user.username)

    # Sistema de referidos (solo desde enlace /start)
    if context.args:
        ref = context.args[0]
        if ref.isdigit() and int(ref) != user.id:
            set_referrer(user.id, int(ref))

    terms_accepted = has_accepted_terms(user_id)
    await update.message.reply_text(
        _miniapp_entry_text(terms_accepted=terms_accepted, is_admin=(user_id == ADMIN_TELEGRAM_ID)),
        reply_markup=_launcher_menu(user_id),
        parse_mode="Markdown"
    )


async def miniapp_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    create_user(update.effective_user.id, update.effective_user.username)
    terms_accepted = has_accepted_terms(user_id)
    await update.message.reply_text(
        _miniapp_entry_text(terms_accepted=terms_accepted, is_admin=(user_id == ADMIN_TELEGRAM_ID)),
        reply_markup=_launcher_menu(user_id),
        parse_mode="Markdown",
    )



# ============================================================
# DASHBOARD
# ============================================================
async def dashboard(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    await _send_miniapp_redirect_message(q, q.from_user.id, "📊 *Dashboard trasladado a la MiniApp*")


# ============================================================
# SUBMENÚ WALLET / PRIVATE KEY
# ============================================================

async def wallet_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    await _send_miniapp_redirect_message(q, q.from_user.id, "💳 *Configuración trasladada a la MiniApp*")


async def set_wallet(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    context.user_data.clear()
    await _send_miniapp_redirect_message(q, q.from_user.id, "🔗 *La wallet ahora se configura en la MiniApp*")


async def set_pk(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    context.user_data.clear()
    await _send_miniapp_redirect_message(q, q.from_user.id, "🔐 *La private key ahora se configura en la MiniApp*")


# ============================================================
# INPUTS
# ============================================================

async def text_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id

    legacy_flow_keys = [
        "awaiting_activate_plan_id",
        "awaiting_user_stats_id",
        "awaiting_wallet",
        "awaiting_pk",
    ]
    had_legacy_state = any(context.user_data.get(key) for key in legacy_flow_keys)
    context.user_data.clear()

    if had_legacy_state:
        await update.message.reply_text(
            "⚠️ Ese flujo de Telegram ya fue retirado. Continúa desde la MiniApp para evitar inconsistencias.",
            reply_markup=_launcher_menu(user_id),
            parse_mode="Markdown",
        )
        return

    await update.message.reply_text(
        "💡 Este bot ahora se usa desde la MiniApp. Telegram queda para alertas, notificaciones y acceso rápido.",
        reply_markup=_launcher_menu(user_id),
        parse_mode="Markdown",
    )


# ============================================================
# ACTIVAR / DESACTIVAR TRADING
# ============================================================

async def activate(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    await _send_miniapp_redirect_message(q, q.from_user.id, "▶ *La activación de trading ahora vive en la MiniApp*")

async def deactivate(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    await _send_miniapp_redirect_message(q, q.from_user.id, "⏸ *La pausa de trading ahora vive en la MiniApp*")

# ============================================================
# OPERACIONES
# ============================================================

async def operations(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    await _send_miniapp_redirect_message(q, q.from_user.id, "📈 *Las operaciones y el track record ahora se consultan en la MiniApp*")

# ============================================================
# REFERIDOS
# ============================================================

async def referrals(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    await _send_miniapp_redirect_message(q, q.from_user.id, "👥 *Los referidos ahora se consultan desde la MiniApp*")

async def admin_panel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    if q.from_user.id != ADMIN_TELEGRAM_ID:
        await q.edit_message_text("⛔ Acceso no autorizado.", reply_markup=_launcher_menu(q.from_user.id))
        return
    await _send_miniapp_redirect_message(q, q.from_user.id, "🛠 *La administración operativa ahora se hace desde la MiniApp*")

async def activate_plan(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()

    if q.from_user.id != ADMIN_TELEGRAM_ID:
        await q.edit_message_text("⛔ Acceso no autorizado.", reply_markup=main_menu(q.from_user.id))
        return

    context.user_data.clear()
    context.user_data["awaiting_activate_plan_id"] = True
    await q.edit_message_text(
        "✅ *Activar Plan Premium*\n\nIngresa el *ID de Telegram* del usuario:",
        parse_mode="Markdown"
    )


# ============================================================
# ADMIN – INFORMACIÓN VISUAL
# ============================================================

async def admin_visual(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()

    if q.from_user.id != ADMIN_TELEGRAM_ID:
        await q.edit_message_text("⛔ Acceso no autorizado.", reply_markup=main_menu(q.from_user.id))
        return

    stats = get_admin_visual_stats() or {}
    msg = (
        "📊 *INFORMACIÓN VISUAL (ADMIN)*\n"
        "───────────────────────────\n"
        f"👤 Total registrados: `{stats.get('total_users', 0)}`\n"
        f"🆓 Free antiguos: `{stats.get('free_old', 0)}`\n"
        f"💎 Premium activos: `{stats.get('premium_active', 0)}`\n"
        f"⌛ Premium vencidos: `{stats.get('premium_expired', 0)}`\n"
        "───────────────────────────"
    )

    await q.edit_message_text(msg, reply_markup=main_menu(q.from_user.id), parse_mode="Markdown")




# ============================================================
# ADMIN – ESTADÍSTICAS DE TRADING (24h / 7d / 30d)
# ============================================================

def _format_pf(pf):
    try:
        if pf == float("inf"):
            return "∞"
        return f"{float(pf):.2f}"
    except Exception:
        return "0.00"



# ============================================================
# ADMIN – ESTADÍSTICAS POR USUARIO (24h / 7d / 30d)
# ============================================================

def _admin_user_stats_menu():
    kb = [
        [
            InlineKeyboardButton("📅 24h", callback_data="admin_user_stats_24h"),
            InlineKeyboardButton("📆 7d", callback_data="admin_user_stats_7d"),
            InlineKeyboardButton("🗓 30d", callback_data="admin_user_stats_30d"),
        ],
        [InlineKeyboardButton("♻️ Reset Usuario", callback_data="admin_user_stats_reset_confirm")],
        [InlineKeyboardButton("⬅ Volver", callback_data="admin_panel")],
    ]
    return InlineKeyboardMarkup(kb)


async def admin_user_stats_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()

    if q.from_user.id != ADMIN_TELEGRAM_ID:
        await q.edit_message_text("⛔ Acceso no autorizado.", reply_markup=main_menu(q.from_user.id))
        return

    context.user_data.clear()
    context.user_data["awaiting_user_stats_id"] = True
    await q.edit_message_text(
        "👤 *ESTADÍSTICAS POR USUARIO*\n\nIngresa el *ID de Telegram* del usuario:",
        parse_mode="Markdown"
    )


async def _admin_user_stats_show(update: Update, context: ContextTypes.DEFAULT_TYPE, label: str, hours: int):
    q = update.callback_query
    await q.answer()

    if q.from_user.id != ADMIN_TELEGRAM_ID:
        await q.edit_message_text("⛔ Acceso no autorizado.", reply_markup=main_menu(q.from_user.id))
        return

    target_id = context.user_data.get("user_stats_target_id")
    if not target_id:
        await q.edit_message_text(
            "👤 *ESTADÍSTICAS POR USUARIO*\n\n⚠️ No hay usuario seleccionado.\nPulsa el botón nuevamente e ingresa el ID.",
            parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("👤 Elegir Usuario", callback_data="admin_user_stats_start")],[InlineKeyboardButton("⬅ Volver", callback_data="admin_panel")]]),
        )
        return

    stats = get_user_trade_stats(user_id=int(target_id), hours=int(hours)) or {}

    total = int(stats.get("total", 0) or 0)
    wins = int(stats.get("wins", 0) or 0)
    losses = int(stats.get("losses", 0) or 0)
    break_evens = int(stats.get("break_evens", 0) or 0)
    win_rate = float(stats.get("win_rate", 0.0) or 0.0)
    win_rate_decisive = float(stats.get("win_rate_decisive", 0.0) or 0.0)
    pnl_total = float(stats.get("pnl_total", 0.0) or 0.0)
    gross_profit = float(stats.get("gross_profit", 0.0) or 0.0)
    gross_loss = float(stats.get("gross_loss", 0.0) or 0.0)
    pf = stats.get("profit_factor", 0.0)

    if total == 0:
        msg = (
            f"👤 *ESTADÍSTICAS POR USUARIO*\n"
            f"Usuario: `{int(target_id)}`\n"
            "───────────────────────────\n"
            f"Período: *{label}*\n"
            "Sin datos en este período.\n"
            "───────────────────────────"
        )
    else:
        msg = (
            f"👤 *ESTADÍSTICAS POR USUARIO*\n"
            f"Usuario: `{int(target_id)}`\n"
            "───────────────────────────\n"
            f"📈 *ESTADÍSTICAS ({label})*\n"
            "───────────────────────────\n"
            f"🧾 Trades: `{total}`\n"
            f"✅ Wins: `{wins}`\n"
            f"❌ Losses: `{losses}`\n"
            f"⚪ BreakEven: `{break_evens}`\n"
            f"🎯 WinRate: `{win_rate:.2f}%`\n"
            f"🎯 WinRate s/BE: `{win_rate_decisive:.2f}%`\n"
            "───────────────────────────\n"
            f"💰 PnL Neto: `{pnl_total:.6f}` USDC\n"
            f"🟢 Ganancias: `{gross_profit:.6f}`\n"
            f"🔴 Pérdidas: `{gross_loss:.6f}`\n"
            f"📊 Profit Factor: `{_format_pf(pf)}`\n"
            "───────────────────────────"
        )

    await q.edit_message_text(msg, parse_mode="Markdown", reply_markup=_admin_user_stats_menu())


async def admin_user_stats_24h(update: Update, context: ContextTypes.DEFAULT_TYPE):
    return await _admin_user_stats_show(update, context, "24h", 24)


async def admin_user_stats_7d(update: Update, context: ContextTypes.DEFAULT_TYPE):
    return await _admin_user_stats_show(update, context, "7 días", 24 * 7)


async def admin_user_stats_30d(update: Update, context: ContextTypes.DEFAULT_TYPE):
    return await _admin_user_stats_show(update, context, "30 días", 24 * 30)


async def admin_user_stats_reset_confirm(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()

    if q.from_user.id != ADMIN_TELEGRAM_ID:
        await q.edit_message_text("⛔ Acceso no autorizado.", reply_markup=main_menu(q.from_user.id))
        return

    target_id = context.user_data.get("user_stats_target_id")
    if not target_id:
        await q.edit_message_text(
            "⚠️ No hay usuario seleccionado.",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("👤 Elegir Usuario", callback_data="admin_user_stats_start")],[InlineKeyboardButton("⬅ Volver", callback_data="admin_panel")]]),
        )
        return

    kb = [[
        InlineKeyboardButton("✅ Sí, resetear", callback_data="admin_user_stats_reset_do"),
        InlineKeyboardButton("❌ Cancelar", callback_data="admin_user_stats_24h"),
    ]]

    msg = (
        "♻️ *RESETEAR ESTADÍSTICAS (USUARIO)*\n"
        "───────────────────────────\n"
        f"Usuario: `{int(target_id)}`\n\n"
        "Esto reinicia el conteo de estadísticas (24h/7d/30d)\n"
        "a partir de *ahora* para este usuario.\n\n"
        "✅ El historial de operaciones NO se borra.\n"
        "Solo cambia el punto de inicio para el panel de estadísticas.\n"
        "───────────────────────────\n"
        "¿Confirmas el reset?"
    )

    await q.edit_message_text(msg, parse_mode="Markdown", reply_markup=InlineKeyboardMarkup(kb))


async def admin_user_stats_reset_do(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()

    if q.from_user.id != ADMIN_TELEGRAM_ID:
        await q.edit_message_text("⛔ Acceso no autorizado.", reply_markup=main_menu(q.from_user.id))
        return

    target_id = context.user_data.get("user_stats_target_id")
    if not target_id:
        await q.edit_message_text(
            "⚠️ No hay usuario seleccionado.",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("👤 Elegir Usuario", callback_data="admin_user_stats_start")],[InlineKeyboardButton("⬅ Volver", callback_data="admin_panel")]]),
        )
        return

    try:
        reset_user_trade_stats_epoch(int(target_id))
        msg = (
            "✅ *Stats del usuario reseteadas*\n"
            f"Usuario: `{int(target_id)}`\n\n"
            "Desde este momento las estadísticas (24h/7d/30d)\n"
            "comienzan en *cero* para este usuario."
        )
    except Exception as e:
        msg = f"⚠ No se pudo resetear stats del usuario: `{e}`"

    await q.edit_message_text(msg, parse_mode="Markdown", reply_markup=_admin_user_stats_menu())

async def admin_stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()

    if q.from_user.id != ADMIN_TELEGRAM_ID:
        await q.edit_message_text("⛔ Acceso no autorizado.", reply_markup=main_menu(q.from_user.id))
        return

    kb = [
        [InlineKeyboardButton("📅 Últimas 24h", callback_data="admin_stats_24h")],
        [InlineKeyboardButton("📆 Últimos 7 días", callback_data="admin_stats_7d")],
        [InlineKeyboardButton("🗓 Últimos 30 días", callback_data="admin_stats_30d")],
        [InlineKeyboardButton("♻️ Reset Stats", callback_data="admin_stats_reset_confirm")],
        [InlineKeyboardButton("⬅ Volver", callback_data="admin_panel")],
    ]
    await q.edit_message_text(
        "📈 *ESTADÍSTICAS DE TRADING (ADMIN)*\nSelecciona el período:",
        parse_mode="Markdown",
        reply_markup=InlineKeyboardMarkup(kb),
    )


async def _admin_stats_show(update: Update, context: ContextTypes.DEFAULT_TYPE, label: str, hours: int):
    q = update.callback_query
    await q.answer()

    if q.from_user.id != ADMIN_TELEGRAM_ID:
        await q.edit_message_text("⛔ Acceso no autorizado.", reply_markup=main_menu(q.from_user.id))
        return

    stats = get_admin_trade_stats(hours=hours) or {}

    total = int(stats.get("total", 0) or 0)
    wins = int(stats.get("wins", 0) or 0)
    losses = int(stats.get("losses", 0) or 0)
    break_evens = int(stats.get("break_evens", 0) or 0)
    win_rate = float(stats.get("win_rate", 0.0) or 0.0)
    win_rate_decisive = float(stats.get("win_rate_decisive", 0.0) or 0.0)
    pnl_total = float(stats.get("pnl_total", 0.0) or 0.0)
    gross_profit = float(stats.get("gross_profit", 0.0) or 0.0)
    gross_loss = float(stats.get("gross_loss", 0.0) or 0.0)
    pf = stats.get("profit_factor", 0.0)

    if total == 0:
        msg = (
            f"📈 *ESTADÍSTICAS ({label})*\n"
            "───────────────────────────\n"
            "Sin datos en este período.\n"
            "───────────────────────────"
        )
    else:
        msg = (
            f"📈 *ESTADÍSTICAS ({label})*\n"
            "───────────────────────────\n"
            f"🧾 Trades: `{total}`\n"
            f"✅ Wins: `{wins}`\n"
            f"❌ Losses: `{losses}`\n"
            f"⚪ BreakEven: `{break_evens}`\n"
            f"🎯 WinRate: `{win_rate:.2f}%`\n"
            f"🎯 WinRate s/BE: `{win_rate_decisive:.2f}%`\n"
            "───────────────────────────\n"
            f"💰 PnL Neto: `{pnl_total:.6f}` USDC\n"
            f"🟢 Ganancias: `{gross_profit:.6f}`\n"
            f"🔴 Pérdidas: `{gross_loss:.6f}`\n"
            f"📊 Profit Factor: `{_format_pf(pf)}`\n"
            "───────────────────────────"
        )

    kb = [
        [InlineKeyboardButton("📅 24h", callback_data="admin_stats_24h"),
         InlineKeyboardButton("📆 7d", callback_data="admin_stats_7d"),
         InlineKeyboardButton("🗓 30d", callback_data="admin_stats_30d")],
        [InlineKeyboardButton("♻️ Reset Stats", callback_data="admin_stats_reset_confirm")],
        [InlineKeyboardButton("⬅ Volver", callback_data="admin_stats")],
    ]

    await q.edit_message_text(msg, parse_mode="Markdown", reply_markup=InlineKeyboardMarkup(kb))


async def admin_stats_24h(update: Update, context: ContextTypes.DEFAULT_TYPE):
    return await _admin_stats_show(update, context, "24h", 24)


async def admin_stats_7d(update: Update, context: ContextTypes.DEFAULT_TYPE):
    return await _admin_stats_show(update, context, "7 días", 24 * 7)


async def admin_stats_30d(update: Update, context: ContextTypes.DEFAULT_TYPE):
    return await _admin_stats_show(update, context, "30 días", 24 * 30)


async def admin_stats_reset_confirm(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()

    if q.from_user.id != ADMIN_TELEGRAM_ID:
        await q.edit_message_text("⛔ Acceso no autorizado.", reply_markup=main_menu(q.from_user.id))
        return

    kb = [
        [
            InlineKeyboardButton("✅ Sí, resetear", callback_data="admin_stats_reset_do"),
            InlineKeyboardButton("❌ Cancelar", callback_data="admin_stats"),
        ]
    ]

    msg = (
        "♻️ *RESETEAR ESTADÍSTICAS*\n"
        "───────────────────────────\n"
        "Esto reinicia el conteo de estadísticas (24h/7d/30d)\n"
        "a partir de *ahora*.\n\n"
        "✅ El historial de operaciones NO se borra.\n"
        "Solo cambia el punto de inicio para el panel de estadísticas.\n"
        "───────────────────────────\n"
        "¿Confirmas el reset?"
    )

    await q.edit_message_text(msg, parse_mode="Markdown", reply_markup=InlineKeyboardMarkup(kb))


async def admin_stats_reset_do(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()

    if q.from_user.id != ADMIN_TELEGRAM_ID:
        await q.edit_message_text("⛔ Acceso no autorizado.", reply_markup=main_menu(q.from_user.id))
        return

    try:
        reset_admin_trade_stats_epoch()
        msg = (
            "✅ *Stats reseteadas*\n"
            "Desde este momento las estadísticas (24h/7d/30d)\n"
            "comienzan en *cero* para la nueva configuración."
        )
    except Exception as e:
        msg = f"⚠ No se pudo resetear stats: `{e}`"

    kb = [
        [InlineKeyboardButton("📈 Ver Estadísticas", callback_data="admin_stats")],
        [InlineKeyboardButton("⬅ Admin", callback_data="admin_panel")],
    ]
    await q.edit_message_text(msg, parse_mode="Markdown", reply_markup=InlineKeyboardMarkup(kb))


# ============================================================
# POLÍTICAS
# ============================================================

POLICY_TEXT = (
    "📜 *POLÍTICAS Y CONDICIONES DE USO*\n"
    "───────────────────────────\n"
    "1) Este bot es una herramienta automática y NO es asesoría financiera.\n"
    "2) El trading de criptomonedas implica alto riesgo de pérdida parcial o total del capital.\n"
    "3) El usuario es el único responsable de su capital, su cuenta del exchange y sus credenciales (wallet/private key).\n"
    "4) Resultados pasados NO garantizan resultados futuros.\n"
    "5) El bot ejecuta operaciones automáticamente mientras el trading esté activo.\n"
    "6) Queda prohibido el uso para fraude, suplantación, lavado de dinero o actividades ilegales.\n"
    "7) No se aceptarán reclamaciones por pérdidas financieras derivadas de la volatilidad del mercado.\n"
    "8) El servicio puede suspenderse si se detecta abuso.\n"
    "───────────────────────────\n"
    "Léelas con atención. La *confirmación final* que habilita la operativa se registra dentro de la *MiniApp*.\n"
)

async def policies(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    kb = [
        [InlineKeyboardButton("✅ Ya las leí", callback_data="policies_accept")],
        [InlineKeyboardButton("⬅️ Volver", callback_data="back")],
    ]
    await q.edit_message_text(POLICY_TEXT, parse_mode="Markdown", reply_markup=InlineKeyboardMarkup(kb))

async def policies_accept(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    already_confirmed = has_accepted_terms(q.from_user.id)
    message = (
        "📜 *Políticas revisadas.*\n\n"
        "El siguiente paso es abrir la *MiniApp* y usar *Confirmar aceptación* para registrar el consentimiento que habilita la operativa."
    )
    if already_confirmed:
        message = (
            "✅ *La confirmación de políticas ya está registrada.*\n\n"
            "Puedes volver a la *MiniApp* para seguir operando con normalidad."
        )
    await q.edit_message_text(
        message,
        parse_mode="Markdown",
        reply_markup=main_menu(q.from_user.id),
    )

# ============================================================
# INFO
# ============================================================

async def info(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()

    user_id = q.from_user.id
    msg = (
        "ℹ️ *INFORMACIÓN DEL SERVICIO*\n"
        "───────────────────────────\n"
        "• Telegram queda para notificaciones, alertas y acceso rápido.\n"
        "• La configuración, control, historial y administración se gestionan desde la MiniApp.\n"
        "• Si cambias wallet, private key, plan o estado operativo, hazlo desde la MiniApp para mantener coherencia."
    )
    await q.edit_message_text(msg, parse_mode="Markdown", reply_markup=_launcher_menu(user_id))

async def back_to_main(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    await q.edit_message_text(
        _miniapp_entry_text(
            terms_accepted=has_accepted_terms(q.from_user.id),
            is_admin=(q.from_user.id == ADMIN_TELEGRAM_ID),
        ),
        reply_markup=_launcher_menu(q.from_user.id),
        parse_mode="Markdown",
    )

# ============================================================
# ROUTER
# ============================================================

routes = {
    "dashboard": dashboard,
    "wallet_menu": wallet_menu,
    "set_wallet": set_wallet,
    "set_pk": set_pk,
    "activate": activate,
    "deactivate": deactivate,
    "operations": operations,
    "referrals": referrals,
    "activate_plan": activate_plan,
    "admin_visual": admin_visual,
    "admin_stats": admin_stats,
    "admin_stats_24h": admin_stats_24h,
    "admin_stats_7d": admin_stats_7d,
    "admin_stats_30d": admin_stats_30d,
    "admin_stats_reset_confirm": admin_stats_reset_confirm,
    "admin_stats_reset_do": admin_stats_reset_do,
    "admin_user_stats_start": admin_user_stats_start,
    "admin_user_stats_24h": admin_user_stats_24h,
    "admin_user_stats_7d": admin_user_stats_7d,
    "admin_user_stats_30d": admin_user_stats_30d,
    "admin_user_stats_reset_confirm": admin_user_stats_reset_confirm,
    "admin_user_stats_reset_do": admin_user_stats_reset_do,
    "admin_panel": admin_panel,
    "info": info,
    "policies": policies,
    "policies_accept": policies_accept,
    "back": back_to_main,
}


async def callback_router(update: Update, context: ContextTypes.DEFAULT_TYPE):
    data = update.callback_query.data
    handler = routes.get(data)
    if handler:
        await handler(update, context)


# ============================================================
# MAIN
# ============================================================

async def bot_runtime_heartbeat(context: ContextTypes.DEFAULT_TYPE):
    touch_runtime_component(
        'telegram_bot',
        'online',
        metadata={
            'bot_username': BOT_USERNAME,
            'miniapp_url_configured': bool(MINIAPP_URL),
            'admin_telegram_id': int(ADMIN_TELEGRAM_ID or 0),
        },
    )


def run_bot():

    app = (
        ApplicationBuilder()
        .token(TELEGRAM_BOT_TOKEN)
        .build()
    )

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("menu", start))
    app.add_handler(CommandHandler("panel", start))
    app.add_handler(CommandHandler("miniapp", miniapp_command))
    app.add_handler(CallbackQueryHandler(callback_router))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, text_handler))

    # ✅ CORRECCIÓN CRÍTICA:
    # Se pasa la MISMA Application al trading loop
    touch_runtime_component(
        'telegram_bot',
        'online',
        metadata={
            'bot_username': BOT_USERNAME,
            'miniapp_url_configured': bool(MINIAPP_URL),
            'admin_telegram_id': int(ADMIN_TELEGRAM_ID or 0),
            'phase': 'startup',
        },
    )

    app.job_queue.run_repeating(bot_runtime_heartbeat, interval=30, first=1)
    app.job_queue.run_once(
        lambda ctx: asyncio.create_task(trading_loop(app)),
        when=3
    )

    print("🤖 Trading X Hyper Pro – Bot ejecutándose...")
    app.run_polling()


# ============================================================
# ENTRYPOINT
# ============================================================

if __name__ == "__main__":
    run_bot()
