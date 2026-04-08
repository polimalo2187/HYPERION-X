# ============================================================
# TRADING LOOP – Trading X Hyper Pro
# PRODUCCIÓN REAL 24/7 — BANK GRADE (BLINDADO)
# ============================================================

import asyncio
import logging
import random
import time
from datetime import datetime

from telegram.ext import Application
from telegram import error as tg_error

from app.database import (
    get_all_users,
    user_is_ready,
    is_plan_expired,
    should_notify_expired,
    mark_expiry_notified,
    save_last_open,
    save_last_close,
    touch_runtime_component,
    get_user_cycle_policy,
    touch_user_operational_state,
    get_user_public_snapshot,
)
from app.trading_engine import execute_trade_cycle
from app.config import SCAN_INTERVAL, ADMIN_TELEGRAM_ID

# ============================================================
# CONFIG BANK GRADE
# ============================================================

MAX_CONCURRENT_USERS = 5          # Control de carga
TRADE_TIMEOUT_SECONDS = 45        # Timeout duro por usuario
ERROR_BACKOFF_SECONDS = 3

# ✅ FIX: evita que al hacer deploy “arranque tirando órdenes” inmediatamente
STARTUP_GRACE_SECONDS = 20        # espera inicial antes de escanear/operar

# ✅ FIX: reparte llamadas (evita picos, evita todos al mismo símbolo al mismo tiempo)
USER_JITTER_MAX_SECONDS = 2.0     # jitter aleatorio por usuario antes de ejecutar su ciclo

# ============================================================
# STATE
# ============================================================

user_locks: dict[int, asyncio.Lock] = {}
telegram_blacklist: set[int] = set()

# timestamp de arranque del loop
_loop_started_at = 0.0

# ============================================================
# LOG HARDENING (evita leaks de token en httpx logs)
# ============================================================

def _harden_logging():
    try:
        logging.getLogger("httpx").setLevel(logging.WARNING)
        logging.getLogger("httpcore").setLevel(logging.WARNING)
    except Exception:
        pass

# ============================================================
# LOG
# ============================================================

def log(msg: str, level: str = "INFO"):
    try:
        safe_msg = str(msg).encode("utf-8", errors="replace").decode("utf-8", errors="replace")
    except Exception:
        safe_msg = str(msg)

    print(f"[LOOP {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')}] {level} {safe_msg}")

# ============================================================
# MENSAJERÍA SEGURA (BANK GRADE)
# ============================================================

async def send_message_safe(app: Application, user_id: int, text: str):
    if user_id in telegram_blacklist:
        return

    try:
        await app.bot.send_message(
            chat_id=user_id,
            text=text,
            parse_mode="Markdown"
        )

    except tg_error.Forbidden:
        telegram_blacklist.add(user_id)
        log(f"Usuario {user_id} bloqueó el bot (blacklisted)", "WARN")

    except tg_error.RetryAfter as e:
        await asyncio.sleep(int(e.retry_after) + 1)
        try:
            await app.bot.send_message(chat_id=user_id, text=text, parse_mode="Markdown")
        except Exception as inner_e:
            log(f"Error Telegram retry usuario {user_id}: {inner_e}", "ERROR")

    except Exception as e:
        log(f"Error Telegram usuario {user_id}: {e}", "ERROR")


_admin_push_last_sent: dict[str, float] = {}

def _admin_user_label(user_id: int) -> str:
    try:
        snap = get_user_public_snapshot(int(user_id)) or {}
        if snap.get('username'):
            return f"@{snap['username']}"
    except Exception:
        pass
    return f"user_id={int(user_id)}"


async def send_admin_push_safe(app: Application, text: str, *, dedupe_key: str | None = None, cooldown_seconds: int = 0):
    try:
        admin_id = int(ADMIN_TELEGRAM_ID or 0)
    except Exception:
        admin_id = 0
    if admin_id <= 0:
        return

    if dedupe_key and cooldown_seconds > 0:
        now_ts = time.time()
        last_ts = float(_admin_push_last_sent.get(dedupe_key) or 0.0)
        if now_ts - last_ts < float(cooldown_seconds):
            return
        _admin_push_last_sent[dedupe_key] = now_ts

    try:
        await app.bot.send_message(chat_id=admin_id, text=text)
    except tg_error.RetryAfter as e:
        await asyncio.sleep(int(e.retry_after) + 1)
        try:
            await app.bot.send_message(chat_id=admin_id, text=text)
        except Exception as inner_e:
            log(f"Error admin push retry: {inner_e}", "ERROR")
    except Exception as e:
        log(f"Error admin push: {e}", "ERROR")


def _compose_admin_open_message(user_id: int, open_data: dict) -> str:
    symbol = (open_data or {}).get('symbol') or (open_data or {}).get('coin') or '—'
    side = str((open_data or {}).get('side') or (open_data or {}).get('direction') or '—').upper()
    entry = (open_data or {}).get('entry_price')
    qty = (open_data or {}).get('qty')
    bits = ["🟢 Nueva operación abierta", f"Usuario: {_admin_user_label(user_id)}", f"Símbolo: {symbol}", f"Lado: {side}"]
    if entry is not None:
        bits.append(f"Entry: {entry}")
    if qty is not None:
        bits.append(f"Qty: {qty}")
    return "\n".join(bits)


def _compose_admin_close_message(user_id: int, close_data: dict) -> str:
    symbol = (close_data or {}).get('symbol') or (close_data or {}).get('coin') or '—'
    side = str((close_data or {}).get('side') or (close_data or {}).get('direction') or '—').upper()
    profit = (close_data or {}).get('profit')
    exit_reason = (close_data or {}).get('exit_reason') or (close_data or {}).get('close_source') or (close_data or {}).get('message')
    header = '✅ Operación cerrada'
    try:
        pnl = float(profit)
        if pnl < 0:
            header = '🔴 Operación cerrada'
    except Exception:
        pnl = None
    bits = [header, f"Usuario: {_admin_user_label(user_id)}", f"Símbolo: {symbol}", f"Lado: {side}"]
    if profit is not None:
        bits.append(f"PnL: {profit}")
    if exit_reason:
        bits.append(f"Motivo: {str(exit_reason)[:180]}")
    return "\n".join(bits)

# ============================================================
# EJECUCIÓN SEGURA POR USUARIO
# ============================================================

async def execute_user_cycle(app: Application, user_id: int, semaphore: asyncio.Semaphore):
    if user_id not in user_locks:
        user_locks[user_id] = asyncio.Lock()

    lock = user_locks[user_id]

    # evita reentradas por usuario
    if lock.locked():
        log(f"Usuario {user_id} ya en ejecución — skip")
        return None

    async with semaphore:
        async with lock:
            # ✅ jitter para repartir carga (por usuario)
            try:
                if USER_JITTER_MAX_SECONDS > 0:
                    await asyncio.sleep(random.uniform(0.0, float(USER_JITTER_MAX_SECONDS)))
            except Exception:
                pass

            try:
                touch_runtime_component(
                    'scanner',
                    'online',
                    metadata={
                        'user_id': int(user_id),
                        'phase': 'execute_cycle',
                    },
                )
                loop = asyncio.get_running_loop()

                result = await asyncio.wait_for(
                    loop.run_in_executor(None, execute_trade_cycle, user_id),
                    timeout=TRADE_TIMEOUT_SECONDS
                )

                touch_runtime_component(
                    'scanner',
                    'online',
                    metadata={
                        'user_id': int(user_id),
                        'phase': 'cycle_finished',
                        'last_event': (result or {}).get('event') if isinstance(result, dict) else None,
                        'symbol': (((result or {}).get('manager') or {}).get('symbol') if isinstance(result, dict) else None)
                            or (((result or {}).get('open') or {}).get('symbol') if isinstance(result, dict) else None),
                    },
                )
                return result

            except asyncio.TimeoutError:
                touch_runtime_component('scanner', 'error', metadata={'user_id': int(user_id), 'phase': 'timeout', 'error': 'execute_user_cycle timeout'})
                log(f"Timeout ejecución usuario {user_id}", "WARN")
                await send_admin_push_safe(app, f"⚠️ Timeout en ciclo de trading\nUsuario: {_admin_user_label(user_id)}\nEl ciclo excedió {TRADE_TIMEOUT_SECONDS}s.", dedupe_key=f"timeout:{user_id}", cooldown_seconds=300)
                return None

            except Exception as e:
                touch_runtime_component('scanner', 'error', metadata={'user_id': int(user_id), 'phase': 'exception', 'error': str(e)[:300]})
                log(f"Error crítico usuario {user_id}: {e}", "ERROR")
                await send_admin_push_safe(app, f"🚨 Error crítico de ejecución\nUsuario: {_admin_user_label(user_id)}\nDetalle: {str(e)[:250]}", dedupe_key=f"cycle_error:{user_id}:{str(e)[:80]}", cooldown_seconds=300)
                return None

# ============================================================
# LOOP PRINCIPAL
# ============================================================

async def trading_loop(app: Application):
    global _loop_started_at

    _harden_logging()
    _loop_started_at = time.time()

    log("Trading Loop iniciado — BANK GRADE 24/7")
    log(f"Startup grace: {STARTUP_GRACE_SECONDS}s (no escanea/operará durante este tiempo)")
    touch_runtime_component(
        'trading_loop',
        'online',
        metadata={
            'phase': 'started',
            'scan_interval': SCAN_INTERVAL,
            'startup_grace_seconds': STARTUP_GRACE_SECONDS,
        },
    )

    semaphore = asyncio.Semaphore(MAX_CONCURRENT_USERS)

    while True:
        try:
            # ✅ FIX: no arrancar “a operar” inmediatamente tras deploy/restart
            if STARTUP_GRACE_SECONDS > 0:
                elapsed = time.time() - float(_loop_started_at or time.time())
                if elapsed < float(STARTUP_GRACE_SECONDS):
                    touch_runtime_component(
                        'trading_loop',
                        'online',
                        metadata={
                            'phase': 'startup_grace',
                            'seconds_remaining': max(0, int(float(STARTUP_GRACE_SECONDS) - elapsed)),
                        },
                    )
                    await asyncio.sleep(1.0)
                    continue

            users = get_all_users() or []
            touch_runtime_component(
                'trading_loop',
                'online',
                metadata={
                    'phase': 'loop_tick',
                    'users_loaded': len(users),
                    'max_concurrent_users': MAX_CONCURRENT_USERS,
                },
            )
            log(f"Usuarios activos: {len(users)}")

            tasks = []
            task_user_ids = []

            for user in users:
                raw_user_id = user.get("user_id")
                if not raw_user_id:
                    continue

                try:
                    user_id = int(raw_user_id)
                except Exception:
                    log(f"user_id inválido: {raw_user_id}", "WARN")
                    continue

                try:
                    policy = get_user_cycle_policy(user_id)

                    if is_plan_expired(user_id) and should_notify_expired(user_id):
                        await send_message_safe(
                            app,
                            user_id,
                            "⛔ Tu plan ha vencido. Las nuevas entradas quedan bloqueadas hasta reactivar el acceso."
                        )
                        mark_expiry_notified(user_id)

                    if not policy.get('should_run_cycle', False):
                        touch_user_operational_state(
                            user_id,
                            policy.get('runtime_state') or 'idle',
                            policy.get('runtime_message') or 'Sin actividad operativa.',
                            mode=policy.get('runtime_mode'),
                            source='trading_loop',
                            live_trade=bool(policy.get('live_trade')),
                            active_symbol=policy.get('active_symbol'),
                            metadata={'phase': 'skip_policy'},
                        )
                        continue
                except Exception as e:
                    log(f"Error verificando readiness usuario {user_id}: {e}", "ERROR")
                    touch_user_operational_state(
                        user_id,
                        'error',
                        f'No se pudo evaluar el estado operativo: {str(e)[:180]}',
                        mode='error',
                        source='trading_loop',
                        metadata={'phase': 'policy_exception'},
                    )
                    continue

                touch_user_operational_state(
                    user_id,
                    'cycle_running',
                    'El motor está revisando esta cuenta en tiempo real.',
                    mode='cycle_running',
                    source='trading_loop',
                    live_trade=bool(policy.get('live_trade')),
                    active_symbol=policy.get('active_symbol'),
                    metadata={'phase': 'scheduled'},
                )
                tasks.append(execute_user_cycle(app, user_id, semaphore))
                task_user_ids.append(user_id)

            if not tasks:
                touch_runtime_component(
                    'scanner',
                    'online',
                    metadata={
                        'phase': 'idle',
                        'users_loaded': len(users),
                        'users_ready': 0,
                    },
                )
                await asyncio.sleep(max(1, int(SCAN_INTERVAL or 1)))
                continue

            results = await asyncio.gather(*tasks, return_exceptions=True)
            touch_runtime_component(
                'trading_loop',
                'online',
                metadata={
                    'phase': 'batch_completed',
                    'tasks_executed': len(tasks),
                },
            )

            for user_id, result in zip(task_user_ids, results):
                if isinstance(result, Exception):
                    log(f"Error ciclo usuario {user_id}: {result}", "ERROR")
                    touch_user_operational_state(
                        user_id,
                        'error',
                        f'El motor devolvió un error: {str(result)[:180]}',
                        mode='error',
                        source='trading_loop',
                        metadata={'phase': 'result_exception'},
                    )
                    await send_admin_push_safe(app, f"🚨 Error en resultado del loop\nUsuario: {_admin_user_label(user_id)}\nDetalle: {str(result)[:250]}", dedupe_key=f"result_error:{user_id}:{str(result)[:80]}", cooldown_seconds=300)
                    continue

                if not isinstance(result, dict):
                    policy = get_user_cycle_policy(user_id)
                    touch_user_operational_state(
                        user_id,
                        policy.get('runtime_state') or 'idle',
                        policy.get('runtime_message') or 'Sin cambios operativos en este ciclo.',
                        mode=policy.get('runtime_mode'),
                        source='trading_loop',
                        live_trade=bool(policy.get('live_trade')),
                        active_symbol=policy.get('active_symbol'),
                        metadata={'phase': 'result_empty'},
                    )
                    continue

                event_name = str(result.get('event') or '').upper()

                if event_name in ('OPEN', 'BOTH'):
                    touch_user_operational_state(
                        user_id,
                        'cycle_completed',
                        'Se abrió una operación y el motor quedó sincronizado con la nueva posición.',
                        mode='entries_enabled',
                        source='trading_loop',
                        live_trade=True,
                        active_symbol=((result.get('open') or {}).get('symbol')),
                        metadata={'phase': 'open_event'},
                    )
                elif event_name in ('CLOSE', 'RECONCILE_CLOSED', 'BOTH'):
                    policy = get_user_cycle_policy(user_id)
                    touch_user_operational_state(
                        user_id,
                        policy.get('runtime_state') or 'idle',
                        'El motor cerró o reconcilió una operación reciente y volvió a su estado operativo actual.',
                        mode=policy.get('runtime_mode'),
                        source='trading_loop',
                        live_trade=bool(policy.get('live_trade')),
                        active_symbol=policy.get('active_symbol'),
                        metadata={'phase': 'close_event', 'event': event_name},
                    )
                else:
                    policy = get_user_cycle_policy(user_id)
                    touch_user_operational_state(
                        user_id,
                        policy.get('runtime_state') or 'idle',
                        policy.get('runtime_message') or 'Ciclo completado sin nuevas aperturas.',
                        mode=policy.get('runtime_mode'),
                        source='trading_loop',
                        live_trade=bool(policy.get('live_trade')),
                        active_symbol=policy.get('active_symbol'),
                        metadata={'phase': 'cycle_complete', 'event': event_name or 'none'},
                    )

                # ================================
                # GUARDAR INFO OPERACIÓN (OPEN)
                # ================================
                if result.get("event") in ("OPEN", "BOTH"):
                    open_data = result.get("open") or {}
                    try:
                        save_last_open(user_id, open_data)
                    except Exception as e:
                        log(f"Error guardando last_open user {user_id}: {e}", "ERROR")

                    msg = open_data.get("message")
                    if msg:
                        await send_message_safe(app, user_id, msg)
                    try:
                        await send_admin_push_safe(app, _compose_admin_open_message(user_id, open_data))
                    except Exception as e:
                        log(f"Error push admin OPEN user {user_id}: {e}", "ERROR")

                # ================================
                # GUARDAR INFO OPERACIÓN (CLOSE)
                # ================================
                if result.get("event") in ("CLOSE", "BOTH"):
                    close_data = result.get("close") or {}
                    try:
                        save_last_close(user_id, close_data)
                    except Exception as e:
                        log(f"Error guardando last_close user {user_id}: {e}", "ERROR")

                    msg = close_data.get("message")
                    if msg:
                        await send_message_safe(app, user_id, msg)
                    try:
                        await send_admin_push_safe(app, _compose_admin_close_message(user_id, close_data))
                    except Exception as e:
                        log(f"Error push admin CLOSE user {user_id}: {e}", "ERROR")

        except Exception as e:
            log(f"FALLO SISTÉMICO trading_loop: {e}", "CRITICAL")
            await send_admin_push_safe(app, f"🚨 Fallo sistémico del trading loop\nDetalle: {str(e)[:250]}", dedupe_key=f"systemic:{str(e)[:120]}", cooldown_seconds=300)
            await asyncio.sleep(float(ERROR_BACKOFF_SECONDS or 3))

        await asyncio.sleep(max(1, int(SCAN_INTERVAL or 1)))
