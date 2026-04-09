# ============================================================
# CONFIGURACIÓN GLOBAL – TRADING X HYPER PRO
# PRODUCCIÓN REAL
# ============================================================

import os

# ============================================================
# BOT DE TELEGRAM
# ============================================================

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
BOT_NAME = "TradingXHyperProBot"

if not TELEGRAM_BOT_TOKEN:
    raise RuntimeError("❌ TELEGRAM_BOT_TOKEN no está definido en variables de entorno")

# ============================================================
# ADMIN (SEGURIDAD)
# ============================================================

ADMIN_TELEGRAM_ID = int(os.getenv("ADMIN_TELEGRAM_ID", "0"))
if ADMIN_TELEGRAM_ID <= 0:
    raise RuntimeError("❌ ADMIN_TELEGRAM_ID no está definido en variables de entorno")

# ============================================================
# CONTACTO ADMIN (WHATSAPP)
# ============================================================

ADMIN_WHATSAPP_LINK = os.getenv("ADMIN_WHATSAPP_LINK", "").strip()

# ============================================================
# BASE DE DATOS – MongoDB Atlas
# ============================================================

MONGO_URI = os.getenv("MONGO_URL")
DB_NAME = os.getenv("MONGO_DB_NAME", "TRADING_X_HIPER_PRO")

if not MONGO_URI:
    raise RuntimeError("❌ MONGO_URL no está definido en variables de entorno")

# ============================================================
# EXCHANGE – HYPERLIQUID (PRODUCCIÓN)
# ============================================================

HYPER_BASE_URL = "https://api.hyperliquid.xyz"
DEFAULT_PAIR = "BTC-USDC"

REQUEST_TIMEOUT = int(os.getenv("REQUEST_TIMEOUT", "10"))

# ============================================================
# SISTEMA DE SCANEO AUTOMÁTICO DE MERCADO (PROD)
# ============================================================

SCAN_INTERVAL = int(os.getenv("SCAN_INTERVAL", "15"))
SCANNER_DEPTH = int(os.getenv("SCANNER_DEPTH", "80"))

SCANNER_MIN_24H_NOTIONAL = float(os.getenv("SCANNER_MIN_24H_NOTIONAL", "2000000"))  # 2,000,000
SCANNER_MIN_OPEN_INTEREST = float(os.getenv("SCANNER_MIN_OPEN_INTEREST", "250000"))  # 250,000
SCANNER_MAX_SPREAD_BPS = float(os.getenv("SCANNER_MAX_SPREAD_BPS", "25"))
SCANNER_MIN_TOP_BOOK_NOTIONAL = float(os.getenv("SCANNER_MIN_TOP_BOOK_NOTIONAL", "2000"))  # 2,000 USDC
SCANNER_SHORTLIST_DEPTH_FOR_L2 = int(os.getenv("SCANNER_SHORTLIST_DEPTH_FOR_L2", "25"))

SCANNER_STATS_CACHE_TTL = float(os.getenv("SCANNER_STATS_CACHE_TTL", "5.0"))
SCANNER_L2_CACHE_TTL = float(os.getenv("SCANNER_L2_CACHE_TTL", "1.5"))

# ============================================================
# ESTRATEGIA – (Compatibilidad)
# ============================================================

ENTRY_SIGNAL_THRESHOLD = float(os.getenv("ENTRY_SIGNAL_THRESHOLD", "0.58"))

# ============================================================
# TP / SL (ALINEADO CON trading_engine.py)
# ------------------------------------------------------------
# IMPORTANTE:
# - Todo es % de PRECIO (no ROE, no margen).
# - TP de activación == SL (misma cifra), como acordaste.
# - Trailing (cierre dinámico) = retroceso desde el máximo.
# ============================================================

# ✅ TP activación (igual que SL): 1.17% precio
TP_ACTIVATE_TRAIL_PRICE = float(os.getenv("TP_ACTIVATE_TRAIL_PRICE", "0.0117"))   # +1.17% precio

# ✅ Ajuste pedido por ti:
#    Antes: 0.00585 (0.585% precio). Ahora: 0.00100 (0.10% precio)
TRAIL_RETRACE_PRICE     = float(os.getenv("TRAIL_RETRACE_PRICE", "0.00100"))     # 0.10% precio

# ✅ SL fijo (igual al TP de activación)
SL_MIN_PRICE = float(os.getenv("SL_MIN_PRICE", "0.0117"))  # -1.17% precio
SL_MAX_PRICE = float(os.getenv("SL_MAX_PRICE", "0.0117"))  # fijo = min = max

# ------------------------------------------------------------
# Backwards-compat / legado (por si otra parte del bot usa estos)
# Los igualamos para que NO haya incoherencias.
# ============================================================

TP_MIN = float(os.getenv("TP_MIN", str(TP_ACTIVATE_TRAIL_PRICE)))
TP_MAX = float(os.getenv("TP_MAX", str(TP_ACTIVATE_TRAIL_PRICE)))

SL_MIN = float(os.getenv("SL_MIN", str(SL_MIN_PRICE)))
SL_MAX = float(os.getenv("SL_MAX", str(SL_MAX_PRICE)))

# ============================================================
# GESTIÓN DE RIESGO
# ============================================================

MIN_CAPITAL = float(os.getenv("MIN_CAPITAL", "5.0"))
POSITION_PERCENT = float(os.getenv("POSITION_PERCENT", "1.0"))

# ✅ Un solo trade a la vez
MAX_CONCURRENT_TRADES = int(os.getenv("MAX_CONCURRENT_TRADES", "1"))

# ============================================================
# SISTEMA DE FEES
# ============================================================

OWNER_FEE_PERCENT = float(os.getenv("OWNER_FEE_PERCENT", "0.15"))
REFERRAL_FEE_PERCENT = float(os.getenv("REFERRAL_FEE_PERCENT", "0.05"))

DAILY_FEE_COLLECTION_HOUR = int(os.getenv("DAILY_FEE_COLLECTION_HOUR", "23"))
DAILY_FEE_COLLECTION_MINUTE = int(os.getenv("DAILY_FEE_COLLECTION_MINUTE", "59"))

REFERRAL_PAYOUT_DAY = os.getenv("REFERRAL_PAYOUT_DAY", "sunday")
REFERRAL_PAYOUT_HOUR = int(os.getenv("REFERRAL_PAYOUT_HOUR", "23"))
REFERRAL_PAYOUT_MINUTE = int(os.getenv("REFERRAL_PAYOUT_MINUTE", "59"))

# ============================================================
# LOGS / SISTEMA
# ============================================================

VERBOSE_LOGS = (os.getenv("VERBOSE_LOGS", "False").lower() == "true")
PRODUCTION_MODE = (os.getenv("PRODUCTION_MODE", "True").lower() == "true")


# ============================================================
# MINIAPP / API BACKEND
# ============================================================

MINIAPP_URL = os.getenv("MINIAPP_URL", "").strip()
API_PORT = int(os.getenv("PORT", os.getenv("API_PORT", "8000")))

WEBAPP_INITDATA_MAX_AGE_SECONDS = int(
    os.getenv("WEBAPP_INITDATA_MAX_AGE_SECONDS", "300")
)
WEBAPP_SESSION_TTL_SECONDS = int(
    os.getenv("WEBAPP_SESSION_TTL_SECONDS", "43200")
)
WEBAPP_SESSION_SECRET = os.getenv("WEBAPP_SESSION_SECRET", TELEGRAM_BOT_TOKEN)
PRIVATE_KEY_ENCRYPTION_SECRET = os.getenv(
    "PRIVATE_KEY_ENCRYPTION_SECRET",
    WEBAPP_SESSION_SECRET,
)
PRIVATE_KEY_ENCRYPTION_SECRET_FALLBACKS = [
    item.strip()
    for item in os.getenv("PRIVATE_KEY_ENCRYPTION_SECRET_FALLBACKS", "").split(",")
    if item.strip() and item.strip() != PRIVATE_KEY_ENCRYPTION_SECRET
]

MINIAPP_ALLOWED_ORIGINS = [
    origin.strip()
    for origin in os.getenv("MINIAPP_ALLOWED_ORIGINS", "*").split(",")
    if origin.strip()
]


# ============================================================
# PAGOS AUTOMÁTICOS USDT BEP-20
# ============================================================

def get_payment_network() -> str:
    return os.getenv("PAYMENT_NETWORK", "bep20").strip().lower() or "bep20"


def get_payment_token_symbol() -> str:
    return os.getenv("PAYMENT_TOKEN_SYMBOL", "USDT").strip().upper() or "USDT"


def get_payment_token_contract() -> str:
    return os.getenv("PAYMENT_TOKEN_CONTRACT", "").strip().lower()


def get_payment_receiver_address() -> str:
    return os.getenv("PAYMENT_RECEIVER_ADDRESS", "").strip().lower()


def get_bsc_rpc_http_url() -> str:
    return os.getenv("BSC_RPC_HTTP_URL", "").strip()


def get_payment_min_confirmations() -> int:
    try:
        return max(int(os.getenv("PAYMENT_MIN_CONFIRMATIONS", "3")), 1)
    except Exception:
        return 3


def get_payment_order_ttl_minutes() -> int:
    try:
        return max(int(os.getenv("PAYMENT_ORDER_TTL_MINUTES", "30")), 5)
    except Exception:
        return 30


def get_payment_unique_max_delta() -> float:
    try:
        value = float(os.getenv("PAYMENT_UNIQUE_MAX_DELTA", "0.150"))
    except Exception:
        return 0.150
    return max(0.001, min(value, 0.150))


def get_payment_token_decimals() -> int:
    try:
        return max(int(os.getenv("PAYMENT_TOKEN_DECIMALS", "18")), 0)
    except Exception:
        return 18


def get_payment_lookback_blocks() -> int:
    try:
        return max(int(os.getenv("PAYMENT_LOOKBACK_BLOCKS", "2500")), 100)
    except Exception:
        return 2500


def get_payment_configuration_status() -> dict:
    checks = [
        {
            "key": "BSC_RPC_HTTP_URL",
            "label": "RPC BSC",
            "value_present": bool(get_bsc_rpc_http_url()),
        },
        {
            "key": "PAYMENT_TOKEN_CONTRACT",
            "label": "Contrato del token",
            "value_present": bool(get_payment_token_contract()),
        },
        {
            "key": "PAYMENT_RECEIVER_ADDRESS",
            "label": "Wallet receptora",
            "value_present": bool(get_payment_receiver_address()),
        },
    ]
    missing = [item["key"] for item in checks if not item["value_present"]]
    return {
        "ready": not missing,
        "checks": checks,
        "missing_keys": missing,
        "network": get_payment_network(),
        "token_symbol": get_payment_token_symbol(),
    }


def is_payment_configuration_ready() -> bool:
    return bool(get_payment_configuration_status().get("ready"))
