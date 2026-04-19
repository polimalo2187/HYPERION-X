import os
import time
from typing import Dict, Any, List, Optional, Tuple

from app.strategies.base import BaseStrategy
from app.hyperliquid_client import make_request, norm_coin
from app.market_context import build_market_context

TF_5M = "5m"
TF_15M = "15m"
TF_1H = "1h"
LOOKBACK_5M = 320
LOOKBACK_15M = 240
LOOKBACK_1H = 240

EMA_FAST = 20
EMA_MID = 50
EMA_SLOW = 200
ADX_PERIOD = 14
ATR_PERIOD = 14
EMA_SLOPE_LOOKBACK = 6

_ALLOWED_ENV = os.getenv("ALLOWED_TRADE_SYMBOLS", "").strip()
ALLOWED_SYMBOLS = {x.strip().upper() for x in _ALLOWED_ENV.split(",") if x.strip()}

_BLOCKED_MEME_ENV = os.getenv("BLOCKED_MEME_KEYWORDS", "").strip()
_DEFAULT_MEME_KEYWORDS = {
    "DOGE", "SHIB", "PEPE", "BONK", "FLOKI", "WIF", "POPCAT", "PENGU", "TURBO",
    "MOG", "BOME", "MYRO", "BRETT", "NEIRO", "MEME", "BABYDOGE", "KISHU", "WOJAK",
    "PONKE", "MEW", "TRUMP", "MAGA", "HARRYPOTTEROBAMA", "HYPE",
}
BLOCKED_MEME_KEYWORDS = {x.strip().upper() for x in _BLOCKED_MEME_ENV.split(",") if x.strip()} or _DEFAULT_MEME_KEYWORDS
MIN_CANDLES_REQUIRED = 260
MIN_NONZERO_VOLUME_RATIO = 0.92

# MTF simple pura: sesgo 1H + confirmación 15M + reset/continuación 5M.
H1_ADX_MIN = 12.0
M15_ADX_MIN = 11.0
M5_ADX_MIN = 9.5
ATR_PCT_MIN = 0.00075
ATR_PCT_MAX = 0.0180
TREND_STACK_MIN_PCT = 0.00030
RESET_LOOKBACK_BARS = 5
RESET_TOUCH_TOL_ATR = 0.38
RESET_BREAK_TOL_ATR = 0.48
TRIGGER_MAX_EMA20_EXTENSION_ATR = 0.95
CONTINUATION_CONFIRM_TOL_ATR = 0.10
MTF_SL_MIN_PCT = 0.0045
MTF_SL_MAX_PCT = 0.0068
MTF_SL_ATR_MULT = 0.88
MTF_SL_BUFFER_ATR = 0.10
MTF_TP_MIN_PCT = 0.0060
MTF_TP_MAX_PCT = 0.0085
MTF_RR_MIN = 1.05
MTF_RR_MAX = 1.30
MIN_RR_TO_SIGNAL = 0.95
MAX_SCORE = 100.0
MIN_SCORE_TO_SIGNAL = 69.0
STRENGTH_MIN = 0.20
STRENGTH_MAX = 0.97
LOG_SIGNAL_DIAGNOSTICS = True


def _log(msg: str):
    try:
        print(f"[STRATEGY {time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime())}] {msg}")
    except Exception:
        pass


def _interval_ms(interval: str) -> int:
    return {"5m": 300_000, "15m": 900_000, "1h": 3_600_000}.get(interval, 0)


def _parse_candle(x: dict) -> Optional[dict]:
    try:
        return {
            "t": int(x.get("t", 0)),
            "o": float(x.get("o", 0)),
            "h": float(x.get("h", 0)),
            "l": float(x.get("l", 0)),
            "c": float(x.get("c", 0)),
            "v": float(x.get("v", 0)),
        }
    except Exception:
        return None


def _fetch_candles(coin: str, interval: str, limit: int):
    coin = norm_coin(coin)
    if not coin:
        return [], "BAD_SYMBOL"
    step = _interval_ms(interval)
    if step <= 0:
        return [], "BAD_INTERVAL"

    try:
        now = int(time.time() * 1000)
        start = now - step * max(int(limit), 50)
        payload = {
            "type": "candleSnapshot",
            "req": {
                "coin": coin,
                "interval": interval,
                "startTime": start,
                "endTime": now,
            },
        }
        resp = make_request("/info", payload, retries=3, backoff=0.35, timeout=5.0)
    except Exception:
        return [], "API_FAIL"

    if resp == {} or resp is None:
        return [], "API_FAIL"
    if not isinstance(resp, list) or not resp:
        return [], "EMPTY"

    candles: List[dict] = []
    for row in resp:
        if isinstance(row, dict):
            item = _parse_candle(row)
            if item:
                candles.append(item)

    if not candles:
        return [], "EMPTY"

    try:
        candles.sort(key=lambda x: int(x.get("t", 0)))
    except Exception:
        pass

    if len(candles) > limit:
        candles = candles[-limit:]
    return candles, "OK"


def _extract(candles):
    o, h, l, c, v = [], [], [], [], []
    for x in candles:
        o.append(float(x["o"]))
        h.append(float(x["h"]))
        l.append(float(x["l"]))
        c.append(float(x["c"]))
        v.append(float(x["v"]))
    return o, h, l, c, v


def _ema(series, period):
    if not series:
        return []
    out = [float(series[0])]
    k = 2.0 / (float(period) + 1.0)
    for i in range(1, len(series)):
        out.append((float(series[i]) * k) + (out[-1] * (1.0 - k)))
    return out


def _rma(series, period):
    if not series:
        return []
    period = max(1, int(period))
    if len(series) < period:
        avg = sum(float(x) for x in series) / len(series)
        return [avg for _ in series]
    out = [0.0] * len(series)
    first = sum(float(x) for x in series[:period]) / period
    out[period - 1] = first
    for i in range(period, len(series)):
        out[i] = ((out[i - 1] * (period - 1)) + float(series[i])) / period
    for i in range(period - 1):
        out[i] = out[period - 1]
    return out


def _adx(h, l, c, period):
    if len(h) < period + 2 or len(l) < period + 2 or len(c) < period + 2:
        return []
    plus_dm, minus_dm, tr = [0.0], [0.0], [0.0]
    for i in range(1, len(c)):
        up = float(h[i]) - float(h[i - 1])
        down = float(l[i - 1]) - float(l[i])
        plus_dm.append(up if up > down and up > 0 else 0.0)
        minus_dm.append(down if down > up and down > 0 else 0.0)
        tr.append(max(float(h[i]) - float(l[i]), abs(float(h[i]) - float(c[i - 1])), abs(float(l[i]) - float(c[i - 1]))))
    atr = _rma(tr, period)
    plus = [100.0 * (p / a) if a else 0.0 for p, a in zip(_rma(plus_dm, period), atr)]
    minus = [100.0 * (m / a) if a else 0.0 for m, a in zip(_rma(minus_dm, period), atr)]
    dx = [100.0 * abs(p - m) / (p + m) if (p + m) else 0.0 for p, m in zip(plus, minus)]
    return _rma(dx, period)


def _last(x):
    return x[-1] if x else None


def _atr(h, l, c, period=14):
    if len(h) < 2:
        return 0.0
    tr = [0.0]
    for i in range(1, len(c)):
        tr.append(max(float(h[i]) - float(l[i]), abs(float(h[i]) - float(c[i - 1])), abs(float(l[i]) - float(c[i - 1]))))
    return float(_last(_rma(tr, period)) or 0.0)


def _clamp(value: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, value))


def _pct_change(now: float, prev: float) -> float:
    if prev == 0:
        return 0.0
    return (now - prev) / prev


def _median(values: List[float]) -> float:
    vals = sorted(float(x) for x in values if x is not None)
    if not vals:
        return 0.0
    mid = len(vals) // 2
    if len(vals) % 2:
        return float(vals[mid])
    return float((vals[mid - 1] + vals[mid]) / 2.0)


def _relative_volume(volumes: List[float], idx: int, lookback: int = 20) -> float:
    if idx < 0 or idx >= len(volumes):
        return 1.0
    start = max(0, idx - max(lookback, 8))
    window = [max(float(x), 0.0) for x in volumes[start:idx]]
    nonzero = [x for x in window if x > 0.0]
    baseline = _median(nonzero or window)
    if baseline <= 0.0:
        return 1.0
    return max(0.0, float(volumes[idx]) / baseline)


def _close_position_in_range(o: float, h: float, l: float, c: float) -> float:
    rng = max(h - l, 1e-12)
    return _clamp((c - l) / rng, 0.0, 1.0)


def _body_ratio(o: float, h: float, l: float, c: float) -> float:
    rng = max(h - l, 1e-12)
    return abs(c - o) / rng


def _is_stale(candles: List[dict], interval: str) -> Tuple[bool, float, int]:
    if not candles:
        return True, 9e9, 0
    last_t = int(candles[-1]["t"])
    age_s = max(0.0, (time.time() * 1000.0 - last_t) / 1000.0)
    interval_s = _interval_ms(interval) / 1000.0
    return age_s > (interval_s * 3.0), age_s, last_t


def _base_coin(symbol: str) -> str:
    s = str(symbol or "").upper()
    for suffix in ("-PERP", "-USDC", "-USD", "-USDT"):
        if s.endswith(suffix):
            s = s[: -len(suffix)]
    return s


def _is_probable_meme_symbol(symbol: str) -> bool:
    base = _base_coin(symbol)
    if not base:
        return False
    return any(key in base for key in BLOCKED_MEME_KEYWORDS)


def _validate_symbol_quality(coin: str, candles: List[dict]) -> Tuple[bool, str, Dict[str, Any]]:
    if ALLOWED_SYMBOLS and coin.upper() not in ALLOWED_SYMBOLS:
        return False, "SYMBOL_NOT_ALLOWED", {"coin": coin}
    if _is_probable_meme_symbol(coin):
        return False, "MEME_SYMBOL_BLOCKED", {"coin": coin, "base": _base_coin(coin)}
    if not candles or len(candles) < MIN_CANDLES_REQUIRED:
        return False, "NO_CANDLES", {"bars": len(candles) if candles else 0, "min_bars": MIN_CANDLES_REQUIRED}
    valid = [x for x in candles if float(x.get("c", 0.0) or 0.0) > 0.0 and float(x.get("h", 0.0) or 0.0) >= float(x.get("l", 0.0) or 0.0)]
    if len(valid) < MIN_CANDLES_REQUIRED:
        return False, "BAD_CANDLES_PARSE", {"valid_bars": len(valid), "min_bars": MIN_CANDLES_REQUIRED}
    recent = valid[-MIN_CANDLES_REQUIRED:]
    nonzero_vol_ratio = sum(1 for x in recent if float(x.get("v", 0.0) or 0.0) > 0.0) / max(len(recent), 1)
    if nonzero_vol_ratio < MIN_NONZERO_VOLUME_RATIO:
        return False, "LOW_ACTIVITY_SYMBOL", {"nonzero_vol_ratio": round(nonzero_vol_ratio, 4)}
    return True, "OK", {"base": _base_coin(coin), "bars": len(valid), "nonzero_vol_ratio": round(nonzero_vol_ratio, 4)}


def _volatility_regime_from_atr_pct(atr_pct: float) -> str:
    if atr_pct <= 0.0030:
        return "low"
    if atr_pct <= 0.0075:
        return "normal"
    if atr_pct <= 0.0120:
        return "high"
    return "extreme"


def _tf_snapshot_from_candles(candles: List[dict], interval: str) -> Dict[str, Any]:
    snapshot: Dict[str, Any] = {
        "interval": interval,
        "status": "EMPTY",
        "detail": "EMPTY",
        "context_ok": False,
        "candles": candles,
        "stale": True,
        "age_s": 9e9,
        "last_t": 0,
        "o": [], "h": [], "l": [], "c": [], "v": [],
        "close": 0.0,
        "atr": 0.0,
        "atr_pct": 0.0,
        "adx": 0.0,
    }
    if not candles:
        return snapshot
    stale, age_s, last_t = _is_stale(candles, interval)
    o, h, l, c, v = _extract(candles)
    close = float(c[-1]) if c else 0.0
    atr_value = float(_atr(h, l, c, ATR_PERIOD) or 0.0)
    adx_series = _adx(h, l, c, ADX_PERIOD)
    snapshot.update({
        "status": "STALE_CANDLES" if stale else "OK",
        "detail": "STALE_CANDLES" if stale else "OK",
        "context_ok": not bool(stale),
        "stale": bool(stale),
        "age_s": float(age_s),
        "last_t": int(last_t),
        "o": o, "h": h, "l": l, "c": c, "v": v,
        "close": close,
        "atr": atr_value,
        "atr_pct": (atr_value / close) if close > 0 else 0.0,
        "adx": float(_last(adx_series) or 0.0),
        "adx_series": adx_series,
        f"ema{EMA_FAST}": _ema(c, EMA_FAST),
        f"ema{EMA_MID}": _ema(c, EMA_MID),
        f"ema{EMA_SLOW}": _ema(c, EMA_SLOW),
    })
    return snapshot


def _resolve_tf_snapshot(symbol: str, interval: str, limit: int, provided_market_context: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    if interval == TF_5M and isinstance(provided_market_context, dict):
        tf_ctx = ((provided_market_context.get("timeframes") or {}).get(TF_5M) if isinstance(provided_market_context.get("timeframes"), dict) else None)
        if isinstance(tf_ctx, dict) and tf_ctx.get("candles"):
            return tf_ctx
    if interval == TF_5M:
        ctx = build_market_context(
            symbol=symbol,
            interval=TF_5M,
            limit=LOOKBACK_5M,
            ema_periods=(EMA_FAST, EMA_MID, EMA_SLOW),
            adx_period=ADX_PERIOD,
            atr_period=ATR_PERIOD,
        )
        tf_ctx = ((ctx.get("timeframes") or {}).get(TF_5M) if isinstance(ctx.get("timeframes"), dict) else None)
        if isinstance(tf_ctx, dict):
            return tf_ctx
    candles, status = _fetch_candles(symbol, interval, limit)
    out = _tf_snapshot_from_candles(candles, interval)
    if status != "OK":
        out["status"] = status
        out["detail"] = status
        out["context_ok"] = False
    return out


def _bias_from_tf(tf_ctx: Dict[str, Any], *, adx_min: float, require_close_on_fast: bool = True) -> Tuple[str, Dict[str, Any]]:
    c = tf_ctx.get("c") or []
    ema20 = tf_ctx.get(f"ema{EMA_FAST}") or []
    ema50 = tf_ctx.get(f"ema{EMA_MID}") or []
    ema200 = tf_ctx.get(f"ema{EMA_SLOW}") or []
    if not c or not ema20 or not ema50 or not ema200:
        return "none", {"reason": "NO_TF_DATA"}
    close = float(c[-1])
    adx_val = float(tf_ctx.get("adx") or 0.0)
    atr_pct = float(tf_ctx.get("atr_pct") or 0.0)
    slope20 = _pct_change(float(ema20[-1]), float(ema20[max(0, len(ema20) - 1 - EMA_SLOPE_LOOKBACK)] or ema20[-1]))
    slope50 = _pct_change(float(ema50[-1]), float(ema50[max(0, len(ema50) - 1 - EMA_SLOPE_LOOKBACK)] or ema50[-1]))
    stack_spread = abs(float(ema20[-1]) - float(ema50[-1])) / max(close, 1e-12)
    long_bias = (
        close > ema20[-1] > ema50[-1] > ema200[-1]
        and slope20 > 0.0
        and slope50 >= -0.00010
        and adx_val >= adx_min
        and stack_spread >= TREND_STACK_MIN_PCT
        and (not require_close_on_fast or close >= ema20[-1])
    )
    short_bias = (
        close < ema20[-1] < ema50[-1] < ema200[-1]
        and slope20 < 0.0
        and slope50 <= 0.00010
        and adx_val >= adx_min
        and stack_spread >= TREND_STACK_MIN_PCT
        and (not require_close_on_fast or close <= ema20[-1])
    )
    direction = "long" if long_bias else "short" if short_bias else "none"
    return direction, {
        "adx": round(adx_val, 2),
        "atr_pct": round(atr_pct, 6),
        "slope20": round(slope20, 6),
        "slope50": round(slope50, 6),
        "stack_spread": round(stack_spread, 6),
        "close": round(close, 8),
        "ema20": round(float(ema20[-1]), 8),
        "ema50": round(float(ema50[-1]), 8),
        "ema200": round(float(ema200[-1]), 8),
    }


def _detect_simple_mtf_trigger(
    *,
    direction: str,
    o: List[float],
    h: List[float],
    l: List[float],
    c: List[float],
    v: List[float],
    ema20: List[float],
    ema50: List[float],
    ema200: List[float],
    atr: float,
) -> Tuple[bool, str, Dict[str, Any]]:
    if len(c) < max(EMA_SLOW + 5, 80):
        return False, "NOT_ENOUGH_BARS", {}

    i = len(c) - 1
    recent_idx = list(range(max(0, i - RESET_LOOKBACK_BARS), i))
    if not recent_idx:
        return False, "NO_RESET_WINDOW", {}

    reset_low = min(l[j] for j in recent_idx)
    reset_high = max(h[j] for j in recent_idx)
    min_ema20 = min(float(ema20[j]) for j in recent_idx)
    max_ema20 = max(float(ema20[j]) for j in recent_idx)
    min_ema50 = min(float(ema50[j]) for j in recent_idx)
    max_ema50 = max(float(ema50[j]) for j in recent_idx)
    extension_atr = abs(float(c[i]) - float(ema20[i])) / max(atr, 1e-12)
    prev_high = float(h[i - 1]) if i >= 1 else float(h[i])
    prev_low = float(l[i - 1]) if i >= 1 else float(l[i])
    bars_since_reset = int(i - recent_idx[-1])

    confirm_tol = atr * CONTINUATION_CONFIRM_TOL_ATR

    if direction == "long":
        reset_touched = reset_low <= (max_ema20 + atr * RESET_TOUCH_TOL_ATR)
        reset_not_broken = reset_low >= (min_ema50 - atr * RESET_BREAK_TOL_ATR)
        reclaim_ok = (
            float(c[i]) > float(ema20[i])
            and float(c[i]) > float(o[i])
            and (
                float(c[i]) >= (prev_high - confirm_tol)
                or float(h[i]) >= prev_high
            )
        )
        diag = {
            "reset_low": round(float(reset_low), 8),
            "ema20_ref": round(float(max_ema20), 8),
            "ema50_ref": round(float(min_ema50), 8),
            "extension_atr": round(float(extension_atr), 4),
            "bars_since_reset": bars_since_reset,
            "confirm_close": round(float(c[i]), 8),
            "prev_high": round(float(prev_high), 8),
            "confirm_tol_atr": round(float(CONTINUATION_CONFIRM_TOL_ATR), 4),
            "confirm_level": round(float(prev_high - confirm_tol), 8),
        }
        if not reset_touched:
            return False, "NO_5M_RESET_TOUCH", diag
        if not reset_not_broken:
            return False, "RESET_TOO_DEEP", diag
        if not reclaim_ok:
            return False, "NO_5M_CONTINUATION_CONFIRM", diag
        if extension_atr > TRIGGER_MAX_EMA20_EXTENSION_ATR:
            return False, "TOO_EXTENDED_AFTER_CONFIRM", diag
        return True, "OK", diag

    reset_touched = reset_high >= (min_ema20 - atr * RESET_TOUCH_TOL_ATR)
    reset_not_broken = reset_high <= (max_ema50 + atr * RESET_BREAK_TOL_ATR)
    reclaim_ok = (
        float(c[i]) < float(ema20[i])
        and float(c[i]) < float(o[i])
        and (
            float(c[i]) <= (prev_low + confirm_tol)
            or float(l[i]) <= prev_low
        )
    )
    diag = {
        "reset_high": round(float(reset_high), 8),
        "ema20_ref": round(float(min_ema20), 8),
        "ema50_ref": round(float(max_ema50), 8),
        "extension_atr": round(float(extension_atr), 4),
        "bars_since_reset": bars_since_reset,
        "confirm_close": round(float(c[i]), 8),
        "prev_low": round(float(prev_low), 8),
        "confirm_tol_atr": round(float(CONTINUATION_CONFIRM_TOL_ATR), 4),
        "confirm_level": round(float(prev_low + confirm_tol), 8),
    }
    if not reset_touched:
        return False, "NO_5M_RESET_TOUCH", diag
    if not reset_not_broken:
        return False, "RESET_TOO_DEEP", diag
    if not reclaim_ok:
        return False, "NO_5M_CONTINUATION_CONFIRM", diag
    if extension_atr > TRIGGER_MAX_EMA20_EXTENSION_ATR:
        return False, "TOO_EXTENDED_AFTER_CONFIRM", diag
    return True, "OK", diag


def _compute_simple_mtf_fixed_tp_pct(*, score: float, atr_pct: float, sl_pct: float) -> Tuple[float, float]:
    score = float(score or 0.0)
    atr_pct = float(atr_pct or 0.0)
    sl_pct = float(sl_pct or MTF_SL_MIN_PCT)

    rr_target = 1.14
    rr_target += _clamp((score - 82.0) * 0.0048, -0.08, 0.10)
    rr_target += _clamp((atr_pct - 0.0060) * 6.0, -0.04, 0.04)
    rr_target = _clamp(rr_target, MTF_RR_MIN, MTF_RR_MAX)

    tp_pct = sl_pct * rr_target
    tp_pct = _clamp(tp_pct, MTF_TP_MIN_PCT, MTF_TP_MAX_PCT)
    rr_real = tp_pct / max(sl_pct, 1e-12)
    return round(tp_pct, 6), round(rr_real, 4)


def _dynamic_trade_management_params(
    strength: float,
    score: float,
    atr_pct: Optional[float] = None,
    *,
    sl_pct: Optional[float] = None,
) -> Dict[str, Any]:
    atr_pct = float(atr_pct or 0.0)
    score = float(score or 0.0)
    strength = float(strength or 0.0)
    sl_pct = float(sl_pct or MTF_SL_MIN_PCT)

    tp_fixed, rr_target = _compute_simple_mtf_fixed_tp_pct(score=score, atr_pct=atr_pct, sl_pct=sl_pct)

    if score >= 88.0 or strength >= 0.88:
        bucket = "strong"
        be_ratio = 0.56
        be_offset = 0.00070
    elif score >= 79.0 or strength >= 0.76:
        bucket = "base"
        be_ratio = 0.50
        be_offset = 0.00060
    else:
        bucket = "weak"
        be_ratio = 0.45
        be_offset = 0.00055

    be_activation = _clamp(min(tp_fixed * be_ratio, sl_pct * 0.95), 0.0028, 0.0049)
    return {
        "tp_activation_price": round(tp_fixed, 6),
        "trail_retrace_price": 0.0,
        "force_min_profit_price": 999999.0,
        "force_min_strength": 0.0,
        "partial_tp_activation_price": 999999.0,
        "partial_tp_close_fraction": 0.0,
        "break_even_activation_price": round(be_activation, 6),
        "break_even_offset_price": round(be_offset, 6),
        "bucket": bucket,
        "vol_regime": _volatility_regime_from_atr_pct(atr_pct),
        "tp_rr_multiple": rr_target,
    }


def get_trade_management_params(strength: float, score: float, atr_pct: Optional[float] = None) -> Dict[str, Any]:
    return _dynamic_trade_management_params(strength, score, atr_pct)


def _evaluate_market_context(market_context: Dict[str, Any]) -> dict:
    coin = str(market_context.get("coin") or market_context.get("symbol") or "").upper()
    try:
        tf5 = _resolve_tf_snapshot(coin, TF_5M, LOOKBACK_5M, provided_market_context=market_context)
        candles5 = tf5.get("candles") or []
        ok_symbol, reason_symbol, symbol_diag = _validate_symbol_quality(coin, candles5)
        if not ok_symbol:
            return {"signal": False, "reason": reason_symbol, "coin": coin, "diag": symbol_diag}
        if str(tf5.get("status") or "") != "OK" or bool(tf5.get("stale", True)):
            return {"signal": False, "reason": "STALE_OR_BAD_5M", "coin": coin, "diag": {"status": tf5.get("status"), "age_s": round(float(tf5.get("age_s", 0.0) or 0.0), 1)}}

        tf15 = _resolve_tf_snapshot(coin, TF_15M, LOOKBACK_15M)
        tf1h = _resolve_tf_snapshot(coin, TF_1H, LOOKBACK_1H)
        if str(tf15.get("status") or "") != "OK" or str(tf1h.get("status") or "") != "OK":
            return {
                "signal": False,
                "reason": "BAD_HIGHER_TF_DATA",
                "coin": coin,
                "diag": {"tf15": tf15.get("status"), "tf1h": tf1h.get("status")},
            }

        o5 = tf5.get("o") or []
        h5 = tf5.get("h") or []
        l5 = tf5.get("l") or []
        c5 = tf5.get("c") or []
        v5 = tf5.get("v") or []
        ema20_5 = tf5.get(f"ema{EMA_FAST}") or []
        ema50_5 = tf5.get(f"ema{EMA_MID}") or []
        ema200_5 = tf5.get(f"ema{EMA_SLOW}") or []
        if not c5 or not ema20_5 or not ema50_5 or not ema200_5:
            return {"signal": False, "reason": "NO_5M_TREND_DATA", "coin": coin}

        close5 = float(tf5.get("close") or c5[-1])
        adx5 = float(tf5.get("adx") or 0.0)
        atr5 = float(tf5.get("atr") or 0.0)
        atr_pct = float(tf5.get("atr_pct") or 0.0)
        if atr_pct < ATR_PCT_MIN:
            return {"signal": False, "reason": "ATR_TOO_LOW", "coin": coin, "diag": {"atr_pct": round(atr_pct, 6)}}
        if atr_pct > ATR_PCT_MAX:
            return {"signal": False, "reason": "ATR_TOO_HIGH", "coin": coin, "diag": {"atr_pct": round(atr_pct, 6)}}
        if adx5 < M5_ADX_MIN:
            return {"signal": False, "reason": "ADX_5M_TOO_LOW", "coin": coin, "diag": {"adx5": round(adx5, 2)}}

        bias1h, diag1h = _bias_from_tf(tf1h, adx_min=H1_ADX_MIN, require_close_on_fast=True)
        bias15, diag15 = _bias_from_tf(tf15, adx_min=M15_ADX_MIN, require_close_on_fast=True)
        if bias1h == "none":
            return {"signal": False, "reason": "NO_H1_BIAS", "coin": coin, "diag": diag1h}
        if bias15 == "none":
            return {"signal": False, "reason": "NO_M15_BIAS", "coin": coin, "diag": diag15}
        if bias1h != bias15:
            return {"signal": False, "reason": "MTF_BIAS_MISMATCH", "coin": coin, "diag": {"h1": bias1h, "m15": bias15, "diag1h": diag1h, "diag15": diag15}}

        direction = bias1h
        ok_trigger, reason5, diag5 = _detect_simple_mtf_trigger(
            direction=direction,
            o=o5,
            h=h5,
            l=l5,
            c=c5,
            v=v5,
            ema20=ema20_5,
            ema50=ema50_5,
            ema200=ema200_5,
            atr=atr5,
        )
        if not ok_trigger:
            if LOG_SIGNAL_DIAGNOSTICS:
                _log(f"BLOCK coin={coin} dir={direction} reason={reason5} diag={diag5}")
            return {"signal": False, "reason": reason5, "coin": coin, "diag": diag5}

        if direction == "long":
            reset_extreme = min(l5[max(0, len(l5) - 1 - RESET_LOOKBACK_BARS): len(l5) - 1])
            structural_pct = max(0.0, (close5 - reset_extreme) / max(close5, 1e-12))
        else:
            reset_extreme = max(h5[max(0, len(h5) - 1 - RESET_LOOKBACK_BARS): len(h5) - 1])
            structural_pct = max(0.0, (reset_extreme - close5) / max(close5, 1e-12))

        sl_from_atr = atr_pct * MTF_SL_ATR_MULT
        sl_from_structure = structural_pct + ((atr5 / max(close5, 1e-12)) * MTF_SL_BUFFER_ATR)
        sl_pct = _clamp(max(sl_from_atr, sl_from_structure), MTF_SL_MIN_PCT, MTF_SL_MAX_PCT)

        extension_atr = float(diag5.get("extension_atr", 0.0) or 0.0)

        h1_strength = _clamp((float(diag1h.get("adx", 0.0)) - H1_ADX_MIN) / 15.0, 0.0, 1.0)
        m15_strength = _clamp((float(diag15.get("adx", 0.0)) - M15_ADX_MIN) / 14.0, 0.0, 1.0)
        m5_strength = _clamp((adx5 - M5_ADX_MIN) / 13.0, 0.0, 1.0)
        trend_alignment_quality = _clamp((float(diag1h.get("stack_spread", 0.0)) + float(diag15.get("stack_spread", 0.0))) / max(TREND_STACK_MIN_PCT * 6.0, 1e-12), 0.0, 1.0)
        reset_quality = _clamp(1.0 - _clamp(extension_atr / max(TRIGGER_MAX_EMA20_EXTENSION_ATR, 1e-12), 0.0, 1.0), 0.0, 1.0)

        quality = _clamp(
            (0.30 * h1_strength)
            + (0.25 * m15_strength)
            + (0.20 * m5_strength)
            + (0.15 * trend_alignment_quality)
            + (0.10 * reset_quality),
            0.0,
            1.0,
        )
        score = round(min(MAX_SCORE, 69.0 + (28.0 * quality)), 2)
        if score < MIN_SCORE_TO_SIGNAL:
            return {
                "signal": False,
                "reason": "SCORE_TOO_LOW",
                "coin": coin,
                "diag": {
                    "score": score,
                    "min_score": MIN_SCORE_TO_SIGNAL,
                    "h1_adx": diag1h.get("adx"),
                    "m15_adx": diag15.get("adx"),
                    "extension_atr": round(extension_atr, 4),
                },
            }

        strength = _clamp(score / 100.0, STRENGTH_MIN, STRENGTH_MAX)
        mgmt = _dynamic_trade_management_params(
            strength,
            score,
            atr_pct,
            sl_pct=sl_pct,
        )
        if float(mgmt.get("tp_rr_multiple", 0.0) or 0.0) < MIN_RR_TO_SIGNAL:
            return {
                "signal": False,
                "reason": "RR_TOO_LOW",
                "coin": coin,
                "diag": {
                    "tp_rr": round(float(mgmt.get("tp_rr_multiple", 0.0) or 0.0), 4),
                    "sl_pct": round(sl_pct, 6),
                    "tp_fixed": round(float(mgmt.get("tp_activation_price", 0.0) or 0.0), 6),
                    "min_rr": MIN_RR_TO_SIGNAL,
                },
            }

        t5 = int(tf5.get("last_t") or candles5[-1].get("t") or 0)
        age5 = max(0.0, (time.time() * 1000.0 - t5) / 1000.0) if t5 else 0.0
        out = {
            "signal": True,
            "direction": direction,
            "strength": round(strength, 4),
            "score": float(score),
            "sl_price_pct": round(sl_pct, 6),
            "tp_activation_price": float(mgmt["tp_activation_price"]),
            "trail_retrace_price": float(mgmt["trail_retrace_price"]),
            "force_min_profit_price": float(mgmt["force_min_profit_price"]),
            "force_min_strength": float(mgmt["force_min_strength"]),
            "partial_tp_activation_price": float(mgmt["partial_tp_activation_price"]),
            "partial_tp_close_fraction": float(mgmt["partial_tp_close_fraction"]),
            "break_even_activation_price": float(mgmt["break_even_activation_price"]),
            "break_even_offset_price": float(mgmt["break_even_offset_price"]),
            "mgmt_bucket": str(mgmt["bucket"]),
            "vol_regime": str(mgmt.get("vol_regime", _volatility_regime_from_atr_pct(atr_pct))),
            "atr_pct": round(float(atr_pct), 6),
            "ema_stack_pct": round(float(diag1h.get("stack_spread", 0.0)), 6),
            "coin": coin,
            "close_5": round(close5, 6),
            "last_candle_t_5m": int(t5),
            "ema20_5m": round(float(ema20_5[-1]), 6),
            "adx1": round(float(diag1h.get("adx", 0.0)), 2),
            "adx15": round(float(diag15.get("adx", 0.0)), 2),
            "strategy_model": "mtf_simple_continuation_5m_v2",
            "market_context_status": str(market_context.get("status") or tf5.get("status") or "OK"),
            "tp_rr_multiple": float(mgmt.get("tp_rr_multiple", 0.0) or 0.0),
            "h1_bias": direction,
            "m15_bias": bias15,
        }
        if LOG_SIGNAL_DIAGNOSTICS:
            _log(
                f"SIGNAL coin={coin} dir={out['direction']} close_5={out['close_5']} t5={out['last_candle_t_5m']} age5s={round(age5,1)} "
                f"adx5={round(adx5,2)} adx15={out['adx15']} adx1={out['adx1']} atr_pct={out['atr_pct']} score={out['score']} "
                f"h1_bias={out['h1_bias']} m15_bias={out['m15_bias']} sl_pct={out['sl_price_pct']} "
                f"tp_fixed={out['tp_activation_price']} tp_rr={out['tp_rr_multiple']:.4f} be_act={out['break_even_activation_price']} "
                f"be_offset={out['break_even_offset_price']} bucket={out['mgmt_bucket']}"
            )
        return out
    except Exception as e:
        return {"signal": False, "reason": "STRATEGY_EXCEPTION", "error": str(e)[:180]}


def get_entry_signal(symbol: str, market_context: Optional[Dict[str, Any]] = None) -> dict:
    market_context = market_context or build_market_context(
        symbol=symbol,
        interval=TF_5M,
        limit=LOOKBACK_5M,
        ema_periods=(EMA_FAST, EMA_MID, EMA_SLOW),
        adx_period=ADX_PERIOD,
        atr_period=ATR_PERIOD,
    )
    return _evaluate_market_context(market_context)


STRATEGY_ID = "mtf_simple"
STRATEGY_VERSION = "v1"
STRATEGY_MODEL = "mtf_simple_continuation_5m_v3_pure"


class MtfSimpleStrategy(BaseStrategy):
    strategy_id = STRATEGY_ID
    strategy_version = STRATEGY_VERSION
    strategy_model = STRATEGY_MODEL

    def evaluate(self, symbol: str, market_context: Optional[Dict[str, Any]] = None) -> dict:
        out = get_entry_signal(symbol, market_context=market_context)
        if isinstance(out, dict) and out.get("signal"):
            out.setdefault("strategy_id", self.strategy_id)
            out.setdefault("strategy_version", self.strategy_version)
            out.setdefault("strategy_model", self.strategy_model)
        return out

    def get_trade_management_params(self, strength: float, score: float, atr_pct: Optional[float] = None) -> Dict[str, Any]:
        return get_trade_management_params(strength, score, atr_pct)


DEFAULT_STRATEGY = MtfSimpleStrategy()


def evaluate_symbol(symbol: str) -> dict:
    return DEFAULT_STRATEGY.evaluate(symbol)
