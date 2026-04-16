from __future__ import annotations

import os
import time
from typing import Any, Dict, List, Optional, Tuple

from app.market_context import build_market_context
from app.strategies.base import BaseStrategy
from app.strategies.breakout_reset import (
    ADX_PERIOD,
    ALLOWED_SYMBOLS,
    ATR_PERIOD,
    BLOCKED_MEME_KEYWORDS,
    EMA_FAST,
    EMA_MID,
    EMA_SLOW,
    LOOKBACK_5M,
    MIN_CANDLES_REQUIRED,
    MIN_NONZERO_VOLUME_RATIO,
    TF_5M,
)

STRATEGY_ID = "liquidity_sweep_reversal"
STRATEGY_VERSION = "v1"
STRATEGY_MODEL = "liquidity_sweep_reversal_5m_v1"

SWEEP_LOOKBACK = 34
SWEEP_MAX_AGE_BARS = 6
SWEEP_MIN_DEPTH_ATR = 0.14
SWEEP_MIN_WICK_RATIO = 0.36
SWEEP_MIN_RVOL = 0.78
SWEEP_RECOVER_TOL_ATR = 0.28
TRIGGER_MIN_RVOL = 0.72
TRIGGER_MIN_BODY_RATIO = 0.17
TRIGGER_CLOSE_POS_LONG_MIN = 0.52
TRIGGER_CLOSE_POS_SHORT_MAX = 0.48
TRIGGER_EXTENSION_MAX_ATR = 1.90
TRIGGER_EMA20_RECOVER_TOL_ATR = 0.55
TRIGGER_EMA50_RECOVER_TOL_ATR = 1.05
RETEST_INVALIDATION_ATR = 0.42
SL_BUFFER_ATR = 0.18
SL_MIN_PCT = 0.0058
SL_MAX_PCT = 0.0148
TARGET_LOOKBACK = 48
MIN_RR = 1.12
ATR_PCT_MIN = 0.0017
ATR_PCT_MAX = 0.0280
LOG_SIGNAL_DIAGNOSTICS = os.getenv("LOG_LIQUIDITY_SIGNAL_DIAGNOSTICS", "1").strip().lower() not in {"0", "false", "no", "off"}


def _log(msg: str) -> None:
    try:
        print(f"[STRATEGY-LIQ {time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime())}] {msg}")
    except Exception:
        pass


def _clamp(value: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, value))


def _median(values: List[float]) -> float:
    vals = sorted(float(x) for x in values if x is not None)
    if not vals:
        return 0.0
    mid = len(vals) // 2
    if len(vals) % 2:
        return float(vals[mid])
    return float((vals[mid - 1] + vals[mid]) / 2.0)


def _base_coin(symbol: str) -> str:
    s = str(symbol or "").upper()
    for suffix in ("-PERP", "-USDC", "-USD", "-USDT"):
        if s.endswith(suffix):
            s = s[: -len(suffix)]
    return s


def _is_probable_meme_symbol(symbol: str) -> bool:
    base = _base_coin(symbol)
    return bool(base) and any(key in base for key in BLOCKED_MEME_KEYWORDS)


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


def _body_ratio(o: float, h: float, l: float, c: float) -> float:
    rng = max(h - l, 1e-12)
    return abs(c - o) / rng


def _close_position_in_range(o: float, h: float, l: float, c: float) -> float:
    rng = max(h - l, 1e-12)
    return _clamp((c - l) / rng, 0.0, 1.0)


def _lower_wick_ratio(o: float, h: float, l: float, c: float) -> float:
    rng = max(h - l, 1e-12)
    return _clamp((min(o, c) - l) / rng, 0.0, 1.0)


def _upper_wick_ratio(o: float, h: float, l: float, c: float) -> float:
    rng = max(h - l, 1e-12)
    return _clamp((h - max(o, c)) / rng, 0.0, 1.0)


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


def _compute_management_params(strength: float, score: float, atr_pct: Optional[float] = None) -> Dict[str, Any]:
    atr_pct = float(atr_pct or 0.0)
    score = float(score or 0.0)
    strength = float(strength or 0.0)

    if score >= 89.0:
        bucket = "liquidity_strong"
        break_even_activation = 0.0053
        partial_tp = 0.0083
        partial_frac = 0.30
        tp_act = 0.0126
        retrace = 0.0034
        force = 0.0088
    elif score >= 82.0:
        bucket = "liquidity_base"
        break_even_activation = 0.0048
        partial_tp = 0.0076
        partial_frac = 0.34
        tp_act = 0.0112
        retrace = 0.0037
        force = 0.0080
    else:
        bucket = "liquidity_weak"
        break_even_activation = 0.0043
        partial_tp = 0.0069
        partial_frac = 0.38
        tp_act = 0.0100
        retrace = 0.0039
        force = 0.0073

    vol_add = _clamp((atr_pct - 0.0070) * 0.16, -0.0007, 0.0013)
    break_even_activation = _clamp(break_even_activation + (vol_add * 0.30), 0.0040, 0.0072)
    partial_tp = _clamp(partial_tp + (vol_add * 0.65), break_even_activation + 0.0017, 0.0106)
    tp_act = _clamp(tp_act + vol_add, partial_tp + 0.0015, 0.0158)
    retrace = _clamp(retrace + (vol_add * 0.55), 0.0030, 0.0048)
    force = _clamp(force + (vol_add * 0.60), partial_tp, tp_act - 0.0010)
    break_even_offset = _clamp(max(atr_pct * 0.07, 0.00055), 0.00055, 0.00130)
    force_strength = _clamp(max(0.12, strength * 0.56), 0.12, 0.84)

    return {
        "bucket": bucket,
        "tp_activation_price": round(tp_act, 6),
        "trail_retrace_price": round(retrace, 6),
        "force_min_profit_price": round(force, 6),
        "force_min_strength": round(force_strength, 4),
        "partial_tp_activation_price": round(partial_tp, 6),
        "partial_tp_close_fraction": round(partial_frac, 4),
        "break_even_activation_price": round(break_even_activation, 6),
        "break_even_offset_price": round(break_even_offset, 6),
        "vol_regime": "volatile" if atr_pct >= 0.0095 else ("active" if atr_pct >= 0.0050 else "normal"),
    }


def get_trade_management_params(strength: float, score: float, atr_pct: Optional[float] = None) -> Dict[str, Any]:
    return _compute_management_params(strength, score, atr_pct)


def _score_candidate(
    *,
    sweep_depth_atr: float,
    sweep_wick_ratio: float,
    sweep_rvol: float,
    trigger_rvol: float,
    trigger_body_ratio: float,
    trigger_close_pos: float,
    extension_atr: float,
    rr_estimate: float,
    bars_since_sweep: int,
) -> float:
    depth_q = _clamp((sweep_depth_atr - SWEEP_MIN_DEPTH_ATR) / 0.70, 0.0, 1.0)
    wick_q = _clamp((sweep_wick_ratio - SWEEP_MIN_WICK_RATIO) / 0.42, 0.0, 1.0)
    sweep_rvol_q = _clamp((sweep_rvol - 0.85) / 0.90, 0.0, 1.0)
    trigger_rvol_q = _clamp((trigger_rvol - 0.80) / 0.85, 0.0, 1.0)
    body_q = _clamp((trigger_body_ratio - TRIGGER_MIN_BODY_RATIO) / 0.45, 0.0, 1.0)
    close_q = _clamp((trigger_close_pos - 0.50) / 0.40, 0.0, 1.0)
    extension_penalty = _clamp(extension_atr / TRIGGER_EXTENSION_MAX_ATR, 0.0, 1.0)
    rr_q = _clamp((rr_estimate - MIN_RR) / 1.4, 0.0, 1.0)
    age_penalty = _clamp((bars_since_sweep - 1) / max(SWEEP_MAX_AGE_BARS - 1, 1), 0.0, 1.0)

    quality = _clamp(
        (0.19 * depth_q)
        + (0.16 * wick_q)
        + (0.12 * sweep_rvol_q)
        + (0.13 * trigger_rvol_q)
        + (0.14 * body_q)
        + (0.10 * close_q)
        + (0.18 * rr_q)
        - (0.08 * extension_penalty)
        - (0.06 * age_penalty),
        0.0,
        1.0,
    )
    return round(min(100.0, 66.0 + (34.0 * quality)), 2)


def _detect_long_sweep(
    o: List[float],
    h: List[float],
    l: List[float],
    c: List[float],
    v: List[float],
    ema20: List[float],
    ema50: List[float],
    atr_value: float,
) -> Tuple[bool, str, Dict[str, Any]]:
    if len(c) < max(SWEEP_LOOKBACK + SWEEP_MAX_AGE_BARS + 6, 90):
        return False, "NOT_ENOUGH_BARS", {}

    trigger_idx = len(c) - 1
    best: Optional[Dict[str, Any]] = None

    for sweep_idx in range(max(SWEEP_LOOKBACK + 2, trigger_idx - SWEEP_MAX_AGE_BARS), trigger_idx):
        left_start = max(0, sweep_idx - SWEEP_LOOKBACK)
        if sweep_idx - left_start < 12:
            continue
        liquidity_level = min(l[left_start:sweep_idx])
        sweep_low = float(l[sweep_idx])
        sweep_depth_atr = (float(liquidity_level) - sweep_low) / max(atr_value, 1e-12)
        if sweep_depth_atr < SWEEP_MIN_DEPTH_ATR:
            continue

        sweep_close_pos = _close_position_in_range(o[sweep_idx], h[sweep_idx], l[sweep_idx], c[sweep_idx])
        sweep_wick_ratio = _lower_wick_ratio(o[sweep_idx], h[sweep_idx], l[sweep_idx], c[sweep_idx])
        sweep_rvol = _relative_volume(v, sweep_idx, 24)
        if sweep_wick_ratio < SWEEP_MIN_WICK_RATIO:
            continue
        if sweep_rvol < SWEEP_MIN_RVOL:
            continue
        if float(c[sweep_idx]) < float(liquidity_level) - (atr_value * SWEEP_RECOVER_TOL_ATR):
            continue
        if sweep_close_pos < 0.46:
            continue

        bars_since_sweep = trigger_idx - sweep_idx
        if bars_since_sweep <= 0 or bars_since_sweep > SWEEP_MAX_AGE_BARS:
            continue

        invalidation_low = min(l[sweep_idx + 1:trigger_idx + 1]) if trigger_idx > sweep_idx else sweep_low
        if invalidation_low < sweep_low - (atr_value * RETEST_INVALIDATION_ATR):
            continue

        trigger_rvol = _relative_volume(v, trigger_idx, 24)
        trigger_body_ratio = _body_ratio(o[trigger_idx], h[trigger_idx], l[trigger_idx], c[trigger_idx])
        trigger_close_pos = _close_position_in_range(o[trigger_idx], h[trigger_idx], l[trigger_idx], c[trigger_idx])
        extension_atr = (float(c[trigger_idx]) - float(liquidity_level)) / max(atr_value, 1e-12)
        trigger_ok = (
            float(c[trigger_idx]) > float(liquidity_level)
            and float(c[trigger_idx]) >= float(c[sweep_idx])
            and float(c[trigger_idx]) >= float(ema20[trigger_idx]) - (atr_value * TRIGGER_EMA20_RECOVER_TOL_ATR)
            and float(c[trigger_idx]) >= float(ema50[trigger_idx]) - (atr_value * TRIGGER_EMA50_RECOVER_TOL_ATR)
            and trigger_rvol >= TRIGGER_MIN_RVOL
            and trigger_body_ratio >= TRIGGER_MIN_BODY_RATIO
            and trigger_close_pos >= TRIGGER_CLOSE_POS_LONG_MIN
            and extension_atr <= TRIGGER_EXTENSION_MAX_ATR
        )
        if not trigger_ok:
            continue

        left_target_start = max(0, sweep_idx - TARGET_LOOKBACK)
        target_level = max(h[left_target_start:sweep_idx]) if sweep_idx > left_target_start else max(h[:sweep_idx] or [0.0])
        stop_price = sweep_low - (atr_value * SL_BUFFER_ATR)
        risk_abs = max(float(c[trigger_idx]) - stop_price, 1e-12)
        reward_abs = max(float(target_level) - float(c[trigger_idx]), 0.0)
        rr_estimate = reward_abs / risk_abs if risk_abs > 0 else 0.0
        if rr_estimate < MIN_RR:
            continue

        diag = {
            "liquidity_level": round(float(liquidity_level), 8),
            "sweep_idx": int(sweep_idx),
            "bars_since_sweep": int(bars_since_sweep),
            "sweep_depth_atr": round(float(sweep_depth_atr), 4),
            "sweep_wick_ratio": round(float(sweep_wick_ratio), 4),
            "sweep_rvol": round(float(sweep_rvol), 4),
            "trigger_rvol": round(float(trigger_rvol), 4),
            "trigger_body_ratio": round(float(trigger_body_ratio), 4),
            "trigger_close_pos": round(float(trigger_close_pos), 4),
            "trigger_extension_atr": round(float(extension_atr), 4),
            "rr_estimate": round(float(rr_estimate), 4),
            "target_level": round(float(target_level), 8),
            "stop_price": round(float(stop_price), 8),
            "direction": "long",
        }
        score = _score_candidate(
            sweep_depth_atr=float(sweep_depth_atr),
            sweep_wick_ratio=float(sweep_wick_ratio),
            sweep_rvol=float(sweep_rvol),
            trigger_rvol=float(trigger_rvol),
            trigger_body_ratio=float(trigger_body_ratio),
            trigger_close_pos=float(trigger_close_pos),
            extension_atr=float(extension_atr),
            rr_estimate=float(rr_estimate),
            bars_since_sweep=int(bars_since_sweep),
        )
        diag["score"] = float(score)
        if best is None or float(score) > float(best.get("score", 0.0)):
            best = diag

    if not best:
        return False, "NO_VALID_LONG_SWEEP", {}
    return True, "OK", best


def _detect_short_sweep(
    o: List[float],
    h: List[float],
    l: List[float],
    c: List[float],
    v: List[float],
    ema20: List[float],
    ema50: List[float],
    atr_value: float,
) -> Tuple[bool, str, Dict[str, Any]]:
    if len(c) < max(SWEEP_LOOKBACK + SWEEP_MAX_AGE_BARS + 6, 90):
        return False, "NOT_ENOUGH_BARS", {}

    trigger_idx = len(c) - 1
    best: Optional[Dict[str, Any]] = None

    for sweep_idx in range(max(SWEEP_LOOKBACK + 2, trigger_idx - SWEEP_MAX_AGE_BARS), trigger_idx):
        left_start = max(0, sweep_idx - SWEEP_LOOKBACK)
        if sweep_idx - left_start < 12:
            continue
        liquidity_level = max(h[left_start:sweep_idx])
        sweep_high = float(h[sweep_idx])
        sweep_depth_atr = (sweep_high - float(liquidity_level)) / max(atr_value, 1e-12)
        if sweep_depth_atr < SWEEP_MIN_DEPTH_ATR:
            continue

        sweep_close_pos = _close_position_in_range(o[sweep_idx], h[sweep_idx], l[sweep_idx], c[sweep_idx])
        sweep_wick_ratio = _upper_wick_ratio(o[sweep_idx], h[sweep_idx], l[sweep_idx], c[sweep_idx])
        sweep_rvol = _relative_volume(v, sweep_idx, 24)
        if sweep_wick_ratio < SWEEP_MIN_WICK_RATIO:
            continue
        if sweep_rvol < SWEEP_MIN_RVOL:
            continue
        if float(c[sweep_idx]) > float(liquidity_level) + (atr_value * SWEEP_RECOVER_TOL_ATR):
            continue
        if sweep_close_pos > 0.54:
            continue

        bars_since_sweep = trigger_idx - sweep_idx
        if bars_since_sweep <= 0 or bars_since_sweep > SWEEP_MAX_AGE_BARS:
            continue

        invalidation_high = max(h[sweep_idx + 1:trigger_idx + 1]) if trigger_idx > sweep_idx else sweep_high
        if invalidation_high > sweep_high + (atr_value * RETEST_INVALIDATION_ATR):
            continue

        trigger_rvol = _relative_volume(v, trigger_idx, 24)
        trigger_body_ratio = _body_ratio(o[trigger_idx], h[trigger_idx], l[trigger_idx], c[trigger_idx])
        trigger_close_pos = _close_position_in_range(o[trigger_idx], h[trigger_idx], l[trigger_idx], c[trigger_idx])
        extension_atr = (float(liquidity_level) - float(c[trigger_idx])) / max(atr_value, 1e-12)
        trigger_ok = (
            float(c[trigger_idx]) < float(liquidity_level)
            and float(c[trigger_idx]) <= float(c[sweep_idx])
            and float(c[trigger_idx]) <= float(ema20[trigger_idx]) + (atr_value * TRIGGER_EMA20_RECOVER_TOL_ATR)
            and float(c[trigger_idx]) <= float(ema50[trigger_idx]) + (atr_value * TRIGGER_EMA50_RECOVER_TOL_ATR)
            and trigger_rvol >= TRIGGER_MIN_RVOL
            and trigger_body_ratio >= TRIGGER_MIN_BODY_RATIO
            and trigger_close_pos <= TRIGGER_CLOSE_POS_SHORT_MAX
            and extension_atr <= TRIGGER_EXTENSION_MAX_ATR
        )
        if not trigger_ok:
            continue

        left_target_start = max(0, sweep_idx - TARGET_LOOKBACK)
        target_level = min(l[left_target_start:sweep_idx]) if sweep_idx > left_target_start else min(l[:sweep_idx] or [0.0])
        stop_price = sweep_high + (atr_value * SL_BUFFER_ATR)
        risk_abs = max(stop_price - float(c[trigger_idx]), 1e-12)
        reward_abs = max(float(c[trigger_idx]) - float(target_level), 0.0)
        rr_estimate = reward_abs / risk_abs if risk_abs > 0 else 0.0
        if rr_estimate < MIN_RR:
            continue

        diag = {
            "liquidity_level": round(float(liquidity_level), 8),
            "sweep_idx": int(sweep_idx),
            "bars_since_sweep": int(bars_since_sweep),
            "sweep_depth_atr": round(float(sweep_depth_atr), 4),
            "sweep_wick_ratio": round(float(sweep_wick_ratio), 4),
            "sweep_rvol": round(float(sweep_rvol), 4),
            "trigger_rvol": round(float(trigger_rvol), 4),
            "trigger_body_ratio": round(float(trigger_body_ratio), 4),
            "trigger_close_pos": round(float(trigger_close_pos), 4),
            "trigger_extension_atr": round(float(extension_atr), 4),
            "rr_estimate": round(float(rr_estimate), 4),
            "target_level": round(float(target_level), 8),
            "stop_price": round(float(stop_price), 8),
            "direction": "short",
        }
        score = _score_candidate(
            sweep_depth_atr=float(sweep_depth_atr),
            sweep_wick_ratio=float(sweep_wick_ratio),
            sweep_rvol=float(sweep_rvol),
            trigger_rvol=float(trigger_rvol),
            trigger_body_ratio=float(trigger_body_ratio),
            trigger_close_pos=float(1.0 - trigger_close_pos),
            extension_atr=float(extension_atr),
            rr_estimate=float(rr_estimate),
            bars_since_sweep=int(bars_since_sweep),
        )
        diag["score"] = float(score)
        if best is None or float(score) > float(best.get("score", 0.0)):
            best = diag

    if not best:
        return False, "NO_VALID_SHORT_SWEEP", {}
    return True, "OK", best


def _evaluate_market_context(market_context: Dict[str, Any]) -> Dict[str, Any]:
    try:
        coin = str(market_context.get("coin") or "").strip()
        if not coin:
            return {"signal": False, "reason": "BAD_SYMBOL"}

        tf5 = (market_context.get("timeframes") or {}).get(TF_5M) or {}
        status = str(tf5.get("status") or "UNKNOWN")
        candles = tf5.get("candles") or []
        if status in ("API_FAIL", "BAD_SYMBOL", "BAD_INTERVAL"):
            return {"signal": False, "reason": "CANDLES_FETCH_FAIL", "detail": {TF_5M: status}, "coin": coin}
        if not candles:
            return {"signal": False, "reason": "NO_CANDLES", "coin": coin}

        quality_ok, quality_reason, quality_diag = _validate_symbol_quality(coin, candles)
        if not quality_ok:
            return {"signal": False, "reason": quality_reason, "coin": coin, "diag": quality_diag}

        stale = bool(tf5.get("stale", True))
        age_s = float(tf5.get("age_s", 0.0) or 0.0)
        last_t = int(tf5.get("last_t", 0) or 0)
        if stale:
            return {"signal": False, "reason": "STALE_CANDLES", "coin": coin, "age_s": {TF_5M: round(age_s, 1)}, "last_t": {TF_5M: last_t}}

        o = tf5.get("o") or []
        h = tf5.get("h") or []
        l = tf5.get("l") or []
        c = tf5.get("c") or []
        v = tf5.get("v") or []
        ema20 = tf5.get(f"ema{EMA_FAST}") or []
        ema50 = tf5.get(f"ema{EMA_MID}") or []
        ema200 = tf5.get(f"ema{EMA_SLOW}") or []
        if not (c and ema20 and ema50 and ema200):
            return {"signal": False, "reason": "NO_TREND_DATA", "coin": coin}

        close5 = float(tf5.get("close") or c[-1])
        atr_value = float(tf5.get("atr") or 0.0)
        atr_pct = float(tf5.get("atr_pct") or 0.0)
        adx5 = float(tf5.get("adx") or 0.0)
        if atr_pct < ATR_PCT_MIN:
            return {"signal": False, "reason": "ATR_TOO_LOW", "coin": coin, "diag": {"atr_pct": round(atr_pct, 6)}}
        if atr_pct > ATR_PCT_MAX:
            return {"signal": False, "reason": "ATR_TOO_HIGH", "coin": coin, "diag": {"atr_pct": round(atr_pct, 6)}}

        long_ok, long_reason, long_diag = _detect_long_sweep(o, h, l, c, v, ema20, ema50, atr_value)
        short_ok, short_reason, short_diag = _detect_short_sweep(o, h, l, c, v, ema20, ema50, atr_value)

        if not long_ok and not short_ok:
            if LOG_SIGNAL_DIAGNOSTICS:
                _log(f"BLOCK coin={coin} long={long_reason} short={short_reason} atr_pct={atr_pct:.6f} adx={adx5:.2f}")
            return {
                "signal": False,
                "reason": "NO_LIQUIDITY_SWEEP_SETUP",
                "coin": coin,
                "diag": {
                    "long_reason": long_reason,
                    "short_reason": short_reason,
                    "atr_pct": round(atr_pct, 6),
                    "adx5": round(adx5, 2),
                },
            }

        candidate = None
        if long_ok and short_ok:
            candidate = long_diag if float(long_diag.get("score", 0.0)) >= float(short_diag.get("score", 0.0)) else short_diag
        else:
            candidate = long_diag if long_ok else short_diag

        direction = str(candidate.get("direction") or "")
        if direction not in {"long", "short"}:
            return {"signal": False, "reason": "BAD_DIRECTION", "coin": coin}

        stop_price = float(candidate.get("stop_price") or 0.0)
        sl_pct = ((close5 - stop_price) / max(close5, 1e-12)) if direction == "long" else ((stop_price - close5) / max(close5, 1e-12))
        sl_pct = _clamp(float(sl_pct), SL_MIN_PCT, SL_MAX_PCT)

        score = float(candidate.get("score") or 0.0)
        if score < 74.0:
            return {
                "signal": False,
                "reason": "SCORE_TOO_LOW",
                "coin": coin,
                "diag": {"score": round(score, 2), "min_score": 74.0, "candidate": dict(candidate)},
            }

        strength = _clamp(score / 100.0, 0.22, 0.96)
        mgmt = _compute_management_params(strength, score, atr_pct)

        out = {
            "signal": True,
            "direction": direction,
            "strength": round(strength, 4),
            "score": round(score, 2),
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
            "vol_regime": str(mgmt.get("vol_regime", "volatile")),
            "atr_pct": round(float(atr_pct), 6),
            "close_5": round(close5, 6),
            "last_candle_t_5m": int(last_t),
            "ema20_5m": round(float(ema20[-1]), 6),
            "adx1": round(adx5, 2),
            "adx15": round(adx5, 2),
            "coin": coin,
            "strategy_model": STRATEGY_MODEL,
            "sweep_level": float(candidate.get("liquidity_level") or 0.0),
            "sweep_idx": int(candidate.get("sweep_idx") or 0),
            "bars_since_sweep": int(candidate.get("bars_since_sweep") or 0),
            "sweep_depth_atr": float(candidate.get("sweep_depth_atr") or 0.0),
            "sweep_wick_ratio": float(candidate.get("sweep_wick_ratio") or 0.0),
            "sweep_rvol": float(candidate.get("sweep_rvol") or 0.0),
            "trigger_rvol": float(candidate.get("trigger_rvol") or 0.0),
            "trigger_body_ratio": float(candidate.get("trigger_body_ratio") or 0.0),
            "trigger_close_pos": float(candidate.get("trigger_close_pos") or 0.0),
            "rr_estimate": float(candidate.get("rr_estimate") or 0.0),
            "market_context_status": str(market_context.get("status") or status),
        }
        if LOG_SIGNAL_DIAGNOSTICS:
            _log(
                f"SIGNAL coin={coin} dir={direction} close_5={out['close_5']} adx5={adx5:.2f} atr_pct={atr_pct:.6f} "
                f"score={out['score']:.2f} strength={out['strength']:.4f} sweep_depth_atr={out['sweep_depth_atr']:.4f} "
                f"bars_since_sweep={out['bars_since_sweep']} rr={out['rr_estimate']:.4f} sl_pct={out['sl_price_pct']:.6f} "
                f"tp_fixed={out['tp_activation_price']:.6f} be_act={out['break_even_activation_price']:.6f} be_offset={out['break_even_offset_price']:.6f}"
            )
        return out
    except Exception as e:
        return {"signal": False, "reason": "STRATEGY_EXCEPTION", "error": str(e)[:180]}


def get_entry_signal(symbol: str, market_context: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    market_context = market_context or build_market_context(
        symbol=symbol,
        interval=TF_5M,
        limit=LOOKBACK_5M,
        ema_periods=(EMA_FAST, EMA_MID, EMA_SLOW),
        adx_period=ADX_PERIOD,
        atr_period=ATR_PERIOD,
    )
    return _evaluate_market_context(market_context)


class LiquiditySweepReversalStrategy(BaseStrategy):
    strategy_id = STRATEGY_ID
    strategy_version = STRATEGY_VERSION
    strategy_model = STRATEGY_MODEL

    def evaluate(self, symbol: str, market_context: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        out = get_entry_signal(symbol, market_context=market_context)
        if isinstance(out, dict) and out.get("signal"):
            out.setdefault("strategy_id", self.strategy_id)
            out.setdefault("strategy_version", self.strategy_version)
            out.setdefault("strategy_model", self.strategy_model)
        return out

    def get_trade_management_params(self, strength: float, score: float, atr_pct: Optional[float] = None) -> Dict[str, Any]:
        return get_trade_management_params(strength, score, atr_pct)


DEFAULT_STRATEGY = LiquiditySweepReversalStrategy()
