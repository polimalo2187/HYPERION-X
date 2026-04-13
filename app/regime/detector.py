from __future__ import annotations

import os
from typing import Any, Dict, Optional

TF_5M = "5m"
from app.regime.features import build_regime_features
from app.regime.state_machine import (
    DEFAULT_CONFIRM_BARS,
    DEFAULT_COOLDOWN_BARS,
    DEFAULT_MIN_ACTIVE_BARS,
    advance_regime_state,
)

REGIME_TREND = "TREND_CONTINUATION"
REGIME_VOLATILE = "VOLATILE_SWEEP"
REGIME_RANGE = "RANGE"
REGIME_UNKNOWN = "UNKNOWN"
DETECTOR_VERSION = "v1"


def _env_int(name: str, default: int) -> int:
    try:
        return max(int(os.getenv(name, str(default))), 0)
    except Exception:
        return default



def _env_float(name: str, default: float) -> float:
    try:
        return float(os.getenv(name, str(default)))
    except Exception:
        return default



def _score_bool(condition: bool) -> int:
    return 1 if condition else 0



def classify_candidate_regime(features: Dict[str, Any]) -> Dict[str, Any]:
    reasons: list[str] = []
    confidence = 0.0

    if not features.get("context_ok"):
        reasons.append(f"symbol_context_not_ok:{features.get('context_status')}")
        return {
            "candidate_regime": REGIME_UNKNOWN,
            "confidence": 0.0,
            "reasons": reasons,
            "scores": {REGIME_TREND: 0, REGIME_VOLATILE: 0, REGIME_RANGE: 0},
            "regime_bias": "neutral",
        }

    adx = float(features.get("adx") or 0.0)
    chop = float(features.get("choppiness") or 50.0)
    efficiency = float(features.get("efficiency_ratio") or 0.0)
    wick_instability = float(features.get("wick_instability") or 0.0)
    body_quality = float(features.get("body_quality") or 0.0)
    breakout_failure_ratio = float(features.get("breakout_failure_ratio") or 0.0)
    atr_pct = float(features.get("atr_pct") or 0.0)
    btc_shock_ratio = float(features.get("btc_shock_ratio") or 0.0)
    distance_to_vwap_atr = float(features.get("distance_to_vwap_atr") or 0.0)
    ema_stack_alignment = float(features.get("ema_stack_alignment") or 0.0)
    trend_bias = str(features.get("trend_bias") or "neutral")

    trend_adx_min = _env_float("REGIME_TREND_ADX_MIN", 18.0)
    trend_chop_max = _env_float("REGIME_TREND_CHOP_MAX", 51.0)
    trend_eff_min = _env_float("REGIME_TREND_EFFICIENCY_MIN", 0.34)
    volatile_btc_shock_min = _env_float("REGIME_VOLATILE_BTC_SHOCK_MIN", 1.85)
    volatile_wick_min = _env_float("REGIME_VOLATILE_WICK_MIN", 0.60)
    volatile_breakout_fail_min = _env_float("REGIME_VOLATILE_BREAKOUT_FAIL_MIN", 0.34)
    volatile_atr_pct_min = _env_float("REGIME_VOLATILE_ATR_PCT_MIN", 0.010)
    range_adx_max = _env_float("REGIME_RANGE_ADX_MAX", 16.0)
    range_chop_min = _env_float("REGIME_RANGE_CHOP_MIN", 58.0)
    range_eff_max = _env_float("REGIME_RANGE_EFFICIENCY_MAX", 0.28)
    range_vwap_dist_max = _env_float("REGIME_RANGE_VWAP_DIST_ATR_MAX", 0.95)

    volatile_score = 0
    volatile_score += _score_bool(btc_shock_ratio >= volatile_btc_shock_min)
    volatile_score += _score_bool(wick_instability >= volatile_wick_min)
    volatile_score += _score_bool(breakout_failure_ratio >= volatile_breakout_fail_min)
    volatile_score += _score_bool(atr_pct >= volatile_atr_pct_min)
    volatile_score += _score_bool(body_quality <= 0.38 and efficiency <= 0.42)

    trend_score = 0
    trend_score += _score_bool(adx >= trend_adx_min)
    trend_score += _score_bool(chop <= trend_chop_max)
    trend_score += _score_bool(efficiency >= trend_eff_min)
    trend_score += _score_bool(ema_stack_alignment >= 0.66)
    trend_score += _score_bool(breakout_failure_ratio <= 0.15)

    range_score = 0
    range_score += _score_bool(adx <= range_adx_max)
    range_score += _score_bool(chop >= range_chop_min)
    range_score += _score_bool(efficiency <= range_eff_max)
    range_score += _score_bool(distance_to_vwap_atr <= range_vwap_dist_max)
    range_score += _score_bool(breakout_failure_ratio >= 0.12)

    if volatile_score >= 3 and volatile_score >= trend_score and volatile_score >= range_score:
        candidate = REGIME_VOLATILE
        confidence = min(0.99, 0.45 + 0.11 * volatile_score)
        reasons.extend([
            f"btc_shock_ratio={btc_shock_ratio:.2f}",
            f"wick_instability={wick_instability:.2f}",
            f"breakout_failure_ratio={breakout_failure_ratio:.2f}",
        ])
    elif trend_score >= 4 and trend_score > volatile_score and trend_score >= range_score:
        candidate = REGIME_TREND
        confidence = min(0.99, 0.40 + 0.10 * trend_score)
        reasons.extend([
            f"adx={adx:.2f}",
            f"choppiness={chop:.2f}",
            f"efficiency_ratio={efficiency:.2f}",
            f"ema_stack_alignment={ema_stack_alignment:.2f}",
        ])
    elif range_score >= 4 and volatile_score <= 2:
        candidate = REGIME_RANGE
        confidence = min(0.99, 0.40 + 0.10 * range_score)
        reasons.extend([
            f"adx={adx:.2f}",
            f"choppiness={chop:.2f}",
            f"efficiency_ratio={efficiency:.2f}",
            f"distance_to_vwap_atr={distance_to_vwap_atr:.2f}",
        ])
    else:
        candidate = REGIME_UNKNOWN
        confidence = 0.20
        reasons.extend([
            f"mixed_scores trend={trend_score} volatile={volatile_score} range={range_score}",
            f"adx={adx:.2f}",
            f"choppiness={chop:.2f}",
        ])

    return {
        "candidate_regime": candidate,
        "confidence": confidence,
        "reasons": reasons,
        "scores": {
            REGIME_TREND: trend_score,
            REGIME_VOLATILE: volatile_score,
            REGIME_RANGE: range_score,
        },
        "regime_bias": trend_bias,
    }



def detect_regime(
    symbol: str,
    market_context: Optional[Dict[str, Any]] = None,
    btc_context: Optional[Dict[str, Any]] = None,
    previous_state: Optional[Dict[str, Any]] = None,
    *,
    interval: str = TF_5M,
) -> Dict[str, Any]:
    if market_context is None or btc_context is None:
        from app.market_context import build_market_context

    ctx = market_context or build_market_context(symbol, interval=interval)
    btc_ctx = btc_context or build_market_context("BTC-USDC", interval=interval)
    features = build_regime_features(symbol, market_context=ctx, btc_context=btc_ctx, interval=interval)
    candidate = classify_candidate_regime(features)

    state = advance_regime_state(
        candidate_regime=str(candidate["candidate_regime"]),
        previous_state=previous_state,
        confirm_bars=_env_int("REGIME_CONFIRM_BARS", DEFAULT_CONFIRM_BARS),
        cooldown_bars=_env_int("REGIME_COOLDOWN_BARS", DEFAULT_COOLDOWN_BARS),
        min_active_bars=_env_int("REGIME_MIN_ACTIVE_BARS", DEFAULT_MIN_ACTIVE_BARS),
    )

    return {
        "symbol": symbol,
        "interval": interval,
        "detector_version": DETECTOR_VERSION,
        "features": features,
        "candidate_regime": candidate["candidate_regime"],
        "candidate_confidence": candidate["confidence"],
        "candidate_reasons": candidate["reasons"],
        "candidate_scores": candidate["scores"],
        "regime_bias": candidate["regime_bias"],
        "state": state,
        "active_regime": state.get("active_regime", REGIME_UNKNOWN),
        "changed": bool(state.get("changed")),
    }


__all__ = [
    "DETECTOR_VERSION",
    "REGIME_RANGE",
    "REGIME_TREND",
    "REGIME_UNKNOWN",
    "REGIME_VOLATILE",
    "classify_candidate_regime",
    "detect_regime",
]
