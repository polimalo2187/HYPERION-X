from __future__ import annotations

import time
from typing import Any, Dict, List, Optional, Tuple

from app.hyperliquid_client import make_request, norm_coin


TF_5M = "5m"


def interval_ms(interval: str) -> int:
    return {"5m": 300_000}.get(interval, 0)



def parse_candle(row: dict) -> Optional[dict]:
    try:
        return {
            "t": int(row.get("t", 0)),
            "o": float(row.get("o", 0)),
            "h": float(row.get("h", 0)),
            "l": float(row.get("l", 0)),
            "c": float(row.get("c", 0)),
            "v": float(row.get("v", 0)),
        }
    except Exception:
        return None



def fetch_candles(coin: str, interval: str, limit: int) -> Tuple[List[dict], str]:
    coin = norm_coin(coin)
    if not coin:
        return [], "BAD_SYMBOL"
    step = interval_ms(interval)
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
        resp = make_request("/info", payload)
    except Exception:
        return [], "API_FAIL"

    if resp == {} or resp is None:
        return [], "API_FAIL"
    if not isinstance(resp, list) or not resp:
        return [], "EMPTY"

    candles: List[dict] = []
    for raw in resp:
        if isinstance(raw, dict):
            item = parse_candle(raw)
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



def extract_series(candles: List[dict]) -> Tuple[List[float], List[float], List[float], List[float], List[float]]:
    o, h, l, c, v = [], [], [], [], []
    for x in candles:
        o.append(float(x["o"]))
        h.append(float(x["h"]))
        l.append(float(x["l"]))
        c.append(float(x["c"]))
        v.append(float(x["v"]))
    return o, h, l, c, v



def ema(series: List[float], period: int) -> List[float]:
    if not series:
        return []
    out = [float(series[0])]
    k = 2.0 / (float(period) + 1.0)
    for i in range(1, len(series)):
        out.append((float(series[i]) * k) + (out[-1] * (1.0 - k)))
    return out



def rma(series: List[float], period: int) -> List[float]:
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



def adx(h: List[float], l: List[float], c: List[float], period: int) -> List[float]:
    if len(h) < period + 2 or len(l) < period + 2 or len(c) < period + 2:
        return []
    plus_dm, minus_dm, tr = [0.0], [0.0], [0.0]
    for i in range(1, len(c)):
        up = float(h[i]) - float(h[i - 1])
        down = float(l[i - 1]) - float(l[i])
        plus_dm.append(up if up > down and up > 0 else 0.0)
        minus_dm.append(down if down > up and down > 0 else 0.0)
        tr.append(max(float(h[i]) - float(l[i]), abs(float(h[i]) - float(c[i - 1])), abs(float(l[i]) - float(c[i - 1]))))
    atr_series = rma(tr, period)
    plus = [100.0 * (p / a) if a else 0.0 for p, a in zip(rma(plus_dm, period), atr_series)]
    minus = [100.0 * (m / a) if a else 0.0 for m, a in zip(rma(minus_dm, period), atr_series)]
    dx = [100.0 * abs(p - m) / (p + m) if (p + m) else 0.0 for p, m in zip(plus, minus)]
    return rma(dx, period)



def last(values: List[float]) -> Optional[float]:
    return values[-1] if values else None



def atr(h: List[float], l: List[float], c: List[float], period: int = 14) -> float:
    if len(h) < 2:
        return 0.0
    tr = [0.0]
    for i in range(1, len(c)):
        tr.append(max(float(h[i]) - float(l[i]), abs(float(h[i]) - float(c[i - 1])), abs(float(l[i]) - float(c[i - 1]))))
    return float(last(rma(tr, period)) or 0.0)



def pct_change(now: float, prev: float) -> float:
    if prev == 0:
        return 0.0
    return (now - prev) / prev



def is_stale(candles: List[dict], interval: str) -> Tuple[bool, float, int]:
    if not candles:
        return True, 9e9, 0
    last_t = int(candles[-1]["t"])
    age_s = max(0.0, (time.time() * 1000.0 - last_t) / 1000.0)
    interval_s = interval_ms(interval) / 1000.0
    return age_s > (interval_s * 3.0), age_s, last_t



def build_timeframe_context(coin: str, interval: str, limit: int, ema_periods: tuple[int, ...] = (20, 50, 200), adx_period: int = 14, atr_period: int = 14) -> Dict[str, Any]:
    candles, status = fetch_candles(coin, interval, limit)
    snapshot: Dict[str, Any] = {
        "interval": interval,
        "limit": int(limit),
        "status": status,
        "candles": candles,
        "stale": True,
        "age_s": 9e9,
        "last_t": 0,
        "o": [],
        "h": [],
        "l": [],
        "c": [],
        "v": [],
        "close": 0.0,
        "atr": 0.0,
        "atr_pct": 0.0,
        "adx": 0.0,
    }
    if status != "OK" or not candles:
        return snapshot

    stale, age_s, last_t = is_stale(candles, interval)
    o, h, l, c, v = extract_series(candles)
    close = float(c[-1]) if c else 0.0
    atr_value = float(atr(h, l, c, atr_period) or 0.0)
    adx_series = adx(h, l, c, adx_period)

    snapshot.update({
        "stale": bool(stale),
        "age_s": float(age_s),
        "last_t": int(last_t),
        "o": o,
        "h": h,
        "l": l,
        "c": c,
        "v": v,
        "close": close,
        "atr": atr_value,
        "atr_pct": (atr_value / close) if close > 0 else 0.0,
        "adx": float(last(adx_series) or 0.0),
        "adx_series": adx_series,
    })

    for period in ema_periods:
        snapshot[f"ema{int(period)}"] = ema(c, int(period))

    return snapshot



def build_market_context(symbol: str, interval: str = TF_5M, limit: int = 320, ema_periods: tuple[int, ...] = (20, 50, 200), adx_period: int = 14, atr_period: int = 14) -> Dict[str, Any]:
    coin = norm_coin(symbol)
    if not coin:
        return {
            "symbol": symbol,
            "coin": "",
            "status": "BAD_SYMBOL",
            "timeframes": {},
        }

    tf_ctx = build_timeframe_context(
        coin=coin,
        interval=interval,
        limit=limit,
        ema_periods=ema_periods,
        adx_period=adx_period,
        atr_period=atr_period,
    )
    return {
        "symbol": symbol,
        "coin": coin,
        "status": tf_ctx.get("status", "UNKNOWN"),
        "timeframes": {interval: tf_ctx},
    }


__all__ = [
    "TF_5M",
    "adx",
    "atr",
    "build_market_context",
    "build_timeframe_context",
    "ema",
    "extract_series",
    "fetch_candles",
    "interval_ms",
    "is_stale",
    "last",
    "parse_candle",
    "pct_change",
    "rma",
]
