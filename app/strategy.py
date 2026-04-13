from __future__ import annotations

from typing import Any, Dict, Optional

from app.strategies.breakout_reset import DEFAULT_STRATEGY, STRATEGY_ID, STRATEGY_MODEL, STRATEGY_VERSION
from app.strategies.router import get_strategy_router


# Shim de compatibilidad: el engine actual sigue importando desde app.strategy.
# En Fase 4 ya delegamos al router central, pero mantenemos intacto el contrato
# legacy para no romper el engine en producción.


def _default_strategy():
    return DEFAULT_STRATEGY


def _router():
    return get_strategy_router()


def get_trade_management_params(strength: float, score: float, atr_pct: Optional[float] = None) -> Dict[str, Any]:
    return _default_strategy().get_trade_management_params(strength, score, atr_pct)


def get_entry_signal(symbol: str) -> dict:
    out = _router().route_symbol(symbol)
    if isinstance(out, dict) and out.get("signal"):
        out.setdefault("strategy_id", STRATEGY_ID)
        out.setdefault("strategy_version", STRATEGY_VERSION)
        out.setdefault("strategy_model", STRATEGY_MODEL)
    return out


__all__ = [
    "get_entry_signal",
    "get_trade_management_params",
    "STRATEGY_ID",
    "STRATEGY_MODEL",
    "STRATEGY_VERSION",
]
