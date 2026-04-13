from __future__ import annotations

from typing import Dict

from app.strategies.base import BaseStrategy

DEFAULT_STRATEGY_ID = "breakout_reset"


class StrategyRegistry:
    """Registro simple de estrategias.

    En Fase 1 solo mantiene la estrategia actual para preparar la migración
    multi-estrategia sin cambiar el contrato del engine.
    """

    def __init__(self) -> None:
        from app.strategies.breakout_reset import DEFAULT_STRATEGY

        self._strategies: Dict[str, BaseStrategy] = {DEFAULT_STRATEGY_ID: DEFAULT_STRATEGY}

    def register(self, strategy: BaseStrategy) -> None:
        if strategy is None:
            raise ValueError("strategy is required")
        strategy_id = str(getattr(strategy, "strategy_id", "") or "").strip()
        if not strategy_id:
            raise ValueError("strategy_id is required")
        self._strategies[strategy_id] = strategy

    def get(self, strategy_id: str = DEFAULT_STRATEGY_ID) -> BaseStrategy:
        sid = str(strategy_id or DEFAULT_STRATEGY_ID).strip() or DEFAULT_STRATEGY_ID
        if sid not in self._strategies:
            raise KeyError(f"Unknown strategy_id: {sid}")
        return self._strategies[sid]

    def get_default(self) -> BaseStrategy:
        return self.get(DEFAULT_STRATEGY_ID)

    def list_ids(self) -> list[str]:
        return list(self._strategies.keys())


_REGISTRY = StrategyRegistry()


def get_strategy_registry() -> StrategyRegistry:
    return _REGISTRY
