"""Infraestructura base para estrategias del engine."""

from app.strategies.base import BaseStrategy
from app.strategies.registry import StrategyRegistry, get_strategy_registry

__all__ = [
    "BaseStrategy",
    "StrategyRegistry",
    "get_strategy_registry",
]
