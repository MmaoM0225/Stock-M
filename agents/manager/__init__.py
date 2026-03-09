"""
Manager 模块 - 各类经理（策略经理等）
"""

from .strategy_manager import (
    create_strategy_graph,
    StrategyState,
    create_daily_pipeline,
)

__all__ = [
    "create_strategy_graph",
    "StrategyState",
    "create_daily_pipeline",
]
