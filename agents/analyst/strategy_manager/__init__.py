"""
策略经理 - 综合宏观与新闻分析，输出可执行策略建议。
"""

from .graph import create_strategy_graph, StrategyState
from .daily_pipeline import create_daily_pipeline

__all__ = ["create_strategy_graph", "StrategyState", "create_daily_pipeline"]
