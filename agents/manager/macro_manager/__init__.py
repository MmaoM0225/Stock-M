"""
Macro Manager（宏观管理器）- 综合各分析师与新闻，输出可执行策略建议。
"""

from .graph import create_macro_manager_graph, MacroManagerState
from .daily_pipeline import create_daily_pipeline

__all__ = ["create_macro_manager_graph", "MacroManagerState", "create_daily_pipeline"]
