"""
Macro Manager（宏观管理器）- 编排多个微观分析师子图，控制并发并汇总结果。
"""

from .graph import create_macro_manager_graph

__all__ = [
    "create_macro_manager_graph",
]
