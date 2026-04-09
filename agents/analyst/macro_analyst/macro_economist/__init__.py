"""
Macro Economist（宏观经济分析师）模块。

专注分析：GDP、CPI、PMI、利率、M2。
当前：GDP、CPI、LPR（利率）、社融、PMI、M2 已全部接入。
"""

from .graph import create_macro_economist_graph
from .node import create_macro_economist_fetch_node, create_macro_economist_analysis_node

__all__ = [
    "create_macro_economist_graph",
    "create_macro_economist_fetch_node",
    "create_macro_economist_analysis_node",
]
