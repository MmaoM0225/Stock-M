"""
Portfolio Decision（组合决策）模块导出。

计算工具集（供Agent调用）：
- calculate_target_shares: 计算目标股数（考虑整手规则）
- calculate_cost_price: 计算成本价（加权平均法）
- calculate_realized_pnl: 计算实现盈亏（减仓/清仓时）
- calculate_unrealized_pnl: 计算持仓盈亏（未实现）
- calculate_total_pnl: 计算总盈亏
- calculate_position_change: 一站式计算仓位变化的所有指标
- normalize_weights: 仓位归一化
- get_calculation_tool: 获取指定工具
- list_calculation_tools: 列出所有可用工具
"""

from .calculation_tools import (
    calculate_cost_price,
    calculate_position_change,
    calculate_realized_pnl,
    calculate_target_shares,
    calculate_total_pnl,
    calculate_unrealized_pnl,
    get_calculation_tool,
    list_calculation_tools,
    normalize_weights,
)
from .graph import create_portfolio_decision_graph

__all__ = [
    "create_portfolio_decision_graph",
    # 计算工具
    "calculate_target_shares",
    "calculate_cost_price",
    "calculate_realized_pnl",
    "calculate_unrealized_pnl",
    "calculate_total_pnl",
    "calculate_position_change",
    "normalize_weights",
    # 工具管理
    "get_calculation_tool",
    "list_calculation_tools",
]
