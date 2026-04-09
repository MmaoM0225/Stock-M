"""
Stock Screener（股票筛选分析师）- 图结构

流程：START → parse_criteria → fetch_stock_pool → apply_filters → format_output → END
"""
from typing import Any, Dict

from langgraph.graph import END, START, StateGraph

from .node import (
    create_apply_filters_node,
    create_fetch_stock_pool_node,
    create_format_output_node,
    create_parse_criteria_node,
)


def _route_after_parse(state: Dict[str, Any]) -> str:
    """解析条件后路由：如果有错误直接跳转到格式化输出"""
    errors = state.get("_criteria_errors")
    if errors:
        return "format_output"
    return "fetch_stock_pool"


def _route_after_fetch(state: Dict[str, Any]) -> str:
    """获取股票池后路由：如果有错误直接跳转到格式化输出"""
    error = state.get("_fetch_error")
    if error:
        return "format_output"
    return "apply_filters"


def create_stock_screener_graph() -> Any:
    """
    构建 Stock Screener 图

    流程:
    1. parse_criteria: 解析筛选条件
    2. fetch_stock_pool: 获取初始股票池
    3. apply_filters: 应用筛选条件
    4. format_output: 格式化输出结果

    Returns:
        已编译的 Stock Screener 图
    """
    builder = StateGraph(dict)

    # 添加节点
    builder.add_node("parse_criteria", create_parse_criteria_node())
    builder.add_node("fetch_stock_pool", create_fetch_stock_pool_node())
    builder.add_node("apply_filters", create_apply_filters_node())
    builder.add_node("format_output", create_format_output_node())

    # 添加边
    builder.add_edge(START, "parse_criteria")
    builder.add_conditional_edges("parse_criteria", _route_after_parse)
    builder.add_conditional_edges("fetch_stock_pool", _route_after_fetch)
    builder.add_edge("apply_filters", "format_output")
    builder.add_edge("format_output", END)

    return builder.compile()


__all__ = ["create_stock_screener_graph"]
