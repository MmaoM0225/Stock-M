"""
Stock Fundamental Analyst（股票基本面分析师）- 图结构

流程：
- fetch_only: detect_cache -> [cache_hit?] -> fetch -> analysis -> END
- full: detect_cache -> [cache_hit?] -> fetch -> analysis -> company_basic_insight -> [5个map并行] -> reduce -> persist -> END
                              └-> (skip to END)
"""
from typing import Any, Dict, List

from langgraph.graph import END, START, StateGraph
from typing_extensions import TypedDict

from .node import (
    create_detect_fundamental_cache_node,
    create_dividend_map_node,
    create_fundamental_persist_node,
    create_fundamental_reduce_node,
    create_cashflow_map_node,
    create_balancesheet_map_node,
    create_company_basic_insight_node,
    create_income_map_node,
    create_stock_fundamental_analysis_node,
    create_stock_fundamental_fetch_node,
    create_valuation_map_node,
)


class StockFundamentalState(TypedDict, total=False):
    ts_code: str
    trade_date: str
    stock_fundamental_meta: Dict[str, Any]
    stock_company_info: Dict[str, Any]
    stock_fundamental_daily: List[Dict[str, Any]]
    stock_income_data: List[Dict[str, Any]]
    stock_cashflow_data: List[Dict[str, Any]]
    stock_balancesheet_data: List[Dict[str, Any]]
    stock_dividend_data: List[Dict[str, Any]]
    stock_fundamental_facts: Dict[str, Any]
    fundamental_base_profile: Dict[str, Any]
    company_profile_text: str
    company_basic_analysis: Dict[str, Any]
    valuation_map_analysis: Dict[str, Any]
    income_map_analysis: Dict[str, Any]
    cashflow_map_analysis: Dict[str, Any]
    balancesheet_map_analysis: Dict[str, Any]
    dividend_map_analysis: Dict[str, Any]
    fundamental_reduce_result: Dict[str, Any]


def create_stock_fundamental_fetch_only_graph() -> Any:
    """仅执行基础数据获取与标准化封装。"""
    builder = StateGraph(StockFundamentalState)
    builder.add_node("detect_cache", create_detect_fundamental_cache_node())
    builder.add_node("stock_fundamental_fetch", create_stock_fundamental_fetch_node())
    builder.add_node("stock_fundamental_analysis", create_stock_fundamental_analysis_node())

    builder.add_edge(START, "detect_cache")

    def cache_router(state):
        if state.get("fundamental_cache_hit"):
            return END
        return "stock_fundamental_fetch"

    builder.add_conditional_edges("detect_cache", cache_router)
    builder.add_edge("stock_fundamental_fetch", "stock_fundamental_analysis")
    builder.add_edge("stock_fundamental_analysis", END)
    return builder.compile()


def create_stock_fundamental_analyst_graph(llm=None) -> Any:
    """默认编译完整图；llm 为空时降级为 fetch-only 图。"""
    if llm is None:
        return create_stock_fundamental_fetch_only_graph()

    builder = StateGraph(StockFundamentalState)
    builder.add_node("detect_cache", create_detect_fundamental_cache_node())
    builder.add_node("stock_fundamental_fetch", create_stock_fundamental_fetch_node())
    builder.add_node("stock_fundamental_analysis", create_stock_fundamental_analysis_node())
    builder.add_node("company_basic_insight", create_company_basic_insight_node(llm))
    builder.add_node("valuation_map", create_valuation_map_node(llm))
    builder.add_node("income_map", create_income_map_node(llm))
    builder.add_node("cashflow_map", create_cashflow_map_node(llm))
    builder.add_node("balancesheet_map", create_balancesheet_map_node(llm))
    builder.add_node("dividend_map", create_dividend_map_node(llm))
    builder.add_node("fundamental_reduce", create_fundamental_reduce_node(llm))
    builder.add_node("persist", create_fundamental_persist_node())

    # 添加边和条件路由
    builder.add_edge(START, "detect_cache")

    def cache_router(state):
        if state.get("fundamental_cache_hit"):
            return END
        return "stock_fundamental_fetch"

    builder.add_conditional_edges("detect_cache", cache_router)
    builder.add_edge("stock_fundamental_fetch", "stock_fundamental_analysis")
    builder.add_edge("stock_fundamental_analysis", "company_basic_insight")
    builder.add_edge("company_basic_insight", "valuation_map")
    builder.add_edge("company_basic_insight", "income_map")
    builder.add_edge("company_basic_insight", "cashflow_map")
    builder.add_edge("company_basic_insight", "balancesheet_map")
    builder.add_edge("company_basic_insight", "dividend_map")

    builder.add_edge("valuation_map", "fundamental_reduce")
    builder.add_edge("income_map", "fundamental_reduce")
    builder.add_edge("cashflow_map", "fundamental_reduce")
    builder.add_edge("balancesheet_map", "fundamental_reduce")
    builder.add_edge("dividend_map", "fundamental_reduce")
    builder.add_edge("fundamental_reduce", "persist")
    builder.add_edge("persist", END)
    return builder.compile()


__all__ = [
    "StockFundamentalState",
    "create_stock_fundamental_fetch_only_graph",
    "create_stock_fundamental_analyst_graph",
]

