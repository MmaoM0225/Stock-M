"""
Stock Technical Analyst（个股技术面分析师）- 图结构

流程：detect_cache -> [cache_hit?] -> fetch -> analysis -> insight -> persist -> END
                              └-> (skip to END)
"""
from typing import Any, Dict, List

from langgraph.graph import END, START, StateGraph
from typing_extensions import TypedDict

from .node import (
    create_detect_technical_cache_node,
    create_stock_technical_analysis_node,
    create_stock_technical_fetch_node,
    create_stock_technical_insight_node,
    create_technical_persist_node,
)


class StockTechnicalState(TypedDict, total=False):
    ts_code: str
    trade_date: str
    stock_technical_meta: Dict[str, Any]
    stock_kline_data: List[Dict[str, Any]]
    stock_technical_facts: Dict[str, Any]
    technical_analysis: Dict[str, Any]


def create_stock_technical_fetch_only_graph() -> Any:
    builder = StateGraph(StockTechnicalState)
    builder.add_node("detect_cache", create_detect_technical_cache_node())
    builder.add_node("stock_technical_fetch", create_stock_technical_fetch_node())
    builder.add_node("stock_technical_analysis", create_stock_technical_analysis_node())

    builder.add_edge(START, "detect_cache")

    def cache_router(state):
        if state.get("technical_cache_hit"):
            return END
        return "stock_technical_fetch"

    builder.add_conditional_edges("detect_cache", cache_router)
    builder.add_edge("stock_technical_fetch", "stock_technical_analysis")
    builder.add_edge("stock_technical_analysis", END)
    return builder.compile()


def create_stock_technical_analyst_graph(llm=None) -> Any:
    if llm is None:
        return create_stock_technical_fetch_only_graph()

    builder = StateGraph(StockTechnicalState)
    builder.add_node("detect_cache", create_detect_technical_cache_node())
    builder.add_node("stock_technical_fetch", create_stock_technical_fetch_node())
    builder.add_node("stock_technical_analysis", create_stock_technical_analysis_node())
    builder.add_node("stock_technical_insight", create_stock_technical_insight_node(llm))
    builder.add_node("persist", create_technical_persist_node())

    builder.add_edge(START, "detect_cache")

    def cache_router(state):
        if state.get("technical_cache_hit"):
            return END
        return "stock_technical_fetch"

    builder.add_conditional_edges("detect_cache", cache_router)
    builder.add_edge("stock_technical_fetch", "stock_technical_analysis")
    builder.add_edge("stock_technical_analysis", "stock_technical_insight")
    builder.add_edge("stock_technical_insight", "persist")
    builder.add_edge("persist", END)
    return builder.compile()


__all__ = [
    "StockTechnicalState",
    "create_stock_technical_fetch_only_graph",
    "create_stock_technical_analyst_graph",
]
