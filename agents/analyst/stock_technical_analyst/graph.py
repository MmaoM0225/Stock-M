"""
Stock Technical Analyst（个股技术面分析师）- 图结构

流程：fetch -> analysis -> insight -> END
"""
from typing import Any, Dict, List

from langgraph.graph import END, START, StateGraph
from typing_extensions import TypedDict

from .node import (
    create_stock_technical_analysis_node,
    create_stock_technical_fetch_node,
    create_stock_technical_insight_node,
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
    builder.add_node("stock_technical_fetch", create_stock_technical_fetch_node())
    builder.add_node("stock_technical_analysis", create_stock_technical_analysis_node())
    builder.add_edge(START, "stock_technical_fetch")
    builder.add_edge("stock_technical_fetch", "stock_technical_analysis")
    builder.add_edge("stock_technical_analysis", END)
    return builder.compile()


def create_stock_technical_analyst_graph(llm=None) -> Any:
    if llm is None:
        return create_stock_technical_fetch_only_graph()

    builder = StateGraph(StockTechnicalState)
    builder.add_node("stock_technical_fetch", create_stock_technical_fetch_node())
    builder.add_node("stock_technical_analysis", create_stock_technical_analysis_node())
    builder.add_node("stock_technical_insight", create_stock_technical_insight_node(llm))

    builder.add_edge(START, "stock_technical_fetch")
    builder.add_edge("stock_technical_fetch", "stock_technical_analysis")
    builder.add_edge("stock_technical_analysis", "stock_technical_insight")
    builder.add_edge("stock_technical_insight", END)
    return builder.compile()


__all__ = [
    "StockTechnicalState",
    "create_stock_technical_fetch_only_graph",
    "create_stock_technical_analyst_graph",
]
