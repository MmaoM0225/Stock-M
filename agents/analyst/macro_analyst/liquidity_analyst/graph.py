"""
Liquidity Analyst（流动性分析师）- 图结构

流程：liquidity_fetch → liquidity_analysis → liquidity_result_persist → END
"""
from typing import Dict, Any, Optional

from langgraph.graph import StateGraph, START, END

from .node import (
    create_liquidity_fetch_node,
    create_liquidity_analysis_node,
    create_liquidity_result_persist_node,
)


def create_liquidity_analyst_graph(
    llm=None,
    market_fetcher=None,
):
    """
    构建 Liquidity Analyst 图。

    流程：fetch（LPR、M2、社融）→ analysis → 持久化结果 → END

    Args:
        llm: LLM 实例，用于分析节点
        market_fetcher: MarketDataFetcher，不传则节点内创建

    Returns:
        已编译的 Liquidity Analyst 图。
    """
    builder = StateGraph(dict)

    builder.add_node("liquidity_fetch", create_liquidity_fetch_node(market_fetcher))
    builder.add_node("liquidity_analysis", create_liquidity_analysis_node(llm))
    builder.add_node("liquidity_result_persist", create_liquidity_result_persist_node())

    builder.add_edge(START, "liquidity_fetch")
    builder.add_edge("liquidity_fetch", "liquidity_analysis")
    builder.add_edge("liquidity_analysis", "liquidity_result_persist")
    builder.add_edge("liquidity_result_persist", END)

    return builder.compile()
