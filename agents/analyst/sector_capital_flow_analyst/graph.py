"""
Sector Capital Flow Analyst（板块资金流分析师）- 图结构

流程：fetch → analysis → insight → END
"""
from typing import Any, Dict

from langgraph.graph import END, START, StateGraph
from typing_extensions import TypedDict

from .node import (
    create_sector_capital_flow_fetch_node,
    create_sector_capital_flow_analysis_node,
    create_sector_capital_flow_insight_node,
)


class SectorCapitalFlowState(TypedDict, total=False):
    """图内各节点读写需保留的键。"""
    trade_date: str
    sector_moneyflow_data: Any
    sector_moneyflow_meta: Dict
    sector_capital_flow_top: Dict
    sector_capital_flow_insight: Dict


def create_sector_capital_flow_fetch_only_graph() -> Any:
    """
    仅 fetch/analysis 的图（无 LLM 洞察）。
    """
    builder = StateGraph(SectorCapitalFlowState)
    builder.add_node("sector_capital_flow_fetch", create_sector_capital_flow_fetch_node())
    builder.add_node("sector_capital_flow_analysis", create_sector_capital_flow_analysis_node())
    builder.add_edge(START, "sector_capital_flow_fetch")
    builder.add_edge("sector_capital_flow_fetch", "sector_capital_flow_analysis")
    builder.add_edge("sector_capital_flow_analysis", END)
    return builder.compile()


def create_sector_capital_flow_analyst_graph(llm=None) -> Any:
    """
    构建 Sector Capital Flow Analyst 图。

    流程：fetch → analysis → insight → END

    Args:
        llm: LLM 实例，用于板块资金流文字解读（可选，None 时仅执行 fetch/analysis）。

    Returns:
        已编译的 Sector Capital Flow Analyst 图。
    """
    if llm is None:
        return create_sector_capital_flow_fetch_only_graph()

    builder = StateGraph(SectorCapitalFlowState)
    builder.add_node("sector_capital_flow_fetch", create_sector_capital_flow_fetch_node())
    builder.add_node("sector_capital_flow_analysis", create_sector_capital_flow_analysis_node())
    builder.add_node("sector_capital_flow_insight", create_sector_capital_flow_insight_node(llm))

    builder.add_edge(START, "sector_capital_flow_fetch")
    builder.add_edge("sector_capital_flow_fetch", "sector_capital_flow_analysis")
    builder.add_edge("sector_capital_flow_analysis", "sector_capital_flow_insight")
    builder.add_edge("sector_capital_flow_insight", END)
    return builder.compile()


__all__ = [
    "create_sector_capital_flow_fetch_only_graph",
    "create_sector_capital_flow_analyst_graph",
]
