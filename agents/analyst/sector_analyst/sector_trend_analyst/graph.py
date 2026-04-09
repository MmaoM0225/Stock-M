"""
Sector Trend Analyst（行业趋势分析师）- 图结构

流程：fetch → analysis → insight → END
"""
from typing import Any, Dict

from langgraph.graph import END, START, StateGraph
from typing_extensions import TypedDict

from .node import (
    create_sector_trend_analysis_node,
    create_sector_trend_fetch_node,
    create_sector_trend_insight_node,
)


class SectorTrendState(TypedDict, total=False):
    trade_date: str
    ths_daily_data: Any
    ths_code_to_name: Dict
    sector_trend_meta: Dict
    sector_trend_rank: Dict
    sector_trend_insight: Dict


def create_sector_trend_fetch_only_graph() -> Any:
    builder = StateGraph(SectorTrendState)
    builder.add_node("sector_trend_fetch", create_sector_trend_fetch_node())
    builder.add_node("sector_trend_analysis", create_sector_trend_analysis_node())
    builder.add_edge(START, "sector_trend_fetch")
    builder.add_edge("sector_trend_fetch", "sector_trend_analysis")
    builder.add_edge("sector_trend_analysis", END)
    return builder.compile()


def create_sector_trend_analyst_graph(llm=None) -> Any:
    if llm is None:
        return create_sector_trend_fetch_only_graph()

    builder = StateGraph(SectorTrendState)
    builder.add_node("sector_trend_fetch", create_sector_trend_fetch_node())
    builder.add_node("sector_trend_analysis", create_sector_trend_analysis_node())
    builder.add_node("sector_trend_insight", create_sector_trend_insight_node(llm))

    builder.add_edge(START, "sector_trend_fetch")
    builder.add_edge("sector_trend_fetch", "sector_trend_analysis")
    builder.add_edge("sector_trend_analysis", "sector_trend_insight")
    builder.add_edge("sector_trend_insight", END)
    return builder.compile()


__all__ = [
    "create_sector_trend_fetch_only_graph",
    "create_sector_trend_analyst_graph",
]
