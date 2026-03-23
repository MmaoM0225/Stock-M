"""
Sector Trend Analyst（行业趋势分析师）- 图结构

流程：
1. fetch_only: fetch → analysis → END
2. full_graph: fetch → analysis → [Send(map, ths) | Send(map, sw)] → llm_reduce → END
"""
import operator
from typing import Any, Dict, List

from langchain_core.runnables import RunnableConfig
from langgraph.constants import Send
from langgraph.graph import END, START, StateGraph
from typing_extensions import Annotated, TypedDict

from .node import (
    create_sector_trend_analysis_node,
    create_sector_trend_fetch_node,
    create_sector_trend_llm_map_node,
    create_sector_trend_llm_reduce_node,
)


class SectorTrendState(TypedDict, total=False):
    trade_date: str
    sw_daily_data: Any
    ths_daily_data: Any
    ths_code_to_name: Dict
    sector_trend_meta: Dict
    sector_trend_rank: Dict
    sector_trend_map_results: Annotated[List[Dict], operator.add]
    sector_trend_insight: Dict


def create_sector_trend_fetch_only_graph() -> Any:
    builder = StateGraph(SectorTrendState)
    builder.add_node("sector_trend_fetch", create_sector_trend_fetch_node())
    builder.add_node("sector_trend_analysis", create_sector_trend_analysis_node())
    builder.add_edge(START, "sector_trend_fetch")
    builder.add_edge("sector_trend_fetch", "sector_trend_analysis")
    builder.add_edge("sector_trend_analysis", END)
    return builder.compile()


def _fan_out_to_map(state: Dict, config: RunnableConfig | None = None) -> List[Send]:
    return [
        Send("sector_trend_llm_map", {**state, "_map_source": "ths_concept"}),
        Send("sector_trend_llm_map", {**state, "_map_source": "sw_industry"}),
    ]


def create_sector_trend_analyst_graph(llm=None) -> Any:
    if llm is None:
        return create_sector_trend_fetch_only_graph()

    builder = StateGraph(SectorTrendState)
    builder.add_node("sector_trend_fetch", create_sector_trend_fetch_node())
    builder.add_node("sector_trend_analysis", create_sector_trend_analysis_node())
    builder.add_node("sector_trend_llm_map", create_sector_trend_llm_map_node(llm, source=None))
    builder.add_node("sector_trend_llm_reduce", create_sector_trend_llm_reduce_node(llm))

    builder.add_edge(START, "sector_trend_fetch")
    builder.add_edge("sector_trend_fetch", "sector_trend_analysis")
    builder.add_conditional_edges("sector_trend_analysis", _fan_out_to_map)
    builder.add_edge("sector_trend_llm_map", "sector_trend_llm_reduce")
    builder.add_edge("sector_trend_llm_reduce", END)
    return builder.compile()


__all__ = [
    "create_sector_trend_fetch_only_graph",
    "create_sector_trend_analyst_graph",
]



