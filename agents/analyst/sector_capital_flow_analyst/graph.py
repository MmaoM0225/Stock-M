"""
Sector Capital Flow Analyst（板块资金流分析师）- 图结构

流程：fetch → analysis → [Send(map, ths) | Send(map, sw)] 并行 → llm_reduce → END
（两路 Map 并行，结果通过 sector_flow_map_results 用 operator.add 合并，再 Reduce 一次）
"""
import operator
from typing import Any, Dict, List

from langgraph.constants import Send
from langgraph.graph import StateGraph, START, END
from langchain_core.runnables import RunnableConfig
from typing_extensions import Annotated, TypedDict

from .node import (
    create_sector_capital_flow_fetch_node,
    create_sector_capital_flow_analysis_node,
    create_sector_capital_flow_llm_map_node,
    create_sector_capital_flow_llm_reduce_node,
)


class SectorCapitalFlowState(TypedDict, total=False):
    """图内各节点读写需保留的键；仅 sector_flow_map_results 用 operator.add 做并行合并。"""
    trade_date: str
    sector_moneyflow_data: Any
    sw_industry_daily_data: Any
    sector_moneyflow_meta: Dict
    sector_capital_flow_top: Dict
    sector_flow_map_results: Annotated[List[Dict], operator.add]
    sector_capital_flow_insight: Dict


def _fan_out_to_map(state: Dict, config: RunnableConfig | None = None) -> List[Send]:
    """analysis 后并行发起两路 Map：同花顺 / 申万，由同一 map 节点根据 _map_source 分发。"""
    return [
        Send("sector_capital_flow_llm_map", {**state, "_map_source": "ths_concept"}),
        Send("sector_capital_flow_llm_map", {**state, "_map_source": "sw_industry"}),
    ]


def create_sector_capital_flow_analyst_graph(llm) -> Any:
    """
    构建 Sector Capital Flow Analyst 图。

    流程：fetch → analysis → 两路并行 Map(ths_concept | sw_industry) → llm_reduce → END

    Args:
        llm: LLM 实例，用于板块资金流文字解读（必须传入）。

    Returns:
        已编译的 Sector Capital Flow Analyst 图。
    """
    builder = StateGraph(SectorCapitalFlowState)

    builder.add_node(
        "sector_capital_flow_fetch",
        create_sector_capital_flow_fetch_node(),
    )
    builder.add_node(
        "sector_capital_flow_analysis",
        create_sector_capital_flow_analysis_node(),
    )
    builder.add_node(
        "sector_capital_flow_llm_map",
        create_sector_capital_flow_llm_map_node(llm, source=None),
    )
    builder.add_node(
        "sector_capital_flow_llm_reduce",
        create_sector_capital_flow_llm_reduce_node(llm),
    )

    builder.add_edge(START, "sector_capital_flow_fetch")
    builder.add_edge("sector_capital_flow_fetch", "sector_capital_flow_analysis")
    builder.add_conditional_edges("sector_capital_flow_analysis", _fan_out_to_map)
    builder.add_edge("sector_capital_flow_llm_map", "sector_capital_flow_llm_reduce")
    builder.add_edge("sector_capital_flow_llm_reduce", END)

    return builder.compile()


__all__ = [
    "create_sector_capital_flow_analyst_graph",
]

