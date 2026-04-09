"""
Macro Economist（宏观经济分析师）- 图结构

流程：macro_economist_fetch → macro_economist_analysis → macro_economist_result_persist → END
"""
from typing import Dict, Any, Optional

from langgraph.graph import StateGraph, START, END

from .node import (
    create_macro_economist_fetch_node,
    create_macro_economist_analysis_node,
    create_macro_economist_result_persist_node,
)


def create_macro_economist_graph(
    llm=None,
    market_fetcher=None,
):
    """
    构建 Macro Economist 图。

    流程：fetch（GDP、CPI、LPR、社融、PMI、M2）→ analysis → 持久化结果 → END

    Args:
        llm: LLM 实例，用于分析节点（可为 None，则仅做数据汇总）
        market_fetcher: MarketDataFetcher，不传则节点内创建

    Returns:
        已编译的 Macro Economist 图。
    """
    builder = StateGraph(dict)  # 使用 dict 以兼容父图合并

    builder.add_node(
        "macro_economist_fetch",
        create_macro_economist_fetch_node(market_fetcher),
    )
    builder.add_node(
        "macro_economist_analysis",
        create_macro_economist_analysis_node(llm),
    )
    builder.add_node(
        "macro_economist_result_persist",
        create_macro_economist_result_persist_node(),
    )

    builder.add_edge(START, "macro_economist_fetch")
    builder.add_edge("macro_economist_fetch", "macro_economist_analysis")
    builder.add_edge("macro_economist_analysis", "macro_economist_result_persist")
    builder.add_edge("macro_economist_result_persist", END)

    return builder.compile()
