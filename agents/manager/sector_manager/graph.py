"""
Sector Manager（行业管理器）- 图结构

流程：START → run_analysts（并行运行 2 个行业分析师子图）
     → sector_summary → END
"""
from typing import Any, List, Optional, Tuple

from langgraph.graph import END, START, StateGraph

from agents.config import SECTOR_MANAGER_MAX_CONCURRENT_SUBGRAPHS

from .node import (
    create_run_analysts_node,
    create_sector_summary_node,
)


def create_sector_manager_graph(
    llm: Any,
    max_concurrent_subgraphs: Optional[int] = None,
) -> Any:
    """
    构建 Sector Manager 图：编排行业趋势与板块资金流两个分析师子图，控制并发并汇总结果。

    流程：run_analysts 内并行执行 2 个子图 → sector_summary（LLM 综合结论）→ END。
    """
    max_workers = (
        max_concurrent_subgraphs
        if max_concurrent_subgraphs is not None
        else SECTOR_MANAGER_MAX_CONCURRENT_SUBGRAPHS
    )

    from agents.analyst.sector_analyst.sector_capital_flow_analyst.graph import (
        create_sector_capital_flow_analyst_graph,
    )
    from agents.analyst.sector_analyst.sector_trend_analyst.graph import (
        create_sector_trend_analyst_graph,
    )

    sector_trend_graph = create_sector_trend_analyst_graph(llm=llm)
    sector_capital_flow_graph = create_sector_capital_flow_analyst_graph(llm=llm)

    analyst_tasks: List[Tuple[str, Any, str]] = [
        ("sector_trend", sector_trend_graph, "sector_trend_insight"),
        ("sector_capital_flow", sector_capital_flow_graph, "sector_capital_flow_insight"),
    ]

    builder = StateGraph(dict)
    builder.add_node(
        "run_analysts",
        create_run_analysts_node(
            analyst_tasks=analyst_tasks,
            max_workers=max_workers,
        ),
    )
    builder.add_node("sector_summary", create_sector_summary_node(llm=llm))

    builder.add_edge(START, "run_analysts")
    builder.add_edge("run_analysts", "sector_summary")
    builder.add_edge("sector_summary", END)

    return builder.compile()


__all__ = [
    "create_sector_manager_graph",
]
