"""
Sector Manager（行业管理器）- 图结构

流程：START → run_analysts（并行运行 2 个行业分析师子图）
     → sector_summary → [write_sector_report] → END
"""
from typing import Any, List, Optional, Tuple

from langchain_core.runnables import RunnableConfig
from langgraph.graph import END, START, StateGraph

from agents.config import (
    SECTOR_GENERATE_MARKDOWN,
    SECTOR_MANAGER_MAX_CONCURRENT_SUBGRAPHS,
)

from .node import (
    create_run_analysts_node,
    create_sector_summary_node,
    create_write_sector_report_node,
    get_sector_config,
)


def _route_after_sector_summary(state: dict, config: RunnableConfig | None = None) -> str:
    """sector_summary 之后：若 SECTOR_GENERATE_MARKDOWN 则写报告，否则直接结束。"""
    cfg = get_sector_config(config)
    return "write_sector_report" if cfg.get("generate_markdown", SECTOR_GENERATE_MARKDOWN) else END


def create_sector_manager_graph(
    llm: Any,
    max_concurrent_subgraphs: Optional[int] = None,
) -> Any:
    """
    构建 Sector Manager 图：编排行业趋势与板块资金流两个分析师子图，控制并发并汇总结果。

    流程：run_analysts 内并行执行 2 个子图 → sector_summary（LLM 综合结论与报告）
    → write_sector_report 将结果写入 data/analysis/{trade_date}_sector_report.md。
    """
    max_workers = (
        max_concurrent_subgraphs
        if max_concurrent_subgraphs is not None
        else SECTOR_MANAGER_MAX_CONCURRENT_SUBGRAPHS
    )

    from agents.analyst.sector_capital_flow_analyst.graph import (
        create_sector_capital_flow_analyst_graph,
    )
    from agents.analyst.sector_trend_analyst.graph import (
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
    builder.add_node("write_sector_report", create_write_sector_report_node())

    builder.add_edge(START, "run_analysts")
    builder.add_edge("run_analysts", "sector_summary")
    builder.add_conditional_edges("sector_summary", _route_after_sector_summary)
    builder.add_edge("write_sector_report", END)

    return builder.compile()


__all__ = [
    "create_sector_manager_graph",
]
