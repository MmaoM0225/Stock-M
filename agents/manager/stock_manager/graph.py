"""
Stock Manager（个股研究经理）- 图结构

流程：START -> run_analysts（并行 basic+technical） -> stock_summary -> END
"""
from typing import Any, List, Optional, Tuple

from langgraph.graph import END, START, StateGraph

from .node import create_run_analysts_node, create_stock_summary_node


def create_stock_manager_graph(
    llm: Any,
    max_concurrent_subgraphs: Optional[int] = None,
) -> Any:
    max_workers = max_concurrent_subgraphs if max_concurrent_subgraphs is not None else 2

    from agents.analyst.stock_fundamental_analyst.graph import create_stock_fundamental_analyst_graph
    from agents.analyst.stock_technical_analyst.graph import create_stock_technical_analyst_graph

    fundamental_graph = create_stock_fundamental_analyst_graph(llm=llm)
    technical_graph = create_stock_technical_analyst_graph(llm=llm)

    analyst_tasks: List[Tuple[str, Any, str]] = [
        ("stock_fundamental", fundamental_graph, "fundamental_reduce_result"),
        ("stock_technical", technical_graph, "technical_analysis"),
    ]

    builder = StateGraph(dict)
    builder.add_node(
        "run_analysts",
        create_run_analysts_node(
            analyst_tasks=analyst_tasks,
            max_workers=max_workers,
        ),
    )
    builder.add_node("stock_summary", create_stock_summary_node(llm=llm))
    builder.add_edge(START, "run_analysts")
    builder.add_edge("run_analysts", "stock_summary")
    builder.add_edge("stock_summary", END)
    return builder.compile()


__all__ = ["create_stock_manager_graph"]
