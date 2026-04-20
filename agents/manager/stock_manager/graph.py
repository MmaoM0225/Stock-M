"""
Stock Manager（个股研究经理）- 图结构

流程：START -> detect_cache -> [cache_hit?] -> run_analysts -> stock_summary -> persist -> END
                              └-> (skip to END)
"""
from typing import Any, List, Optional, Tuple

from langgraph.graph import END, START, StateGraph

from .node import (
    create_detect_stock_manager_cache_node,
    create_persist_stock_manager_node,
    create_run_analysts_node,
    create_stock_summary_node,
)


def create_stock_manager_graph(
    llm: Any,
    max_concurrent_subgraphs: Optional[int] = None,
) -> Any:
    max_workers = max_concurrent_subgraphs if max_concurrent_subgraphs is not None else 2

    from agents.analyst.stock_analyst.stock_fundamental_analyst.graph import create_stock_fundamental_analyst_graph
    from agents.analyst.stock_analyst.stock_technical_analyst.graph import create_stock_technical_analyst_graph

    fundamental_graph = create_stock_fundamental_analyst_graph(llm=llm)
    technical_graph = create_stock_technical_analyst_graph(llm=llm)

    analyst_tasks: List[Tuple[str, Any, str]] = [
        ("stock_fundamental", fundamental_graph, "fundamental_reduce_result"),
        ("stock_technical", technical_graph, "technical_analysis"),
    ]

    builder = StateGraph(dict)
    builder.add_node("detect_cache", create_detect_stock_manager_cache_node())
    builder.add_node(
        "run_analysts",
        create_run_analysts_node(
            analyst_tasks=analyst_tasks,
            max_workers=max_workers,
        ),
    )
    builder.add_node("stock_summary", create_stock_summary_node(llm=llm))
    builder.add_node("persist", create_persist_stock_manager_node())

    # 添加边
    builder.add_edge(START, "detect_cache")

    # 条件边：如果缓存命中，直接到 END；否则执行分析师
    def cache_router(state):
        if state.get("stock_manager_cache_hit"):
            return END
        return "run_analysts"

    builder.add_conditional_edges("detect_cache", cache_router)
    builder.add_edge("run_analysts", "stock_summary")
    builder.add_edge("stock_summary", "persist")
    builder.add_edge("persist", END)

    return builder.compile()


__all__ = ["create_stock_manager_graph"]
