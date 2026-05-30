"""
Full Position Decision（满仓长期决策）- 图结构
"""
from typing import Any

from langgraph.graph import END, START, StateGraph

from agents.manager.stock_manager.graph import create_stock_manager_graph

from .node import (
    create_build_decision_context_node,
    create_llm_make_decision_node,
    create_load_upstream_artifacts_node,
    create_persist_portfolio_decision_node,
)


def create_full_position_decision_graph(llm: Any) -> Any:
    stock_manager_graph = create_stock_manager_graph(llm=llm) if llm is not None else None
    builder = StateGraph(dict)
    builder.add_node("load_upstream_artifacts", create_load_upstream_artifacts_node())
    builder.add_node("build_decision_context", create_build_decision_context_node())
    builder.add_node(
        "llm_make_decision",
        create_llm_make_decision_node(llm=llm, stock_manager_graph=stock_manager_graph),
    )
    builder.add_node("persist_decision", create_persist_portfolio_decision_node())

    builder.add_edge(START, "load_upstream_artifacts")
    builder.add_edge("load_upstream_artifacts", "build_decision_context")
    builder.add_edge("build_decision_context", "llm_make_decision")
    builder.add_edge("llm_make_decision", "persist_decision")
    builder.add_edge("persist_decision", END)
    return builder.compile()


__all__ = ["create_full_position_decision_graph"]

