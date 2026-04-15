"""
Stock Pool Manager（批量个股池经理）- 图结构

流程：START → load_screener_pool → run_stock_pool → pool_reduce → persist → END

对 stock_screener 产出的股票池逐只复用单票 `stock_manager`（基本面+技术面+单票汇总）。
"""
from typing import Any, Optional

from langgraph.graph import END, START, StateGraph

from agents.config import STOCK_POOL_MANAGER_MAX_CONCURRENT_STOCKS

from ..stock_manager.graph import create_stock_manager_graph
from .node import (
    create_load_screener_pool_node,
    create_persist_stock_pool_manager_node,
    create_pool_reduce_node,
    create_run_stock_pool_node,
)


def create_stock_pool_manager_graph(
    llm: Any,
    *,
    max_concurrent_stocks: Optional[int] = None,
    stock_manager_subgraph_concurrency: Optional[int] = None,
) -> Any:
    """
    构建批量个股池管理图。

    Args:
        llm: 与单票 stock_manager 相同，供基本面/技术面/单票汇总使用。
        max_concurrent_stocks: 同时分析的股票数量上限；默认取 config。
        stock_manager_subgraph_concurrency: 传入单票 manager 内两子图并行度（默认同 stock_manager）。
    """
    concurrent_stocks = (
        max_concurrent_stocks
        if max_concurrent_stocks is not None
        else STOCK_POOL_MANAGER_MAX_CONCURRENT_STOCKS
    )
    stock_manager = create_stock_manager_graph(
        llm=llm,
        max_concurrent_subgraphs=stock_manager_subgraph_concurrency,
    )

    builder = StateGraph(dict)
    builder.add_node("load_screener_pool", create_load_screener_pool_node())
    builder.add_node(
        "run_stock_pool",
        create_run_stock_pool_node(stock_manager, max_concurrent_stocks=concurrent_stocks),
    )
    builder.add_node("pool_reduce", create_pool_reduce_node())
    builder.add_node("persist", create_persist_stock_pool_manager_node())

    builder.add_edge(START, "load_screener_pool")
    builder.add_edge("load_screener_pool", "run_stock_pool")
    builder.add_edge("run_stock_pool", "pool_reduce")
    builder.add_edge("pool_reduce", "persist")
    builder.add_edge("persist", END)

    return builder.compile()


__all__ = ["create_stock_pool_manager_graph"]
