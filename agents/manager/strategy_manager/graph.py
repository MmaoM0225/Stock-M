"""
策略经理 - 图结构

流程：strategy_synthesize → [generate_markdown?] → strategy_markdown_write | END
配置通过 RunnableConfig 传入。
"""
from typing import Dict, Any, Optional

from langchain_core.runnables import RunnableConfig
from typing_extensions import TypedDict
from langgraph.graph import StateGraph, START, END

from ...config import STRATEGY_GENERATE_MARKDOWN
from ...utils import get_strategy_config

from .node import (
    create_strategy_synthesize_node,
    create_strategy_markdown_write_node,
)


class StrategyState(TypedDict, total=False):
    """策略经理状态定义。"""

    trade_date: str
    macro_analysis: Dict[str, Any]
    news_analysis: Dict[str, Any]
    strategy_analysis: Dict[str, Any]
    messages: list


def _route_after_strategy_synthesize(
    state: Dict[str, Any], config: RunnableConfig | None = None
) -> str:
    """strategy_synthesize 之后：若配置 generate_markdown 则进入 markdown_write，否则 END。"""
    cfg = get_strategy_config(config) if config else {}
    generate = cfg.get("generate_markdown", STRATEGY_GENERATE_MARKDOWN)
    return "strategy_markdown_write" if generate else END


def create_strategy_graph(llm=None):
    """
    构建策略经理图。

    流程：
    1. strategy_synthesize：综合 macro_analysis 与 news_analysis，生成 strategy_analysis
    2. 条件分支：若 generate_markdown 则写入 Markdown 文件

    Args:
        llm: LLM 实例，用于策略综合节点（可为 None，则输出占位结果）

    Returns:
        已编译的策略经理图。
    """
    builder = StateGraph(StrategyState)

    builder.add_node(
        "strategy_synthesize",
        create_strategy_synthesize_node(llm),
    )
    builder.add_node(
        "strategy_markdown_write",
        create_strategy_markdown_write_node(llm),
    )

    builder.add_edge(START, "strategy_synthesize")
    builder.add_conditional_edges(
        "strategy_synthesize",
        _route_after_strategy_synthesize,
    )
    builder.add_edge("strategy_markdown_write", END)

    return builder.compile()
