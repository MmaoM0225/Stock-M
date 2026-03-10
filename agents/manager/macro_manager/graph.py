"""
Macro Manager - 图结构

流程：macro_synthesize → [generate_markdown?] → macro_markdown_write | END
配置通过 RunnableConfig 传入。
"""
from typing import Dict, Any, Optional

from langchain_core.runnables import RunnableConfig
from typing_extensions import TypedDict
from langgraph.graph import StateGraph, START, END

from ...config import STRATEGY_GENERATE_MARKDOWN
from ...utils import get_strategy_config

from .node import (
    create_macro_synthesize_node,
    create_macro_markdown_write_node,
)


class MacroManagerState(TypedDict, total=False):
    """Macro Manager 状态定义。"""

    trade_date: str
    macro_analysis: Dict[str, Any]
    news_analysis: Dict[str, Any]
    strategy_analysis: Dict[str, Any]
    messages: list


def _route_after_macro_synthesize(
    state: Dict[str, Any], config: RunnableConfig | None = None
) -> str:
    """macro_synthesize 之后：若配置 generate_markdown 则进入 markdown_write，否则 END。"""
    cfg = get_strategy_config(config) if config else {}
    generate = cfg.get("generate_markdown", STRATEGY_GENERATE_MARKDOWN)
    return "macro_markdown_write" if generate else END


def create_macro_manager_graph(llm=None):
    """
    构建 Macro Manager 图。

    流程：
    1. macro_synthesize：综合 macro_analysis 与 news_analysis，生成 strategy_analysis
    2. 条件分支：若 generate_markdown 则写入 Markdown 文件

    Args:
        llm: LLM 实例，用于策略综合节点

    Returns:
        已编译的 Macro Manager 图。
    """
    builder = StateGraph(MacroManagerState)

    builder.add_node(
        "macro_synthesize",
        create_macro_synthesize_node(llm),
    )
    builder.add_node(
        "macro_markdown_write",
        create_macro_markdown_write_node(llm),
    )

    builder.add_edge(START, "macro_synthesize")
    builder.add_conditional_edges(
        "macro_synthesize",
        _route_after_macro_synthesize,
    )
    builder.add_edge("macro_markdown_write", END)

    return builder.compile()
