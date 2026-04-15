"""
Macro Manager（宏观管理器）- 图结构

流程：START → detect_available_analysts → run_analysts（仅补跑缺失子图）→ macro_summary → END
"""
from typing import Any, List, Optional, Tuple

from langgraph.graph import END, START, StateGraph

from agents.config import MACRO_MANAGER_MAX_CONCURRENT_SUBGRAPHS

from .node import (
    create_detect_available_analysts_node,
    create_macro_summary_node,
    create_run_analysts_node,
)


def create_macro_manager_graph(
    llm: Any,
    news_fetcher: Optional[Any] = None,
    max_concurrent_subgraphs: Optional[int] = None,
) -> Any:
    """
    构建 Macro Manager 图：编排 5 个微观分析师子图，控制并发并汇总结果。

    流程：先检测本地已存在的 analyst 结果，仅补跑缺失子图，再统一执行 macro_summary → END。

    Args:
        llm: LLM 实例，供各分析师子图共用
        news_fetcher: NewsSentimentFetcher，供新闻子图使用；不传则新闻子图节点内可能自建或失败
        max_concurrent_subgraphs: 最大并发子图数，不传则使用 agents.config.MACRO_MANAGER_MAX_CONCURRENT_SUBGRAPHS

    Returns:
        已编译的 Macro Manager 图。
    """
    max_workers = (
        max_concurrent_subgraphs
        if max_concurrent_subgraphs is not None
        else MACRO_MANAGER_MAX_CONCURRENT_SUBGRAPHS
    )

    # 构建 5 个分析师子图
    from agents.analyst.macro_analyst.news_analyst.graph import create_news_graph
    from agents.analyst.macro_analyst.market_sentiment_analyst.graph import (
        create_market_sentiment_analyst_graph,
    )
    from agents.analyst.macro_analyst.liquidity_analyst.graph import create_liquidity_analyst_graph
    from agents.analyst.macro_analyst.commodity_analyst.graph import create_commodity_analyst_graph
    from agents.analyst.macro_analyst.macro_economist.graph import create_macro_economist_graph

    if news_fetcher is None:
        try:
            from dataflow.news_sentiment import NewsSentimentFetcher
            news_fetcher = NewsSentimentFetcher()
        except Exception:
            news_fetcher = None

    news_graph = create_news_graph(llm, news_fetcher)
    market_sentiment_graph = create_market_sentiment_analyst_graph(llm=llm)
    liquidity_graph = create_liquidity_analyst_graph(llm=llm)
    commodity_graph = create_commodity_analyst_graph(llm=llm)
    macro_economist_graph = create_macro_economist_graph(llm=llm)

    analyst_tasks: List[Tuple[str, Any, str]] = [
        ("news", news_graph, "news_analysis"),
        ("market_sentiment", market_sentiment_graph, "market_sentiment_analyst_summary"),
        ("liquidity", liquidity_graph, "liquidity_analyst_summary"),
        ("commodity", commodity_graph, "commodity_analyst_summary"),
        ("macro_economist", macro_economist_graph, "macro_economist_analysis"),
    ]

    run_analysts_node = create_run_analysts_node(
        analyst_tasks,
        max_workers=max_workers,
    )
    detect_available_analysts_node = create_detect_available_analysts_node(analyst_tasks)
    macro_summary_node = create_macro_summary_node(llm=llm)
    builder = StateGraph(dict)
    builder.add_node("detect_available_analysts", detect_available_analysts_node)
    builder.add_node("run_analysts", run_analysts_node)
    builder.add_node("macro_summary", macro_summary_node)
    builder.add_edge(START, "detect_available_analysts")
    builder.add_edge("detect_available_analysts", "run_analysts")
    builder.add_edge("run_analysts", "macro_summary")
    builder.add_edge("macro_summary", END)

    return builder.compile()
