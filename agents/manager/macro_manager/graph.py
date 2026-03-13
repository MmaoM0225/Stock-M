"""
Macro Manager（宏观管理器）- 图结构

流程：START → run_analysts（并行运行 5 个分析师子图）→ END
"""
from typing import Any, Dict, List, Optional, Tuple

from langchain_core.runnables import RunnableConfig
from langgraph.graph import END, StateGraph, START

from agents.config import (
    MACRO_GENERATE_MARKDOWN,
    MACRO_MANAGER_MAX_CONCURRENT_SUBGRAPHS,
)

from .node import (
    create_run_analysts_node,
    create_macro_summary_node,
    create_write_macro_report_node,
    get_macro_config,
)


def _route_after_macro_summary(state: dict, config: RunnableConfig | None = None) -> str:
    """macro_summary 之后：若 MACRO_GENERATE_MARKDOWN 则写报告，否则直接结束。"""
    cfg = get_macro_config(config)
    return "write_macro_report" if cfg.get("generate_markdown", MACRO_GENERATE_MARKDOWN) else END


def create_macro_manager_graph(
    llm: Any,
    news_fetcher: Optional[Any] = None,
    max_concurrent_subgraphs: Optional[int] = None,
) -> Any:
    """
    构建 Macro Manager 图：编排 5 个微观分析师子图，控制并发并汇总结果。

    流程：run_analysts 内并行执行 5 个子图 → macro_summary（LLM 综合结论与报告）→ write_macro_report 将结果写入 data/analysis/{trade_date}_macro_report.md。

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
    from agents.analyst.news_analyst.graph import create_news_graph
    from agents.analyst.market_sentiment_analyst.graph import (
        create_market_sentiment_analyst_graph,
    )
    from agents.analyst.liquidity_analyst.graph import create_liquidity_analyst_graph
    from agents.analyst.commodity_analyst.graph import create_commodity_analyst_graph
    from agents.analyst.macro_economist.graph import create_macro_economist_graph

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
    macro_summary_node = create_macro_summary_node(llm=llm)
    write_macro_report_node = create_write_macro_report_node()

    builder = StateGraph(dict)
    builder.add_node("run_analysts", run_analysts_node)
    builder.add_node("macro_summary", macro_summary_node)
    builder.add_node("write_macro_report", write_macro_report_node)
    builder.add_edge(START, "run_analysts")
    builder.add_edge("run_analysts", "macro_summary")
    builder.add_conditional_edges("macro_summary", _route_after_macro_summary)
    builder.add_edge("write_macro_report", END)

    return builder.compile()
