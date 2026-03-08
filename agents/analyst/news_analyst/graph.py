from typing import Dict, Any, List, Optional
from typing_extensions import Annotated, TypedDict
import operator

from langchain_core.runnables import RunnableConfig
from langgraph.graph import StateGraph, START, END

from ...config import NEWS_GENERATE_MARKDOWN
from ...utils import get_news_config

from .node import (
    create_news_fetch_node,
    map_sections_to_extract,
    create_news_extract_node,
    create_news_reduce_node,
    create_news_markdown_write_node,
)


class NewsState(TypedDict, total=False):
    """
    新闻分析子图的状态定义。

    - 输入：trade_date（新闻由 fetch 节点通过 fetcher 拉取）
    - 输出：在 news_analysis 中给出结构化分析结果
    """

    trade_date: str
    news_sections: List[Dict[str, Any]]
    all_industries: List[str]
    section: Dict[str, Any]
    # 使用 Annotated + operator.add 让 LangGraph 自动做 map-reduce 聚合
    events: Annotated[List[Dict[str, Any]], operator.add]
    news_analysis: Dict[str, Any]
    messages: List[Any]


def _route_after_news_reduce(state: Dict[str, Any], config: RunnableConfig | None = None) -> str:
    """news_reduce 之后：若配置 generate_markdown 则进入 markdown_write，否则 END。"""
    cfg = get_news_config(config) if config else {}
    generate = cfg.get("generate_markdown", NEWS_GENERATE_MARKDOWN)
    return "news_markdown_write" if generate else END


def create_news_graph(llm, fetcher: Optional[Any] = None):
    """
    构建新闻分析子图（map-reduce 风格）。

    流程：
    1. news_fetch：判断交易日 → 本地/拉取新闻 → 获取完整行业列表（dataflow.industry_data）
    2. map_sections_to_extract：根据 news_sections 动态生成并行的 news_extract 调用（Send）
    3. news_extract：对单条新闻 section 抽取结构化事件（LLM 判断），写入 events
    4. news_reduce：对所有 events 做板块与宏观环境汇总，写入 news_analysis

    Args:
        llm: 支持 with_structured_output 的 LLM 实例
        fetcher: NewsSentimentFetcher，用于本地读取或远程抓取新闻（必须提供）

    Returns:
        已编译的新闻分析子图，可直接 invoke / stream。
    """
    builder = StateGraph(NewsState)

    # 注册节点
    builder.add_node("news_fetch", create_news_fetch_node(fetcher))
    builder.add_node("news_extract", create_news_extract_node(llm))
    builder.add_node("news_reduce", create_news_reduce_node(llm))

    # 起点：从 START 进入 fetch
    builder.add_edge(START, "news_fetch")

    # map 阶段：按 section 动态并行调用 news_extract
    builder.add_conditional_edges("news_fetch", map_sections_to_extract)

    # 每个 extract 完成后，将事件写入 events，LangGraph 会按 Annotated 规则自动累加
    builder.add_edge("news_extract", "news_reduce")

    # 汇总节点输出最终分析结果；根据配置决定是否写入 Markdown
    builder.add_node("news_markdown_write", create_news_markdown_write_node(llm))
    builder.add_conditional_edges("news_reduce", _route_after_news_reduce)
    builder.add_edge("news_markdown_write", END)

    return builder.compile()
