import operator
from typing import Any, Dict, List, Optional

from typing_extensions import Annotated, TypedDict

from langgraph.graph import StateGraph, START, END

from .node import (
    create_news_fetch_node,
    map_sections_to_extract,
    create_news_extract_node,
    create_news_reduce_node,
    create_news_result_persist_node,
)


class NewsState(TypedDict, total=False):
    """
    新闻分析子图的状态定义。

    - 输入：trade_date（新闻由 fetch 节点通过 fetcher 拉取）
    - 输出：在 news_analysis 中给出结构化分析结果
    """

    trade_date: str
    news_sections: List[Dict[str, Any]]
    news_source: str  # "local" 或 "fetch"
    all_industries: List[str]
    ths_concept_list: List[str]  # 同花顺概念列表，与 all_industries 一并供 LLM 选用
    section: Dict[str, Any]
    # 使用 Annotated + operator.add 让 LangGraph 自动做 map-reduce 聚合
    events: Annotated[List[Dict[str, Any]], operator.add]
    news_analysis: Dict[str, Any]
    news_analysis_artifact_path: str
    news_analysis_manifest_path: str
    messages: List[Any]

def create_news_graph(llm, finlight_fetcher: Optional[Any] = None):
    """
    构建新闻分析子图（map-reduce 风格，全面使用 Finlight API）。

    流程：
    1. news_fetch：判断交易日 → 从 Finlight API 获取新闻（优先本地缓存） → 获取完整行业列表
    2. map_sections_to_extract：根据 news_sections 动态生成并行的 news_extract 调用（Send）
    3. news_extract：对单条新闻 section 抽取结构化事件（LLM 判断），写入 events
    4. news_reduce：对所有 events 做板块与宏观环境汇总，写入 news_analysis
    5. news_result_persist：将最终输出写入本地 artifacts

    Args:
        llm: 支持 with_structured_output 的 LLM 实例
        finlight_fetcher: FinlightDataFetcher 实例（可选，如未提供则自动创建）

    Returns:
        已编译的新闻分析子图，可直接 invoke / stream。
    """
    builder = StateGraph(NewsState)

    # 注册节点
    builder.add_node("news_fetch", create_news_fetch_node(finlight_fetcher))
    builder.add_node("news_extract", create_news_extract_node(llm))
    builder.add_node("news_reduce", create_news_reduce_node(llm))
    builder.add_node("news_result_persist", create_news_result_persist_node())

    # 起点：从 START 进入 fetch
    builder.add_edge(START, "news_fetch")

    # map 阶段：按 section 动态并行调用 news_extract
    builder.add_conditional_edges("news_fetch", map_sections_to_extract)

    # 每个 extract 完成后，将事件写入 events，LangGraph 会按 Annotated 规则自动累加
    builder.add_edge("news_extract", "news_reduce")

    # 汇总完成后将最终结果落盘，再结束
    builder.add_edge("news_reduce", "news_result_persist")
    builder.add_edge("news_result_persist", END)

    return builder.compile()
