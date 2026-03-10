"""
Market Sentiment Analyst（市场情绪分析师）- 图结构

流程：market_sentiment_fetch → [market_sentiment_analysis（按指数并行）] → market_sentiment_reduce → END
"""
import operator
from typing import Dict, Any, List, Optional

from langchain_core.runnables import RunnableConfig
from typing_extensions import Annotated, TypedDict
from langgraph.constants import Send
from langgraph.graph import StateGraph, START, END

from ...config import MACRO_DEFAULT_INDEX_CODES
from ...utils import get_market_sentiment_config, resolve_index_items

from .node import (
    create_market_sentiment_fetch_node,
    create_market_sentiment_analysis_node,
    create_market_sentiment_reduce_node,
)


class MarketSentimentAnalystState(TypedDict, total=False):
    """Market Sentiment Analyst 状态。market_index_chunk 使用 add 合并多路并行结果。"""
    trade_date: str
    index_data: Dict[str, Any]
    index_info: List[Dict[str, Any]]
    market_index_chunk: Annotated[List[Dict[str, Any]], operator.add]
    market_sentiment_analyst_summary: Dict[str, Any]


def _fan_out_to_indices(state: Dict[str, Any], config: RunnableConfig | None = None) -> List[Send]:
    """market_sentiment_fetch 后按指数并行分发到 market_sentiment_analysis。"""
    cfg = get_market_sentiment_config(config) if config else {}
    index_config = (cfg.get("index_codes") if isinstance(cfg, dict) else None) or MACRO_DEFAULT_INDEX_CODES
    index_data = state.get("index_data") or {}
    index_info = state.get("index_info") or []

    sends = []
    for item in index_info:
        code = item.get("code")
        if not code or code not in index_data:
            continue
        payload = {
            **state,
            "index_data": {code: index_data[code]},
            "index_info": [item],
            "current_index_code": code,
        }
        sends.append(Send("market_sentiment_analysis", payload))

    if not sends:
        sends.append(Send("market_sentiment_reduce", state))

    return sends


def create_market_sentiment_analyst_graph(
    llm=None,
    kline_fetcher=None,
    index_config_default: Optional[Dict] = None,
):
    """
    构建 Market Sentiment Analyst 图。

    流程：fetch（指数日线）→ 按指数并行 analysis → reduce → END

    Args:
        llm: LLM 实例，用于分析节点
        kline_fetcher: KLineDataFetcher，不传则节点内创建
        index_config_default: 指数配置默认值（可被 RunnableConfig 覆盖）

    Returns:
        已编译的 Market Sentiment Analyst 图。
    """
    builder = StateGraph(MarketSentimentAnalystState)

    builder.add_node(
        "market_sentiment_fetch",
        create_market_sentiment_fetch_node(kline_fetcher, index_config_default),
    )
    builder.add_node("market_sentiment_analysis", create_market_sentiment_analysis_node(llm))
    builder.add_node("market_sentiment_reduce", create_market_sentiment_reduce_node(llm))

    builder.add_edge(START, "market_sentiment_fetch")
    builder.add_conditional_edges("market_sentiment_fetch", _fan_out_to_indices)
    builder.add_edge("market_sentiment_analysis", "market_sentiment_reduce")
    builder.add_edge("market_sentiment_reduce", END)

    return builder.compile()
