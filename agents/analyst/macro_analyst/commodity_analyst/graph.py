"""
Commodity Analyst（大宗商品分析师）- 图结构

流程：commodity_fetch → [commodity_analysis（按品种并行）] → commodity_reduce → commodity_result_persist → END
"""
import operator
from typing import Dict, Any, List, Optional

from langchain_core.runnables import RunnableConfig
from typing_extensions import Annotated, TypedDict
from langgraph.constants import Send
from langgraph.graph import StateGraph, START, END

from ....config import MACRO_DEFAULT_COMMODITY_CODES
from ....utils import get_commodity_config, resolve_commodity_items

from .node import (
    create_commodity_fetch_node,
    create_commodity_analysis_node,
    create_commodity_reduce_node,
    create_commodity_result_persist_node,
)


class CommodityAnalystState(TypedDict, total=False):
    """Commodity Analyst 状态。commodity_chunk 使用 add 合并多路并行结果。"""
    trade_date: str
    commodity_data: Dict[str, Any]
    commodity_chunk: Annotated[List[Dict[str, Any]], operator.add]
    commodity_analyst_summary: Dict[str, Any]
    commodity_artifact_path: str
    commodity_manifest_path: str


def _fan_out_to_commodities(state: Dict[str, Any], config: RunnableConfig | None = None) -> List[Send]:
    """commodity_fetch 后按品种并行分发到 commodity_analysis。"""
    cfg = get_commodity_config(config) if config else {}
    commodity_config = (cfg.get("commodity_codes") if isinstance(cfg, dict) else None) or MACRO_DEFAULT_COMMODITY_CODES
    commodity_data = state.get("commodity_data") or {}
    commodity_items = resolve_commodity_items(commodity_config)

    sends = []
    for key, name, description in commodity_items:
        if key not in commodity_data:
            continue
        payload = {
            **state,
            "commodity_data": {key: commodity_data[key]},
            "current_commodity_key": key,
            "commodity_info": {"name": name, "description": description},
        }
        sends.append(Send("commodity_analysis", payload))

    if not sends:
        sends.append(Send("commodity_reduce", state))

    return sends


def create_commodity_analyst_graph(
    llm=None,
    market_fetcher=None,
    commodity_config_default: Optional[Dict] = None,
):
    """
    构建 Commodity Analyst 图。

    流程：fetch（黄金、原油、铜等）→ 按品种并行 analysis → reduce → 持久化结果 → END

    Args:
        llm: LLM 实例，用于分析节点（可为 None，则仅做数据汇总）
        market_fetcher: MarketDataFetcher，不传则节点内创建
        commodity_config_default: 大宗商品配置默认值（可被 RunnableConfig 覆盖）

    Returns:
        已编译的 Commodity Analyst 图。
    """
    builder = StateGraph(CommodityAnalystState)

    builder.add_node("commodity_fetch", create_commodity_fetch_node(market_fetcher, commodity_config_default))
    builder.add_node("commodity_analysis", create_commodity_analysis_node(llm))
    builder.add_node("commodity_reduce", create_commodity_reduce_node(llm))
    builder.add_node("commodity_result_persist", create_commodity_result_persist_node())

    builder.add_edge(START, "commodity_fetch")
    builder.add_conditional_edges("commodity_fetch", _fan_out_to_commodities)
    builder.add_edge("commodity_analysis", "commodity_reduce")
    builder.add_edge("commodity_reduce", "commodity_result_persist")
    builder.add_edge("commodity_result_persist", END)

    return builder.compile()
