"""
宏观经济分析师 - 图结构

流程：macro_fetch → [monetary_analysis, commodity_analysis（按品种并行）, market_analysis（按指数并行）] 
     → commodity_reduce → global_analysis
     → market_reduce → macro_reduce → END
配置通过 RunnableConfig 传入。
"""
import operator
from typing import Dict, Any, List, Optional

from langchain_core.runnables import RunnableConfig
from typing_extensions import Annotated, TypedDict
from langgraph.constants import Send
from langgraph.graph import StateGraph, START, END

from ...config import MACRO_GENERATE_MARKDOWN
from ...utils import get_macro_config

from .node import (
    create_macro_fetch_node,
    create_monetary_analysis_node,
    create_commodity_analysis_node,
    create_commodity_reduce_node,
    create_global_analysis_node,
    create_market_analysis_node,
    create_market_reduce_node,
    create_macro_reduce_node,
    create_macro_markdown_write_node,
)


class MacroState(TypedDict, total=False):
    """宏观经济分析师状态定义。"""
    trade_date: str
    lpr_data: List[Dict[str, Any]]
    cpi_data: List[Dict[str, Any]]
    sf_data: List[Dict[str, Any]]
    us_stock_data: List[Dict[str, Any]]
    commodity_data: Dict[str, Any]
    commodity_chunk: Annotated[List[Dict[str, Any]], operator.add]
    commodity_summary: Any
    index_data: Dict[str, Any]
    index_dailybasic: List[Dict[str, Any]]
    index_info: List[Dict[str, Any]]
    monetary_analysis: Dict[str, Any]
    global_analysis: Dict[str, Any]
    market_index_chunk: Annotated[List[Dict[str, Any]], operator.add]
    market_analysis: Dict[str, Any]
    macro_analysis: Dict[str, Any]
    messages: List[Any]


def _fan_out_to_analyses(state: Dict[str, Any], config: RunnableConfig | None = None) -> List[Send]:
    """macro_fetch 后并行分发：货币、大宗（每品种）、市场（每指数）。"""
    sends = [Send("monetary_analysis", state)]
    from ...config import MACRO_DEFAULT_COMMODITY_CODES
    cfg = (config or {}).get("configurable") or {}
    macro_cfg = cfg.get("macro_config") if isinstance(cfg, dict) else {}
    commodity_config = (macro_cfg.get("commodity_codes") if isinstance(macro_cfg, dict) else None) or MACRO_DEFAULT_COMMODITY_CODES
    commodity_data = state.get("commodity_data") or {}
    from ...utils import resolve_commodity_items
    commodity_items = resolve_commodity_items(commodity_config)
    has_commodity = False
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
        has_commodity = True
    if not has_commodity:
        sends.append(Send("commodity_reduce", state))
    index_data = state.get("index_data") or {}
    index_info = state.get("index_info") or []
    index_dailybasic = state.get("index_dailybasic") or []
    has_market = False
    for item in index_info:
        code = item.get("code")
        if not code or code not in index_data:
            continue
        payload = {
            **state,
            "index_data": {code: index_data[code]},
            "index_dailybasic": index_dailybasic,
            "index_info": [item],
            "current_index_code": code,
        }
        sends.append(Send("market_analysis", payload))
        has_market = True
    if not has_market:
        sends.append(Send("market_analysis", state))
    return sends


def _route_after_macro_reduce(state: Dict[str, Any], config: RunnableConfig | None = None) -> str:
    """macro_reduce 之后：若配置 generate_markdown 则进入 markdown_write，否则 END。"""
    cfg = get_macro_config(config) if config else {}
    generate = cfg.get("generate_markdown", MACRO_GENERATE_MARKDOWN)
    return "macro_markdown_write" if generate else END


def create_macro_graph(
    llm=None,
    market_fetcher=None,
    kline_fetcher=None,
    macro_config_default: Optional[Dict] = None,
):
    """
    构建宏观经济分析图。

    流程：
    1. macro_fetch：拉取 LPR、CPI、社融、美股（可选）、大宗、A 股指数
    2. monetary_analysis、global_analysis、market_analysis：三路并行分析
    3. macro_reduce：汇总三块结论，生成 macro_analysis

    Args:
        llm: LLM 实例，用于分析节点（可为 None，则仅做数据汇总）
        market_fetcher: MarketDataFetcher，不传则节点内创建
        kline_fetcher: KLineDataFetcher，不传则节点内创建
        macro_config_default: 宏观配置默认值（可被 RunnableConfig 覆盖）

    Returns:
        已编译的宏观经济分析图。
    """
    builder = StateGraph(MacroState)

    builder.add_node(
        "macro_fetch",
        create_macro_fetch_node(market_fetcher, kline_fetcher, macro_config_default),
    )
    builder.add_node("monetary_analysis", create_monetary_analysis_node(llm))
    builder.add_node("commodity_analysis", create_commodity_analysis_node(llm))
    builder.add_node("commodity_reduce", create_commodity_reduce_node(llm))
    builder.add_node("global_analysis", create_global_analysis_node(llm, macro_config_default))
    builder.add_node("market_analysis", create_market_analysis_node(llm))
    builder.add_node("market_reduce", create_market_reduce_node(llm))
    builder.add_node("macro_reduce", create_macro_reduce_node(llm))

    builder.add_edge(START, "macro_fetch")
    builder.add_conditional_edges("macro_fetch", _fan_out_to_analyses)
    builder.add_edge("monetary_analysis", "macro_reduce")
    builder.add_edge("commodity_analysis", "commodity_reduce")
    builder.add_edge("commodity_reduce", "global_analysis")
    builder.add_edge("global_analysis", "macro_reduce")
    builder.add_edge("market_analysis", "market_reduce")
    builder.add_edge("market_reduce", "macro_reduce")
    builder.add_node("macro_markdown_write", create_macro_markdown_write_node(llm))
    builder.add_conditional_edges("macro_reduce", _route_after_macro_reduce)
    builder.add_edge("macro_markdown_write", END)

    return builder.compile()
