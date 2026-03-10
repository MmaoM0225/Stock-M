"""
Commodity Analyst（大宗商品分析师）- 节点函数

专注：原油、黄金、铜、铁矿石等大宗商品价格趋势及宏观含义。
"""
import logging
from datetime import datetime
from typing import Dict, Optional, Any

from langchain_core.prompts import ChatPromptTemplate
from langchain_core.runnables import RunnableConfig

logger = logging.getLogger(__name__)

from ...config import MACRO_DAILY_LOOKBACK, MACRO_DEFAULT_COMMODITY_CODES
from ...utils import get_commodity_config, date_offset, to_serializable, resolve_commodity_items, extract_json_text


# ---------------------------------------------------------------------------
# commodity_fetch
# ---------------------------------------------------------------------------


def create_commodity_fetch_node(market_fetcher=None, commodity_config_default=None):
    """
    构建大宗商品数据拉取节点。
    按配置拉取黄金、原油、铜、铁矿石等（sge/fut 源）。
    """

    def commodity_fetch_node(state: Dict, config: Optional[RunnableConfig] = None) -> Dict:
        trade_date = state.get("trade_date") or datetime.now().strftime("%Y%m%d")
        cfg = get_commodity_config(config) or commodity_config_default or {}
        commodity_config = cfg.get("commodity_codes") or MACRO_DEFAULT_COMMODITY_CODES

        end_date = trade_date.replace("-", "")[:8]
        start_date = date_offset(end_date, days=MACRO_DAILY_LOOKBACK)

        commodity_data = {}

        market = market_fetcher
        if market is None:
            try:
                from dataflow.market_data import MarketDataFetcher
                market = MarketDataFetcher()
            except Exception as e:
                logger.warning("无法初始化 MarketDataFetcher: %s", e)

        if market and commodity_config:
            for item in commodity_config:
                if not isinstance(item, dict) or "code" not in item:
                    continue
                code = item.get("code")
                name = item.get("name", code)
                key = item.get("key") or name
                source = (item.get("source") or "").lower()
                try:
                    if source == "fut":
                        df = market.fetch_fut_daily(ts_code=code, start_date=start_date, end_date=end_date)
                    elif source == "sge":
                        df = market.fetch_sge_daily(ts_code=code, start_date=start_date, end_date=end_date)
                    else:
                        logger.warning("commodity %s 未知 source=%s，跳过", name, source)
                        continue
                    if df is not None and not df.empty:
                        commodity_data[key] = to_serializable(df.reset_index(drop=True))
                except Exception as e:
                    logger.warning("fetch %s (%s) 失败: %s", name, code, e)

        return {"commodity_data": commodity_data}

    return commodity_fetch_node


# ---------------------------------------------------------------------------
# commodity_analysis（单品种）
# ---------------------------------------------------------------------------


def create_commodity_analysis_node(llm=None):
    """构建单品种大宗商品分析节点。接收 state 中 commodity_data 仅含该品种。"""

    def commodity_analysis_node(state: Dict, config: Optional[RunnableConfig] = None) -> Dict:
        commodity_data = state.get("commodity_data") or {}
        current_key = state.get("current_commodity_key")
        commodity_info = state.get("commodity_info") or {}
        name = commodity_info.get("name", current_key or "unknown")
        description = commodity_info.get("description", "")

        data = commodity_data.get(current_key, []) if current_key else []
        if not data:
            return {
                "commodity_chunk": [
                    {"key": current_key, "name": name, "result": {"trend": "unknown", "price_summary": "无数据", "macro_implication": "无数据"}}
                ]
            }

        data_str = str(data)
        if llm is None:
            return {
                "commodity_chunk": [
                    {
                        "key": current_key,
                        "name": name,
                        "result": {
                            "trend": "neutral",
                            "price_summary": f"{name} 共 {len(data) if isinstance(data, list) else 'N'} 条数据",
                            "macro_implication": "未使用 LLM 分析",
                        },
                    }
                ]
            }

        system_msg = (
            "你是一位大宗商品分析师。根据【{name}】的近期行情数据，分析价格趋势及宏观含义。"
            "返回 JSON：trend(up/down/neutral), price_summary(简要价格与涨跌), macro_implication(对宏观经济的含义)。只输出 JSON。"
        ).format(name=name)
        prompt = ChatPromptTemplate.from_messages(
            [
                ("system", system_msg),
                ("human", "【{name}】{description}\n\n行情数据:\n{data}\n\n请分析并返回 JSON。"),
            ]
        )

        try:
            logger.info("commodity_analysis: 品种 %s 行情分析", name)
            chain = prompt | llm
            raw = chain.invoke({"name": name, "description": description or "", "data": data_str})
            result = extract_json_text(raw)
            return {"commodity_chunk": [{"key": current_key, "name": name, "result": result}]}
        except Exception as e:
            logger.warning("commodity_analysis %s LLM 失败: %s", name, e)
            return {
                "commodity_chunk": [
                    {
                        "key": current_key,
                        "name": name,
                        "result": {
                            "trend": "unknown",
                            "price_summary": "",
                            "macro_implication": f"LLM 分析失败: {e}",
                        },
                    }
                ]
            }

    return commodity_analysis_node


# ---------------------------------------------------------------------------
# commodity_reduce
# ---------------------------------------------------------------------------

_COMMODITY_REDUCE_DEFAULT = {
    "commodity_market_trend": "unknown",
    "macro_signals": {
        "growth_signal": "unknown",
        "inflation_signal": "unknown",
        "risk_sentiment": "unknown",
    },
    "macro_summary": "",
}


def create_commodity_reduce_node(llm=None):
    """汇总各品种分析结果，使用 LLM 综合判断宏观信号，生成 commodity_analyst_summary。"""

    def commodity_reduce_node(state: Dict, config: Optional[RunnableConfig] = None) -> Dict:
        chunks = state.get("commodity_chunk") or []
        if not chunks:
            return {
                "commodity_analyst_summary": {
                    **_COMMODITY_REDUCE_DEFAULT,
                    "overall_trend": "unknown",
                    "per_commodity": {},
                    "combined_summary": "大宗商品无分析结果",
                }
            }

        per_commodity = {c["key"]: {"name": c["name"], **c["result"]} for c in chunks}
        trends = [r.get("trend", "neutral") for c in chunks for r in [c.get("result", {})]]
        overall = "neutral"
        if trends:
            ups = sum(1 for t in trends if t == "up")
            downs = sum(1 for t in trends if t == "down")
            if ups > downs:
                overall = "up"
            elif downs > ups:
                overall = "down"

        summaries = [f"【{c['name']}】{c.get('result', {}).get('price_summary', '')}" for c in chunks]
        combined = "；".join(summaries[:10]) if summaries else "各品种分析已完成"
        chunks_text = "\n\n".join(
            f"【{c['name']}】trend={c.get('result', {}).get('trend', '')} | "
            f"price_summary={c.get('result', {}).get('price_summary', '')} | "
            f"macro_implication={c.get('result', {}).get('macro_implication', '')}"
            for c in chunks
        )

        base_summary = {
            "overall_trend": overall,
            "per_commodity": per_commodity,
            "combined_summary": combined,
        }

        system_msg = """你是一位宏观研究机构的商品策略分析师。

以下是各大宗商品的分析结果。

请综合判断：

1. 商品市场整体趋势
2. 对宏观经济的信号
3. 通胀压力
4. 经济增长动能
5. 市场风险情绪

重要商品权重：

原油：全球经济与通胀
铜：工业需求
黄金：避险情绪
铁矿石：基建与地产

返回严格 JSON，只输出 JSON，不要其他文字：

{{
  "commodity_market_trend": "up | down | mixed",
  "macro_signals": {{
    "growth_signal": "strong | weakening | neutral",
    "inflation_signal": "rising | falling | neutral",
    "risk_sentiment": "risk_on | risk_off | neutral"
  }},
  "macro_summary": "一句话总结"
}}

无数据的指标填 unknown。"""

        human_msg = """【各品种分析结果】

{chunks}

请综合判断并返回上述 JSON。"""

        prompt = ChatPromptTemplate.from_messages(
            [("system", system_msg), ("human", human_msg)]
        )

        logger.info("commodity_reduce: 汇总各品种 → 宏观信号 JSON")
        chain = prompt | llm
        raw = chain.invoke({"chunks": chunks_text})
        data = extract_json_text(raw)
        for k, v in _COMMODITY_REDUCE_DEFAULT.items():
            data.setdefault(k, v)
        return {
            "commodity_analyst_summary": {
                **base_summary,
                "commodity_market_trend": data.get("commodity_market_trend", "unknown"),
                "macro_signals": data.get("macro_signals", _COMMODITY_REDUCE_DEFAULT["macro_signals"]),
                "macro_summary": data.get("macro_summary", ""),
            }
        }

    return commodity_reduce_node
