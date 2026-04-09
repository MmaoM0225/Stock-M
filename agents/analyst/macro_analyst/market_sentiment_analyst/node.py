"""
Market Sentiment Analyst（市场情绪分析师）- 节点函数

专注：指数走势、成交量、波动率等市场情绪指标。
"""
import logging
from datetime import datetime
from typing import Dict, Optional, Any

from langchain_core.prompts import ChatPromptTemplate
from langchain_core.runnables import RunnableConfig

logger = logging.getLogger(__name__)

from ....config import MACRO_DAILY_LOOKBACK, MACRO_DEFAULT_INDEX_CODES
from ....utils import (
    get_market_sentiment_config,
    date_offset,
    to_serializable,
    resolve_index_items,
    extract_json_text,
    format_ma_summary,
    format_rsi_summary,
    format_macd_summary,
    format_kdj_summary,
    format_bollinger_bands_summary,
)


# ---------------------------------------------------------------------------
# market_sentiment_fetch
# ---------------------------------------------------------------------------


def create_market_sentiment_fetch_node(kline_fetcher=None, index_config_default=None):
    """
    构建市场情绪数据拉取节点。
    拉取：指数日线。
    """

    def market_sentiment_fetch_node(state: Dict, config: Optional[RunnableConfig] = None) -> Dict:
        trade_date = state.get("trade_date") or datetime.now().strftime("%Y%m%d")
        cfg = get_market_sentiment_config(config) or index_config_default or {}
        index_config = cfg.get("index_codes") or MACRO_DEFAULT_INDEX_CODES

        end_date = trade_date.replace("-", "")[:8]
        start_date = date_offset(end_date, days=MACRO_DAILY_LOOKBACK)

        index_data = {}
        index_info = [{"code": c, "name": n, "description": d} for c, n, d in resolve_index_items(index_config)]

        kline = kline_fetcher
        if kline is None:
            try:
                from dataflow.kline_data import KLineDataFetcher
                kline = KLineDataFetcher()
            except Exception as e:
                logger.warning("无法初始化 KLineDataFetcher: %s", e)

        if kline:
            for code, _, _ in resolve_index_items(index_config):
                try:
                    df = kline.fetch_index_daily_data(code, start_date=start_date, end_date=end_date)
                    if df is not None and not df.empty:
                        index_data[code] = to_serializable(df.reset_index(drop=True))
                except Exception as e:
                    logger.warning("fetch_index_daily_data %s 失败: %s", code, e)

        return {
            "index_data": index_data,
            "index_info": index_info,
        }

    return market_sentiment_fetch_node


# ---------------------------------------------------------------------------
# market_sentiment_analysis（单指数）
# ---------------------------------------------------------------------------


def create_market_sentiment_analysis_node(llm=None):
    """构建单指数市场情绪分析节点。基于技术指标分析指数走势、成交量、波动率。"""

    def market_sentiment_analysis_node(state: Dict, config: Optional[RunnableConfig] = None) -> Dict:
        index_data = state.get("index_data") or {}
        index_info = state.get("index_info") or []
        current_index_code = state.get("current_index_code")

        code = current_index_code or (list(index_data.keys())[0] if index_data else "unknown")
        item = next((x for x in index_info if x.get("code") == code), {})
        name = item.get("name", code)
        description = item.get("description", "")

        kline_data = index_data.get(code, [])
        if not kline_data:
            return {
                "market_index_chunk": [
                    {"code": code, "name": name, "result": {"index_trend": "unknown", "market_conclusion": "无K线数据"}}
                ]
            }

        index_data_str = str(kline_data)

        indicators = []
        for fn in [format_ma_summary, format_rsi_summary, format_macd_summary, format_kdj_summary, format_bollinger_bands_summary]:
            try:
                res = fn(kline_data)
                if res:
                    indicators.append(res)
            except Exception:
                pass
        indicators_str = "\n\n".join(indicators) if indicators else "（指标计算未获取）"

        system_msg = (
            "你是一位市场情绪分析师。你目前分析的是【{name}】这个指数，他是{description}。"
            "以下为已计算的 MACD、RSI、MA、KDJ、布林带等指标结果，请基于此分析指数走势、成交量、波动率。"
            "返回 JSON：index_trend(up/down/neutral), turnover_summary(成交量/换手率简要), volatility_summary(波动率简要), market_conclusion(市场情绪结论)。只输出 JSON。"
        ).format(name=name, description=description or "A股主要指数")

        prompt = ChatPromptTemplate.from_messages(
            [
                ("system", system_msg),
                (
                    "human",
                    "【技术指标】\n{indicators}\n\n【指数日线（最近数据）】\n{index_data}\n\n请分析并返回 JSON。",
                ),
            ]
        )

        logger.info("正在处理 市场情绪分析：指数 %s 技术分析", name)
        chain = prompt | llm
        raw = chain.invoke(
            {"indicators": indicators_str, "index_data": index_data_str},
            config={**(config or {}), "run_name": "市场情绪分析"},
        )
        data = extract_json_text(raw)
        return {"market_index_chunk": [{"code": code, "name": name, "result": data}]}

    return market_sentiment_analysis_node


# ---------------------------------------------------------------------------
# market_sentiment_reduce
# ---------------------------------------------------------------------------

_MARKET_SENTIMENT_REDUCE_DEFAULT = {
    "market_sentiment": "unknown",
    "index_trend": "unknown",
    "volume_signal": "unknown",
    "volatility_signal": "unknown",
    "sentiment_summary": "",
}


def create_market_sentiment_reduce_node(llm=None):
    """汇总各指数分析结果，使用 LLM 综合判断市场情绪，生成 market_sentiment_analyst_summary。"""

    def market_sentiment_reduce_node(state: Dict, config: Optional[RunnableConfig] = None) -> Dict:
        chunks = state.get("market_index_chunk") or []
        if not chunks:
            return {
                "market_sentiment_analyst_summary": {
                    **_MARKET_SENTIMENT_REDUCE_DEFAULT,
                    "per_index": {},
                    "combined_summary": "无指数分析结果",
                }
            }

        per_index = {c["code"]: {"name": c["name"], **c["result"]} for c in chunks}
        trends = [r.get("index_trend", "neutral") for c in chunks for r in [c.get("result", {})]]
        index_trend = "neutral"
        if trends:
            ups = sum(1 for t in trends if t == "up")
            downs = sum(1 for t in trends if t == "down")
            if ups > downs:
                index_trend = "up"
            elif downs > ups:
                index_trend = "down"

        summaries = [f"【{c['name']}】{c.get('result', {}).get('market_conclusion', '')}" for c in chunks]
        combined = "；".join(summaries[:10]) if summaries else "各指数分析已完成"
        chunks_text = "\n\n".join(
            f"【{c['name']}】index_trend={c.get('result', {}).get('index_trend', '')} | "
            f"turnover={c.get('result', {}).get('turnover_summary', '')} | "
            f"volatility={c.get('result', {}).get('volatility_summary', '')} | "
            f"conclusion={c.get('result', {}).get('market_conclusion', '')}"
            for c in chunks
        )

        base_summary = {
            "per_index": per_index,
            "index_trend": index_trend,
            "combined_summary": combined,
        }

        system_msg = """你是一位市场情绪分析师。

以下是各指数的分析结果（指数走势、成交量、波动率等）。

请综合判断：

1. 市场整体情绪（乐观/谨慎/悲观）
2. 成交量信号（放量/缩量/中性）
3. 波动率水平（高/中/低）
4. 市场风险偏好

返回严格 JSON，只输出 JSON，不要其他文字：

{{
  "market_sentiment": "bullish | neutral | bearish",
  "volume_signal": "expanding | contracting | neutral",
  "volatility_signal": "high | medium | low",
  "sentiment_summary": "一句话总结市场情绪"
}}

无数据的指标填 unknown。"""

        human_msg = """【各指数分析结果】

{chunks}

请综合判断并返回上述 JSON。"""

        prompt = ChatPromptTemplate.from_messages(
            [("system", system_msg), ("human", human_msg)]
        )

        logger.info("正在处理 市场情绪汇总：汇总各指数 → 情绪 JSON")
        chain = prompt | llm
        raw = chain.invoke(
            {"chunks": chunks_text},
            config={**(config or {}), "run_name": "市场情绪汇总"},
        )
        data = extract_json_text(raw)
        for k, v in _MARKET_SENTIMENT_REDUCE_DEFAULT.items():
            data.setdefault(k, v)

        return {
            "market_sentiment_analyst_summary": {
                **base_summary,
                "market_sentiment": data.get("market_sentiment", "unknown"),
                "volume_signal": data.get("volume_signal", "unknown"),
                "volatility_signal": data.get("volatility_signal", "unknown"),
                "sentiment_summary": data.get("sentiment_summary", ""),
            }
        }

    return market_sentiment_reduce_node
