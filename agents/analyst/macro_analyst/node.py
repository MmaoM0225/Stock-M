"""
宏观经济分析师 - 节点函数

包含：macro_fetch, monetary_analysis, global_analysis, market_analysis, macro_reduce
配置通过 RunnableConfig 传入，不纳入 State。
"""
import json
import logging
import os
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional

from langchain_core.prompts import ChatPromptTemplate
from langchain_core.runnables import RunnableConfig

logger = logging.getLogger(__name__)

from ...config import (
    MACRO_DEFAULT_INDEX_CODES,
    MACRO_DEFAULT_COMMODITY_CODES,
    MACRO_DAILY_LOOKBACK,
    MACRO_MONTH_LOOKBACK,
    MACRO_USE_US_STOCK_TREND,
    MACRO_USE_LLM_FOR_MARKDOWN,
)


from ...utils import (
    get_macro_config,
    date_offset,
    resolve_index_items,
    resolve_commodity_items,
    to_serializable,
    extract_json_text,
    format_commodity_for_analysis,
)


# ---------------------------------------------------------------------------
# macro_fetch
# ---------------------------------------------------------------------------


def create_macro_fetch_node(
    market_fetcher=None,
    kline_fetcher=None,
    macro_config_default: Optional[Dict] = None,
):
    """
    构建 macro_fetch 节点。
    从 State 读 trade_date，从 RunnableConfig 读 macro_config。
    """

    def macro_fetch_node(state: Dict, config: Optional[RunnableConfig] = None) -> Dict:
        trade_date = state.get("trade_date") or datetime.now().strftime("%Y%m%d")
        cfg = get_macro_config(config) or macro_config_default or {}
        use_us_stock = cfg.get("macro_use_us_stock_trend", MACRO_USE_US_STOCK_TREND)

        market = market_fetcher
        kline = kline_fetcher
        if market is None:
            try:
                from dataflow.market_data import MarketDataFetcher

                market = MarketDataFetcher()
            except Exception as e:
                logger.warning("无法初始化 MarketDataFetcher: %s", e)
        if kline is None:
            try:
                from dataflow.kline_data import KLineDataFetcher

                kline = KLineDataFetcher()
            except Exception as e:
                logger.warning("无法初始化 KLineDataFetcher: %s", e)

        end_date = trade_date.replace("-", "")[:8]
        start_date = date_offset(end_date, days=MACRO_DAILY_LOOKBACK)
        start_m = date_offset(end_date, months=MACRO_MONTH_LOOKBACK)[:6]
        end_m = end_date[:6]

        lpr_data = cpi_data = sf_data = None
        us_stock_data = None
        commodity_data = {}
        index_data = {}
        index_dailybasic = None

        # 货币环境：LPR、CPI、社融
        if market:
            try:
                lpr_data = market.fetch_shibor_lpr(start_date, end_date)
            except Exception as e:
                logger.warning("fetch_shibor_lpr 失败: %s", e)
            try:
                cpi_data = market.fetch_cpi(start_m=start_m, end_m=end_m)
            except Exception as e:
                logger.warning("fetch_cpi 失败: %s", e)
            try:
                sf_data = market.fetch_sf_month(start_m=start_m, end_m=end_m)
            except Exception as e:
                logger.warning("fetch_sf_month 失败: %s", e)

        # 美股（可选）
        if use_us_stock and market:
            try:
                from dataflow.market_data import YAHOO_INDEX_SYMBOLS

                dfs = []
                for sym in ["SP500", "NASDAQ", "DJI"]:
                    code = YAHOO_INDEX_SYMBOLS.get(sym, sym)
                    df = market.fetch_yahoo_index_daily(code, start_date, end_date)
                    if df is not None and not df.empty:
                        df = df.copy()
                        df["symbol"] = sym
                        dfs.append(df)
                if dfs:
                    import pandas as pd

                    us_stock_data = pd.concat(dfs, ignore_index=True)
            except Exception as e:
                logger.warning("fetch_yahoo_index_daily 失败: %s", e)

        # 大宗商品：按配置拉取。source 区分 sge(fetch_sge_daily) / fut(fetch_fut_daily)
        commodity_config = cfg.get("commodity_codes") or MACRO_DEFAULT_COMMODITY_CODES
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
                        commodity_data[key] = df.reset_index(drop=True)
                except Exception as e:
                    logger.warning("fetch %s (%s) 失败: %s", name, code, e)

        # A 股指数
        index_config = cfg.get("index_codes") or MACRO_DEFAULT_INDEX_CODES
        index_items = resolve_index_items(index_config)
        index_info = [
            {"code": c, "name": n, "description": d}
            for c, n, d in index_items
        ]
        if kline:
            for code, _, _ in index_items:
                try:
                    df = kline.fetch_index_daily_data(code, start_date=start_date, end_date=end_date)
                    if df is not None and not df.empty:
                        index_data[code] = df
                except Exception as e:
                    logger.warning("fetch_index_daily_data %s 失败: %s", code, e)

        if market:
            try:
                dfs_basic = []
                supported = getattr(
                    market, "INDEX_DAILYBASIC_SUPPORTED_CODES", None
                )
                for code, name, _ in index_items:
                    if supported and code not in supported:
                        logger.info(
                            "fetch_index_dailybasic 跳过 %s (%s)：不在 Tushare 支持列表",
                            code,
                            name,
                        )
                        continue
                    try:
                        df = market.fetch_index_dailybasic(
                            ts_code=code,
                            start_date=start_date,
                            end_date=end_date,
                        )
                        if df is not None and not df.empty:
                            dfs_basic.append(df)
                    except Exception as e:
                        logger.warning("fetch_index_dailybasic %s 失败: %s", code, e)
                if dfs_basic:
                    import pandas as pd
                    index_dailybasic = pd.concat(dfs_basic, ignore_index=True)
                    index_dailybasic = index_dailybasic.sort_values("trade_date").reset_index(drop=True)
                else:
                    index_dailybasic = None
            except Exception as e:
                logger.warning("fetch_index_dailybasic 失败: %s", e)
                index_dailybasic = None

        # 序列化输出
        def _ser(v):
            if isinstance(v, dict):
                return {k: to_serializable(vv) if not isinstance(vv, dict) else _ser(vv) for k, vv in v.items()}
            return to_serializable(v)

        out = {
            "lpr_data": to_serializable(lpr_data),
            "cpi_data": to_serializable(cpi_data),
            "sf_data": to_serializable(sf_data),
            "us_stock_data": to_serializable(us_stock_data),
            "commodity_data": _ser(commodity_data) if commodity_data else {},
            "index_data": _ser(index_data) if index_data else {},
            "index_dailybasic": to_serializable(index_dailybasic),
            "index_info": index_info,
        }
        return out

    return macro_fetch_node


# ---------------------------------------------------------------------------
# monetary_analysis
# ---------------------------------------------------------------------------


def create_monetary_analysis_node(llm=None):
    """构建货币环境分析节点。"""

    def monetary_analysis_node(state: Dict, config: Optional[RunnableConfig] = None) -> Dict:
        lpr = state.get("lpr_data")
        cpi = state.get("cpi_data")
        sf = state.get("sf_data")

        if not lpr and not cpi and not sf:
            return {
                "monetary_analysis": {
                    "lpr_trend": "unknown",
                    "cpi_trend": "unknown",
                    "sf_trend": "unknown",
                    "liquidity_summary": "数据缺失",
                    "conclusion": "货币环境数据缺失，无法分析",
                }
            }

        summary = []
        if lpr:
            summary.append(f"LPR 数据: {len(lpr) if isinstance(lpr, list) else 'N'} 条")
        else:
            summary.append("LPR: 无")
        if cpi:
            summary.append(f"CPI 数据: {len(cpi) if isinstance(cpi, list) else 'N'} 条")
        else:
            summary.append("CPI: 无")
        if sf:
            summary.append(f"社融数据: {len(sf) if isinstance(sf, list) else 'N'} 条")
        else:
            summary.append("社融: 无")

        if llm is None:
            return {
                "monetary_analysis": {
                    "lpr_trend": "neutral",
                    "cpi_trend": "neutral",
                    "sf_trend": "neutral",
                    "liquidity_summary": "; ".join(summary),
                    "conclusion": f"货币环境数据已拉取({'; '.join(summary)})，未使用 LLM 分析。",
                }
            }

        system_msg = (
            "你是一位宏观经济分析师。请根据给定的 LPR、CPI、社融数据，分析货币环境。"
            "返回严格 JSON：lpr_trend(up/down/stable), cpi_trend(up/down/stable), "
            "sf_trend(up/down/stable), liquidity_summary(简要), conclusion(一句话结论)。只输出 JSON。"
        )
        prompt = ChatPromptTemplate.from_messages(
            [("system", system_msg), ("human", "LPR:\n{lpr}\nCPI:\n{cpi}\n社融:\n{sf}\n请分析货币环境并返回 JSON。")]
        )

        try:
            chain = prompt | llm
            raw = chain.invoke({"lpr": str(lpr), "cpi": str(cpi), "sf": str(sf)})
            data = extract_json_text(raw)
            return {"monetary_analysis": data}
        except Exception as e:
            logger.warning("monetary_analysis LLM 失败: %s", e)
            return {
                "monetary_analysis": {
                    "lpr_trend": "unknown",
                    "cpi_trend": "unknown",
                    "sf_trend": "unknown",
                    "liquidity_summary": "; ".join(summary),
                    "conclusion": f"LLM 分析失败: {e}",
                }
            }

    return monetary_analysis_node


# ---------------------------------------------------------------------------
# commodity_analysis（单品种，由 graph 按品种分发）
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
                            "price_summary": f"{name} 共 {len(data)} 条数据",
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


def create_commodity_reduce_node(llm=None):
    """汇总各品种分析结果，生成 commodity_summary。"""

    def commodity_reduce_node(state: Dict, config: Optional[RunnableConfig] = None) -> Dict:
        chunks = state.get("commodity_chunk") or []
        if not chunks:
            return {"commodity_summary": "大宗商品无分析结果"}

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

        summaries = [f"{c['name']}: {c.get('result', {}).get('price_summary', '')}" for c in chunks]
        combined = "；".join(summaries[:10]) if summaries else "各品种分析已完成"

        return {
            "commodity_summary": {
                "overall_trend": overall,
                "per_commodity": per_commodity,
                "combined_summary": combined,
            }
        }

    return commodity_reduce_node


# ---------------------------------------------------------------------------
# global_analysis
# ---------------------------------------------------------------------------


def create_global_analysis_node(llm=None, macro_config_default: Optional[Dict] = None):
    """构建全球环境分析节点。含美股（若启用）、大宗（commodity_summary 来自 commodity_reduce）。"""

    def global_analysis_node(state: Dict, config: Optional[RunnableConfig] = None) -> Dict:
        cfg = get_macro_config(config) or macro_config_default or {}
        use_us = cfg.get("macro_use_us_stock_trend", MACRO_USE_US_STOCK_TREND)

        us_stock = state.get("us_stock_data")
        commodity_summary = state.get("commodity_summary")

        if not use_us and not commodity_summary:
            return {
                "global_analysis": {
                    "us_stock_trend": None if not use_us else "unknown",
                    "us_stock_comment": None,
                    "commodity_summary": "数据缺失",
                    "global_conclusion": "全球环境数据缺失。",
                }
            }

        summary = []
        if use_us and us_stock:
            summary.append("美股: 有")
        elif use_us:
            summary.append("美股: 无")
        if commodity_summary:
            if isinstance(commodity_summary, dict):
                per_c = commodity_summary.get("per_commodity") or {}
                summary.append(f"大宗: {list(per_c.keys())}")
            else:
                summary.append("大宗: 有")
        else:
            summary.append("大宗: 无")

        if llm is None:
            return {
                "global_analysis": {
                    "us_stock_trend": "neutral" if use_us else None,
                    "us_stock_comment": None,
                    "commodity_summary": "; ".join(summary),
                    "global_conclusion": f"全球环境数据已拉取({'; '.join(summary)})，未使用 LLM 分析。",
                }
            }

        system_msg = (
            "你是一位宏观经济分析师。根据美股（若提供）和大宗商品汇总分析，综合评估全球环境。"
            "返回 JSON：us_stock_trend(bullish/bearish/neutral 或 null 无数据), us_stock_comment, "
            "commodity_summary(综合大宗结论), global_conclusion。只输出 JSON。"
        )
        prompt = ChatPromptTemplate.from_messages(
            [
                ("system", system_msg),
                ("human", "美股数据:\n{us_stock}\n\n大宗商品汇总:\n{commodity}\n\n请综合评估全球环境并返回 JSON。"),
            ]
        )
        commodity_text = str(commodity_summary) if commodity_summary else "无"

        try:
            chain = prompt | llm
            raw = chain.invoke({
                "us_stock": str(us_stock) if use_us else "未启用",
                "commodity": commodity_text,
            })
            data = extract_json_text(raw)
            return {"global_analysis": data}
        except Exception as e:
            logger.warning("global_analysis LLM 失败: %s", e)
            return {
                "global_analysis": {
                    "us_stock_trend": "unknown" if use_us else None,
                    "us_stock_comment": None,
                    "commodity_summary": "; ".join(summary),
                    "global_conclusion": f"LLM 分析失败: {e}",
                }
            }

    return global_analysis_node


# ---------------------------------------------------------------------------
# market_analysis（单指数，由 graph 按指数分发）
# ---------------------------------------------------------------------------


def create_market_analysis_node(llm=None):
    """
    构建单指数市场分析节点。
    接收 state：index_data 仅含该指数，index_info 仅含该指数说明。
    绑定技术指标工具（数据已预注入），LLM 可调用 calc_ma、calc_rsi、calc_macd 等。
    """

    def market_analysis_node(state: Dict, config: Optional[RunnableConfig] = None) -> Dict:
        index_data = state.get("index_data") or {}
        index_dailybasic = state.get("index_dailybasic") or []
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
                    {"code": code, "name": name, "result": {"index_trend": "unknown", "conclusion": "无K线数据"}}
                ]
            }

        # 过滤该指数的 index_dailybasic
        dailybasic_for_index = [r for r in index_dailybasic if r.get("ts_code") == code]
        index_data_str = str(kline_data)
        dailybasic_str = str(dailybasic_for_index)

        if llm is None:
            return {
                "market_index_chunk": [
                    {
                        "code": code,
                        "name": name,
                        "result": {
                            "index_trend": "neutral",
                            "turnover_summary": f"{code} 有 {len(kline_data)} 日数据",
                            "volatility_summary": "未计算",
                            "market_conclusion": f"未使用 LLM 分析。",
                        },
                    }
                ]
            }

        # 预计算技术指标，注入 prompt
        from ...utils import (
            format_ma_summary,
            format_rsi_summary,
            format_macd_summary,
            format_kdj_summary,
            format_bollinger_bands_summary,
        )

        indicators = []
        for fn in [
            format_ma_summary,
            format_rsi_summary,
            format_macd_summary,
            format_kdj_summary,
            format_bollinger_bands_summary,
        ]:
            try:
                res = fn(kline_data)
                if res:
                    indicators.append(res)
            except Exception:
                pass
        indicators_str = "\n\n".join(indicators) if indicators else "（指标计算未获取）"

        system_msg = (
            "你是一位市场分析师。你目前分析的是【{name}】这个指数，他是{description}。"
            "以下为已计算的 MACD、RSI、MA、KDJ、布林带等指标结果，请基于此分析。"
            "返回 JSON：index_trend(up/down/neutral), turnover_summary, volatility_summary, market_conclusion。只输出 JSON。"
        ).format(name=name, description=description or "A股主要指数")

        prompt = ChatPromptTemplate.from_messages(
            [
                ("system", system_msg),
                (
                    "human",
                    "【技术指标】\n{indicators}\n\n【指数日线（最近数据）】\n{index_data}\n\n【每日指标】\n{index_dailybasic}\n\n请分析并返回 JSON。",
                ),
            ]
        )

        try:
            chain = prompt | llm
            raw = chain.invoke(
                {
                    "indicators": indicators_str,
                    "index_data": index_data_str,
                    "index_dailybasic": dailybasic_str,
                }
            )
            data = extract_json_text(raw)
            return {"market_index_chunk": [{"code": code, "name": name, "result": data}]}
        except Exception as e:
            logger.warning("market_analysis LLM 失败: %s", e)
            return {
                "market_index_chunk": [
                    {
                        "code": code,
                        "name": name,
                        "result": {
                            "index_trend": "unknown",
                            "turnover_summary": "",
                            "volatility_summary": "未计算",
                            "market_conclusion": f"LLM 分析失败: {e}",
                        },
                    }
                ]
            }

    return market_analysis_node


# ---------------------------------------------------------------------------
# market_reduce
# ---------------------------------------------------------------------------


def create_market_reduce_node(llm=None):
    """汇总各指数分析结果，生成 market_analysis。"""

    def market_reduce_node(state: Dict, config: Optional[RunnableConfig] = None) -> Dict:
        chunks = state.get("market_index_chunk") or []
        if not chunks:
            return {"market_analysis": {"per_index": {}, "index_trend": "unknown", "market_conclusion": "无指数分析结果"}}

        per_index = {c["code"]: {"name": c["name"], **c["result"]} for c in chunks}
        index_trend = "neutral"
        trends = [r.get("index_trend", "neutral") for c in chunks for r in [c.get("result", {})]]
        if trends:
            ups = sum(1 for t in trends if t == "up")
            downs = sum(1 for t in trends if t == "down")
            if ups > downs:
                index_trend = "up"
            elif downs > ups:
                index_trend = "down"

        conclusions = [c.get("result", {}).get("market_conclusion", "") for c in chunks if c.get("result", {}).get("market_conclusion")]
        market_conclusion = "；".join(conclusions[:5]) if conclusions else "各指数分析已完成。"

        return {
            "market_analysis": {
                "per_index": per_index,
                "index_trend": index_trend,
                "market_conclusion": market_conclusion,
            }
        }

    return market_reduce_node


# ---------------------------------------------------------------------------
# macro_reduce（含完整 MD 报告写入）
# ---------------------------------------------------------------------------


def _build_macro_analysis_md_programmatic(state: Dict, macro_result: Dict) -> str:
    """根据 state 与宏观汇总结果，生成包含所有分析节点内容的完整 Markdown 报告。"""
    trade_date = macro_result.get("date") or state.get("trade_date") or datetime.now().strftime("%Y%m%d")
    summary = macro_result.get("summary", "")

    monetary = state.get("monetary_analysis") or {}
    commodity_summary = state.get("commodity_summary")
    global_ana = state.get("global_analysis") or {}
    market = state.get("market_analysis") or {}
    market_index_chunk = state.get("market_index_chunk") or []

    lines = [
        f"# 宏观分析报告 {trade_date}",
        "",
        "## 一、摘要",
        "",
        summary or "（无摘要）",
        "",
        "---",
        "",
        "## 二、货币环境分析",
        "",
    ]
    if monetary:
        for k, v in monetary.items():
            if v is None:
                continue
            lines.append(f"### {k}")
            if isinstance(v, (dict, list)):
                lines.append("```json")
                lines.append(json.dumps(v, ensure_ascii=False, indent=2))
                lines.append("```")
            else:
                lines.append(str(v))
            lines.append("")
    else:
        lines.append("（无货币环境分析结果）")
        lines.append("")

    lines.extend([
        "---",
        "",
        "## 三、大宗商品分析",
        "",
    ])
    if commodity_summary:
        if isinstance(commodity_summary, dict):
            lines.append(f"- **整体趋势**: {commodity_summary.get('overall_trend', 'unknown')}")
            per_c = commodity_summary.get("per_commodity") or {}
            if per_c:
                lines.append("")
                lines.append("### 各品种详情")
                lines.append("")
                for key, info in per_c.items():
                    name = info.get("name", key)
                    lines.append(f"#### {name} ({key})")
                    for fk, fv in info.items():
                        if fk == "name":
                            continue
                        if isinstance(fv, (dict, list)):
                            lines.append(f"- **{fk}**:")
                            lines.append("  ```json")
                            for jline in json.dumps(fv, ensure_ascii=False, indent=2).splitlines():
                                lines.append("  " + jline)
                            lines.append("  ```")
                        else:
                            lines.append(f"- **{fk}**: {fv}")
                    lines.append("")
            combined = commodity_summary.get("combined_summary", "")
            if combined:
                lines.append("**综合简述**: " + combined)
                lines.append("")
        else:
            lines.append(str(commodity_summary))
            lines.append("")
    else:
        lines.append("（无大宗商品分析结果）")
        lines.append("")

    lines.extend([
        "---",
        "",
        "## 四、全球环境分析",
        "",
    ])
    if global_ana:
        for k, v in global_ana.items():
            if v is None:
                continue
            lines.append(f"- **{k}**: {v}" if not isinstance(v, (dict, list)) else f"- **{k}**:")
            if isinstance(v, (dict, list)):
                lines.append("  ```json")
                lines.append("  " + json.dumps(v, ensure_ascii=False, indent=2).replace("\n", "\n  "))
                lines.append("  ```")
        lines.append("")
    else:
        lines.append("（无全球环境分析结果）")
        lines.append("")

    lines.extend([
        "---",
        "",
        "## 五、市场分析",
        "",
    ])
    if market:
        lines.append(f"- **指数整体趋势**: {market.get('index_trend', 'unknown')}")
        lines.append(f"- **市场结论**: {market.get('market_conclusion', '')}")
        lines.append("")
        per_index = market.get("per_index") or {}
        if per_index:
            lines.append("### 各指数汇总")
            lines.append("")
            for code, info in per_index.items():
                name = info.get("name", code)
                lines.append(f"#### {name} ({code})")
                for fk, fv in info.items():
                    if fk == "name":
                        continue
                    lines.append(f"- **{fk}**: {fv}")
                lines.append("")
    if market_index_chunk:
        lines.append("### 各指数分析详情")
        lines.append("")
        for chunk in market_index_chunk:
            code = chunk.get("code", "")
            name = chunk.get("name", code)
            result = chunk.get("result") or {}
            lines.append(f"#### {name} ({code})")
            lines.append("")
            for rk, rv in result.items():
                if isinstance(rv, (dict, list)):
                    lines.append(f"- **{rk}**:")
                    lines.append("  ```json")
                    lines.append("  " + json.dumps(rv, ensure_ascii=False, indent=2).replace("\n", "\n  "))
                    lines.append("  ```")
                else:
                    lines.append(f"- **{rk}**: {rv}")
            lines.append("")
    if not market and not market_index_chunk:
        lines.append("（无市场分析结果）")
        lines.append("")

    lines.extend([
        "---",
        "",
        "## 六、报告元数据",
        "",
        f"- 报告日期: {trade_date}",
        f"- 生成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
        "",
    ])
    return "\n".join(lines)


def _build_macro_analysis_md_with_llm(state: Dict, macro_result: Dict, llm, trade_date: str) -> Optional[str]:
    """用 LLM 根据所有分析内容总结生成完整 Markdown 报告。"""
    try:
        content = {
            "摘要": macro_result.get("summary", ""),
            "货币环境": state.get("monetary_analysis") or {},
            "大宗商品": state.get("commodity_summary"),
            "全球环境": state.get("global_analysis") or {},
            "市场分析": state.get("market_analysis") or {},
            "各指数详情": state.get("market_index_chunk") or [],
        }
        system_msg = (
            "你是一位宏观分析师。请根据提供的各模块分析结果，撰写一份完整、易读的宏观分析 Markdown 报告。\n\n"
            "【结构要求】\n"
            "1. 标题：开头为「# 宏观分析报告 {date}」。\n"
            "2. **总结**：紧接着用一个小节「## 总结」（或「核心结论」），用 3～5 句话概括当日宏观要点：货币与流动性、大宗与全球环境、A 股市场趋势的综合结论，便于快速把握全貌。\n"
            "3. 正文：再分模块写货币环境、大宗商品、全球环境、市场分析等，使用二级、三级标题；对各指数、各品种做条理清晰的解读，不要简单罗列原始 JSON。\n"
            "4. 语言专业、逻辑清晰；结尾注明报告日期和生成时间。\n"
            "5. 直接输出完整 Markdown，不要输出其他说明。"
        ).format(date=trade_date)
        prompt = ChatPromptTemplate.from_messages(
            [("system", system_msg), ("human", "分析数据：\n{data}\n\n请生成含「总结」的完整 Markdown 报告。")],
        )
        chain = prompt | llm
        raw = chain.invoke({"data": json.dumps(content, ensure_ascii=False, indent=2, default=str)})
        text = raw.content if hasattr(raw, "content") else str(raw)
        # 确保末尾有元数据
        if "报告日期" not in text and "生成时间" not in text:
            text += f"\n\n---\n\n## 报告元数据\n\n- 报告日期: {trade_date}\n- 生成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
        return text.strip()
    except Exception as e:
        logger.warning("LLM 生成 Markdown 失败: %s，回退到程序化输出", e)
        return None


def _write_macro_analysis_md(
    state: Dict, macro_result: Dict, trade_date: str, llm=None, use_llm_for_md: bool = False
) -> Optional[str]:
    """将完整宏观分析 Markdown 写入 data/analysis/YYYYMMDD_macro_analysis.md。use_llm_for_md=True 时用 LLM 润色（多一次调用）。"""
    try:
        md_content = None
        if use_llm_for_md and llm:
            md_content = _build_macro_analysis_md_with_llm(state, macro_result, llm, trade_date)
        if md_content is None:
            md_content = _build_macro_analysis_md_programmatic(state, macro_result)
        out_dir = os.path.join("data", "analysis")
        os.makedirs(out_dir, exist_ok=True)
        path = os.path.join(out_dir, f"{trade_date}_macro_analysis.md")
        with open(path, "w", encoding="utf-8") as f:
            f.write(md_content)
        logger.info("宏观分析报告已写入: %s", path)
        return path
    except Exception as e:
        logger.warning("写入宏观分析 MD 失败: %s", e)
        return None


def create_macro_markdown_write_node(llm=None):
    """构建宏观 Markdown 报告写入节点。由 configurable.macro_config.generate_markdown 控制是否执行（由 graph 条件边决定）。"""
    from ...config import MACRO_USE_LLM_FOR_MARKDOWN

    def macro_markdown_write_node(state: Dict, config: Optional[RunnableConfig] = None) -> Dict:
        macro_result = state.get("macro_analysis") or {}
        trade_date = macro_result.get("date") or state.get("trade_date") or datetime.now().strftime("%Y%m%d")
        cfg = get_macro_config(config) or {}
        use_llm = cfg.get("use_llm_for_markdown", MACRO_USE_LLM_FOR_MARKDOWN)
        _write_macro_analysis_md(state, macro_result, trade_date, llm=llm, use_llm_for_md=use_llm)
        return {}  # 不修改 state，仅写文件

    return macro_markdown_write_node


def create_macro_reduce_node(llm=None):
    """构建宏观汇总节点，生成最终报告（JSON）。Markdown 由单独节点 macro_markdown_write 负责。"""

    def macro_reduce_node(state: Dict, config: Optional[RunnableConfig] = None) -> Dict:
        trade_date = state.get("trade_date") or datetime.now().strftime("%Y%m%d")
        monetary = state.get("monetary_analysis") or {}
        global_ana = state.get("global_analysis") or {}
        market = state.get("market_analysis") or {}

        base = {
            "date": trade_date,
            "monetary": monetary,
            "global": global_ana,
            "market": market,
        }

        if llm is None:
            base["summary"] = "宏观分析已完成（未使用 LLM 润色）"
            return {"macro_analysis": base}

        system_msg = (
            "你是一位宏观分析师。根据货币、全球、市场三块分析结果，生成一份简洁的宏观日报摘要。"
            "返回 JSON：{{ date, monetary, global, market, summary }}，summary 为一句话概括。只输出 JSON。"
        )
        prompt = ChatPromptTemplate.from_messages(
            [
                ("system", system_msg),
                ("human", "货币: {monetary}\n全球: {global}\n市场: {market}\n请生成宏观日报 JSON。"),
            ]
        )

        try:
            chain = prompt | llm
            raw = chain.invoke({"monetary": str(monetary), "global": str(global_ana), "market": str(market)})
            data = extract_json_text(raw)
            data["date"] = trade_date  # 强制使用用户指定的交易日，不采用 LLM 返回的日期
            data.setdefault("monetary", monetary)
            data.setdefault("global", global_ana)
            data.setdefault("market", market)
            return {"macro_analysis": data}
        except Exception as e:
            logger.warning("macro_reduce LLM 失败: %s", e)
            base["summary"] = f"LLM 润色失败: {e}"
            return {"macro_analysis": base}

    return macro_reduce_node
