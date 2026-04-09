"""
Stock Technical Analyst（个股技术面分析师）- 节点实现

流程：拉取日线K线 -> 计算并整理技术指标数据 -> LLM 生成结构化技术结论。
"""
from __future__ import annotations

import json
import logging
from datetime import datetime
from typing import Any, Dict, List, Optional

import pandas as pd
from langchain_core.runnables import RunnableConfig

from ...utils import date_offset, extract_json_text, to_serializable

logger = logging.getLogger(__name__)


def _norm_date(s: Optional[str]) -> str:
    if not s:
        return ""
    return str(s).replace("-", "")[:8]


def _to_num(v: Any) -> Optional[float]:
    try:
        if v is None:
            return None
        x = float(v)
        if pd.isna(x):
            return None
        return x
    except Exception:
        return None


def create_stock_technical_fetch_node():
    """拉取个股近期日线行情。"""

    def stock_technical_fetch_node(
        state: Dict[str, Any],
        config: Optional[RunnableConfig] = None,
    ) -> Dict[str, Any]:
        _ = config
        raw_code = (state.get("ts_code") or "").strip()
        if not raw_code:
            return {"stock_technical_meta": {"error": "missing ts_code"}, "stock_kline_data": []}

        try:
            from dataflow.utils import normalize_cn_ts_code

            ts_code = normalize_cn_ts_code(raw_code)
        except ValueError as e:
            return {"stock_technical_meta": {"error": str(e)}, "stock_kline_data": []}

        trade_date = _norm_date(state.get("trade_date")) or datetime.now().strftime("%Y%m%d")
        start_date = date_offset(trade_date, days=220)

        records: List[Dict[str, Any]] = []
        try:
            from dataflow.kline_data import KLineDataFetcher

            fetcher = KLineDataFetcher()
            df = fetcher.fetch_daily_data(ts_code=ts_code, start_date=start_date, end_date=trade_date, adj="qfq")
            if df is not None and not df.empty:
                df = df.sort_values("trade_date").tail(180)
                records = to_serializable(df) or []
        except Exception as e:
            logger.warning("拉取日线失败 ts_code=%s: %s", ts_code, e)

        return {
            "ts_code": ts_code,
            "stock_technical_meta": {
                "ts_code": ts_code,
                "trade_date": trade_date,
                "start_date": start_date,
                "kline_ready": bool(records),
            },
            "stock_kline_data": records,
        }

    return stock_technical_fetch_node


def create_stock_technical_analysis_node():
    """基于日线K线计算技术指标并封装为 LLM 输入数据。"""

    def stock_technical_analysis_node(
        state: Dict[str, Any],
        config: Optional[RunnableConfig] = None,
    ) -> Dict[str, Any]:
        _ = config
        meta = state.get("stock_technical_meta") or {}
        if meta.get("error"):
            return {
                "stock_technical_facts": None,
                "technical_analysis": {"ts_code": state.get("ts_code"), "error": meta.get("error")},
            }

        ts_code = meta.get("ts_code") or state.get("ts_code")
        rows: List[Dict[str, Any]] = list(state.get("stock_kline_data") or [])
        if not rows:
            return {
                "stock_technical_facts": None,
                "technical_analysis": {"ts_code": ts_code, "error": "无K线数据"},
            }

        df = pd.DataFrame(rows).copy()
        if "trade_date" in df.columns:
            df["trade_date"] = df["trade_date"].astype(str).str.replace("-", "", regex=False).str[:8]
            df = df.sort_values("trade_date").reset_index(drop=True)
        for c in ["open", "high", "low", "close", "vol", "amount", "pct_chg"]:
            if c in df.columns:
                df[c] = pd.to_numeric(df[c], errors="coerce")

        from dataflow.utils import calculate_bollinger_bands, calculate_kdj, calculate_ma, calculate_macd, calculate_rsi

        df = calculate_ma(df, periods=[5, 10, 20, 60])
        df = calculate_rsi(df, periods=[14])
        df = calculate_macd(df)
        df = calculate_kdj(df)
        df = calculate_bollinger_bands(df, period=20, std_dev=2.0)

        last = df.iloc[-1]

        facts = {
            "ts_code": ts_code,
            "trade_date": meta.get("trade_date"),
            "latest_price": _to_num(last.get("close")),
            "latest_pct_chg": _to_num(last.get("pct_chg")),
            "latest_indicators": {
                "ma5": _to_num(last.get("ma5")),
                "ma10": _to_num(last.get("ma10")),
                "ma20": _to_num(last.get("ma20")),
                "ma60": _to_num(last.get("ma60")),
                "rsi14": _to_num(last.get("rsi14")),
                "macd_dif": _to_num(last.get("macd_dif")),
                "macd_dea": _to_num(last.get("macd_dea")),
                "macd_hist": _to_num(last.get("macd_macd")),
                "kdj_k": _to_num(last.get("kdj_k")) if "kdj_k" in df.columns else _to_num(last.get("k")),
                "kdj_d": _to_num(last.get("kdj_d")) if "kdj_d" in df.columns else _to_num(last.get("d")),
                "kdj_j": _to_num(last.get("kdj_j")) if "kdj_j" in df.columns else _to_num(last.get("j")),
                "boll_up": _to_num(last.get("boll_upper")),
                "boll_mid": _to_num(last.get("boll_mid")),
                "boll_low": _to_num(last.get("boll_lower")),
            },
            # 提供最近窗口给 LLM 自主识别趋势/支撑阻力，不在本地硬编码信号
            "recent_bars": [],
        }
        desired_cols = [
            "trade_date",
            "open",
            "high",
            "low",
            "close",
            "pct_chg",
            "vol",
            "ma5",
            "ma10",
            "ma20",
            "ma60",
            "rsi14",
            "macd_dif",
            "macd_dea",
            "macd_macd",
            "kdj_k",
            "kdj_d",
            "kdj_j",
            "k",
            "d",
            "j",
            "boll_upper",
            "boll_mid",
            "boll_lower",
        ]
        available_cols = [c for c in desired_cols if c in df.columns]
        facts["recent_bars"] = to_serializable(df[available_cols].tail(40)) or []

        stub = {
            "ts_code": ts_code,
            "technical_score": None,
            "trend_signal": "unknown",
            "trend_strength": "unknown",
            "short_term_outlook": "",
            "risk_reminder": "",
            "summary": "已完成技术数据整理，等待 LLM 解读。",
        }
        return {"stock_technical_facts": facts, "technical_analysis": stub}

    return stock_technical_analysis_node


def create_stock_technical_insight_node(llm):
    """LLM 技术面解读节点。"""

    def stock_technical_insight_node(
        state: Dict[str, Any],
        config: Optional[RunnableConfig] = None,
    ) -> Dict[str, Any]:
        facts = state.get("stock_technical_facts")
        meta = state.get("stock_technical_meta") or {}
        if not facts or not isinstance(facts, dict):
            return {"technical_analysis": {"ts_code": meta.get("ts_code"), "error": "无有效技术面事实数据"}}

        from langchain_core.prompts import ChatPromptTemplate

        system_msg = """你是A股技术分析师。基于给定K线与技术指标数据，输出结构化技术结论。
不要编造不存在的价格或历史数据，不给投资建议。只输出 JSON。"""
        human_msg = """输入：
{facts}

输出 JSON，包含键：
- ts_code
- technical_score（0-100）
- trend_signal（uptrend|downtrend|range|unknown）
- trend_strength（strong|medium|weak|unknown）
- support_levels（数组，0-3个数字）
- resistance_levels（数组，0-3个数字）
- technical_indicators（对象，给出 ma/macd/rsi/kdj/boll 关键信号）
- short_term_outlook（1-2句）
- risk_reminder（1-2句）
- summary（2-4句）
"""
        prompt = ChatPromptTemplate.from_messages([("system", system_msg), ("human", human_msg)])
        chain = prompt | llm
        raw = chain.invoke(
            {"facts": json.dumps(facts, ensure_ascii=False, indent=2)},
            config={**(config or {}), "run_name": "个股技术面解读"},
        )
        data = extract_json_text(raw) or {}

        score_raw = data.get("technical_score")
        try:
            technical_score = round(max(0.0, min(100.0, float(score_raw))), 1)
        except Exception:
            technical_score = None

        out = {
            "ts_code": data.get("ts_code") or facts.get("ts_code"),
            "technical_score": technical_score,
            "trend_signal": data.get("trend_signal") or "unknown",
            "trend_strength": data.get("trend_strength") or "unknown",
            "support_levels": data.get("support_levels") or [],
            "resistance_levels": data.get("resistance_levels") or [],
            "technical_indicators": data.get("technical_indicators") or {},
            "short_term_outlook": data.get("short_term_outlook") or "",
            "risk_reminder": data.get("risk_reminder") or "",
            "summary": data.get("summary") or "",
        }
        return {"technical_analysis": out}

    return stock_technical_insight_node


__all__ = [
    "create_stock_technical_fetch_node",
    "create_stock_technical_analysis_node",
    "create_stock_technical_insight_node",
]
