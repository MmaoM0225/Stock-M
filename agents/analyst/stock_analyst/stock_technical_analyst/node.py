"""
Stock Technical Analyst（个股技术面分析师）- 节点实现

流程：拉取日线K线 -> 计算并整理技术指标数据 -> LLM 生成结构化技术结论 -> 持久化存储。
"""
from __future__ import annotations

import json
import logging
import os
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd
from langchain_core.runnables import RunnableConfig

from ....utils import date_offset, extract_json_text, to_serializable

logger = logging.getLogger(__name__)

# 存储路径：data/artifacts/analyst/stock_analyst/stock_technical_analyst/{ts_code}/{trade_date}/result.json
_TECHNICAL_ANALYST_ARTIFACT_ROOT = (
    Path("data") / "artifacts" / "analyst" / "stock_analyst" / "stock_technical_analyst"
)


def _write_json_atomic(path: Path, payload: Dict[str, Any]) -> None:
    """原子写入 JSON，避免中途中断留下半成品。"""
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_suffix(path.suffix + ".tmp")
    with open(tmp_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
    os.replace(tmp_path, path)


def _load_json_file(path: Path) -> Any:
    """读取 JSON 文件并返回解析结果。"""
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def _build_technical_result_path(ts_code: str, trade_date: str) -> Path:
    """构建技术面分析师结果文件路径。"""
    return _TECHNICAL_ANALYST_ARTIFACT_ROOT / ts_code / trade_date / "result.json"


def _build_technical_manifest_path(ts_code: str, trade_date: str) -> Path:
    """构建技术面分析师 manifest 文件路径。"""
    return _TECHNICAL_ANALYST_ARTIFACT_ROOT / ts_code / trade_date / "manifest.json"


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

        # 判断分析是否成功：有K线数据且LLM返回有效评分
        kline_ready = meta.get("kline_ready", False)
        success = kline_ready and technical_score is not None

        out = {
            "ts_code": data.get("ts_code") or facts.get("ts_code"),
            "success": success,
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


def create_detect_technical_cache_node():
    """检测本地是否已有技术面分析的缓存结果。如果之前分析失败，则跳过缓存重新分析。"""

    def detect_cache_node(
        state: Dict[str, Any],
        config: Optional[RunnableConfig] = None,
    ) -> Dict[str, Any]:
        _ = config
        ts_code = (state.get("ts_code") or "").strip()
        trade_date = str(state.get("trade_date") or "").replace("-", "")[:8]

        if not ts_code or not trade_date:
            return {
                **state,
                "technical_cache_hit": False,
                "technical_cache_path": None,
            }

        result_path = _build_technical_result_path(ts_code, trade_date)
        manifest_path = _build_technical_manifest_path(ts_code, trade_date)

        if result_path.exists() and manifest_path.exists():
            try:
                cached_result = _load_json_file(result_path)
                tech_analysis = cached_result.get("technical_analysis") if cached_result else None

                # 检查是否有有效结果
                if not tech_analysis:
                    logger.info("technical_analyst 缓存无效（无分析结果）: %s/%s", ts_code, trade_date)
                    return {
                        **state,
                        "technical_cache_hit": False,
                        "technical_cache_path": None,
                    }

                # 检查之前是否分析失败
                if not tech_analysis.get("success", True):
                    logger.info("technical_analyst 缓存标记为失败，重新分析: %s/%s", ts_code, trade_date)
                    return {
                        **state,
                        "technical_cache_hit": False,
                        "technical_cache_path": None,
                    }

                logger.info("technical_analyst 缓存命中: %s/%s", ts_code, trade_date)
                return {
                    **state,
                    "technical_cache_hit": True,
                    "technical_cache_path": result_path.as_posix(),
                    # 恢复完整状态
                    "stock_technical_meta": cached_result.get("stock_technical_meta"),
                    "stock_kline_data": cached_result.get("stock_kline_data"),
                    "stock_technical_facts": cached_result.get("stock_technical_facts"),
                    "technical_analysis": tech_analysis,
                }
            except Exception as e:
                logger.warning("读取 technical_analyst 缓存失败: %s", e)

        return {
            **state,
            "technical_cache_hit": False,
            "technical_cache_path": None,
        }

    return detect_cache_node


def create_technical_persist_node():
    """将技术面分析结果持久化到本地 artifacts。"""

    def persist_node(
        state: Dict[str, Any],
        config: Optional[RunnableConfig] = None,
    ) -> Dict[str, Any]:
        _ = config
        # 如果缓存已命中，不需要重复保存
        if state.get("technical_cache_hit"):
            return {
                **state,
                "technical_persisted": False,
                "technical_persist_reason": "cache_hit",
            }

        ts_code = (state.get("ts_code") or "").strip()
        trade_date = str(state.get("trade_date") or datetime.now().strftime("%Y%m%d")).replace("-", "")[:8]

        if not ts_code:
            return {**state, "technical_persisted": False, "technical_persist_reason": "missing_ts_code"}

        result = state.get("technical_analysis")
        if not result:
            return {**state, "technical_persisted": False, "technical_persist_reason": "no_result"}

        result_path = _build_technical_result_path(ts_code, trade_date)
        manifest_path = _build_technical_manifest_path(ts_code, trade_date)

        # 构建完整的结果对象
        result_payload = {
            "ts_code": ts_code,
            "trade_date": trade_date,
            "stock_technical_meta": state.get("stock_technical_meta"),
            "stock_kline_data": state.get("stock_kline_data"),
            "stock_technical_facts": state.get("stock_technical_facts"),
            "technical_analysis": result,
        }

        try:
            _write_json_atomic(result_path, result_payload)
            _write_json_atomic(
                manifest_path,
                {
                    "artifact_type": "stock_technical_analyst_result",
                    "module": "agents.analyst.stock_analyst.stock_technical_analyst",
                    "ts_code": ts_code,
                    "trade_date": trade_date,
                    "created_at": datetime.now().astimezone().isoformat(),
                    "status": "success",
                    "result_path": result_path.as_posix(),
                },
            )
            logger.info("technical_analyst 结果已持久化: %s", result_path)
            return {
                **state,
                "technical_persisted": True,
                "technical_result_path": result_path.as_posix(),
                "technical_manifest_path": manifest_path.as_posix(),
            }
        except Exception as e:
            logger.warning("technical_analyst 持久化失败: %s", e)
            return {**state, "technical_persisted": False, "technical_persist_error": str(e)}

    return persist_node


__all__ = [
    "create_stock_technical_fetch_node",
    "create_stock_technical_analysis_node",
    "create_stock_technical_insight_node",
    "create_detect_technical_cache_node",
    "create_technical_persist_node",
]
