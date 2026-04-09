"""
Stock Manager（个股研究经理）- 节点

run_analysts：并行调用个股分析师子图（当前：fundamental + technical）。
stock_summary：汇总子图结果，给出个股层最终结论。
"""
from __future__ import annotations

import json
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Dict, List, Optional, Tuple

from langchain_core.prompts import ChatPromptTemplate
from langchain_core.runnables import RunnableConfig

from ...utils import extract_json_text

logger = logging.getLogger(__name__)


def _invoke_one(
    name: str,
    graph: Any,
    output_key: str,
    invoke_input: Dict[str, Any],
) -> Tuple[str, str, Any]:
    try:
        result = graph.invoke(invoke_input)
        return name, output_key, result.get(output_key, result)
    except Exception as e:
        logger.exception("stock 子图 %s 执行失败: %s", name, e)
        return name, output_key, {"error": str(e)}


def create_run_analysts_node(
    analyst_tasks: List[Tuple[str, Any, str]],
    max_workers: int = 2,
):
    """并行运行个股分析师子图，合并结果到 state。"""

    def run_analysts_node(
        state: Dict[str, Any],
        config: Optional[RunnableConfig] = None,
    ) -> Dict[str, Any]:
        _ = config
        ts_code = (state.get("ts_code") or "").strip()
        trade_date = state.get("trade_date") or ""
        if not ts_code:
            logger.warning("stock run_analysts: 缺少 ts_code，跳过所有子图")
            return {output_key: {"error": "missing ts_code"} for _, _, output_key in analyst_tasks}

        invoke_input: Dict[str, Any] = {"ts_code": ts_code}
        if trade_date:
            invoke_input["trade_date"] = str(trade_date)

        out: Dict[str, Any] = {}
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {
                executor.submit(_invoke_one, name, graph, output_key, invoke_input): (name, output_key)
                for name, graph, output_key in analyst_tasks
            }
            for future in as_completed(futures):
                name, output_key = futures[future]
                try:
                    _, key, value = future.result()
                    out[key] = value
                except Exception as e:
                    logger.exception("获取子图 %s 结果失败: %s", name, e)
                    out[output_key] = {"error": str(e)}

        for _, _, output_key in analyst_tasks:
            if output_key not in out:
                out[output_key] = {"error": "no result"}
        return {**state, **out}

    return run_analysts_node


_STOCK_MANAGER_SUMMARY_DEFAULT: Dict[str, Any] = {
    "overall_score": None,
    "confidence": "低",
    "selection_reason": "",
    "risk_level": "中",
    "component_scores": {
        "fundamental": None,
        "technical": None,
    },
    "action_signal": "watch",
    "signal_reason": "",
    "key_points": [],
    "risks": [],
    "summary": "",
}


def create_stock_summary_node(llm):
    """汇总 fundamental + technical 分析结果，生成个股层综合结论。"""

    def stock_summary_node(
        state: Dict[str, Any],
        config: Optional[RunnableConfig] = None,
    ) -> Dict[str, Any]:
        fa = state.get("fundamental_reduce_result") or {}
        ta = state.get("technical_analysis") or {}
        ts_code = (state.get("ts_code") or fa.get("ts_code") or ta.get("ts_code") or "").strip()

        # 规则降级：无LLM时给可用结果
        if llm is None:
            f_score = fa.get("overall_score")
            t_score = ta.get("technical_score")
            try:
                fs = float(f_score) if f_score is not None else None
            except Exception:
                fs = None
            try:
                ts = float(t_score) if t_score is not None else None
            except Exception:
                ts = None
            if fs is None and ts is None:
                score = None
            elif fs is None:
                score = round(ts, 1)
            elif ts is None:
                score = round(fs, 1)
            else:
                score = round(0.6 * fs + 0.4 * ts, 1)
            if score is None:
                action_signal = "watch"
                signal_reason = "评分缺失，建议先观察。"
            elif score >= 75:
                action_signal = "buy"
                signal_reason = "综合评分较高，且风险可控。"
            elif score >= 60:
                action_signal = "hold"
                signal_reason = "综合评分中上，趋势与基本面尚可。"
            elif score >= 45:
                action_signal = "watch"
                signal_reason = "综合评分一般，建议继续跟踪等待确认。"
            else:
                action_signal = "sell"
                signal_reason = "综合评分偏低，风险收益比不占优。"
            return {
                "stock_manager_summary": {
                    **_STOCK_MANAGER_SUMMARY_DEFAULT,
                    "overall_score": score,
                    "component_scores": {"fundamental": fs, "technical": ts},
                    "action_signal": action_signal,
                    "signal_reason": signal_reason,
                    "summary": "未使用 LLM 汇总，结果为规则加权。",
                }
            }

        payload = {
            "ts_code": ts_code,
            "fundamental_reduce_result": fa,
            "technical_analysis": ta,
        }

        system_msg = """你是个股研究经理。请将基本面综合结果与技术面结果合并成一个最终结论。
要求：
1) 综合评分以 0-100 输出（可参考输入评分）；
2) 解释为什么给出该分；
3) 明确风险等级与关键风险；
4) 仅基于输入，不编造。
仅输出严格 JSON。"""
        human_msg = """输入：
{payload}

请输出 JSON，包含键：
- ts_code
- overall_score（0-100）
- confidence（高|中|低）
- selection_reason（一句话）
- risk_level（低|中|高）
- component_scores（对象，含 fundamental/technical）
- action_signal（buy|hold|sell|watch）
- signal_reason（一句话，解释信号）
- key_points（字符串数组，3-6条）
- risks（字符串数组，1-5条）
- summary（2-4句）
"""
        prompt = ChatPromptTemplate.from_messages([("system", system_msg), ("human", human_msg)])
        chain = prompt | llm
        raw = chain.invoke(
            {"payload": json.dumps(payload, ensure_ascii=False, indent=2)},
            config={**(config or {}), "run_name": "个股Manager汇总"},
        )
        data = extract_json_text(raw) or {}

        score_raw = data.get("overall_score")
        try:
            score = round(max(0.0, min(100.0, float(score_raw))), 1)
        except Exception:
            score = None

        component = data.get("component_scores") or {}
        out = {
            "ts_code": data.get("ts_code") or ts_code,
            "overall_score": score,
            "confidence": data.get("confidence") or "低",
            "selection_reason": data.get("selection_reason") or "",
            "risk_level": data.get("risk_level") or "中",
            "component_scores": {
                "fundamental": component.get("fundamental", fa.get("overall_score")),
                "technical": component.get("technical", ta.get("technical_score")),
            },
            "action_signal": data.get("action_signal") or "watch",
            "signal_reason": data.get("signal_reason") or "",
            "key_points": data.get("key_points") or [],
            "risks": data.get("risks") or [],
            "summary": data.get("summary") or "",
        }
        return {"stock_manager_summary": out}

    return stock_summary_node


__all__ = [
    "create_run_analysts_node",
    "create_stock_summary_node",
]
