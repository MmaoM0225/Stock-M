"""
Stock Manager（个股研究经理）- 节点

run_analysts：并行调用个股分析师子图（当前：fundamental + technical）。
stock_summary：汇总子图结果，给出个股层最终结论。
persist：将个股分析结果持久化到本地 artifacts。
detect_cache：检测本地是否已有缓存结果，避免重复分析。
"""
from __future__ import annotations

import json
import logging
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from langchain_core.prompts import ChatPromptTemplate
from langchain_core.runnables import RunnableConfig

from ...utils import extract_json_text

logger = logging.getLogger(__name__)

# 存储路径：data/artifacts/manager/stock_manager/{ts_code}/{trade_date}/result.json
_STOCK_MANAGER_ARTIFACT_ROOT = Path("data") / "artifacts" / "manager" / "stock_manager"


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


def _build_stock_manager_result_path(ts_code: str, trade_date: str) -> Path:
    """构建 stock_manager 结果文件路径。"""
    return _STOCK_MANAGER_ARTIFACT_ROOT / ts_code / trade_date / "result.json"


def _build_stock_manager_manifest_path(ts_code: str, trade_date: str) -> Path:
    """构建 stock_manager manifest 文件路径。"""
    return _STOCK_MANAGER_ARTIFACT_ROOT / ts_code / trade_date / "manifest.json"


def _invoke_one(
    name: str,
    graph: Any,
    output_key: str,
    invoke_input: Dict[str, Any],
    max_retries: int = 1,
) -> Tuple[str, str, Any]:
    """执行子图，支持失败重试"""
    last_error = None
    for attempt in range(max_retries + 1):
        try:
            result = graph.invoke(invoke_input)
            value = result.get(output_key, result)
            # 检查结果是否有效（非空且不是错误对象）
            if value is None:
                logger.warning("stock 子图 %s 返回 %s 为 None (attempt %d/%d)", name, output_key, attempt + 1, max_retries + 1)
                last_error = f"{output_key} is None"
                if attempt < max_retries:
                    import time
                    time.sleep(0.5)  # 短暂延迟后重试
                    continue
            elif isinstance(value, dict) and value.get("error"):
                logger.warning("stock 子图 %s 返回错误: %s (attempt %d/%d)", name, value.get("error"), attempt + 1, max_retries + 1)
                last_error = value.get("error")
                if attempt < max_retries:
                    import time
                    time.sleep(0.5)
                    continue
            else:
                # 成功
                if attempt > 0:
                    logger.info("stock 子图 %s 在第 %d 次尝试后成功", name, attempt + 1)
                return name, output_key, value
        except Exception as e:
            logger.exception("stock 子图 %s 执行失败 (attempt %d/%d): %s", name, attempt + 1, max_retries + 1, e)
            last_error = str(e)
            if attempt < max_retries:
                import time
                time.sleep(0.5)
                continue

    # 所有尝试都失败
    return name, output_key, {"error": last_error or "max retries exceeded"}


def create_detect_stock_manager_cache_node():
    """检测本地是否已有 stock_manager 的缓存结果。如果之前分析失败，则跳过缓存重新分析。"""

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
                "stock_manager_cache_hit": False,
                "stock_manager_cache_path": None,
            }

        result_path = _build_stock_manager_result_path(ts_code, trade_date)
        manifest_path = _build_stock_manager_manifest_path(ts_code, trade_date)

        if result_path.exists() and manifest_path.exists():
            try:
                cached_result = _load_json_file(result_path)
                summary = cached_result.get("stock_manager_summary") if cached_result else None

                # 检查是否有有效结果
                if not summary:
                    logger.info("stock_manager 缓存无效（无summary）: %s/%s", ts_code, trade_date)
                    return {
                        **state,
                        "stock_manager_cache_hit": False,
                        "stock_manager_cache_path": None,
                    }

                # 检查之前是否分析失败
                if not summary.get("success", True):
                    logger.info("stock_manager 缓存标记为失败，重新分析: %s/%s", ts_code, trade_date)
                    return {
                        **state,
                        "stock_manager_cache_hit": False,
                        "stock_manager_cache_path": None,
                    }

                logger.info("stock_manager 缓存命中: %s/%s", ts_code, trade_date)
                return {
                    **state,
                    "stock_manager_cache_hit": True,
                    "stock_manager_cache_path": result_path.as_posix(),
                    "stock_manager_summary": summary,
                }
            except Exception as e:
                logger.warning("读取 stock_manager 缓存失败: %s", e)

        return {
            **state,
            "stock_manager_cache_hit": False,
            "stock_manager_cache_path": None,
        }

    return detect_cache_node


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
        # 如果缓存命中，直接跳过子图执行
        if state.get("stock_manager_cache_hit"):
            logger.info("stock_manager 缓存命中，跳过分析师子图")
            return {
                **state,
                "analysts_skipped": True,
            }

        ts_code = (state.get("ts_code") or "").strip()
        trade_date = state.get("trade_date") or ""
        if not ts_code:
            logger.warning("stock run_analysts: 缺少 ts_code，跳过所有子图")
            return {output_key: {"error": "missing ts_code"} for _, _, output_key in analyst_tasks}

        invoke_input: Dict[str, Any] = {"ts_code": ts_code}
        if trade_date:
            invoke_input["trade_date"] = str(trade_date)

        out: Dict[str, Any] = {
            # 确保 ts_code 和 trade_date 被保留在状态中
            "ts_code": ts_code,
            "trade_date": trade_date,
        }
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
            # 判断分析是否成功：基本面和技术面都必须成功且有有效评分
            # 只要有一项失败，整体就标记为失败
            success = (
                (fa.get("success") and fs is not None)
                and (ta.get("success") and ts is not None)
            )

            return {
                "ts_code": ts_code,
                "trade_date": state.get("trade_date"),
                "stock_manager_summary": {
                    **_STOCK_MANAGER_SUMMARY_DEFAULT,
                    "success": success,
                    "overall_score": score,
                    "component_scores": {"fundamental": fs, "technical": ts},
                    "action_signal": action_signal,
                    "signal_reason": signal_reason,
                    "summary": "未使用 LLM 汇总，结果为规则加权。",
                },
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

        # 判断分析是否成功：综合评分有效且基本面和技术面都必须成功
        # 注意：fa 可能是 None 或空字典，需要检查
        fundamental_success = isinstance(fa, dict) and fa.get("success", False)
        technical_success = isinstance(ta, dict) and ta.get("success", False)
        # 如果基本面或技术面分析完全缺失（null/None），标记为失败
        fundamental_exists = fa is not None and isinstance(fa, dict) and "success" in fa
        technical_exists = ta is not None and isinstance(ta, dict) and "success" in ta
        # 逻辑：综合评分有效，且所有存在的分析都必须成功
        # 如果任一存在的分析失败，则整体标记为失败
        success = score is not None and fundamental_exists and technical_exists and fundamental_success and technical_success

        out = {
            "ts_code": data.get("ts_code") or ts_code,
            "success": success,
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
        # 保留原始 state 中的 ts_code 和 trade_date
        return {
            "ts_code": ts_code,
            "trade_date": state.get("trade_date"),
            "stock_manager_summary": out,
        }

    return stock_summary_node


def create_persist_stock_manager_node():
    """将 stock_manager 分析结果持久化到本地 artifacts，并同步数据库主表。"""

    def persist_node(
        state: Dict[str, Any],
        config: Optional[RunnableConfig] = None,
    ) -> Dict[str, Any]:
        _ = config
        # 如果缓存已命中，不需要重复保存
        if state.get("stock_manager_cache_hit"):
            logger.debug("stock_manager persist: 缓存命中，跳过保存")
            return {
                **state,
                "stock_manager_persisted": False,
                "stock_manager_persist_reason": "cache_hit",
            }

        ts_code = (state.get("ts_code") or "").strip()
        trade_date = str(state.get("trade_date") or datetime.now().strftime("%Y%m%d")).replace("-", "")[:8]

        logger.debug("stock_manager persist: ts_code=%s, trade_date=%s", ts_code, trade_date)

        if not ts_code:
            logger.warning("stock_manager persist: 缺少 ts_code")
            return {**state, "stock_manager_persisted": False, "stock_manager_persist_reason": "missing_ts_code"}

        summary = state.get("stock_manager_summary")
        if not summary:
            logger.warning("stock_manager persist: 无 summary 数据，state keys=%s", list(state.keys()))
            return {**state, "stock_manager_persisted": False, "stock_manager_persist_reason": "no_summary"}

        result_path = _build_stock_manager_result_path(ts_code, trade_date)
        manifest_path = _build_stock_manager_manifest_path(ts_code, trade_date)

        logger.debug("stock_manager persist: 准备写入 %s", result_path)

        # 构建结果对象（仅保留最终汇总，中间结果已保存在各自 analyst 文件中）
        result_payload = {
            "ts_code": ts_code,
            "trade_date": trade_date,
            "stock_manager_summary": summary,
        }

        try:
            _write_json_atomic(result_path, result_payload)
            _write_json_atomic(
                manifest_path,
                {
                    "artifact_type": "stock_manager_result",
                    "module": "agents.manager.stock_manager",
                    "ts_code": ts_code,
                    "trade_date": trade_date,
                    "created_at": datetime.now().astimezone().isoformat(),
                    "status": "success",
                    "result_path": result_path.as_posix(),
                },
            )
            try:
                from database.data_sync.stock_manager import sync_single_result

                sync_single_result(result_path)
            except Exception as sync_err:
                logger.warning("stock_manager 数据库同步失败: %s", sync_err)
            logger.info("stock_manager 结果已持久化: %s", result_path)
            return {
                **state,
                "stock_manager_persisted": True,
                "stock_manager_result_path": result_path.as_posix(),
                "stock_manager_manifest_path": manifest_path.as_posix(),
            }
        except Exception as e:
            logger.exception("stock_manager 持久化失败: %s", e)
            return {**state, "stock_manager_persisted": False, "stock_manager_persist_error": str(e)}

    return persist_node


__all__ = [
    "create_detect_stock_manager_cache_node",
    "create_run_analysts_node",
    "create_stock_summary_node",
    "create_persist_stock_manager_node",
]
