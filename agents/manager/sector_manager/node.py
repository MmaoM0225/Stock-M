"""
Sector Manager（行业管理器）- 节点

run_analysts：并行调用行业趋势与板块资金流两个分析师子图，合并结果到 state。
sector_summary：综合两个分析师结果，输出行业层综合结论。
"""
import json
import logging
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

from langchain_core.prompts import ChatPromptTemplate
from langchain_core.runnables import RunnableConfig

from ...utils import extract_json_text

logger = logging.getLogger(__name__)

_SECTOR_ANALYST_ARTIFACT_ROOT = Path("data") / "artifacts" / "analyst" / "sector_analyst"
_SECTOR_MANAGER_ARTIFACT_ROOT = Path("data") / "artifacts" / "manager" / "sector_manager"
_MACRO_MANAGER_ARTIFACT_ROOT = Path("data") / "artifacts" / "manager" / "macro_manager"
_SECTOR_ANALYST_ARTIFACT_DIRS: Dict[str, str] = {
    "sector_trend": "sector_trend_analyst",
    "sector_capital_flow": "sector_capital_flow_analyst",
}

_SECTOR_MANAGER_SUMMARY_DEFAULT: Dict[str, Any] = {
    "market_regime": "unknown",
    "market_bias": "neutral",
    "action_bias": "wait_and_see",
    "favored_sectors": [],
    "watchlist_sectors": [],
    "risk_sectors": [],
    "core_signals": [],
    "confidence": 0.0,
    "sector_summary": "",
}


def _build_sector_analyst_result_path(name: str, trade_date: str) -> Optional[Path]:
    """根据分析师名称与交易日，定位本地 artifact 的 result.json 路径。"""
    artifact_dir = _SECTOR_ANALYST_ARTIFACT_DIRS.get(name)
    if not artifact_dir:
        return None
    return _SECTOR_ANALYST_ARTIFACT_ROOT / artifact_dir / trade_date / "result.json"


def _load_json_file(path: Path) -> Any:
    """读取 JSON 文件并返回解析结果。"""
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def _write_json_atomic(path: Path, payload: Dict[str, Any]) -> None:
    """原子写入 JSON，避免中途中断留下半成品。"""
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_suffix(path.suffix + ".tmp")
    with open(tmp_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
    os.replace(tmp_path, path)


def _persist_sector_manager_summary(
    state: Dict[str, Any],
    summary: Dict[str, Any],
) -> Dict[str, Any]:
    """将 sector_manager_summary 持久化到本地 artifacts，并同步数据库主表。"""
    trade_date = str(state.get("trade_date") or datetime.now().strftime("%Y%m%d")).replace("-", "")[:8]
    artifact_dir = _SECTOR_MANAGER_ARTIFACT_ROOT / trade_date
    result_path = artifact_dir / "result.json"
    manifest_path = artifact_dir / "manifest.json"

    _write_json_atomic(result_path, summary)
    _write_json_atomic(
        manifest_path,
        {
            "artifact_type": "sector_manager_summary",
            "module": "agents.manager.sector_manager",
            "trade_date": trade_date,
            "created_at": datetime.now().astimezone().isoformat(),
            "status": "success",
            "result_path": result_path.as_posix(),
        },
    )
    try:
        from database.data_sync.sector_manager import sync_single_result

        sync_single_result(result_path)
    except Exception as sync_err:
        logger.warning("sector_manager 数据库同步失败: %s", sync_err)
    logger.info("sector_manager_summary 已写入本地 artifacts: %s", result_path)
    return {
        **state,
        "sector_manager_summary": summary,
        "sector_manager_artifact_path": result_path.as_posix(),
        "sector_manager_manifest_path": manifest_path.as_posix(),
    }


def create_detect_available_analysts_node(
    analyst_tasks: List[Tuple[str, Any, str]],
):
    """
    检测本地已存在的 analyst artifact。

    - 命中则直接加载到 state 的 output_key
    - 未命中则记录到 missing_analysts
    """

    def detect_available_analysts_node(
        state: Dict[str, Any],
        config: Optional[RunnableConfig] = None,
    ) -> Dict[str, Any]:
        _ = config
        trade_date = str(state.get("trade_date") or "").replace("-", "")[:8]
        available_analysts: List[str] = []
        missing_analysts: List[str] = []
        loaded_artifact_paths: Dict[str, str] = {}
        loaded_values: Dict[str, Any] = {}

        if not trade_date:
            logger.warning("sector detect_available_analysts: 缺少 trade_date，无法检测本地数据")
            return {
                **state,
                "available_analysts": available_analysts,
                "missing_analysts": [name for name, _, _ in analyst_tasks],
                "loaded_artifact_paths": loaded_artifact_paths,
            }

        for name, _, output_key in analyst_tasks:
            existing_value = state.get(output_key)
            if existing_value:
                available_analysts.append(name)
                loaded_artifact_paths[name] = "<state>"
                continue

            result_path = _build_sector_analyst_result_path(name, trade_date)
            if result_path is None or not result_path.exists():
                missing_analysts.append(name)
                continue

            try:
                payload = _load_json_file(result_path)
                if not payload:
                    logger.warning(
                        "sector detect_available_analysts: %s 本地结果为空，视为缺失",
                        result_path,
                    )
                    missing_analysts.append(name)
                    continue
                loaded_values[output_key] = payload
                available_analysts.append(name)
                loaded_artifact_paths[name] = result_path.as_posix()
            except Exception as e:
                logger.warning("sector detect_available_analysts: 读取 %s 失败: %s", result_path, e)
                missing_analysts.append(name)

        logger.info(
            "sector_manager 本地检测完成: 命中 %d 个，缺失 %d 个",
            len(available_analysts),
            len(missing_analysts),
        )
        return {
            **state,
            **loaded_values,
            "available_analysts": available_analysts,
            "missing_analysts": missing_analysts,
            "loaded_artifact_paths": loaded_artifact_paths,
        }

    return detect_available_analysts_node


def _ordered_unique_strings(items: Iterable[Any], max_items: Optional[int] = None) -> List[str]:
    out: List[str] = []
    seen = set()
    for item in items:
        if item is None:
            continue
        text = str(item).strip()
        if not text or text in seen:
            continue
        seen.add(text)
        out.append(text)
        if max_items is not None and len(out) >= max_items:
            break
    return out


def _build_rule_based_sector_summary(state: Dict[str, Any]) -> Dict[str, Any]:
    trend = state.get("sector_trend_insight") or {}
    flow = state.get("sector_capital_flow_insight") or {}
    macro = state.get("macro_manager_summary") or {}

    market_regime = str(trend.get("market_regime") or "mixed")
    market_bias = str(flow.get("market_bias") or "neutral")
    macro_direction = str(macro.get("market_direction") or "neutral")
    macro_position = str(macro.get("target_position") or "medium")
    if market_regime == "trend_following" and market_bias == "bullish":
        action_bias = "follow_leaders"
    elif market_regime == "repair":
        action_bias = "low_buy_repair"
    elif market_regime == "rotation":
        action_bias = "fast_rotation"
    elif market_regime == "risk_off" or market_bias == "bearish":
        action_bias = "defense"
    else:
        action_bias = "wait_and_see"

    if macro_direction == "bearish" or macro_position == "low":
        action_bias = "defense"

    favored_sectors = _ordered_unique_strings(
        [
            *(macro.get("focus_industry_sectors") or []),
            *(trend.get("leading_themes") or []),
            *(flow.get("hot_sectors") or []),
        ],
        max_items=8,
    )
    watchlist_sectors = _ordered_unique_strings(
        trend.get("reversal_opportunities") or [],
        max_items=6,
    )
    risk_sectors = _ordered_unique_strings(
        [
            *(macro.get("avoid_sectors") or []),
            *(trend.get("top_risk_sectors") or []),
            *(flow.get("risk_sectors") or []),
        ],
        max_items=8,
    )
    core_signals = _ordered_unique_strings(
        [
            *(trend.get("highlights") or []),
            *(flow.get("highlights") or []),
        ],
        max_items=6,
    )

    fragments = []
    if trend.get("summary"):
        fragments.append(f"趋势面：{trend['summary']}")
    if flow.get("summary"):
        fragments.append(f"资金面：{flow['summary']}")
    if macro.get("macro_summary"):
        fragments.append(f"宏观面：{macro['macro_summary']}")
    sector_summary = "；".join(fragments) if fragments else "未能生成行业综合结论。"

    confidence = 0.4
    if trend.get("summary") and flow.get("summary"):
        confidence = 0.65
    if macro.get("macro_summary"):
        confidence = min(0.8, confidence + 0.1)

    return {
        "market_regime": market_regime,
        "market_bias": market_bias,
        "action_bias": action_bias,
        "favored_sectors": favored_sectors,
        "watchlist_sectors": watchlist_sectors,
        "risk_sectors": risk_sectors,
        "core_signals": core_signals,
        "confidence": confidence,
        "sector_summary": sector_summary,
    }


def create_load_macro_manager_summary_node():
    """
    加载 macro_manager_summary：
    - 若 state 中已存在则沿用
    - 否则尝试从本地 artifact 读取
    """

    def load_macro_manager_summary_node(
        state: Dict[str, Any],
        config: Optional[RunnableConfig] = None,
    ) -> Dict[str, Any]:
        _ = config
        if state.get("macro_manager_summary"):
            return state

        trade_date = str(state.get("trade_date") or "").replace("-", "")[:8]
        if not trade_date:
            logger.warning("load_macro_manager_summary: 缺少 trade_date，无法加载宏观经理结果")
            return state

        result_path = _MACRO_MANAGER_ARTIFACT_ROOT / trade_date / "result.json"
        if not result_path.exists():
            logger.info("load_macro_manager_summary: 本地未命中 %s", result_path)
            return state

        try:
            payload = _load_json_file(result_path)
            if not payload:
                logger.warning("load_macro_manager_summary: %s 为空", result_path)
                return state
            logger.info("load_macro_manager_summary: 已加载本地宏观经理结果 %s", result_path)
            return {
                **state,
                "macro_manager_summary": payload,
                "macro_manager_artifact_path": result_path.as_posix(),
            }
        except Exception as e:
            logger.warning("load_macro_manager_summary: 读取 %s 失败: %s", result_path, e)
            return state

    return load_macro_manager_summary_node


def create_sector_summary_node(llm=None):
    """综合行业趋势与板块资金流结果，生成行业经理层结论。"""

    def sector_summary_node(
        state: Dict[str, Any],
        config: Optional[RunnableConfig] = None,
    ) -> Dict[str, Any]:
        trade_date = state.get("trade_date") or datetime.now().strftime("%Y%m%d")
        fallback_summary = _build_rule_based_sector_summary(state)

        if llm is None:
            logger.warning("sector_summary: 未提供 LLM，回退为规则汇总")
            summary = {
                **_SECTOR_MANAGER_SUMMARY_DEFAULT,
                **fallback_summary,
            }
            try:
                return _persist_sector_manager_summary(state, summary)
            except Exception as e:
                logger.warning("写入 sector_manager artifacts 失败: %s", e)
                return {
                    **state,
                    "sector_manager_summary": summary,
                }

        payload = {
            "trade_date": trade_date,
            "macro_manager_summary": state.get("macro_manager_summary"),
            "sector_trend_insight": state.get("sector_trend_insight"),
            "sector_capital_flow_insight": state.get("sector_capital_flow_insight"),
        }

        system_msg = """你是一位行业轮动经理（Sector Rotation Manager）。

你会收到某一交易日的结构化分析结果：
0. 宏观经理汇总（可能为空）
1. 行业趋势分析
2. 板块资金流分析

你的任务：仅基于当日输入，输出一份严格 JSON，总结行业层的综合判断。

分析原则：
- 如果提供了 macro_manager_summary，需要把宏观方向、仓位建议、风险因素融入行业判断；
- favored_sectors 既要看行业趋势+资金共振，也要与宏观 focus_industry_sectors 尽量一致；
- risk_sectors 要综合宏观 avoid_sectors 与行业/资金弱势方向。

返回 JSON 结构：
{{
  "market_regime": "trend_following | rotation | repair | risk_off | mixed | unknown",
  "market_bias": "bullish | neutral | bearish",
  "action_bias": "follow_leaders | low_buy_repair | fast_rotation | defense | wait_and_see | unknown",
  "favored_sectors": ["趋势与资金共振的优先方向"],
  "watchlist_sectors": ["可继续观察的修复或轮动方向"],
  "risk_sectors": ["高位转弱或资金持续流出的方向"],
  "core_signals": ["综合后的关键信号，每条一句"],
  "confidence": 0.0 到 1.0 之间数字,
  "sector_summary": "2-4 句话总结当日行业结构、资金偏好与执行建议"
}}

要求：
- favored_sectors 优先保留同时得到趋势与资金验证的方向。
- watchlist_sectors 主要来自修复候选、轮动候选。
- risk_sectors 主要来自高位风险与资金弱势方向。
- 结论必须针对当日输入，不能输出空泛模板话术。
- 所有字段必须存在；无法判断时用 unknown、[] 或 0.0。
- 只输出 JSON，不要额外解释。"""

        human_msg = """以下是 {trade_date} 的两个分析师结果：

```json
{data}
```

请综合输出上述 JSON。"""

        prompt = ChatPromptTemplate.from_messages(
            [("system", system_msg), ("human", human_msg)]
        )
        logger.info("正在处理 行业经理汇总分析：整合行业趋势与资金流")
        try:
            chain = prompt | llm
            raw = chain.invoke(
                {
                    "trade_date": trade_date,
                    "data": json.dumps(payload, ensure_ascii=False, indent=2, default=str),
                },
                config={**(config or {}), "run_name": "行业经理汇总分析"},
            )
            data = extract_json_text(raw)
            for k, v in _SECTOR_MANAGER_SUMMARY_DEFAULT.items():
                data.setdefault(k, v)
            summary = {
                **_SECTOR_MANAGER_SUMMARY_DEFAULT,
                **data,
            }
            try:
                return _persist_sector_manager_summary(state, summary)
            except Exception as e:
                logger.warning("写入 sector_manager artifacts 失败: %s", e)
                return {
                    **state,
                    "sector_manager_summary": summary,
                }
        except Exception as e:
            logger.exception("行业经理汇总失败，回退为规则汇总: %s", e)
            summary = {
                **_SECTOR_MANAGER_SUMMARY_DEFAULT,
                **fallback_summary,
            }
            try:
                return _persist_sector_manager_summary(state, summary)
            except Exception as persist_err:
                logger.warning("写入 sector_manager artifacts 失败: %s", persist_err)
                return {
                    **state,
                    "sector_manager_summary": summary,
                }

    return sector_summary_node


def _invoke_one(
    name: str,
    graph: Any,
    output_key: str,
    invoke_input: Dict[str, Any],
) -> Tuple[str, str, Any]:
    try:
        result = graph.invoke(invoke_input)
        return (name, output_key, result.get(output_key))
    except Exception as e:
        logger.exception("子图 %s 执行失败: %s", name, e)
        return (name, output_key, {"error": str(e)})


def create_run_analysts_node(
    analyst_tasks: List[Tuple[str, Any, str]],
    max_workers: int = 2,
):
    """并行运行两个行业分析师子图，合并结果到 state。"""

    def run_analysts_node(
        state: Dict[str, Any],
        config: Optional[RunnableConfig] = None,
    ) -> Dict[str, Any]:
        trade_date = state.get("trade_date") or ""
        if not trade_date:
            logger.warning("sector run_analysts: 缺少 trade_date，跳过所有子图")
            error_outputs = {
                output_key: {"error": "missing trade_date"}
                for _, _, output_key in analyst_tasks
            }
            return {
                **state,
                "available_analysts": [],
                "missing_analysts": [name for name, _, _ in analyst_tasks],
                **error_outputs,
            }

        missing_analysts = state.get("missing_analysts")
        if isinstance(missing_analysts, list):
            missing_set = {str(x) for x in missing_analysts}
            tasks_to_run = [
                (name, graph, output_key)
                for name, graph, output_key in analyst_tasks
                if name in missing_set
            ]
        else:
            tasks_to_run = analyst_tasks

        if not tasks_to_run:
            logger.info("sector_manager 所有分析师结果均已命中本地 artifacts，跳过子图执行")
            return {
                **state,
                "missing_analysts": [],
            }

        logger.info(
            "开始并行运行 %d 个缺失的行业分析师子图（总计 %d 个）",
            len(tasks_to_run),
            len(analyst_tasks),
        )
        invoke_input: Dict[str, Any] = {"trade_date": trade_date}
        out: Dict[str, Any] = {}

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {
                executor.submit(_invoke_one, name, graph, output_key, invoke_input): (
                    name,
                    output_key,
                )
                for name, graph, output_key in tasks_to_run
            }
            for future in as_completed(futures):
                name, output_key = futures[future]
                try:
                    _, key, value = future.result()
                    out[key] = value
                except Exception as e:
                    logger.exception("获取子图 %s 结果失败: %s", name, e)
                    out[output_key] = {"error": str(e)}

        for _, _, output_key in tasks_to_run:
            if output_key not in out:
                out[output_key] = {"error": "no result"}

        merged_state = {**state, **out}
        remaining_missing: List[str] = []
        available_analysts: List[str] = []
        for name, _, output_key in analyst_tasks:
            value = merged_state.get(output_key)
            if not value or (isinstance(value, dict) and value.get("error")):
                remaining_missing.append(name)
            else:
                available_analysts.append(name)

        logger.info("行业分析师子图执行完毕，即将进入行业经理汇总")
        return {
            **merged_state,
            "available_analysts": available_analysts,
            "missing_analysts": remaining_missing,
        }

    return run_analysts_node


__all__ = [
    "create_detect_available_analysts_node",
    "create_load_macro_manager_summary_node",
    "create_run_analysts_node",
    "create_sector_summary_node",
]
