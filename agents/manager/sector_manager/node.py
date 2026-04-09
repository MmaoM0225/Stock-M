"""
Sector Manager（行业管理器）- 节点

run_analysts：并行调用行业趋势与板块资金流两个分析师子图，合并结果到 state。
sector_summary：综合两个分析师结果，输出行业层综合结论。
"""
import json
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from typing import Any, Dict, Iterable, List, Optional, Tuple

from langchain_core.prompts import ChatPromptTemplate
from langchain_core.runnables import RunnableConfig

from ...utils import extract_json_text

logger = logging.getLogger(__name__)

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

    market_regime = str(trend.get("market_regime") or "mixed")
    market_bias = str(flow.get("market_bias") or "neutral")
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

    favored_sectors = _ordered_unique_strings(
        [
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
    sector_summary = "；".join(fragments) if fragments else "未能生成行业综合结论。"

    confidence = 0.4
    if trend.get("summary") and flow.get("summary"):
        confidence = 0.65

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
            return {
                **state,
                "sector_manager_summary": {
                    **_SECTOR_MANAGER_SUMMARY_DEFAULT,
                    **fallback_summary,
                },
            }

        payload = {
            "trade_date": trade_date,
            "sector_trend_insight": state.get("sector_trend_insight"),
            "sector_capital_flow_insight": state.get("sector_capital_flow_insight"),
        }

        system_msg = """你是一位行业轮动经理（Sector Rotation Manager）。

你会收到某一交易日的两个结构化分析结果：
1. 行业趋势分析
2. 板块资金流分析

你的任务：仅基于当日这两份结果，输出一份严格 JSON，总结行业层的综合判断。

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
            return {
                **state,
                "sector_manager_summary": {
                    **_SECTOR_MANAGER_SUMMARY_DEFAULT,
                    **data,
                },
            }
        except Exception as e:
            logger.exception("行业经理汇总失败，回退为规则汇总: %s", e)
            return {
                **state,
                "sector_manager_summary": {
                    **_SECTOR_MANAGER_SUMMARY_DEFAULT,
                    **fallback_summary,
                },
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
            return {
                output_key: {"error": "missing trade_date"}
                for _, _, output_key in analyst_tasks
            }

        logger.info("开始并行运行 %d 个行业分析师子图", len(analyst_tasks))
        invoke_input: Dict[str, Any] = {"trade_date": trade_date}
        out: Dict[str, Any] = {}

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {
                executor.submit(_invoke_one, name, graph, output_key, invoke_input): (
                    name,
                    output_key,
                )
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

        logger.info("行业分析师子图执行完毕，即将进入行业经理汇总")
        return {**state, **out}

    return run_analysts_node


__all__ = [
    "create_run_analysts_node",
    "create_sector_summary_node",
]
