"""
Full Position Decision（满仓长期决策）- 节点

策略定位：
1) 调仓频率低（20~60个交易日），强调基本面质量与稳健性
2) 组合长期维持满仓（目标总仓位 100%）
3) 保持分散，避免单票与单板块过度集中
"""
from __future__ import annotations

import json
import logging
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

from langchain_core.prompts import ChatPromptTemplate
from langchain_core.runnables import RunnableConfig

from ...utils import extract_json_text
from ..portfolio_decision import node as base_node

logger = logging.getLogger(__name__)

_DEFAULT_FULL_POSITION_DECISION_ARTIFACT_ROOT = (
    Path("data") / "artifacts" / "decision" / "full_position_decision" / "portfolio"
)


def create_load_upstream_artifacts_node():
    base_loader = base_node.create_load_upstream_artifacts_node()

    def load_upstream_artifacts_node(state: Dict[str, Any], config: Optional[RunnableConfig] = None) -> Dict[str, Any]:
        proxied_state = dict(state)
        # 复用基础加载逻辑，但优先使用 full_position 决策根目录，避免和普通决策相互污染。
        if not proxied_state.get("portfolio_decision_root"):
            proxied_state["portfolio_decision_root"] = str(
                proxied_state.get("full_position_decision_root") or _DEFAULT_FULL_POSITION_DECISION_ARTIFACT_ROOT
            )
        return base_loader(proxied_state, config=config)

    return load_upstream_artifacts_node


def create_build_decision_context_node():
    return base_node.create_build_decision_context_node()


def create_llm_make_decision_node(llm: Any, stock_manager_graph: Any = None):
    def llm_make_decision_node(state: Dict[str, Any], config: Optional[RunnableConfig] = None) -> Dict[str, Any]:
        context = state.get("decision_context") or {}
        trade_date = base_node._normalize_trade_date(context.get("meta", {}).get("trade_date") or state.get("trade_date"))
        initial_capital = base_node._to_float(context.get("meta", {}).get("initial_capital"), 500000.0)

        stock_pool_result = state.get("stock_pool_manager_result") or {}
        manager_map = base_node._collect_cached_manager_map(stock_pool_result)

        # 补跑已有持仓但未覆盖的 stock_manager 结果
        for row in context.get("portfolio_table") or []:
            ts_code = str(row.get("ts_code") or "").strip()
            if not ts_code or ts_code in manager_map or stock_manager_graph is None:
                continue
            try:
                res = stock_manager_graph.invoke({"ts_code": ts_code, "trade_date": trade_date})
                sm = res.get("stock_manager_summary")
                if isinstance(sm, dict):
                    manager_map[ts_code] = sm
            except Exception:
                continue

        if llm is None:
            decision_result = {
                "trade_date": trade_date,
                "portfolio_table": context.get("portfolio_table") or [],
                "operation_reason_table": [],
                "decision_summary": "LLM调用失败：未提供LLM实例。",
                "meta": {
                    "initial_capital": initial_capital,
                    "total_capital": initial_capital,
                    "source_portfolio_path": state.get("prev_decision_path"),
                    "warnings": [*(state.get("decision_warnings") or []), "llm_error: llm is None"],
                    "generated_at": datetime.now().astimezone().isoformat(),
                },
            }
            return {**state, "decision_result": decision_result}

        # 强制满仓目标：忽略宏观仓位区间，统一按 100%-100%
        context_for_llm = {
            **context,
            "macro_hint": {**(context.get("macro_hint") or {}), "target_position": "100%-100%"},
        }

        system_msg = """你是“满仓长期决策师”。你的任务是给出可执行的长期持仓方案。
硬性约束：
1) 组合总仓位必须为100%（target_weight_pct总和=1.0）；
2) 操作仅允许：建仓/加仓/减仓/清仓/持有；
3) target_weight_pct 必须是 0-1 小数；
4) 单只股票仓位不超过20%；
5) 单一板块仓位不超过已投资金额的30%；
6) 每条操作必须给出理由，且理由要体现“长期持有（约20~60交易日）”与“基本面质量”；
7) 优先选择基本面稳健、现金流健康、估值合理、股东回报稳定的标的，减少高波动题材暴露。

风格要求：
- 这是低频调仓，不做短线交易表达；
- 不要因为短期技术噪声频繁大幅换仓；
- 若当前持仓满足长期质量要求，可优先“持有/小幅优化”，仅替换明显劣质资产。
"""
        human_msg = """输入：
{payload}

请输出 JSON：
{{
  "operations": [
    {{"ts_code":"", "asset_name":"", "operation":"建仓|加仓|减仓|清仓|持有", "target_weight_pct":0.12, "reason":"", "priority":1}}
  ],
  "decision_summary":""
}}

额外要求：
- 所有资产 target_weight_pct 总和必须等于 1.0（允许微小误差±0.01）；
- 组合建议持有 6-12 只股票，优先分散到多个行业；
- 若给出清仓，必须说明该资产不再适合未来20~60交易日持有。"""

        prompt = ChatPromptTemplate.from_messages([("system", system_msg), ("human", human_msg)])
        chain = prompt | llm

        max_retries = 3
        base_delay = 2
        last_exception: Optional[Exception] = None
        operations: List[Dict[str, Any]] = []
        llm_reasoning = ""

        for attempt in range(max_retries):
            try:
                raw = chain.invoke(
                    {"payload": json.dumps({**context_for_llm, "manager_insights": manager_map}, ensure_ascii=False, indent=2)},
                    config={**(config or {}), "run_name": f"满仓长期决策-尝试{attempt + 1}"},
                )
                data = extract_json_text(raw) or {}
                operations = base_node._normalize_operations(data, [])
                if not operations:
                    raise ValueError("LLM返回为空或格式不符合要求（operations为空）")
                llm_reasoning = str(data.get("decision_summary") or "").strip() or "已基于长期基本面与分散化约束生成满仓决策。"
                logger.info("满仓长期决策 LLM 调用成功（尝试 %s/%s）", attempt + 1, max_retries)
                break
            except Exception as e:
                last_exception = e
                if attempt < max_retries - 1:
                    delay = base_delay * (2 ** attempt)
                    logger.warning("满仓长期决策 LLM 调用失败（尝试 %s/%s）: %s，%s秒后重试...", attempt + 1, max_retries, e, delay)
                    time.sleep(delay)
                else:
                    logger.exception("满仓长期决策 LLM 调用失败（已重试%s次）: %s", max_retries, e)

        if not operations:
            decision_result = {
                "trade_date": trade_date,
                "portfolio_table": context.get("portfolio_table") or [],
                "operation_reason_table": [],
                "decision_summary": f"LLM调用失败（重试{max_retries}次后）: {last_exception}",
                "meta": {
                    "initial_capital": initial_capital,
                    "total_capital": initial_capital,
                    "source_portfolio_path": state.get("prev_decision_path"),
                    "warnings": [*(state.get("decision_warnings") or []), f"llm_error: {last_exception}"],
                    "generated_at": datetime.now().astimezone().isoformat(),
                },
            }
            return {**state, "decision_result": decision_result}

        operations = base_node._append_implicit_hold_operations(
            context.get("portfolio_table") or [],
            operations,
            manager_map=manager_map,
        )

        # 满仓模式：固定仓位目标区间为 100%-100%
        target_position_range = (1.0, 1.0)
        new_table, reason_table, total_capital = base_node._apply_operations_to_table(
            trade_date=trade_date,
            initial_capital=initial_capital,
            old_table=context.get("portfolio_table") or [],
            operations=operations,
            target_position_range=target_position_range,
            stock_pool_result=state.get("stock_pool_manager_result"),
        )
        summary = base_node._build_programmatic_decision_summary(
            old_table=context.get("portfolio_table") or [],
            new_table=new_table,
            reason_table=reason_table,
            llm_reasoning=llm_reasoning,
        )

        decision_result = {
            "trade_date": trade_date,
            "portfolio_table": new_table,
            "operation_reason_table": reason_table,
            "decision_summary": summary,
            "meta": {
                "initial_capital": initial_capital,
                "total_capital": total_capital,
                "llm_reasoning": llm_reasoning,
                "source_portfolio_path": state.get("prev_decision_path"),
                "warnings": state.get("decision_warnings") or [],
                "generated_at": datetime.now().astimezone().isoformat(),
            },
        }
        return {**state, "decision_result": decision_result}

    return llm_make_decision_node


def create_persist_portfolio_decision_node():
    base_persist = base_node.create_persist_portfolio_decision_node()

    def persist_decision_node(state: Dict[str, Any], config: Optional[RunnableConfig] = None) -> Dict[str, Any]:
        proxied_state = dict(state)
        if not proxied_state.get("portfolio_decision_root"):
            proxied_state["portfolio_decision_root"] = str(
                proxied_state.get("full_position_decision_root") or _DEFAULT_FULL_POSITION_DECISION_ARTIFACT_ROOT
            )
        return base_persist(proxied_state, config=config)

    return persist_decision_node


__all__ = [
    "create_load_upstream_artifacts_node",
    "create_build_decision_context_node",
    "create_llm_make_decision_node",
    "create_persist_portfolio_decision_node",
]

