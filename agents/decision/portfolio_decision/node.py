"""
Portfolio Decision（组合决策）- 节点

核心流程：
1) 读取最近一期资产组合表（无则视为首次建仓）
2) 复用 stock_pool_manager 的 stock_manager 结果，缺失项再补跑 stock_manager
3) 给出操作（建仓/加仓/减仓/清仓），并按当日收盘价计算目标市值与仓位变化
4) 输出分离的两张表：资产组合表 + 操作原因表
"""
from __future__ import annotations

import json
import logging
import os
import re
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from langchain_core.prompts import ChatPromptTemplate
from langchain_core.runnables import RunnableConfig

from dataflow.kline_data import KLineDataFetcher

from ...utils import extract_json_text

logger = logging.getLogger(__name__)

_SECTOR_MANAGER_ARTIFACT_ROOT = Path("data") / "artifacts" / "manager" / "sector_manager"
_STOCK_POOL_MANAGER_ARTIFACT_ROOT = Path("data") / "artifacts" / "manager" / "stock_pool_manager"
_MACRO_MANAGER_ARTIFACT_ROOT = Path("data") / "artifacts" / "manager" / "macro_manager"

# 默认路径，可通过 state 传入自定义路径
_DEFAULT_PORTFOLIO_DECISION_ARTIFACT_ROOT = Path("data") / "artifacts" / "decision" /  "portfolio"


def _get_artifact_root(state: Dict[str, Any], key: str, default: Path) -> Path:
    """从 state 中获取 artifact 根目录，支持字符串或 Path"""
    val = state.get(key)
    if val:
        return Path(str(val))
    return default


def _write_json_atomic(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_suffix(path.suffix + ".tmp")
    with open(tmp_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
    os.replace(tmp_path, path)


def _load_json_file(path: Path) -> Any:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def _normalize_trade_date(raw: Any) -> str:
    return str(raw or "").replace("-", "").strip()[:8]


def _to_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except Exception:
        return default


def _pct_to_float(value: Any) -> float:
    if value is None:
        return 0.0
    text = str(value).strip().replace("%", "").replace("+", "")
    return _to_float(text, default=0.0) / 100.0


def _parse_target_position_range(target_position: Any) -> Tuple[Optional[float], Optional[float]]:
    """
    解析 macro_hint.target_position（如 "50%-70%"）为小数区间 (0.5, 0.7)。
    """
    if target_position is None:
        return None, None
    text = str(target_position).strip()
    if not text:
        return None, None

    match = re.search(r"(\d+(?:\.\d+)?)\s*%\s*-\s*(\d+(?:\.\d+)?)\s*%", text)
    if match:
        low = max(0.0, min(1.0, _to_float(match.group(1), 0.0) / 100.0))
        high = max(0.0, min(1.0, _to_float(match.group(2), 0.0) / 100.0))
        if low > high:
            low, high = high, low
        return low, high

    single = re.search(r"(\d+(?:\.\d+)?)\s*%", text)
    if single:
        value = max(0.0, min(1.0, _to_float(single.group(1), 0.0) / 100.0))
        return value, value
    return None, None


def _fmt_pct(value: float) -> str:
    return f"{value * 100:.2f}%"


def _fmt_money(value: float) -> str:
    return f"{value:,.2f}"


def _to_int(value: Any, default: int = 0) -> int:
    try:
        if value is None:
            return default
        text = str(value).strip().replace(",", "")
        if text in {"", "-"}:
            return default
        return int(float(text))
    except Exception:
        return default


def _canonical_holding_row(row: Dict[str, Any]) -> Dict[str, Any]:
    shares = _to_int(row.get("持仓股数", row.get("shares")), 0)
    raw_cost = _to_float(row.get("成本价", row.get("cost_price")), 0.0)
    if raw_cost <= 0 and shares > 0:
        raw_cost = _to_float(row.get("市值 (元)", row.get("market_value", 0.0)), 0.0) / shares
    return {
        "排名": row.get("排名", row.get("rank", "-")),
        "资产名称": row.get("资产名称", row.get("asset_name", "")),
        "ts_code": row.get("ts_code"),
        "市值 (元)": _to_float(row.get("市值 (元)", row.get("market_value", 0.0))),
        "仓位": row.get("仓位", row.get("position_weight", "0.00%")),
        "较上期仓位变化": row.get("较上期仓位变化", row.get("position_change", "0.00%")),
        "总收益 (%)": row.get("总收益 (%)", row.get("total_return_pct", "0.00%")),
        "总盈亏 (元)": _to_float(row.get("总盈亏 (元)", row.get("total_pnl", 0.0))),
        "行业/板块": row.get("行业/板块", row.get("sector", "")),
        "资产类型": row.get("资产类型", row.get("asset_type", "")),
        "核心特征/备注": row.get("核心特征/备注", row.get("notes", "")),
        "持仓股数": row.get("持仓股数", row.get("shares")),
        "成本价": round(raw_cost, 4) if raw_cost > 0 else None,
        "操作": row.get("操作", "-"),
        # 向后兼容：优先读取新字段“收盘价”，旧数据中可能仍是“开盘价”
        "收盘价": row.get("收盘价", row.get("开盘价")),
    }


def _load_upstream_payload(
    state: Dict[str, Any],
    *,
    state_key: str,
    path_key: str,
    default_path: Path,
) -> Tuple[Any, Optional[str], Optional[str]]:
    existing = state.get(state_key)
    if existing:
        return existing, "<state>", None
    path_text = state.get(path_key)
    path = Path(str(path_text)) if path_text else default_path
    if not path.exists():
        return None, path.as_posix(), f"未找到上游产物: {path.as_posix()}"
    try:
        return _load_json_file(path), path.as_posix(), None
    except Exception as e:
        return None, path.as_posix(), f"读取失败: {path.as_posix()} ({e})"


def _find_latest_portfolio_book(trade_date: str, decision_root: Optional[Path] = None) -> Optional[Path]:
    """查找指定日期之前（不含当日）的最新投资组合持仓记录。

    注意：此方法会严格排除 trade_date 当天，只返回之前日期的结果。
    如需获取当天结果，请直接构造路径检查。
    """
    root = decision_root or _DEFAULT_PORTFOLIO_DECISION_ARTIFACT_ROOT
    if not root.exists():
        return None
    dates: List[str] = []
    for p in root.iterdir():
        # 严格小于当前日期（不含等于），确保重新跑时基于真实上期持仓
        if p.is_dir() and p.name.isdigit() and len(p.name) == 8 and p.name < trade_date:
            if (p / "result.json").exists():
                dates.append(p.name)
    if not dates:
        return None
    latest = sorted(dates)[-1]
    return root / latest / "result.json"


def _extract_portfolio_table(data: Dict[str, Any]) -> List[Dict[str, Any]]:
    if not isinstance(data, dict):
        return []
    table = data.get("portfolio_table") or data.get("assets") or data.get("holdings") or []
    out: List[Dict[str, Any]] = []
    for row in table:
        if isinstance(row, dict):
            out.append(_canonical_holding_row(row))
    return out


def create_load_upstream_artifacts_node():
    def load_upstream_artifacts_node(state: Dict[str, Any], config: Optional[RunnableConfig] = None) -> Dict[str, Any]:
        _ = config
        trade_date = _normalize_trade_date(state.get("trade_date") or datetime.now().strftime("%Y%m%d"))
        warnings: List[str] = []

        sector_result, sector_path, sector_err = _load_upstream_payload(
            state,
            state_key="sector_manager_result",
            path_key="sector_manager_result_path",
            default_path=_SECTOR_MANAGER_ARTIFACT_ROOT / trade_date / "result.json",
        )
        stock_pool_result, stock_pool_path, stock_pool_err = _load_upstream_payload(
            state,
            state_key="stock_pool_manager_result",
            path_key="stock_pool_manager_result_path",
            default_path=_STOCK_POOL_MANAGER_ARTIFACT_ROOT / trade_date / "result.json",
        )
        macro_summary, macro_path, macro_err = _load_upstream_payload(
            state,
            state_key="macro_manager_summary",
            path_key="macro_manager_result_path",
            default_path=_MACRO_MANAGER_ARTIFACT_ROOT / trade_date / "result.json",
        )
        for err in [sector_err, stock_pool_err, macro_err]:
            if err:
                warnings.append(err)

        # 从 portfolio_decision 读取上期持仓
        prev_decision_path = None
        if state.get("prev_decision_path"):
            prev_decision_path = Path(str(state.get("prev_decision_path")))
        else:
            decision_root = _get_artifact_root(state, "portfolio_decision_root", _DEFAULT_PORTFOLIO_DECISION_ARTIFACT_ROOT)
            prev_decision_path = _find_latest_portfolio_book(trade_date, decision_root)
        portfolio_table: List[Dict[str, Any]] = []
        if prev_decision_path and prev_decision_path.exists():
            try:
                portfolio_table = _extract_portfolio_table(_load_json_file(prev_decision_path))
            except Exception as e:
                warnings.append(f"读取资产组合表失败: {prev_decision_path.as_posix()} ({e})")
        initial_capital = _to_float(state.get("initial_capital"), 500000.0)
        if initial_capital <= 0:
            initial_capital = 500000.0
        return {
            **state,
            "trade_date": trade_date,
            "sector_manager_result": sector_result or {},
            "stock_pool_manager_result": stock_pool_result or {},
            "macro_manager_summary": macro_summary or {},
            "portfolio_table": portfolio_table,
            "prev_decision_path": prev_decision_path.as_posix() if prev_decision_path else None,
            "initial_capital": initial_capital,
            "upstream_artifact_paths": {
                "sector_manager_result_path": sector_path,
                "stock_pool_manager_result_path": stock_pool_path,
                "macro_manager_result_path": macro_path,
                "prev_decision_path": prev_decision_path.as_posix() if prev_decision_path else None,
            },
            "decision_warnings": warnings,
        }

    return load_upstream_artifacts_node


def create_build_decision_context_node():
    def build_decision_context_node(state: Dict[str, Any], config: Optional[RunnableConfig] = None) -> Dict[str, Any]:
        _ = config
        trade_date = _normalize_trade_date(state.get("trade_date"))
        stock_pool_result = state.get("stock_pool_manager_result") or {}
        candidate_stocks = list(stock_pool_result.get("top_stocks") or stock_pool_result.get("candidate_stocks") or [])
        portfolio_table = list(state.get("portfolio_table") or [])

        cash_row = next((r for r in portfolio_table if str(r.get("资产名称")) == "待投资现金"), None)
        holding_rows = [r for r in portfolio_table if str(r.get("资产名称")) != "待投资现金"]

        total_capital = _to_float(state.get("initial_capital"), 500000.0)
        if portfolio_table:
            current_total = sum(_to_float(r.get("市值 (元)"), 0.0) for r in portfolio_table)
            if current_total > 0:
                total_capital = current_total
        available_cash = _to_float(cash_row.get("市值 (元)") if cash_row else 0.0, max(total_capital, 0.0))
        if not portfolio_table:
            available_cash = total_capital

        decision_context = {
            "meta": {"trade_date": trade_date, "initial_capital": round(total_capital, 2)},
            "market_context": state.get("sector_manager_result") or {},
            "macro_hint": state.get("macro_manager_summary") or {},
            "portfolio_status": {
                "is_first_build": len(holding_rows) == 0,
                "holding_count": len(holding_rows),
                "available_cash": round(available_cash, 2),
            },
            "portfolio_table": portfolio_table,
            "stock_candidates": candidate_stocks[:20],
            "lineage": state.get("upstream_artifact_paths") or {},
        }
        return {**state, "decision_context": decision_context}

    return build_decision_context_node


def _collect_cached_manager_map(stock_pool_result: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    per_stock = stock_pool_result.get("per_stock") or []
    out: Dict[str, Dict[str, Any]] = {}
    for row in per_stock:
        if not isinstance(row, dict):
            continue
        ts_code = str(row.get("ts_code") or "").strip()
        sm = row.get("stock_manager_summary")
        if ts_code and isinstance(sm, dict):
            out[ts_code] = sm
    return out


def _fetch_close_price(ts_code: str, trade_date: str) -> Optional[float]:
    if not ts_code:
        return None
    try:
        fetcher = KLineDataFetcher()
        df = fetcher.fetch_daily_data(ts_code=ts_code, start_date=trade_date, end_date=trade_date, adj=None)
        if df is None or df.empty or "close" not in df.columns:
            return None
        return _to_float(df.iloc[-1]["close"], 0.0)
    except Exception:
        return None


def _normalize_operations(data: Dict[str, Any], fallback_ops: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    raw = data.get("operations")
    if not isinstance(raw, list):
        return fallback_ops
    out: List[Dict[str, Any]] = []
    for i, row in enumerate(raw, start=1):
        if not isinstance(row, dict):
            continue
        op = str(row.get("operation") or "持有")
        if op not in {"建仓", "加仓", "减仓", "清仓", "持有"}:
            op = "持有"
        out.append(
            {
                "ts_code": row.get("ts_code"),
                "asset_name": row.get("asset_name"),
                "operation": op,
                "target_weight_pct": max(0.0, min(1.0, _to_float(row.get("target_weight_pct"), 0.0))),
                "reason": str(row.get("reason") or "").strip() or "未提供原因",
                "priority": int(_to_float(row.get("priority"), i)),
            }
        )
    return out or fallback_ops


def _append_implicit_hold_operations(
    old_table: List[Dict[str, Any]],
    operations: List[Dict[str, Any]],
    manager_map: Optional[Dict[str, Dict[str, Any]]] = None,
) -> List[Dict[str, Any]]:
    """
    对旧持仓做兜底：若某资产未出现在本期操作中，自动补一条“持有”。

    这样可以避免“未给操作=凭空消失”的问题，保证组合账本可追溯。
    若存在 stock_manager 结论，优先使用其信号原因作为“持有”原因。
    """
    manager_map = manager_map or {}
    existing_keys = set()
    max_priority = 0
    for op in operations:
        key = str(op.get("ts_code") or op.get("asset_name") or "").strip()
        if key:
            existing_keys.add(key)
        max_priority = max(max_priority, int(_to_float(op.get("priority"), 0)))

    appended: List[Dict[str, Any]] = []
    next_priority = max_priority + 1
    for row in old_table:
        if str(row.get("资产名称")) == "待投资现金":
            continue
        ts_code = str(row.get("ts_code") or "").strip()
        asset_name = str(row.get("资产名称") or "").strip()
        key = ts_code or asset_name
        if not key or key in existing_keys:
            continue
        manager_summary = manager_map.get(ts_code) if ts_code else None
        hold_reason = "本期未给出该持仓操作，系统自动按持有处理"
        if isinstance(manager_summary, dict):
            hold_reason = (
                str(manager_summary.get("signal_reason") or "").strip()
                or str(manager_summary.get("selection_reason") or "").strip()
                or hold_reason
            )
        appended.append(
            {
                "ts_code": ts_code or None,
                "asset_name": asset_name,
                "operation": "持有",
                "target_weight_pct": _pct_to_float(row.get("仓位")),
                "reason": hold_reason,
                "priority": next_priority,
            }
        )
        existing_keys.add(key)
        next_priority += 1

    return operations + appended


def _build_programmatic_decision_summary(
    old_table: List[Dict[str, Any]],
    new_table: List[Dict[str, Any]],
    reason_table: List[Dict[str, Any]],
    llm_reasoning: str,
) -> str:
    """
    用程序生成带数字的决策摘要，避免 LLM 文案中的数字偏差。
    LLM 仅作为“原因描述补充”，不参与关键数字计算。
    """

    def _position_pct(table: List[Dict[str, Any]]) -> float:
        cash_weight = 0.0
        for row in table:
            if str(row.get("资产名称")) == "待投资现金":
                cash_weight = _pct_to_float(row.get("仓位"))
                break
        return max(0.0, (1.0 - cash_weight) * 100.0)

    old_pos = _position_pct(old_table)
    new_pos = _position_pct(new_table)

    old_stock_cnt = sum(1 for r in old_table if str(r.get("资产名称")) != "待投资现金" and _to_float(r.get("市值 (元)"), 0.0) > 0)
    new_stock_cnt = sum(1 for r in new_table if str(r.get("资产名称")) != "待投资现金" and _to_float(r.get("市值 (元)"), 0.0) > 0)

    op_counter: Dict[str, int] = {"建仓": 0, "加仓": 0, "减仓": 0, "清仓": 0, "持有": 0}
    for row in reason_table:
        op = str(row.get("操作") or "")
        if op in op_counter:
            op_counter[op] += 1

    op_parts = [f"{k}{v}只" for k, v in op_counter.items() if v > 0]
    op_text = "，".join(op_parts) if op_parts else "无有效调仓动作"

    summary = (
        f"本次调仓后总仓位由 {old_pos:.2f}% 变为 {new_pos:.2f}%，"
        f"持仓股票数量由 {old_stock_cnt} 只变为 {new_stock_cnt} 只。"
        f"操作统计：{op_text}。"
    )

    llm_reasoning = (llm_reasoning or "").strip()
    if llm_reasoning:
        summary += f" 策略原因：{llm_reasoning}"
    return summary


def _apply_operations_to_table(
    trade_date: str,
    initial_capital: float,
    old_table: List[Dict[str, Any]],
    operations: List[Dict[str, Any]],
    target_position_range: Optional[Tuple[Optional[float], Optional[float]]] = None,
    stock_pool_result: Optional[Dict[str, Any]] = None,
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], float]:
    # 构建行业映射表：从stock_pool_result中获取每只股票的industry
    industry_map: Dict[str, str] = {}
    if stock_pool_result:
        # 从per_stock中获取行业信息
        per_stock = stock_pool_result.get("per_stock") or []
        for row in per_stock:
            if isinstance(row, dict):
                ts_code = str(row.get("ts_code") or "").strip()
                industry = row.get("industry") or ""
                if ts_code and industry:
                    industry_map[ts_code] = industry
        # 如果per_stock中没有，从candidate_stocks中补充
        if not industry_map:
            candidates = stock_pool_result.get("candidate_stocks") or []
            for row in candidates:
                if isinstance(row, dict):
                    ts_code = str(row.get("ts_code") or "").strip()
                    industry = row.get("industry") or ""
                    if ts_code and industry:
                        industry_map[ts_code] = industry
    old_map: Dict[str, Dict[str, Any]] = {}
    previous_cash = 0.0
    for row in old_table:
        if str(row.get("资产名称")) == "待投资现金":
            previous_cash = _to_float(row.get("市值 (元)"), 0.0)
            continue
        key = str(row.get("ts_code") or row.get("资产名称") or "")
        if key:
            old_map[key] = row

    # 计算上日总资产：持仓按当日收盘价重新估值 + 现金
    previous_holding_value = 0.0
    for row in old_table:
        if str(row.get("资产名称")) == "待投资现金":
            continue
        ts_code = str(row.get("ts_code") or "").strip()
        old_shares = _to_int(row.get("持仓股数"), 0)
        if ts_code and old_shares > 0:
            close_price = _fetch_close_price(ts_code, trade_date)
            if close_price is not None and close_price > 0:
                previous_holding_value += old_shares * close_price
            else:
                # 无法获取收盘价时，沿用旧市值
                previous_holding_value += _to_float(row.get("市值 (元)"), 0.0)
        else:
            previous_holding_value += _to_float(row.get("市值 (元)"), 0.0)

    total_capital = previous_holding_value + previous_cash
    if total_capital <= 0:
        total_capital = initial_capital

    op_reason_rows: List[Dict[str, Any]] = []
    result_rows: List[Dict[str, Any]] = []
    invested = 0.0
    rank = 1

    # 先计算目标仓位并做归一化，保证“仓位=资产占总资产比例”。
    normalized_weights: Dict[int, float] = {}
    active_idx: List[int] = []
    raw_weight_sum = 0.0
    per_stock_max_weight = 0.20
    for idx, op in enumerate(operations):
        operation = str(op.get("operation") or "持有")
        row_weight = _to_float(op.get("target_weight_pct"), 0.0)
        # 兼容 LLM 返回百分制（如 15/30）或小数制（如 0.15/0.3）
        if row_weight > 1.0:
            row_weight = row_weight / 100.0
        row_weight = max(0.0, row_weight)
        if operation == "清仓":
            row_weight = 0.0
        # 持有操作：如果没有指定目标仓位，保持原仓位（避免误清仓）
        if operation == "持有" and row_weight == 0.0:
            # 查找该资产的原仓位
            asset_key = str(op.get("ts_code") or op.get("asset_name") or "").strip()
            for old_row in old_table:
                old_key = str(old_row.get("ts_code") or old_row.get("资产名称") or "").strip()
                if old_key and old_key == asset_key:
                    old_weight_pct = str(old_row.get("仓位", "0.00%")).replace("%", "")
                    row_weight = max(0.0, _to_float(old_weight_pct, 0.0) / 100.0)
                    break
        # 程序级硬约束：单只股票仓位不超过 20%
        row_weight = min(row_weight, per_stock_max_weight)
        normalized_weights[idx] = row_weight
        if row_weight > 0:
            active_idx.append(idx)
            raw_weight_sum += row_weight

    # 程序级硬约束：组合总仓位限制在 target_position 区间（若提供）
    # 规则：先按 LLM 给出的总权重计算，再钳制到 [min_target, max_target]。
    # 不会默认拉满到上限，避免“总是冲到目标区间上沿”。
    min_target = None
    max_target = 1.0
    if target_position_range is not None:
        min_target, max_target = target_position_range
        if max_target is None:
            max_target = 1.0
    target_total = raw_weight_sum
    if max_target is not None and target_total > max_target:
        target_total = max_target
    if min_target is not None and target_total < min_target:
        # 组合仓位低于下限时尝试提升到下限（若后续受单票上限约束达不到，则会停留在可达值）
        target_total = min_target
    if raw_weight_sum > 0 and active_idx:
        target_total = max(0.0, min(1.0, target_total))
        scale = target_total / raw_weight_sum
        for idx in active_idx:
            normalized_weights[idx] = min(normalized_weights[idx] * scale, per_stock_max_weight)

        # 二次分配：若因单票20%上限导致总仓位不足目标，按剩余容量继续补齐
        deficit = target_total - sum(normalized_weights[i] for i in active_idx)
        if deficit > 1e-6:
            expandable = [i for i in active_idx if normalized_weights[i] < per_stock_max_weight - 1e-9]
            while deficit > 1e-6 and expandable:
                per_add = deficit / len(expandable)
                new_expandable: List[int] = []
                for i in expandable:
                    room = per_stock_max_weight - normalized_weights[i]
                    add = min(room, per_add)
                    normalized_weights[i] += add
                    deficit -= add
                    if normalized_weights[i] < per_stock_max_weight - 1e-9:
                        new_expandable.append(i)
                expandable = new_expandable

        # 程序级硬约束：单板块仓位上限按“已投资金额”的30%计算（不是总资产）。
        # 例如总仓位 target_total=60%，则单板块上限=0.6*30%=18%（占总资产口径）。
        sector_cap_on_invested = 0.30
        sector_cap_on_total = target_total * sector_cap_on_invested

        def _get_sector_for_weight_idx(weight_idx: int) -> str:
            one_op = operations[weight_idx] if 0 <= weight_idx < len(operations) else {}
            ts = str(one_op.get("ts_code") or "").strip()
            name = str(one_op.get("asset_name") or "").strip()
            if ts and industry_map.get(ts):
                return str(industry_map.get(ts))
            old_key = ts or name
            old_row = old_map.get(old_key, {})
            sector = str(old_row.get("行业/板块") or "").strip()
            return sector or "未知"

        sector_weights: Dict[str, float] = {}
        sector_to_idx: Dict[str, List[int]] = {}
        for idx in active_idx:
            w = normalized_weights.get(idx, 0.0)
            if w <= 0:
                continue
            sec = _get_sector_for_weight_idx(idx)
            sector_weights[sec] = sector_weights.get(sec, 0.0) + w
            sector_to_idx.setdefault(sec, []).append(idx)

        overweight_sectors = [s for s, w in sector_weights.items() if w > sector_cap_on_total + 1e-9]
        if overweight_sectors:
            for sec in overweight_sectors:
                sec_total = sector_weights.get(sec, 0.0)
                if sec_total <= sector_cap_on_total + 1e-9:
                    continue
                cut_ratio = sector_cap_on_total / sec_total if sec_total > 0 else 1.0
                for idx in sector_to_idx.get(sec, []):
                    normalized_weights[idx] *= cut_ratio

            # 将超限削减出来的权重，优先分配给未超限板块的股票（仍受单票20%约束）
            current_total = sum(normalized_weights[i] for i in active_idx)
            deficit = max(0.0, target_total - current_total)
            if deficit > 1e-6:
                for _ in range(3):
                    if deficit <= 1e-6:
                        break
                    sector_weights = {}
                    for idx in active_idx:
                        w = normalized_weights.get(idx, 0.0)
                        if w <= 0:
                            continue
                        sec = _get_sector_for_weight_idx(idx)
                        sector_weights[sec] = sector_weights.get(sec, 0.0) + w
                    expandable = []
                    for idx in active_idx:
                        sec = _get_sector_for_weight_idx(idx)
                        if sector_weights.get(sec, 0.0) >= sector_cap_on_total - 1e-9:
                            continue
                        if normalized_weights[idx] < per_stock_max_weight - 1e-9:
                            expandable.append(idx)
                    if not expandable:
                        break
                    per_add = deficit / len(expandable)
                    progressed = 0.0
                    for idx in expandable:
                        sec = _get_sector_for_weight_idx(idx)
                        sector_room = max(0.0, sector_cap_on_total - sector_weights.get(sec, 0.0))
                        stock_room = max(0.0, per_stock_max_weight - normalized_weights[idx])
                        add = min(per_add, sector_room, stock_room, deficit)
                        if add <= 0:
                            continue
                        normalized_weights[idx] += add
                        sector_weights[sec] = sector_weights.get(sec, 0.0) + add
                        deficit -= add
                        progressed += add
                    if progressed <= 1e-9:
                        break

    for op_idx, op in sorted(enumerate(operations), key=lambda x: int(x[1].get("priority") or 9999)):
        ts_code = str(op.get("ts_code") or "").strip()
        name = str(op.get("asset_name") or "").strip()
        key = ts_code or name
        if not key:
            continue
        old = old_map.get(key, {})
        old_w = _pct_to_float(old.get("仓位"))
        old_shares = _to_int(old.get("持仓股数"), 0)
        old_cost = _to_float(old.get("成本价"), 0.0)
        if old_cost <= 0 and old_shares > 0:
            old_cost = _to_float(old.get("市值 (元)"), 0.0) / max(old_shares, 1)
        if old_cost <= 0:
            old_cost = _to_float(old.get("收盘价", old.get("开盘价")), 0.0)
        new_w = normalized_weights.get(op_idx, _to_float(op.get("target_weight_pct"), old_w))
        operation = str(op.get("operation") or "持有")
        auto_rebalance_reason: Optional[str] = None
        if operation == "清仓":
            new_w = 0.0
        close_price = _fetch_close_price(ts_code, trade_date) if ts_code else None
        target_budget = round(total_capital * new_w, 2)
        amount = 0.0
        shares: Any = 0
        actual_w = 0.0
        cost_price: Optional[float] = None

        # 若仓位约束要求调仓，则把“持有”自动转成“加仓/减仓”执行。
        # 这样可以真正落实总仓位区间，而不是文案层面约束。
        if operation == "持有" and ts_code:
            eps = 1e-4
            if new_w > old_w + eps:
                operation = "加仓"
                auto_rebalance_reason = "为满足目标仓位区间，系统将持有自动调整为加仓"
            elif new_w < old_w - eps:
                operation = "减仓"
                auto_rebalance_reason = "为满足目标仓位区间，系统将持有自动调整为减仓"

        # A股按整手（100股）计算可买股数；再用可成交总价回算真实仓位。
        # 逻辑：四舍五入到最接近目标金额的整手数，让实际成交金额更接近目标金额
        if operation != "清仓" and close_price is not None and close_price > 0 and ts_code:
            lot_size = 100

            if operation == "持有":
                # 持有：保持原有股数，只按当日收盘价重新估值
                shares = old_shares if old_shares > 0 else 0
                amount = round(shares * close_price, 2) if shares > 0 else 0.0
                actual_w = (amount / total_capital) if total_capital > 0 else 0.0
                cost_price = old_cost if old_cost > 0 else close_price
            else:
                # 建仓/加仓/减仓：根据目标金额计算股数
                # 若当前仅 1 手持仓且给出“减仓”，按业务规则直接清仓
                if operation == "减仓" and old_shares <= lot_size:
                    shares = 0
                    amount = 0.0
                    actual_w = 0.0
                    operation = "清仓"
                    cost_price = old_cost if old_cost > 0 else close_price
                    auto_rebalance_reason = (
                        f"{auto_rebalance_reason}；当前仅1手持仓，减仓自动转为清仓"
                        if auto_rebalance_reason
                        else "当前仅1手持仓，减仓自动转为清仓"
                    )
                    # 该分支已完成处理，跳过后续按目标金额计算
                    tradable_shares = 0
                else:
                    # 计算目标金额能买多少手（向上取整，确保买够目标仓位）
                    target_lots = target_budget / (close_price * lot_size)
                    # 向上取整到整手数（ceiling），确保买够
                    lots = int(target_lots) if target_lots == int(target_lots) else int(target_lots) + 1
                    # 建仓/加仓时：确保至少1手（如果目标金额够买至少1手）
                    if lots <= 0 and target_budget >= close_price * lot_size:
                        lots = 1
                    # 减仓时：如果目标仓位为0或无法计算，清仓
                    if operation == "减仓" and lots <= 0:
                        lots = 0
                    tradable_shares = lots * lot_size

                    # 处理减仓：如果新计算股数小于原持仓，则为减仓
                    if operation == "减仓" and old_shares > 0 and tradable_shares < old_shares:
                        # 判断是否为极小幅减仓：如果剩余股数<=1手或减仓幅度<5%，转为清仓
                        min_hold_lots = 1  # 最少保留1手（100股），否则直接清仓
                        reduced_lots = (old_shares - tradable_shares) / lot_size
                        old_lots = old_shares / lot_size
                        # 如果减仓后剩余不足1手，直接清仓；
                        # 如果减仓比例<5%，则视为微调噪声，不执行调整（保持原仓位）。
                        # 注意：减1手允许执行，不再触发自动清仓。
                        if tradable_shares < min_hold_lots * lot_size:
                            shares = 0
                            amount = 0.0
                            actual_w = 0.0
                            operation = "清仓"  # 转为清仓操作
                            cost_price = None
                        elif (reduced_lots / old_lots < 0.05):
                            shares = old_shares
                            amount = round(shares * close_price, 2)
                            actual_w = (amount / total_capital) if total_capital > 0 else 0.0
                            operation = "持有"
                            cost_price = old_cost if old_cost > 0 else close_price
                            auto_rebalance_reason = (
                                f"{auto_rebalance_reason}；减仓幅度小于5%，系统判定为微调噪声并保持原仓位不调整"
                                if auto_rebalance_reason
                                else "减仓幅度小于5%，系统判定为微调噪声并保持原仓位不调整"
                            )
                        else:
                            shares = tradable_shares
                            amount = round(shares * close_price, 2)
                            actual_w = (amount / total_capital) if total_capital > 0 else 0.0
                            # 减仓：按剩余仓位重新计算加权持仓成本（与加仓逻辑一致）
                            reduce_shares = old_shares - shares
                            base_cost = old_cost if old_cost > 0 else close_price
                            # 加权平均：原成本按新旧股数比例重新分配
                            cost_price = ((old_shares * base_cost) - (reduce_shares * close_price)) / max(shares, 1)
                    elif operation == "加仓" and old_shares > 0 and tradable_shares > old_shares:
                        # 加仓：按旧仓+新增仓做加权成本
                        shares = tradable_shares
                        amount = round(shares * close_price, 2)
                        actual_w = (amount / total_capital) if total_capital > 0 else 0.0
                        add_shares = shares - old_shares
                        base_cost = old_cost if old_cost > 0 else close_price
                        cost_price = ((old_shares * base_cost) + (add_shares * close_price)) / max(shares, 1)
                    else:
                        # 建仓或其他情况
                        shares = tradable_shares
                        amount = round(tradable_shares * close_price, 2)
                        actual_w = (amount / total_capital) if total_capital > 0 else 0.0
                        if old_shares <= 0 or operation == "建仓":
                            cost_price = close_price
                        else:
                            cost_price = old_cost if old_cost > 0 else close_price
        else:
            shares = 0 if ts_code else "-"
            amount = 0.0
            actual_w = 0.0
            cost_price = None

        if operation == "持有" and close_price is None:
            # 无收盘价时，持有场景保留原仓位和市值，避免无谓归零。
            amount = _to_float(old.get("市值 (元)"), 0.0)
            actual_w = old_w
            shares = old.get("持仓股数", "-")
            cost_price = old_cost if old_cost > 0 else None

        if operation == "清仓":
            shares = 0 if ts_code else "-"
            cost_price = None

        # 收益计算（持仓维度，按“当日收盘价 vs 成本价”估值）
        if isinstance(shares, int) and shares > 0 and close_price is not None and cost_price is not None:
            total_pnl = round((close_price - cost_price) * shares, 2)
            # 成本为正时：使用标准收益率公式
            # 成本为负时：表示已实现盈利覆盖成本，收益率使用特殊计算
            if cost_price > 0:
                total_ret = (close_price / cost_price - 1.0)
            else:
                # 负成本时，市值本身就是盈利，收益率 = (收盘价 - 成本) / |成本|
                total_ret = (close_price - cost_price) / abs(cost_price)
        else:
            total_pnl = 0.0
            total_ret = 0.0

        invested += amount

        # 优先从stock_pool_result获取行业信息，其次使用旧持仓的行业，最后兜底为"-"
        industry = industry_map.get(ts_code, "") or old.get("行业/板块") or "-"
        asset_type = old.get("资产类型") or ("个股" if ts_code else "其他")
        notes = old.get("核心特征/备注") or "-"
        if amount > 0:
            result_rows.append(
                {
                    "排名": rank,
                    "资产名称": name or old.get("资产名称") or ts_code,
                    "ts_code": ts_code or old.get("ts_code"),
                    "市值 (元)": amount,
                    "仓位": _fmt_pct(actual_w),
                    "较上期仓位变化": _fmt_pct(actual_w - old_w),
                    "总收益 (%)": old.get("总收益 (%)", "0.00%"),
                    "总盈亏 (元)": total_pnl,
                    "行业/板块": industry,
                    "资产类型": asset_type,
                    "核心特征/备注": notes,
                    "持仓股数": shares,
                    "成本价": round(cost_price, 4) if isinstance(cost_price, (int, float)) and cost_price != 0 else None,
                    "总收益 (%)": _fmt_pct(total_ret),
                    "操作": operation,
                    "收盘价": close_price,
                }
            )
            rank += 1
        op_reason_rows.append(
            {
                "资产名称": name or old.get("资产名称") or ts_code,
                "ts_code": ts_code or old.get("ts_code"),
                "行业/板块": industry,
                "操作": operation,
                "原仓位": _fmt_pct(old_w),
                "新仓位": _fmt_pct(actual_w),
                "仓位变化": _fmt_pct(actual_w - old_w),
                "执行价格(收盘价)": close_price,
                "目标金额(元)": target_budget,
                "实际成交金额(元)": amount,
                "成交股数": shares,
                "成本价": round(cost_price, 4) if isinstance(cost_price, (int, float)) and cost_price != 0 else None,
                "操作原因": (op.get("reason") or "") + (f"；{auto_rebalance_reason}" if auto_rebalance_reason else ""),
            }
        )

    # 按仓位大小对股票进行排序（降序），并重新分配排名
    def _parse_position_weight(row: Dict[str, Any]) -> float:
        """解析仓位百分比字符串为浮点数"""
        weight_str = str(row.get("仓位", "0.00%")).replace("%", "").strip()
        try:
            return float(weight_str) / 100.0
        except (ValueError, TypeError):
            return 0.0

    result_rows.sort(key=_parse_position_weight, reverse=True)

    # 重新分配排名
    for idx, row in enumerate(result_rows, start=1):
        row["排名"] = idx

    cash_amt = round(max(total_capital - invested, 0.0), 2)
    result_rows.append(
        {
            "排名": "-",
            "资产名称": "待投资现金",
            "ts_code": None,
            "市值 (元)": cash_amt,
            "仓位": _fmt_pct(cash_amt / total_capital if total_capital > 0 else 0.0),
            "较上期仓位变化": _fmt_pct((cash_amt - previous_cash) / total_capital if total_capital > 0 else 0.0),
            "总收益 (%)": "0.00%",
            "总盈亏 (元)": 0.0,
            "行业/板块": "现金",
            "资产类型": "其他",
            "核心特征/备注": "-",
            "持仓股数": "-",
            "成本价": None,
            "操作": "-",
            "收盘价": None,
        }
    )
    return result_rows, op_reason_rows, round(total_capital, 2)


def create_llm_make_decision_node(llm: Any, stock_manager_graph: Any = None):
    def llm_make_decision_node(state: Dict[str, Any], config: Optional[RunnableConfig] = None) -> Dict[str, Any]:
        context = state.get("decision_context") or {}
        trade_date = _normalize_trade_date(context.get("meta", {}).get("trade_date") or state.get("trade_date"))
        initial_capital = _to_float(context.get("meta", {}).get("initial_capital"), 500000.0)

        stock_pool_result = state.get("stock_pool_manager_result") or {}
        manager_map = _collect_cached_manager_map(stock_pool_result)

        # 补跑：组合中已有 ts_code 且 stock_pool 没覆盖时，用 stock_manager 补分析
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
                    "warnings": [
                        *(state.get("decision_warnings") or []),
                        "llm_error: llm is None",
                    ],
                    "generated_at": datetime.now().astimezone().isoformat(),
                },
            }
            return {**state, "decision_result": decision_result}
        else:
            system_msg = """你是组合决策官。目标：生成可执行的调仓动作。
约束：
1) 操作仅允许：建仓/加仓/减仓/清仓/持有；注意：若减仓后剩余持仓不足1手（100股）或减仓幅度小于5%，系统将自动转为清仓处理；
2) 若是首次建仓（is_first_build=true），优先给建仓；
3) 每条操作必须给出原因；
4) 输出严格JSON；
5) 必须参考 macro_hint.target_position 给出的仓位区间建议（如"20%-40%"），将总仓位控制在建议区间内；
6) target_weight_pct 为单个资产的目标仓位比例（0-1之间的小数，如0.12表示12%）；
7) 单只股票最大仓位不超过20%（0.20），首次建仓建议5%-15%；
8) 保持仓位多样性：避免过度集中于少数股票，建议持有3-8只不同股票分散风险，避免单一板块过度集中（单板块不超过已投资金额的30%）。"""
            human_msg = """输入：
{payload}

请输出 JSON：
{{
  "operations": [
    {{"ts_code":"", "asset_name":"", "operation":"建仓|加仓|减仓|清仓|持有", "target_weight_pct":0.12, "reason":"", "priority":1}}
  ],
  "decision_summary":""
}}

注意：
- 请严格遵循 macro_hint.target_position 的仓位区间建议
- 所有资产的 target_weight_pct 总和应在建议区间内
- 首次建仓时分散配置，单只仓位建议 5%至20%（不超过20%上限）
- 严格遵守单只股票最大仓位20%的限制
- 仓位多样性：建议持有5-10只股票，避免单只股票或单一板块过度集中（单板块不超过已投资金额的30%），通过分散投资降低组合风险"""
            prompt = ChatPromptTemplate.from_messages([("system", system_msg), ("human", human_msg)])
            chain = prompt | llm

            # 带指数退避的LLM调用重试机制
            max_retries = 3
            base_delay = 2  # 初始延迟2秒
            last_exception: Optional[Exception] = None
            data: Dict[str, Any] = {}
            operations: List[Dict[str, Any]] = []
            llm_reasoning = ""

            for attempt in range(max_retries):
                try:
                    raw = chain.invoke(
                        {"payload": json.dumps({**context, "manager_insights": manager_map}, ensure_ascii=False, indent=2)},
                        config={**(config or {}), "run_name": f"组合决策重构版-尝试{attempt + 1}"},
                    )
                    data = extract_json_text(raw) or {}
                    operations = _normalize_operations(data, [])
                    if not operations:
                        raise ValueError("LLM返回为空或格式不符合要求（operations为空）")
                    llm_reasoning = str(data.get("decision_summary") or "").strip() or "已基于资产组合与候选池生成操作。"
                    logger.info(f"组合决策 LLM 调用成功（尝试 {attempt + 1}/{max_retries}）")
                    break  # 成功则退出重试循环
                except Exception as e:
                    last_exception = e
                    if attempt < max_retries - 1:
                        delay = base_delay * (2 ** attempt)  # 指数退避：2, 4, 8秒
                        logger.warning(f"组合决策 LLM 调用失败（尝试 {attempt + 1}/{max_retries}）: {e}，{delay}秒后重试...")
                        time.sleep(delay)
                    else:
                        logger.exception(f"组合决策 LLM 调用失败（已重试{max_retries}次）: {e}")

            if not operations:
                # 所有重试都失败，返回错误结果
                decision_result = {
                    "trade_date": trade_date,
                    "portfolio_table": context.get("portfolio_table") or [],
                    "operation_reason_table": [],
                    "decision_summary": f"LLM调用失败（重试{max_retries}次后）: {last_exception}",
                    "meta": {
                        "initial_capital": initial_capital,
                        "total_capital": initial_capital,
                        "source_portfolio_path": state.get("prev_decision_path"),
                        "warnings": [
                            *(state.get("decision_warnings") or []),
                            f"llm_error: {last_exception}",
                        ],
                        "generated_at": datetime.now().astimezone().isoformat(),
                    },
                }
                return {**state, "decision_result": decision_result}

        operations = _append_implicit_hold_operations(
            context.get("portfolio_table") or [],
            operations,
            manager_map=manager_map,
        )

        target_position_range = _parse_target_position_range((context.get("macro_hint") or {}).get("target_position"))

        new_table, reason_table, total_capital = _apply_operations_to_table(
            trade_date=trade_date,
            initial_capital=initial_capital,
            old_table=context.get("portfolio_table") or [],
            operations=operations,
            target_position_range=target_position_range,
            stock_pool_result=state.get("stock_pool_manager_result"),
        )
        summary = _build_programmatic_decision_summary(
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
    def persist_decision_node(state: Dict[str, Any], config: Optional[RunnableConfig] = None) -> Dict[str, Any]:
        _ = config
        payload = state.get("decision_result")
        if not payload:
            return state
        trade_date = _normalize_trade_date(payload.get("trade_date") or state.get("trade_date"))

        # 从 state 获取自定义根目录，或使用默认路径
        decision_root = _get_artifact_root(state, "portfolio_decision_root", _DEFAULT_PORTFOLIO_DECISION_ARTIFACT_ROOT)

        decision_dir = decision_root / trade_date
        decision_result_path = decision_dir / "result.json"
        decision_manifest_path = decision_dir / "manifest.json"

        try:
            _write_json_atomic(decision_result_path, payload)
            _write_json_atomic(
                decision_manifest_path,
                {
                    "artifact_type": "portfolio_decision_result",
                    "module": "agents.decision.portfolio_decision",
                    "trade_date": trade_date,
                    "created_at": datetime.now().astimezone().isoformat(),
                    "status": "success",
                    "result_path": decision_result_path.as_posix(),
                },
            )
            try:
                from database.data_sync.portfolio_decision import sync_single_result

                sync_single_result(decision_result_path)
            except Exception as sync_err:
                logger.warning("portfolio_decision 数据库同步失败: %s", sync_err)
            return {
                **state,
                "decision_artifact_path": decision_result_path.as_posix(),
                "decision_manifest_path": decision_manifest_path.as_posix(),
            }
        except Exception as e:
            logger.warning("写入决策产物失败: %s", e)
            return state

    return persist_decision_node


__all__ = [
    "create_load_upstream_artifacts_node",
    "create_build_decision_context_node",
    "create_llm_make_decision_node",
    "create_persist_portfolio_decision_node",
]
