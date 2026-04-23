"""
板块资金流 HTTP 接口所需的小型快照（与 api.services.data_service 中逻辑一致）。

persist 时只写入 api_snapshot，避免把数万条 sector_moneyflow_data 落盘。
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional, Set


def _to_float_or_none(value: Any) -> Optional[float]:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _normalize_moneyflow_rows(raw_rows: List[Any]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for row in raw_rows:
        if not isinstance(row, dict):
            continue
        td = str(row.get("trade_date", "")).replace("-", "")[:8]
        if len(td) != 8 or not td.isdigit():
            continue
        out.append({**row, "trade_date": td})
    return out


def _sum_sector_moneyflow_by_dates(rows: List[Dict[str, Any]], dates: Set[str]) -> Optional[float]:
    if not dates:
        return None
    total = 0.0
    has_value = False
    for row in rows:
        if str(row.get("trade_date")) not in dates:
            continue
        v = _to_float_or_none(row.get("net_amount"))
        if v is None:
            continue
        total += v
        has_value = True
    return round(total, 2) if has_value else None


def _sum_sector_moneyflow_recent_days(rows: List[Dict[str, Any]], days: int) -> Optional[float]:
    if days <= 0:
        return None
    date_set = sorted(
        {
            str(row.get("trade_date"))
            for row in rows
            if isinstance(row, dict)
            and str(row.get("trade_date", "")).isdigit()
            and len(str(row.get("trade_date", ""))) == 8
        },
        reverse=True,
    )
    if not date_set:
        return None
    selected = set(date_set[:days])
    return _sum_sector_moneyflow_by_dates(rows, selected)


def _build_one_day_sector_flow(one_day_rows: List[Dict[str, Any]], top_k: int = 20) -> List[Dict[str, Any]]:
    by_name: Dict[str, float] = {}
    for row in one_day_rows:
        name = row.get("name")
        if not isinstance(name, str) or not name:
            continue
        net_amount = _to_float_or_none(row.get("net_amount"))
        if net_amount is None:
            continue
        by_name[name] = by_name.get(name, 0.0) + net_amount
    items = [{"name": name, "net_amount": round(amount, 2)} for name, amount in by_name.items()]
    if top_k <= 0:
        return []

    half = top_k // 2
    inflow = sorted(
        [item for item in items if float(item.get("net_amount") or 0.0) > 0],
        key=lambda x: float(x.get("net_amount") or 0.0),
        reverse=True,
    )[:half]
    outflow_selected = sorted(
        [item for item in items if float(item.get("net_amount") or 0.0) < 0],
        key=lambda x: float(x.get("net_amount") or 0.0),
    )[:half]
    outflow = sorted(outflow_selected, key=lambda x: float(x.get("net_amount") or 0.0), reverse=True)

    selected_names = {item["name"] for item in inflow + outflow if isinstance(item.get("name"), str)}
    if len(inflow) < half:
        need = half - len(inflow)
        extra_selected = [
            item
            for item in sorted(
                [x for x in items if float(x.get("net_amount") or 0.0) < 0 and x.get("name") not in selected_names],
                key=lambda x: float(x.get("net_amount") or 0.0),
            )
        ][:need]
        extra = sorted(extra_selected, key=lambda x: float(x.get("net_amount") or 0.0), reverse=True)
        outflow.extend(extra)
        selected_names.update(item["name"] for item in extra if isinstance(item.get("name"), str))
    if len(outflow) < half:
        need = half - len(outflow)
        extra = [
            item
            for item in sorted(
                [x for x in items if float(x.get("net_amount") or 0.0) > 0 and x.get("name") not in selected_names],
                key=lambda x: float(x.get("net_amount") or 0.0),
                reverse=True,
            )
        ][:need]
        inflow.extend(extra)

    return inflow + outflow


def _build_sector_moneyflow_rows(rows: List[Dict[str, Any]], top_k: int = 60) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for row in rows:
        td = str(row.get("trade_date", ""))
        if len(td) != 8 or not td.isdigit():
            continue
        out.append(
            {
                "trade_date": td,
                "ts_code": row.get("ts_code"),
                "name": row.get("name"),
                "lead_stock": row.get("lead_stock"),
                "pct_change": _to_float_or_none(row.get("pct_change")),
                "net_amount": _to_float_or_none(row.get("net_amount")),
            }
        )
    if top_k <= 0:
        return []
    half = top_k // 2

    inflow = sorted(
        [item for item in out if float(item.get("net_amount") or 0.0) > 0],
        key=lambda x: float(x.get("net_amount") or 0.0),
        reverse=True,
    )[:half]
    outflow_selected = sorted(
        [item for item in out if float(item.get("net_amount") or 0.0) < 0],
        key=lambda x: float(x.get("net_amount") or 0.0),
    )[:half]
    outflow = sorted(outflow_selected, key=lambda x: float(x.get("net_amount") or 0.0), reverse=True)

    selected = {(item.get("ts_code"), item.get("trade_date")) for item in inflow + outflow}
    if len(inflow) < half:
        need = half - len(inflow)
        extra_selected = [
            item
            for item in sorted(
                [
                    x
                    for x in out
                    if float(x.get("net_amount") or 0.0) < 0 and (x.get("ts_code"), x.get("trade_date")) not in selected
                ],
                key=lambda x: float(x.get("net_amount") or 0.0),
            )
        ][:need]
        extra = sorted(extra_selected, key=lambda x: float(x.get("net_amount") or 0.0), reverse=True)
        outflow.extend(extra)
        selected.update((item.get("ts_code"), item.get("trade_date")) for item in extra)
    if len(outflow) < half:
        need = half - len(outflow)
        extra = [
            item
            for item in sorted(
                [
                    x
                    for x in out
                    if float(x.get("net_amount") or 0.0) > 0 and (x.get("ts_code"), x.get("trade_date")) not in selected
                ],
                key=lambda x: float(x.get("net_amount") or 0.0),
                reverse=True,
            )
        ][:need]
        inflow.extend(extra)

    return inflow + outflow


def build_capital_flow_api_snapshot(raw_rows: List[Any], trade_date: str) -> Dict[str, Any]:
    """
    从原始 sector_moneyflow_data 生成 HTTP 接口用的小快照（不含全量 rows）。
    """
    td = str(trade_date or "").replace("-", "")[:8]
    rows = _normalize_moneyflow_rows(raw_rows if isinstance(raw_rows, list) else [])
    one_day_rows_raw = [row for row in rows if str(row.get("trade_date", "")) == td]
    return {
        "api_snapshot_version": 1,
        "trade_date": td,
        "one_day_net_amount": _sum_sector_moneyflow_by_dates(rows, {td}),
        "five_day_net_amount": _sum_sector_moneyflow_recent_days(rows, days=5),
        "twenty_day_net_amount": _sum_sector_moneyflow_recent_days(rows, days=20),
        "one_day_rows": _build_sector_moneyflow_rows(one_day_rows_raw, top_k=20),
        "one_day_sector_flow": _build_one_day_sector_flow(one_day_rows_raw, top_k=10),
    }


__all__ = ["build_capital_flow_api_snapshot"]
