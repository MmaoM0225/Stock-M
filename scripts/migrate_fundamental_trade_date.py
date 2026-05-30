"""
将 stock_fundamental_analyst 旧版目录升级为新版节点结果结构（就地升级）。

功能：
1) 从旧版聚合文件 result.json 自动拆出/修复以下节点文件：
   - company_basic_insight_result.json
   - valuation_map_result.json
   - income_map_result.json
   - cashflow_map_result.json
   - balancesheet_map_result.json
   - dividend_map_result.json
   - fundamental_reduce_result.json
2) 为每个节点文件补齐新版字段：
   - node_name
   - ts_code
   - trade_date
   - cache_key（按当前代码逻辑计算的输入哈希）
   - created_at
   - output

示例：
    # 升级全部历史目录（仅补缺，不覆盖）
    python scripts/migrate_fundamental_trade_date.py

    # 只升级单只股票
    python scripts/migrate_fundamental_trade_date.py --ts-code 600519.SH

    # 已有节点文件也强制重写
    python scripts/migrate_fundamental_trade_date.py --overwrite
"""

from __future__ import annotations

import argparse
import hashlib
import json
from datetime import datetime
from pathlib import Path
from typing import Any


ARTIFACT_ROOT = Path("data") / "artifacts" / "analyst" / "stock_analyst" / "stock_fundamental_analyst"
NODE_FILES = {
    "company_basic_insight": "company_basic_insight_result.json",
    "valuation_map": "valuation_map_result.json",
    "income_map": "income_map_result.json",
    "cashflow_map": "cashflow_map_result.json",
    "balancesheet_map": "balancesheet_map_result.json",
    "dividend_map": "dividend_map_result.json",
    "fundamental_reduce": "fundamental_reduce_result.json",
}


def _json_hash(payload: dict[str, Any]) -> str:
    body = json.dumps(payload, ensure_ascii=False, sort_keys=True, default=str)
    return hashlib.sha256(body.encode("utf-8")).hexdigest()


def _read_json(path: Path) -> dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
    if not isinstance(data, dict):
        raise ValueError(f"{path.as_posix()} 不是 JSON 对象")
    return data


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)


def _build_node_payloads(agg: dict[str, Any]) -> dict[str, dict[str, Any]]:
    ts_code = str(agg.get("ts_code") or "").strip()
    trade_date = str(agg.get("trade_date") or "")
    facts = agg.get("stock_fundamental_facts") or {}

    outputs: dict[str, dict[str, Any]] = {
        "company_basic_insight": {
            "company_profile_text": agg.get("company_profile_text", ""),
            "company_basic_analysis": agg.get("company_basic_analysis") or {},
        },
        "valuation_map": {"valuation_map_analysis": agg.get("valuation_map_analysis") or {}},
        "income_map": {"income_map_analysis": agg.get("income_map_analysis") or {}},
        "cashflow_map": {"cashflow_map_analysis": agg.get("cashflow_map_analysis") or {}},
        "balancesheet_map": {"balancesheet_map_analysis": agg.get("balancesheet_map_analysis") or {}},
        "dividend_map": {"dividend_map_analysis": agg.get("dividend_map_analysis") or {}},
        "fundamental_reduce": {"fundamental_reduce_result": agg.get("fundamental_reduce_result") or {}},
    }

    hash_inputs: dict[str, dict[str, Any]] = {
        "company_basic_insight": {
            "ts_code": ts_code,
            "company_profile": (facts.get("company_profile") or {}),
        },
        "valuation_map": {
            "ts_code": ts_code,
            "valuation_snapshot": (facts.get("valuation_snapshot") or {}),
        },
        "income_map": {
            "ts_code": ts_code,
            "income_snapshot": (facts.get("income_snapshot") or {}),
        },
        "cashflow_map": {
            "ts_code": ts_code,
            "cashflow_snapshot": (facts.get("cashflow_snapshot") or {}),
        },
        "balancesheet_map": {
            "ts_code": ts_code,
            "balancesheet_snapshot": (facts.get("balancesheet_snapshot") or {}),
        },
        "dividend_map": {
            "ts_code": ts_code,
            "dividend_snapshot": (facts.get("dividend_snapshot") or {}),
        },
        "fundamental_reduce": {
            "ts_code": ts_code,
            "company_basic_analysis": agg.get("company_basic_analysis") or {},
            "valuation_map_analysis": agg.get("valuation_map_analysis") or {},
            "income_map_analysis": agg.get("income_map_analysis") or {},
            "cashflow_map_analysis": agg.get("cashflow_map_analysis") or {},
            "balancesheet_map_analysis": agg.get("balancesheet_map_analysis") or {},
            "dividend_map_analysis": agg.get("dividend_map_analysis") or {},
        },
    }

    now = datetime.now().astimezone().isoformat()
    node_payloads: dict[str, dict[str, Any]] = {}
    for node_name, output in outputs.items():
        node_payloads[node_name] = {
            "node_name": node_name,
            "ts_code": ts_code,
            "trade_date": trade_date,
            "cache_key": _json_hash(hash_inputs[node_name]),
            "created_at": now,
            "output": output,
        }
    return node_payloads


def _migrate_one_day(day_dir: Path, overwrite: bool) -> tuple[int, int]:
    result_path = day_dir / "result.json"
    if not result_path.exists():
        return 0, 0
    agg = _read_json(result_path)
    node_payloads = _build_node_payloads(agg)
    written = 0
    skipped = 0
    for node_name, filename in NODE_FILES.items():
        target = day_dir / filename
        if target.exists() and not overwrite:
            skipped += 1
            continue
        _write_json(target, node_payloads[node_name])
        written += 1
    return written, skipped


def _plan_one_day(day_dir: Path, overwrite: bool) -> tuple[int, int]:
    """仅计算将写入/跳过数量，不落盘。"""
    result_path = day_dir / "result.json"
    if not result_path.exists():
        return 0, 0
    to_write = 0
    to_skip = 0
    for filename in NODE_FILES.values():
        target = day_dir / filename
        if target.exists() and not overwrite:
            to_skip += 1
        else:
            to_write += 1
    return to_write, to_skip


def main() -> None:
    parser = argparse.ArgumentParser(description="升级 stock_fundamental_analyst 旧版目录为新版节点结果结构")
    parser.add_argument("--ts-code", default=None, help="可选，仅处理单只股票，如 600519.SH")
    parser.add_argument("--overwrite", action="store_true", help="已存在节点文件时仍重写")
    parser.add_argument("--dry-run", action="store_true", help="仅预览将变更的文件，不实际写入")
    args = parser.parse_args()

    if args.ts_code:
        ts_dirs = [ARTIFACT_ROOT / args.ts_code.strip()]
    else:
        ts_dirs = [p for p in ARTIFACT_ROOT.iterdir() if p.is_dir() and p.name != "_node_cache"]

    total_written = 0
    total_skipped = 0
    total_days = 0
    changed_days: list[str] = []
    for ts_dir in sorted(ts_dirs):
        if not ts_dir.exists():
            continue
        for day_dir in sorted([p for p in ts_dir.iterdir() if p.is_dir() and p.name.isdigit()]):
            if args.dry_run:
                written, skipped = _plan_one_day(day_dir, overwrite=args.overwrite)
            else:
                written, skipped = _migrate_one_day(day_dir, overwrite=args.overwrite)
            if written or skipped:
                total_days += 1
            if written > 0:
                changed_days.append(day_dir.as_posix())
            total_written += written
            total_skipped += skipped

    mode = "预演完成" if args.dry_run else "升级完成"
    print(f"{mode}：处理目录 {total_days} 个，写入节点文件 {total_written} 个，跳过已有文件 {total_skipped} 个")
    if args.dry_run and changed_days:
        print("\n以下目录将发生写入：")
        for p in changed_days:
            print(f"  - {p}")


if __name__ == "__main__":
    main()

