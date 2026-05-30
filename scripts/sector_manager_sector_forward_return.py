"""
根据 sector_manager 的 result.json（行业经理汇总），统计 favored / watchlist / risk 等板块
自基准日起「向后 N 个交易日」的涨跌幅与等权组合收益。行为与 macro 版脚本一致（单日 / 滚动 / JSON）。

sector_manager 产物字段：favored_sectors, watchlist_sectors, risk_sectors（见 agents/manager/sector_manager/node.py）。

默认 JSON：`data/artifacts/sector_manager_sector_forward/result.json`

示例：
    python -m scripts.sector_manager_sector_forward_return --result-json data/artifacts/manager/sector_manager/20260325/result.json
    python -m scripts.sector_manager_sector_forward_return --roll --sector-root data/artifacts/manager/sector_manager --range-start 20240108 --range-end 20260430

--lists：favored, watchlist, risk, focus(=favored+watchlist), all
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Optional

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from scripts.sector_forward_return_core import (
    build_name_maps_from_api,
    collect_sector_manager_names,
    compute_equal_weight_returns,
    discover_artifact_dates,
    end_trade_after_n_days,
    first_open_on_or_after,
    infer_trade_date_from_path,
    load_open_days,
    run_roll_mode,
    sector_row_for_json,
    utc_now_iso,
    write_json_report,
)


def main() -> int:
    parser = argparse.ArgumentParser(description="sector_manager result.json 板块未来 N 个交易日涨跌幅统计")
    parser.add_argument(
        "--result-json",
        type=Path,
        default=None,
        help="sector_manager 单日 result.json；与 --roll 二选一（单日模式必填）",
    )
    parser.add_argument(
        "--trade-date",
        type=str,
        default="",
        help="基准日 YYYYMMDD；默认从上级目录名推断",
    )
    parser.add_argument(
        "--trading-days",
        type=int,
        default=21,
        help="自起始交易日起向后的交易日个数（默认 21）",
    )
    parser.add_argument(
        "--lists",
        type=str,
        default="favored,watchlist",
        help="逗号分隔: favored, watchlist, risk, focus(=favored+watchlist), all",
    )
    parser.add_argument(
        "--exchange",
        type=str,
        default="SSE",
        help="交易日历交易所（默认 SSE）",
    )
    parser.add_argument(
        "--roll",
        action="store_true",
        help="滚动多期：每期读取 sector-root 下当日的 result.json",
    )
    parser.add_argument(
        "--sector-root",
        type=Path,
        default=Path("data/artifacts/manager/sector_manager"),
        help="行业经理结果根目录（默认 data/artifacts/manager/sector_manager）",
    )
    parser.add_argument(
        "--range-start",
        type=str,
        default="",
        help="滚动：首期锚点不早于该自然日 YYYYMMDD",
    )
    parser.add_argument(
        "--range-end",
        type=str,
        default="",
        help="滚动：锚点自然日不晚于该日",
    )
    parser.add_argument(
        "--step-trading-days",
        type=int,
        default=0,
        help="滚动：下一期起点前进的交易日数；0 表示与 --trading-days 相同（首尾相接）",
    )
    parser.add_argument(
        "--json-out",
        type=Path,
        default=Path("data/artifacts/sector_manager_sector_forward/result.json"),
        help="将结果写入该 JSON 路径",
    )
    parser.add_argument(
        "--no-json-out",
        action="store_true",
        help="不写入 JSON，仅终端输出",
    )
    args = parser.parse_args()

    json_out: Optional[Path] = None if args.no_json_out else args.json_out

    if args.roll:
        disc = discover_artifact_dates(args.sector_root)
        if not disc:
            print(f"未在 {args.sector_root} 下发现 */result.json", file=sys.stderr)
            return 1
        rs = (args.range_start or "").strip().replace("-", "")[:8]
        re_ = (args.range_end or "").strip().replace("-", "")[:8]
        if not rs:
            rs = disc[0]
        if not re_:
            re_ = disc[-1]
        if len(rs) != 8 or len(re_) != 8:
            print("range-start / range-end 须为 YYYYMMDD", file=sys.stderr)
            return 1
        map_i, map_n = build_name_maps_from_api()
        return run_roll_mode(
            args.sector_root,
            rs,
            re_,
            args.trading_days,
            args.step_trading_days,
            args.lists,
            args.exchange,
            map_i,
            map_n,
            json_out,
            collect_sector_manager_names,
            "sector_manager_root",
            "sector_manager_root",
        )

    path = args.result_json
    if path is None or not str(path).strip():
        print("单日模式请提供 --result-json，或使用 --roll", file=sys.stderr)
        return 1
    if not path.is_file():
        print(f"文件不存在: {path}", file=sys.stderr)
        return 1

    trade_date = (args.trade_date or "").strip().replace("-", "")[:8]
    if not trade_date:
        trade_date = infer_trade_date_from_path(path) or ""
    if len(trade_date) != 8 or not trade_date.isdigit():
        print("无法确定基准日，请使用 --trade-date YYYYMMDD", file=sys.stderr)
        return 1

    with open(path, "r", encoding="utf-8") as f:
        payload = json.load(f)

    items = collect_sector_manager_names(payload, args.lists)
    if not items:
        print("JSON 中无可用板块名称（检查 favored/watchlist/risk 与 --lists）", file=sys.stderr)
        return 1

    start_dt = datetime.strptime(trade_date, "%Y%m%d")
    cal_end = (start_dt + timedelta(days=120)).strftime("%Y%m%d")
    open_days = load_open_days(trade_date, cal_end, exchange=args.exchange)
    if not open_days:
        print("无法获取交易日历，请检查 Tushare 配置", file=sys.stderr)
        return 1

    start_trade = first_open_on_or_after(open_days, trade_date)
    if not start_trade:
        print("基准日附近无交易日", file=sys.stderr)
        return 1

    end_trade, cal_err = end_trade_after_n_days(open_days, start_trade, args.trading_days)
    if not end_trade:
        print(cal_err, file=sys.stderr)
        return 1

    map_i, map_n = build_name_maps_from_api()

    from database.config import get_db_session

    rows_out: List[dict] = []
    with get_db_session() as session:
        rows_out, _, _, _ = compute_equal_weight_returns(
            payload,
            args.lists,
            start_trade,
            end_trade,
            session,
            map_i,
            map_n,
            collect_sector_manager_names,
        )

    print(
        f"基准日(自然日)={trade_date} 起始交易日={start_trade}  "
        f"持有交易日数={args.trading_days} 结束交易日={end_trade}"
    )
    colw_name = max(len("name"), max(len(str(x["name"])) for x in rows_out))
    colw_code = max(len("ths_code"), max(len(str(x["ths_code"])) for x in rows_out))
    hdr = f"{'name':<{colw_name}} {'ths_code':<{colw_code}} {'type':^4} {'ret%':>10}  note"
    print(hdr)
    print("-" * len(hdr))
    for x in rows_out:
        ret_s = f"{x['forward_return_pct']:>10.4f}" if x["forward_return_pct"] is not None else f"{'N/A':>10}"
        note = x["error"] or ""
        print(
            f"{x['name']:<{colw_name}} {x['ths_code']:<{colw_code}} "
            f"{str(x['type']):^4} {ret_s}  {note}"
        )

    rets = [x["forward_return_pct"] for x in rows_out if x["forward_return_pct"] is not None]
    n_ok, n_all = len(rets), len(rows_out)
    if rets:
        avg_pct = round(sum(rets) / n_ok, 4)
        print(
            f"等权组合平均收益率(%)={avg_pct}  "
            f"(对 {n_ok}/{n_all} 个有效板块等权；缺失板块未计入)"
        )
    else:
        print("等权组合平均收益率: 无有效样本", file=sys.stderr)

    if json_out:
        avg_pct_json = round(sum(rets) / n_ok, 4) if rets else None
        report = {
            "generated_at": utc_now_iso(),
            "mode": "single",
            "source": "sector_manager",
            "result_json": str(path.resolve()),
            "trade_date": trade_date,
            "start_trade": start_trade,
            "end_trade": end_trade,
            "trading_days": args.trading_days,
            "lists": args.lists,
            "exchange": args.exchange,
            "equal_weight_return_pct": avg_pct_json,
            "n_ok": n_ok,
            "n_all": n_all,
            "sectors": [sector_row_for_json(x) for x in rows_out],
        }
        write_json_report(json_out, report)
        print(f"已写入 JSON: {json_out.resolve()}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
