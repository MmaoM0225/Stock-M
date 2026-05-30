"""
板块前向收益统计共享逻辑（macro_manager / sector_manager 复用）。
"""

from __future__ import annotations

import json
import sys
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

CollectNamesFn = Callable[[dict, str], List[Tuple[str, str, str]]]


def norm_cal_date(value: object) -> str:
    s = str(value).strip().replace("-", "")[:8]
    return s if len(s) == 8 and s.isdigit() else ""


def infer_trade_date_from_path(result_path: Path) -> Optional[str]:
    parent = result_path.parent.name
    if len(parent) == 8 and parent.isdigit():
        return parent
    return None


def load_open_days(start: str, end: str, exchange: str = "SSE") -> List[str]:
    from dataflow.market_data import fetch_trade_cal

    df = fetch_trade_cal(exchange=exchange, start_date=start, end_date=end, is_open="1")
    if df is None or df.empty:
        return []
    out: List[str] = []
    for x in df["cal_date"].tolist():
        s = norm_cal_date(x)
        if s:
            out.append(s)
    return sorted(set(out))


def first_open_on_or_after(open_days: Sequence[str], ymd: str) -> Optional[str]:
    for d in open_days:
        if d >= ymd:
            return d
    return None


def end_trade_after_n_days(open_days: Sequence[str], start_trade: str, n: int) -> Tuple[Optional[str], str]:
    if n < 1:
        return None, "trading-days 须 >= 1"
    try:
        idx = open_days.index(start_trade)
    except ValueError:
        return None, "起始交易日不在交易日历中"
    j = idx + n
    if j >= len(open_days):
        return None, f"日历区间不足（需起始后再有 {n} 个交易日，当前仅 {len(open_days) - idx - 1} 个）"
    return open_days[j], ""


@dataclass
class ResolvedSector:
    name: str
    source: str
    ts_code: Optional[str]
    index_type: Optional[str]
    note: str = ""


def build_name_maps_from_api() -> Tuple[Dict[str, str], Dict[str, str]]:
    from dataflow.industry_data import fetch_ths_index

    map_i: Dict[str, str] = {}
    map_n: Dict[str, str] = {}
    df_i = fetch_ths_index(index_type="I")
    if df_i is not None and not df_i.empty and "name" in df_i.columns and "ts_code" in df_i.columns:
        for _, row in df_i.iterrows():
            n = str(row["name"]).strip()
            c = str(row["ts_code"]).strip()
            if n and c:
                map_i.setdefault(n, c)
    df_n = fetch_ths_index(index_type="N")
    if df_n is not None and not df_n.empty and "name" in df_n.columns and "ts_code" in df_n.columns:
        for _, row in df_n.iterrows():
            n = str(row["name"]).strip()
            c = str(row["ts_code"]).strip()
            if n and c:
                map_n.setdefault(n, c)
    return map_i, map_n


def resolve_sector_name(
    name: str,
    preferred: str,
    session,
    map_i: Dict[str, str],
    map_n: Dict[str, str],
) -> ResolvedSector:
    from database import ThsIndex

    raw = name.strip()
    if not raw:
        return ResolvedSector(name=name, source="", ts_code=None, index_type=None, note="空名称")

    def from_maps(pref: str) -> Optional[Tuple[str, str]]:
        if pref == "I":
            c = map_i.get(raw)
            if c:
                return c, "I"
        else:
            c = map_n.get(raw)
            if c:
                return c, "N"
        if pref == "I":
            c = map_n.get(raw)
            if c:
                return c, "N"
        else:
            c = map_i.get(raw)
            if c:
                return c, "I"
        return None

    try:
        q = session.query(ThsIndex).filter(ThsIndex.name == raw)
        rows = q.all()
        if rows:
            preferred_rows = [r for r in rows if (r.index_type or "").upper() == preferred.upper()]
            pick = preferred_rows[0] if preferred_rows else rows[0]
            return ResolvedSector(
                name=raw,
                source="db",
                ts_code=pick.ts_code,
                index_type=pick.index_type,
                note="",
            )
    except Exception:
        pass

    hit = from_maps(preferred)
    if hit:
        code, typ = hit
        return ResolvedSector(name=raw, source="api_map", ts_code=code, index_type=typ, note="")

    return ResolvedSector(name=raw, source="", ts_code=None, index_type=None, note="未匹配到同花顺板块")


def pick_close_on_or_after(df, ymd: str) -> Optional[float]:
    if df is None or df.empty or "trade_date" not in df.columns or "close" not in df.columns:
        return None
    sub = df.copy()
    sub["_d"] = sub["trade_date"].map(norm_cal_date)
    sub = sub[sub["_d"] >= ymd].sort_values("_d")
    if sub.empty:
        return None
    try:
        v = float(sub.iloc[0]["close"])
        return v if v > 0 else None
    except (TypeError, ValueError):
        return None


def forward_return_pct(
    ts_code: str,
    start_trade: str,
    end_trade: str,
) -> Tuple[Optional[float], str]:
    from dataflow.industry_data import fetch_ths_daily

    if start_trade > end_trade:
        return None, "起始日晚于结束日"
    buf_end = (datetime.strptime(end_trade, "%Y%m%d") + timedelta(days=7)).strftime("%Y%m%d")
    try:
        df = fetch_ths_daily(ts_code=ts_code, start_date=start_trade, end_date=buf_end)
    except Exception as e:
        return None, f"拉取行情失败: {e}"

    if df is None or df.empty:
        return None, "无行情数据"

    p0 = pick_close_on_or_after(df, start_trade)
    p1 = pick_close_on_or_after(df, end_trade)
    if p0 is None:
        return None, f"缺少起始收盘({start_trade}及以后)"
    if p1 is None:
        return None, f"缺少结束收盘({end_trade}及以后)"
    if p0 <= 0:
        return None, "起始价无效"
    return round((p1 / p0 - 1.0) * 100.0, 4), ""


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def write_json_report(path: Path, report: Dict[str, Any]) -> None:
    path = path.resolve()
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(report, f, ensure_ascii=False, indent=2)


def sector_row_for_json(row: dict) -> dict:
    return {
        "name": row["name"],
        "ths_code": row["ths_code"],
        "type": row["type"],
        "forward_return_pct": row["forward_return_pct"],
        "error": row["error"] or None,
    }


def discover_artifact_dates(artifact_root: Path) -> List[str]:
    dates: List[str] = []
    if not artifact_root.is_dir():
        return dates
    for p in artifact_root.iterdir():
        if (
            p.is_dir()
            and len(p.name) == 8
            and p.name.isdigit()
            and (p / "result.json").is_file()
        ):
            dates.append(p.name)
    return sorted(dates)


def load_artifact_payload(artifact_root: Path, ymd: str) -> Optional[dict]:
    fp = artifact_root / ymd / "result.json"
    if not fp.is_file():
        return None
    try:
        with open(fp, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return None


def collect_macro_manager_names(data: dict, lists: str) -> List[Tuple[str, str, str]]:
    out: List[Tuple[str, str, str]] = []
    want = {x.strip().lower() for x in lists.split(",") if x.strip()}
    if "all" in want:
        want = {"industry", "concept", "avoid"}
    elif "focus" in want:
        want.discard("focus")
        want.update({"industry", "concept"})

    def extend(key: str, pref: str, flag: str):
        if flag not in want:
            return
        for x in data.get(key) or []:
            if isinstance(x, str) and x.strip():
                out.append((x.strip(), key, pref))

    extend("focus_industry_sectors", "I", "industry")
    extend("focus_concept_sectors", "N", "concept")
    extend("avoid_sectors", "I", "avoid")
    return out


def collect_sector_manager_names(data: dict, lists: str) -> List[Tuple[str, str, str]]:
    """sector_manager summary：favored / watchlist / risk 板块名（优先按同花顺行业 I 解析）。"""
    out: List[Tuple[str, str, str]] = []
    want = {x.strip().lower() for x in lists.split(",") if x.strip()}
    if "all" in want:
        want = {"favored", "watchlist", "risk"}
    elif "focus" in want:
        want.discard("focus")
        want.update({"favored", "watchlist"})

    def extend(key: str, pref: str, flag: str):
        if flag not in want:
            return
        for x in data.get(key) or []:
            if isinstance(x, str) and x.strip():
                out.append((x.strip(), key, pref))

    extend("favored_sectors", "I", "favored")
    extend("watchlist_sectors", "I", "watchlist")
    extend("risk_sectors", "I", "risk")
    return out


def compute_equal_weight_returns(
    payload: dict,
    lists: str,
    start_trade: str,
    end_trade: str,
    session,
    map_i: Dict[str, str],
    map_n: Dict[str, str],
    collect_names_fn: CollectNamesFn,
) -> Tuple[List[dict], Optional[float], int, int]:
    items = collect_names_fn(payload, lists)
    rows_out: List[dict] = []
    for name, list_key, pref in items:
        r = resolve_sector_name(name, pref, session, map_i, map_n)
        ret: Optional[float] = None
        err_msg = r.note
        if r.ts_code:
            ret, err_msg = forward_return_pct(r.ts_code, start_trade, end_trade)
        rows_out.append(
            {
                "list": list_key,
                "name": r.name,
                "ths_code": r.ts_code or "",
                "type": r.index_type or "",
                "resolve": r.source,
                "forward_return_pct": ret,
                "error": err_msg if ret is None else "",
            }
        )
    rets = [x["forward_return_pct"] for x in rows_out if x["forward_return_pct"] is not None]
    n_ok, n_all = len(rets), len(rows_out)
    avg: Optional[float] = round(sum(rets) / n_ok, 4) if rets else None
    return rows_out, avg, n_ok, n_all


def iter_roll_windows(
    open_days: Sequence[str],
    range_start: str,
    range_end: str,
    horizon: int,
    step: int,
) -> List[Tuple[str, str]]:
    out: List[Tuple[str, str]] = []
    first = first_open_on_or_after(open_days, range_start)
    if not first:
        return out
    try:
        i = open_days.index(first)
    except ValueError:
        return out
    step_adv = horizon if step <= 0 else step
    while i + horizon < len(open_days):
        s, e = open_days[i], open_days[i + horizon]
        if s > range_end:
            break
        out.append((s, e))
        i += step_adv
    return out


def run_roll_mode(
    artifact_root: Path,
    range_start: str,
    range_end: str,
    trading_days: int,
    step_trading_days: int,
    lists: str,
    exchange: str,
    map_i: Dict[str, str],
    map_n: Dict[str, str],
    json_out: Optional[Path],
    collect_names_fn: CollectNamesFn,
    report_artifact_key: str,
    roll_source_label: str,
) -> int:
    cal_tail = (datetime.strptime(range_end, "%Y%m%d") + timedelta(days=800)).strftime("%Y%m%d")
    open_days = load_open_days(range_start, cal_tail, exchange=exchange)
    if not open_days:
        print("无法获取交易日历", file=sys.stderr)
        return 1

    windows = iter_roll_windows(open_days, range_start, range_end, trading_days, step_trading_days)
    if not windows:
        print("区间内无完整交易窗口（检查 range 与 trading-days）", file=sys.stderr)
        return 1

    hdr = f"{'start':<10} {'end':<10} {'ok/tot':>7} {'eq_ret%':>12}  note"
    print(
        f"滚动模式 持有={trading_days}交易日 "
        f"步进={'首尾相接' if step_trading_days <= 0 else f'{step_trading_days}交易日'} "
        f"{roll_source_label}={artifact_root}"
    )
    print(hdr)
    print("-" * len(hdr))

    from database.config import get_db_session

    series: List[float] = []
    skipped = 0
    period_rows: List[Dict[str, Any]] = []
    with get_db_session() as session:
        for start_trade, end_trade in windows:
            payload = load_artifact_payload(artifact_root, start_trade)
            if not payload:
                print(f"{start_trade:<10} {end_trade:<10} {'-':>7} {'N/A':>12}  无 result.json")
                skipped += 1
                period_rows.append(
                    {
                        "start": start_trade,
                        "end": end_trade,
                        "missing_result_json": True,
                        "n_ok": 0,
                        "n_all": 0,
                        "equal_weight_return_pct": None,
                        "note": "无 result.json",
                        "sectors": [],
                    }
                )
                continue
            _rows, avg, n_ok, n_all = compute_equal_weight_returns(
                payload,
                lists,
                start_trade,
                end_trade,
                session,
                map_i,
                map_n,
                collect_names_fn,
            )
            note = ""
            if avg is None:
                note = "无有效板块收益"
            else:
                series.append(avg)
            ret_s = f"{avg:>12.4f}" if avg is not None else f"{'N/A':>12}"
            ok_tot = f"{n_ok}/{n_all}"
            print(f"{start_trade:<10} {end_trade:<10} {ok_tot:>7} {ret_s}  {note}")
            period_rows.append(
                {
                    "start": start_trade,
                    "end": end_trade,
                    "missing_result_json": False,
                    "n_ok": n_ok,
                    "n_all": n_all,
                    "equal_weight_return_pct": avg,
                    "note": note or None,
                    "sectors": [sector_row_for_json(x) for x in _rows],
                }
            )

    mean_roll: Optional[float] = None
    if series:
        mean_roll = round(sum(series) / len(series), 4)
        print(
            f"--- 共 {len(series)} 期有效（跳过 {skipped} 期无 JSON），"
            f"各期等权收益算术均值(%)={mean_roll}"
        )
    else:
        print("无有效滚动期", file=sys.stderr)

    if json_out:
        report: Dict[str, Any] = {
            "generated_at": utc_now_iso(),
            "mode": "roll",
            report_artifact_key: str(artifact_root),
            "range_start": range_start,
            "range_end": range_end,
            "trading_days": trading_days,
            "step_trading_days": step_trading_days,
            "step_mode": "contiguous" if step_trading_days <= 0 else "fixed_step",
            "lists": lists,
            "exchange": exchange,
            "periods": period_rows,
            "valid_periods": len(series),
            "skipped_missing_json": skipped,
            "mean_equal_weight_return_pct": mean_roll,
        }
        write_json_report(json_out, report)
        print(f"已写入 JSON: {json_out.resolve()}")

    if not series:
        return 1
    return 0
