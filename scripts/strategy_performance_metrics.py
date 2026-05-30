"""
策略绩效指标计算脚本（独立于可视化模块）。

功能：
1) 自动扫描 data/artifacts/decision/*/portfolio 下的策略
2) 逐策略构建每日净值序列
3) 计算并输出关键指标：
   - 年化收益率（Annualized Return）
   - 最大回撤（Maximum Drawdown）
   - 夏普比率（Sharpe Ratio）
   - 胜率（Win Rate，按日收益率口径）

示例：
    python -m scripts.strategy_performance_metrics
    python -m scripts.strategy_performance_metrics --strategy full_position_decision
    python -m scripts.strategy_performance_metrics --risk-free-rate 0.02 --trading-days 252
"""

from __future__ import annotations

import argparse
import contextlib
import io
import json
from pathlib import Path
from typing import Any, Dict, List, Tuple

from visualization.portfolio_nav_index_only_visualization import build_daily_nav_from_holdings
from visualization.portfolio_nav_visualization import calculate_performance_metrics


def discover_strategies(data_dir: Path) -> List[str]:
    """自动发现拥有 portfolio 结果的策略目录。"""
    decision_dir = data_dir / "artifacts" / "decision"
    if not decision_dir.exists():
        return []

    strategies: List[str] = []
    for strategy_dir in sorted(decision_dir.iterdir()):
        if not strategy_dir.is_dir():
            continue
        if (strategy_dir / "portfolio").exists():
            strategies.append(strategy_dir.name)
    return strategies


def read_portfolio_snapshots(data_dir: Path, strategy_name: str) -> List[Dict]:
    """读取策略 portfolio 快照。"""
    portfolio_dir = data_dir / "artifacts" / "decision" / strategy_name / "portfolio"
    if not portfolio_dir.exists():
        return []

    snapshots: List[Dict] = []
    for date_dir in sorted(portfolio_dir.iterdir()):
        if not date_dir.is_dir():
            continue
        result_file = date_dir / "result.json"
        if not result_file.exists():
            continue
        try:
            with open(result_file, "r", encoding="utf-8") as f:
                payload = json.load(f)
            snapshots.append(payload)
        except Exception:
            continue

    snapshots = [x for x in snapshots if str(x.get("trade_date", "")).strip()]
    return sorted(snapshots, key=lambda x: str(x.get("trade_date")))


def infer_strategy_end_date(snapshots: List[Dict]) -> str | None:
    """推断单策略可用数据的结束日期。"""
    if not snapshots:
        return None
    last_date = str(snapshots[-1].get("trade_date", "")).strip()
    return last_date or None


def _to_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None:
            return default
        if isinstance(value, str):
            text = value.strip().replace(",", "")
            if text in {"", "-", "None", "null"}:
                return default
            return float(text)
        return float(value)
    except Exception:
        return default


def normalize_snapshots_for_index_only(snapshots: List[Dict]) -> List[Dict]:
    """转换为 index_only 回放函数需要的快照格式。"""
    normalized: List[Dict] = []
    for snap in snapshots:
        trade_date = str(snap.get("trade_date", "")).strip()
        if not trade_date:
            continue
        meta = snap.get("meta", {}) if isinstance(snap.get("meta"), dict) else {}
        normalized.append(
            {
                "trade_date": trade_date,
                "portfolio_table": snap.get("portfolio_table", []),
                "initial_capital": _to_float(meta.get("initial_capital"), 0.0),
                "total_capital": _to_float(meta.get("total_capital"), 0.0),
                "operation_reason_table": snap.get("operation_reason_table", []),
            }
        )
    return normalized


def _extract_stock_holdings(snapshot: Dict) -> Dict[str, Dict[str, float]]:
    """提取个股持仓: ts_code -> {shares, cost}。"""
    holdings: Dict[str, Dict[str, float]] = {}
    for row in snapshot.get("portfolio_table", []):
        ts_code = row.get("ts_code")
        asset_type = str(row.get("资产类型") or "")
        if not ts_code or asset_type != "个股":
            continue
        shares = _to_float(row.get("持仓股数"), 0.0)
        if shares <= 0:
            continue
        holdings[str(ts_code)] = {
            "shares": shares,
            "cost": _to_float(row.get("成本价"), 0.0),
        }
    return holdings


def _extract_clear_prices(snapshot: Dict) -> Dict[str, float]:
    """从 operation_reason_table 中提取清仓执行价。"""
    clear_prices: Dict[str, float] = {}
    for row in snapshot.get("operation_reason_table", []):
        if str(row.get("操作") or "") != "清仓":
            continue
        ts_code = row.get("ts_code")
        if not ts_code:
            continue
        clear_prices[str(ts_code)] = _to_float(row.get("执行价格(收盘价)"), 0.0)
    return clear_prices


def calculate_trade_win_rate(snapshots: List[Dict]) -> Tuple[float, int, int]:
    """
    按“建仓->清仓”闭环统计胜率：
    - 仅统计从持仓股数>0 变为 0 的清仓事件
    - 清仓盈亏 = (清仓执行价 - 上期持仓成本价) * 上期持仓股数
    """
    if len(snapshots) < 2:
        return 0.0, 0, 0

    total_closed = 0
    win_closed = 0

    prev_holdings = _extract_stock_holdings(snapshots[0])
    for i in range(1, len(snapshots)):
        curr_snapshot = snapshots[i]
        curr_holdings = _extract_stock_holdings(curr_snapshot)
        clear_prices = _extract_clear_prices(curr_snapshot)

        for code, info in prev_holdings.items():
            prev_shares = info.get("shares", 0.0)
            if prev_shares <= 0:
                continue
            curr_shares = curr_holdings.get(code, {}).get("shares", 0.0)
            if curr_shares > 0:
                continue

            # 发生了清仓
            exec_price = clear_prices.get(code, 0.0)
            if exec_price <= 0:
                # 无清仓执行价时，不纳入统计，避免错误判定
                continue
            cost = info.get("cost", 0.0)
            pnl = (exec_price - cost) * prev_shares

            total_closed += 1
            if pnl > 0:
                win_closed += 1

        prev_holdings = curr_holdings

    win_rate = (win_closed / total_closed) if total_closed > 0 else 0.0
    return win_rate, win_closed, total_closed


def fmt_pct(value: float) -> str:
    return f"{value * 100:.2f}%"


def run_for_strategy(
    data_dir: Path,
    strategy_name: str,
    end_date: str | None = None,
) -> Dict[str, float] | None:
    snapshots = read_portfolio_snapshots(data_dir, strategy_name)
    if not snapshots:
        return None

    if end_date:
        snapshots = [s for s in snapshots if str(s.get("trade_date", "")) <= end_date]
        if not snapshots:
            return None

    normalized_snapshots = normalize_snapshots_for_index_only(snapshots)
    # 复用 index_only 计算链路，但静默其内部过程日志，避免污染表格输出
    with contextlib.redirect_stdout(io.StringIO()):
        dates, nav_values, _, _, _ = build_daily_nav_from_holdings(normalized_snapshots, end_date=end_date)
    if len(nav_values) < 2:
        return None

    # 与 index_only 可视化一致：指标计算复用同一函数
    metrics = calculate_performance_metrics(dates, nav_values, sh_nav=[], hs300_nav=[])
    win_rate, win_trades, total_trades = calculate_trade_win_rate(snapshots)
    total_days = (dates[-1] - dates[0]).days if len(dates) >= 2 else 0
    metrics["win_rate"] = win_rate * 100.0
    metrics["win_trades"] = float(win_trades)
    metrics["total_trades"] = float(total_trades)
    metrics["observation_days"] = float(total_days)
    return metrics


def main() -> None:
    parser = argparse.ArgumentParser(description="计算各策略绩效指标（年化、回撤、夏普、胜率）")
    parser.add_argument("--strategy", type=str, default=None, help="指定单个策略名；不传则扫描全部")
    parser.add_argument("--end-date", type=str, default=None, help="统一结束日期(YYYYMMDD)")
    args = parser.parse_args()

    project_root = Path(__file__).parent.parent
    data_dir = project_root / "data"

    strategies = [args.strategy] if args.strategy else discover_strategies(data_dir)
    if not strategies:
        print("未发现可计算的策略目录。")
        return

    aligned_end_date: str | None = args.end_date

    header = (
        f"{'策略':<34} {'总收益':>10} {'年化收益':>10} {'最大回撤':>10} "
        f"{'夏普比率':>10} {'胜率':>10} {'交易胜/总':>12} {'区间天数':>10}"
    )
    line_width = len(header)

    print("=" * line_width)
    print("策略绩效指标（Annualized Return / Max Drawdown / Sharpe / Win Rate）")
    print("口径: 与 portfolio_nav_index_only_visualization 一致")
    if aligned_end_date:
        print(f"统一截止日: {aligned_end_date}")
    print("=" * line_width)
    print(header)
    print("-" * line_width)

    for strategy in strategies:
        metrics = run_for_strategy(
            data_dir=data_dir,
            strategy_name=strategy,
            end_date=aligned_end_date,
        )
        if metrics is None:
            print(
                f"{strategy:<34} {'N/A':>10} {'N/A':>10} {'N/A':>10} {'N/A':>10} "
                f"{'N/A':>10} {'N/A':>12} {'N/A':>10}"
            )
            continue

        total_return = f"{metrics['total_return']:.2f}%"
        annualized_return = f"{metrics['annualized_return']:.2f}%"
        max_drawdown = f"{metrics['max_drawdown']:.2f}%"
        win_rate = f"{metrics['win_rate']:.2f}%"
        trade_win_total = f"{int(metrics['win_trades'])}/{int(metrics['total_trades'])}"

        print(
            f"{strategy:<34} "
            f"{total_return:>10} "
            f"{annualized_return:>10} "
            f"{max_drawdown:>10} "
            f"{metrics['sharpe_ratio']:>10.3f} "
            f"{win_rate:>10} "
            f"{trade_win_total:>12} "
            f"{int(metrics['observation_days']):>10d}"
        )

    print("-" * line_width)
    print("注：胜率按“建仓后最终清仓”为一次闭环交易，并以清仓实现盈亏判定。")


if __name__ == "__main__":
    main()
