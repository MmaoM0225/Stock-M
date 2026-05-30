"""
投资组合净值可视化（按持仓逐日回放）

核心逻辑：
1) 读取每个决策日的持仓（含持仓股数、现金）
2) 在相邻决策日区间内，持仓股数保持不变
3) 通过请求个股日线收盘价，计算每天组合总资产与净值
4) 叠加上证指数与沪深300净值曲线

使用方法:
    python -m visualization.portfolio_nav_index_only_visualization
    python -m visualization.portfolio_nav_index_only_visualization --no-show
    python -m visualization.portfolio_nav_index_only_visualization --strategy full_position_decision
    python -m visualization.portfolio_nav_index_only_visualization -s full_position_decision -o custom.png
"""
from __future__ import annotations

import argparse
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import matplotlib.dates as mdates
import matplotlib.pyplot as plt

from dataflow.kline_data import KLineDataFetcher
from visualization.portfolio_nav_visualization import (
    align_sh_index,
    calculate_performance_metrics,
    load_index_data,
)


plt.rcParams["font.sans-serif"] = ["SimHei", "Microsoft YaHei", "Arial Unicode MS", "DejaVu Sans"]
plt.rcParams["axes.unicode_minus"] = False


def read_portfolio_snapshots(data_dir: str, strategy_name: str) -> List[Dict[str, Any]]:
    """读取决策日持仓快照（包含持仓股数与现金）。"""
    portfolio_dir = Path(data_dir) / "artifacts" / "decision" / strategy_name / "portfolio"
    if not portfolio_dir.exists():
        raise FileNotFoundError(f"Portfolio 目录不存在: {portfolio_dir}")

    snapshots: List[Dict[str, Any]] = []
    for date_dir in sorted(portfolio_dir.iterdir()):
        if not date_dir.is_dir():
            continue
        result_file = date_dir / "result.json"
        if not result_file.exists():
            continue
        try:
            import json

            with open(result_file, "r", encoding="utf-8") as f:
                data = json.load(f)
            snapshots.append(
                {
                    "trade_date": data.get("trade_date"),
                    "portfolio_table": data.get("portfolio_table", []),
                    "initial_capital": float(data.get("meta", {}).get("initial_capital", 0) or 0),
                    "total_capital": float(data.get("meta", {}).get("total_capital", 0) or 0),
                }
            )
        except Exception as exc:
            print(f"警告: 读取失败 {result_file}: {exc}")
    snapshots = [x for x in snapshots if x.get("trade_date")]
    return sorted(snapshots, key=lambda x: str(x["trade_date"]))


def _to_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None:
            return default
        if isinstance(value, str):
            text = value.strip()
            if text in {"", "-"}:
                return default
            return float(text.replace(",", ""))
        return float(value)
    except Exception:
        return default


def extract_holdings(snapshot: Dict[str, Any]) -> Tuple[Dict[str, float], float]:
    """
    从单个决策快照提取：
    - shares_map: {ts_code: 持仓股数}
    - cash_amount: 现金金额
    """
    shares_map: Dict[str, float] = {}
    cash_amount = 0.0
    total_capital = _to_float(snapshot.get("total_capital"), 0.0)

    for row in snapshot.get("portfolio_table", []):
        asset_name = str(row.get("资产名称") or "")
        asset_type = str(row.get("资产类型") or "")
        ts_code = row.get("ts_code")
        shares = _to_float(row.get("持仓股数"), 0.0)

        if asset_type == "其他" or asset_name == "待投资现金" or not ts_code:
            cash_amount = _to_float(row.get("市值 (元)"), 0.0)
            continue

        if shares > 0:
            shares_map[str(ts_code)] = shares

    if cash_amount <= 0 and total_capital > 0:
        stock_value = 0.0
        for row in snapshot.get("portfolio_table", []):
            if row.get("ts_code"):
                stock_value += _to_float(row.get("市值 (元)"), 0.0)
        cash_amount = max(total_capital - stock_value, 0.0)

    return shares_map, cash_amount


def fetch_interval_stock_prices(
    fetcher: KLineDataFetcher, ts_codes: List[str], start_date: str, end_date: str
) -> Dict[str, Dict[str, float]]:
    """按单个调仓区间请求持仓股票日线收盘价。"""
    price_map: Dict[str, Dict[str, float]] = {}

    for code in sorted(set(ts_codes)):
        try:
            df = fetcher.fetch_daily_data(ts_code=code, start_date=start_date, end_date=end_date, adj="qfq")
            if df.empty:
                print(f"警告: {code} 未获取到日线数据")
                price_map[code] = {}
                continue
            code_map: Dict[str, float] = {}
            for _, row in df.iterrows():
                d = str(row["trade_date"])
                code_map[d] = float(row["close"])
            price_map[code] = code_map
        except Exception as exc:
            print(f"警告: 获取 {code} 失败: {exc}")
            price_map[code] = {}

    return price_map


def build_daily_nav_from_holdings(
    snapshots: List[Dict[str, Any]], end_date: Optional[str] = None
) -> Tuple[List[datetime], List[float], List[float], str, str]:
    """按调仓区间+持仓股数回放每日净值。"""
    if not snapshots:
        return [], [], [], "", ""

    decision_dates = [str(x["trade_date"]) for x in snapshots]
    start_date = decision_dates[0]
    latest_decision_date = decision_dates[-1]
    # 若传入 end_date 且晚于最后调仓日，则按最后一期持仓继续回放到 end_date
    effective_end_date = end_date or latest_decision_date

    if effective_end_date < start_date:
        raise ValueError(f"结束日期 {effective_end_date} 早于起始调仓日 {start_date}")

    fetcher = KLineDataFetcher()
    interval_price_maps: List[Dict[str, Dict[str, float]]] = []
    total_interval_codes = 0

    for idx, snap in enumerate(snapshots):
        interval_start = decision_dates[idx]
        interval_end = decision_dates[idx + 1] if idx + 1 < len(decision_dates) else effective_end_date
        interval_end = min(interval_end, effective_end_date)
        if interval_start > effective_end_date:
            interval_price_maps.append({})
            continue
        interval_shares, _ = extract_holdings(snap)
        interval_codes = list(interval_shares.keys())
        total_interval_codes += len(interval_codes)
        if interval_codes:
            print(
                f"请求区间行情: {interval_start} ~ {interval_end}，持仓股票数: {len(interval_codes)}"
            )
            interval_price_maps.append(
                fetch_interval_stock_prices(fetcher, interval_codes, interval_start, interval_end)
            )
        else:
            interval_price_maps.append({})
    print(f"分段请求完成，共 {len(snapshots)} 个区间，累计持仓请求次数: {total_interval_codes}")

    # 交易日历使用上证指数交易日，保证连续且稳定
    sh_index_data = load_index_data("000001.SH", start_date=start_date, end_date=effective_end_date)
    trading_dates = sorted(sh_index_data.keys())
    if not trading_dates:
        raise RuntimeError("无法获取交易日历（上证指数数据为空）")

    # 每日净值回放
    daily_total_capitals: List[float] = []
    daily_dates: List[datetime] = []

    snapshot_idx = 0
    current_shares, current_cash = extract_holdings(snapshots[snapshot_idx])
    current_price_map = interval_price_maps[snapshot_idx] if interval_price_maps else {}
    fallback_total = _to_float(snapshots[snapshot_idx].get("total_capital"), 0.0)

    next_decision_date = decision_dates[snapshot_idx + 1] if snapshot_idx + 1 < len(decision_dates) else None

    for d in trading_dates:
        # 到达下个决策日后，切换到新持仓（当日按新持仓估值）
        while next_decision_date and d >= next_decision_date:
            snapshot_idx += 1
            current_shares, current_cash = extract_holdings(snapshots[snapshot_idx])
            current_price_map = interval_price_maps[snapshot_idx] if snapshot_idx < len(interval_price_maps) else {}
            fallback_total = _to_float(snapshots[snapshot_idx].get("total_capital"), fallback_total)
            next_decision_date = decision_dates[snapshot_idx + 1] if snapshot_idx + 1 < len(decision_dates) else None

        stock_total = 0.0
        has_price = False
        for code, shares in current_shares.items():
            code_prices = current_price_map.get(code, {})
            if d in code_prices:
                stock_total += shares * code_prices[d]
                has_price = True

        total = current_cash + stock_total
        if total <= 0 and fallback_total > 0:
            total = fallback_total
        if not has_price and fallback_total > 0:
            total = fallback_total

        daily_dates.append(datetime.strptime(d, "%Y%m%d"))
        daily_total_capitals.append(total)

    # 使用策略初始资金作为净值基准，允许首日净值不等于1
    base_capital = _to_float(snapshots[0].get("initial_capital"), 0.0)
    if base_capital <= 0:
        base_capital = daily_total_capitals[0] if daily_total_capitals else 1.0
    if base_capital <= 0:
        base_capital = 1.0

    daily_nav = [x / base_capital for x in daily_total_capitals]
    return daily_dates, daily_nav, daily_total_capitals, start_date, effective_end_date


def find_max_drawdown_interval(nav_values: List[float]) -> Optional[Tuple[int, int, float]]:
    """返回最大回撤区间 (peak_idx, trough_idx, drawdown_ratio)。"""
    if len(nav_values) < 2:
        return None

    peak_idx = 0
    peak_val = nav_values[0]
    max_dd = 0.0
    best_peak = 0
    best_trough = 0

    for i in range(1, len(nav_values)):
        value = nav_values[i]
        if value > peak_val:
            peak_val = value
            peak_idx = i
            continue

        if peak_val > 0:
            dd = (peak_val - value) / peak_val
            if dd > max_dd:
                max_dd = dd
                best_peak = peak_idx
                best_trough = i

    if max_dd <= 0:
        return None
    return best_peak, best_trough, max_dd


def plot_nav_only_chart(
    dates_nav: List[datetime],
    nav_values: List[float],
    sh_nav: List[float],
    hs300_nav: Optional[List[float]],
    total_capitals: List[float],
    metrics: Dict[str, float],
    output_path: Optional[str] = None,
    show_plot: bool = True,
    strategy_name: str = "default",
) -> None:
    if not show_plot:
        plt.switch_backend("Agg")

    fig, ax = plt.subplots(1, 1, figsize=(14, 7))

    ax.plot(dates_nav, nav_values, "b-", linewidth=2.2, label="投资组合净值(逐日回放)")
    if sh_nav:
        ax.plot(dates_nav, sh_nav, "r--", linewidth=1.4, label="上证指数", alpha=0.8)
    if hs300_nav:
        ax.plot(dates_nav, hs300_nav, "g--", linewidth=1.4, label="沪深300", alpha=0.8)

    # 最大回撤区间标注（峰值 -> 谷值）
    drawdown_info = find_max_drawdown_interval(nav_values)
    if drawdown_info:
        peak_idx, trough_idx, dd_ratio = drawdown_info
        peak_date = dates_nav[peak_idx]
        trough_date = dates_nav[trough_idx]
        peak_nav = nav_values[peak_idx]
        trough_nav = nav_values[trough_idx]

        ax.axvspan(peak_date, trough_date, color="orange", alpha=0.15, label="最大回撤区间")
        ax.scatter([peak_date], [peak_nav], color="darkorange", s=30, zorder=5)
        ax.scatter([trough_date], [trough_nav], color="red", s=30, zorder=5)

        mid_idx = (peak_idx + trough_idx) // 2
        ax.text(
            dates_nav[mid_idx],
            min(peak_nav, trough_nav),
            f"最大回撤 {dd_ratio * 100:.2f}%",
            fontsize=9,
            color="darkred",
            ha="center",
            va="top",
            bbox=dict(boxstyle="round", facecolor="white", alpha=0.6),
        )

    ax.set_ylabel("净值（初始=1）", fontsize=12)
    ax.set_title(f"[{strategy_name}] 投资组合每日净值 vs 上证/沪深300", fontsize=14, fontweight="bold")
    ax.grid(True, alpha=0.3)
    ax.legend(loc="upper left", fontsize=10)
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m-%d"))
    ax.xaxis.set_major_locator(mdates.WeekdayLocator(interval=2))
    plt.setp(ax.xaxis.get_majorticklabels(), rotation=45, ha="right")

    metrics_text = f"""业绩指标:
组合收益: {metrics.get('total_return', 0):.2f}%
上证收益: {metrics.get('sh_total_return', 0):.2f}%
沪深300收益: {metrics.get('hs300_total_return', 0):.2f}%
超额收益(相对上证): {metrics.get('excess_return', 0):.2f}%
超额收益(相对沪深300): {metrics.get('excess_return_hs300', 0):.2f}%
最大回撤: {metrics.get('max_drawdown', 0):.2f}%
年化收益: {metrics.get('annualized_return', 0):.2f}%
夏普比率: {metrics.get('sharpe_ratio', 0):.2f}"""

    ax.text(
        0.98,
        0.02,
        metrics_text,
        transform=ax.transAxes,
        fontsize=9,
        verticalalignment="bottom",
        horizontalalignment="right",
        bbox=dict(boxstyle="round", facecolor="wheat", alpha=0.5),
    )

    if total_capitals:
        capital_text = f"资金: {total_capitals[0]/10000:.1f}万 -> {total_capitals[-1]/10000:.1f}万"
        ax.text(
            0.02,
            0.98,
            capital_text,
            transform=ax.transAxes,
            fontsize=9,
            verticalalignment="top",
            horizontalalignment="left",
            bbox=dict(boxstyle="round", facecolor="lightblue", alpha=0.5),
        )

    plt.tight_layout()
    if output_path:
        plt.savefig(output_path, dpi=150, bbox_inches="tight")
        print(f"图表已保存: {output_path}")
    if show_plot:
        plt.show()
    else:
        plt.close(fig)


def main(
    show_plot: bool = True,
    strategy_name: str = "full_position_decision",
    output_path: Optional[str] = None,
    end_date: Optional[str] = None,
) -> None:
    script_dir = Path(__file__).parent
    data_dir = script_dir.parent / "data"

    print("=" * 60)
    print(f"净值对比可视化（按持仓逐日回放）- 策略: {strategy_name}")
    print("=" * 60)

    snapshots = read_portfolio_snapshots(str(data_dir), strategy_name)
    if not snapshots:
        print(f"未找到任何 {strategy_name} 持仓快照")
        return
    if end_date:
        snapshots = [s for s in snapshots if str(s.get("trade_date", "")) <= end_date]
        if not snapshots:
            print(f"结束日期 {end_date} 之前未找到任何 {strategy_name} 持仓快照")
            return
    print(f"成功读取 {len(snapshots)} 个决策日快照")

    daily_dates, daily_nav, daily_total_capitals, start_date, end_date = build_daily_nav_from_holdings(
        snapshots, end_date=end_date
    )
    if not daily_dates:
        print("未生成有效的每日净值数据")
        return

    print(f"每日净值区间: {start_date} ~ {end_date}，共 {len(daily_dates)} 个交易日")

    print("\n加载指数数据...")
    sh_index_data = load_index_data("000001.SH", start_date=start_date, end_date=end_date)
    _, sh_nav = align_sh_index(daily_dates, daily_nav, sh_index_data)

    hs300_data = load_index_data("000300.SH", start_date=start_date, end_date=end_date)
    _, hs300_nav = align_sh_index(daily_dates, daily_nav, hs300_data)

    metrics = calculate_performance_metrics(daily_dates, daily_nav, sh_nav, hs300_nav)

    print("\n业绩指标:")
    print(f"  组合总收益率: {metrics.get('total_return', 0):.2f}%")
    print(f"  上证总收益率: {metrics.get('sh_total_return', 0):.2f}%")
    print(f"  沪深300收益率: {metrics.get('hs300_total_return', 0):.2f}%")
    print(f"  超额收益(相对上证): {metrics.get('excess_return', 0):.2f}%")
    print(f"  超额收益(相对沪深300): {metrics.get('excess_return_hs300', 0):.2f}%")
    print(f"  最大回撤: {metrics.get('max_drawdown', 0):.2f}%")

    print("\n生成可视化图表...")
    if output_path:
        final_output_path = Path(output_path)
    else:
        safe_strategy_name = strategy_name.replace(".", "_")
        final_output_path = data_dir / "visualization" / f"portfolio_{safe_strategy_name}_daily_nav_chart.png"
    final_output_path.parent.mkdir(parents=True, exist_ok=True)

    plot_nav_only_chart(
        dates_nav=daily_dates,
        nav_values=daily_nav,
        sh_nav=sh_nav,
        hs300_nav=hs300_nav,
        total_capitals=daily_total_capitals,
        metrics=metrics,
        output_path=str(final_output_path),
        show_plot=show_plot,
        strategy_name=strategy_name,
    )

    print(f"图表文件: {final_output_path}")
    print("\n完成!")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="投资组合净值可视化（按持仓逐日回放）")
    parser.add_argument("--no-show", action="store_true", help="不显示图表，仅保存到文件")
    parser.add_argument("--output", "-o", type=str, default=None, help="输出文件路径")
    parser.add_argument("--strategy", "-s", type=str, default="full_position_decision", help="策略名称")
    parser.add_argument("--end-date", type=str, default=None, help="结束日期(YYYYMMDD)")
    args = parser.parse_args()

    if args.end_date:
        try:
            datetime.strptime(args.end_date, "%Y%m%d")
        except ValueError:
            raise ValueError("参数 --end-date 格式错误，请使用 YYYYMMDD")

    main(
        show_plot=not args.no_show,
        strategy_name=args.strategy,
        output_path=args.output,
        end_date=args.end_date,
    )
