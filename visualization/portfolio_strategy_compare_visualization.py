"""
多策略净值对比可视化（逐日回放）

将一个、两个或三个策略的每日净值曲线绘制到同一张图，并叠加上证指数与沪深300。

使用方法:
    python -m visualization.portfolio_strategy_compare_visualization --no-show
    python -m visualization.portfolio_strategy_compare_visualization --strategy-a full_position_decision_63d --label-a 最优组合 --no-show
    python -m visualization.portfolio_strategy_compare_visualization --strategy-a full_position_decision_63d --strategy-b overall_ver1.9
"""
from __future__ import annotations

import argparse
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import matplotlib.dates as mdates
import matplotlib.pyplot as plt

from visualization.portfolio_nav_index_only_visualization import (
    build_daily_nav_from_holdings,
    read_portfolio_snapshots,
)
from visualization.portfolio_nav_visualization import align_sh_index, load_index_data


plt.rcParams["font.sans-serif"] = ["SimSun", "SimHei", "Microsoft YaHei", "Arial Unicode MS", "DejaVu Sans"]
plt.rcParams["axes.unicode_minus"] = False
plt.rcParams["font.size"] = 10.5

STRATEGY_LABEL_MAP: Dict[str, str] = {
    "full_position_decision_63d": "最优组合",
}


def _to_dt(date_str: str) -> datetime:
    return datetime.strptime(date_str, "%Y%m%d")


def _to_ymd(date_obj: datetime) -> str:
    return date_obj.strftime("%Y%m%d")


def _resolve_label(strategy_name: Optional[str], custom_label: Optional[str]) -> Optional[str]:
    if custom_label:
        return custom_label
    if not strategy_name:
        return None
    return STRATEGY_LABEL_MAP.get(strategy_name, strategy_name)


def align_to_common_dates(
    dates_a: List[datetime],
    nav_a: List[float],
    dates_b: List[datetime],
    nav_b: List[float],
) -> Tuple[List[datetime], List[float], List[float], Dict[datetime, float], Dict[datetime, float]]:
    """将两个策略净值序列按日期对齐到共同交易日。"""
    map_a: Dict[datetime, float] = {d: v for d, v in zip(dates_a, nav_a)}
    map_b: Dict[datetime, float] = {d: v for d, v in zip(dates_b, nav_b)}

    common_dates = sorted(set(map_a.keys()) & set(map_b.keys()))
    if not common_dates:
        return [], [], [], map_a, map_b

    common_nav_a = [map_a[d] for d in common_dates]
    common_nav_b = [map_b[d] for d in common_dates]
    return common_dates, common_nav_a, common_nav_b, map_a, map_b


def plot_compare_chart(
    dates: List[datetime],
    nav_a: List[float],
    nav_b: Optional[List[float]],
    nav_c: Optional[List[float]],
    label_a: str,
    label_b: Optional[str],
    label_c: Optional[str],
    sh_label: str,
    hs300_label: str,
    sh_nav: List[float],
    hs300_nav: List[float],
    output_path: Optional[str],
    show_plot: bool,
    dpi: int = 1600,
) -> None:
    if not show_plot:
        plt.switch_backend("Agg")

    fig, ax = plt.subplots(1, 1, figsize=(15, 8))
    ax.plot(dates, nav_a, color="#1f77b4", linewidth=2.4, label=label_a)
    if nav_b:
        ax.plot(dates, nav_b, color="#ff7f0e", linewidth=2.4, label=label_b or "策略B")
    if nav_c:
        ax.plot(dates, nav_c, color="#9467bd", linewidth=2.4, label=label_c or "策略C")
    if sh_nav:
        ax.plot(dates, sh_nav, "r--", linewidth=1.6, alpha=0.85, label=sh_label)
    if hs300_nav:
        ax.plot(dates, hs300_nav, "g--", linewidth=1.6, alpha=0.85, label=hs300_label)

    ax.set_ylabel("净值（初始=1）")
    ax.set_xlabel("日期")
    ax.grid(True, alpha=0.28)
    ax.legend(loc="upper left")
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m-%d"))
    ax.xaxis.set_major_locator(mdates.MonthLocator(interval=2))
    plt.setp(ax.xaxis.get_majorticklabels(), rotation=45, ha="right")
    plt.tight_layout()

    if output_path:
        plt.savefig(output_path, dpi=dpi, bbox_inches="tight")
        print(f"图表已保存: {output_path}")

    if show_plot:
        plt.show()
    else:
        plt.close(fig)


def main(
    strategy_a: str = "full_position_decision_63d",
    strategy_b: Optional[str] = None,
    strategy_c: Optional[str] = None,
    label_a: Optional[str] = None,
    label_b: Optional[str] = None,
    label_c: Optional[str] = None,
    sh_label: str = "上证指数",
    hs300_label: str = "沪深300",
    show_plot: bool = True,
    output_path: Optional[str] = None,
    dpi: int = 300,
    end_date: Optional[str] = None,
) -> None:
    project_root = Path(__file__).parent.parent
    data_dir = project_root / "data"

    print("=" * 70)
    title = f"策略净值可视化: {strategy_a}"
    if strategy_b:
        title = f"{title} vs {strategy_b}"
    if strategy_c:
        title = f"{title} vs {strategy_c}"
    print(title)
    print("=" * 70)

    snapshots_a = read_portfolio_snapshots(str(data_dir), strategy_a)
    snapshots_b = read_portfolio_snapshots(str(data_dir), strategy_b) if strategy_b else []
    if not snapshots_a:
        raise FileNotFoundError(f"未找到策略 {strategy_a} 的持仓快照")
    if strategy_b and not snapshots_b:
        raise FileNotFoundError(f"未找到策略 {strategy_b} 的持仓快照")
    snapshots_c = read_portfolio_snapshots(str(data_dir), strategy_c) if strategy_c else []
    if strategy_c and not snapshots_c:
        raise FileNotFoundError(f"未找到策略 {strategy_c} 的持仓快照")

    dates_a, nav_a, _, start_a, end_a = build_daily_nav_from_holdings(snapshots_a, end_date=end_date)
    dates_b: List[datetime] = []
    nav_b: List[float] = []
    start_b = end_b = ""
    if snapshots_b:
        dates_b, nav_b, _, start_b, end_b = build_daily_nav_from_holdings(snapshots_b, end_date=end_date)
    if not dates_a or (strategy_b and not dates_b):
        raise RuntimeError("至少一个策略未能生成每日净值")
    dates_c: List[datetime] = []
    nav_c: List[float] = []
    start_c = end_c = ""
    if snapshots_c:
        dates_c, nav_c, _, start_c, end_c = build_daily_nav_from_holdings(snapshots_c, end_date=end_date)
        if not dates_c:
            raise RuntimeError(f"策略 {strategy_c} 未能生成每日净值")

    common_dates = dates_a
    common_nav_a = nav_a
    common_nav_b: Optional[List[float]] = None
    map_a = {d: v for d, v in zip(dates_a, nav_a)}
    map_b: Dict[datetime, float] = {}
    if strategy_b:
        common_dates, common_nav_a, nav_b_aligned, map_a, map_b = align_to_common_dates(dates_a, nav_a, dates_b, nav_b)
        if not common_dates:
            raise RuntimeError("两个策略没有共同交易日，无法进行同图比较")
        common_nav_b = nav_b_aligned
    common_nav_c: Optional[List[float]] = None
    if nav_c:
        map_c = {d: v for d, v in zip(dates_c, nav_c)}
        common_dates = sorted(set(common_dates) & set(map_c.keys()))
        if not common_dates:
            if strategy_b:
                raise RuntimeError("三个策略没有共同交易日，无法进行同图比较")
            raise RuntimeError("两个策略没有共同交易日，无法进行同图比较")
        common_nav_a = [map_a[d] for d in common_dates]
        if strategy_b:
            common_nav_b = [map_b[d] for d in common_dates]
        common_nav_c = [map_c[d] for d in common_dates]

    common_start = _to_ymd(common_dates[0])
    common_end = _to_ymd(common_dates[-1])
    print(f"{strategy_a} 数据区间: {start_a} ~ {end_a}，交易日数: {len(dates_a)}")
    if strategy_b:
        print(f"{strategy_b} 数据区间: {start_b} ~ {end_b}，交易日数: {len(dates_b)}")
    if strategy_c:
        print(f"{strategy_c} 数据区间: {start_c} ~ {end_c}，交易日数: {len(dates_c)}")
    if strategy_b or strategy_c:
        print(f"共同对比区间: {common_start} ~ {common_end}，交易日数: {len(common_dates)}")
    else:
        print(f"可视化区间: {common_start} ~ {common_end}，交易日数: {len(common_dates)}")

    sh_index_data = load_index_data("000001.SH", start_date=common_start, end_date=common_end)
    hs300_index_data = load_index_data("000300.SH", start_date=common_start, end_date=common_end)
    _, sh_nav = align_sh_index(common_dates, common_nav_a, sh_index_data)
    _, hs300_nav = align_sh_index(common_dates, common_nav_a, hs300_index_data)

    if output_path:
        final_output_path = Path(output_path)
    else:
        safe_a = strategy_a.replace(".", "_")
        if strategy_b and strategy_c:
            safe_c = strategy_c.replace(".", "_")
            safe_b = strategy_b.replace(".", "_")
            final_output_path = data_dir / "visualization" / f"portfolio_compare_{safe_a}_vs_{safe_b}_vs_{safe_c}.png"
        elif strategy_b:
            safe_b = strategy_b.replace(".", "_")
            final_output_path = data_dir / "visualization" / f"portfolio_compare_{safe_a}_vs_{safe_b}.png"
        else:
            final_output_path = data_dir / "visualization" / f"portfolio_{safe_a}.png"
    final_output_path.parent.mkdir(parents=True, exist_ok=True)

    plot_compare_chart(
        dates=common_dates,
        nav_a=common_nav_a,
        nav_b=common_nav_b,
        nav_c=common_nav_c,
        label_a=_resolve_label(strategy_a, label_a) or strategy_a,
        label_b=_resolve_label(strategy_b, label_b),
        label_c=_resolve_label(strategy_c, label_c),
        sh_label=sh_label,
        hs300_label=hs300_label,
        sh_nav=sh_nav,
        hs300_nav=hs300_nav,
        output_path=str(final_output_path),
        show_plot=show_plot,
        dpi=dpi,
    )
    print(f"输出文件: {final_output_path}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="多策略净值对比可视化（逐日回放）")
    parser.add_argument("--strategy-a", type=str, default="full_position_decision_63d", help="策略A名称")
    parser.add_argument("--strategy-b", type=str, default=None, help="策略B名称（可选）")
    parser.add_argument("--strategy-c", type=str, default=None, help="策略C名称（可选）")
    parser.add_argument("--label-a", type=str, default=None, help="策略A曲线显示名（可选）")
    parser.add_argument("--label-b", type=str, default=None, help="策略B曲线显示名（可选）")
    parser.add_argument("--label-c", type=str, default=None, help="策略C曲线显示名（可选）")
    parser.add_argument("--label-sh", type=str, default="上证指数", help="上证曲线显示名")
    parser.add_argument("--label-hs300", type=str, default="沪深300", help="沪深300曲线显示名")
    parser.add_argument("--no-show", action="store_true", help="不显示图表，仅保存文件")
    parser.add_argument("--output", "-o", type=str, default=None, help="输出文件路径")
    parser.add_argument("--dpi", type=int, default=300, help="保存图片DPI（默认300）")
    parser.add_argument("--end-date", type=str, default=None, help="结束日期(YYYYMMDD)")
    args = parser.parse_args()

    if args.end_date:
        try:
            datetime.strptime(args.end_date, "%Y%m%d")
        except ValueError as exc:
            raise ValueError("参数 --end-date 格式错误，请使用 YYYYMMDD") from exc

    main(
        strategy_a=args.strategy_a,
        strategy_b=args.strategy_b,
        strategy_c=args.strategy_c,
        label_a=args.label_a,
        label_b=args.label_b,
        label_c=args.label_c,
        sh_label=args.label_sh,
        hs300_label=args.label_hs300,
        show_plot=not args.no_show,
        output_path=args.output,
        dpi=args.dpi,
        end_date=args.end_date,
    )
