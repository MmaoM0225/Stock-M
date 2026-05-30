"""
投资组合净值可视化程序
读取历史 portfolio 数据，绘制资金净值曲线并与上证指数对比

使用方法:
    python -m visualization.portfolio_nav_visualization                    # 可视化默认策略 (2025_7d_for_once)
    python -m visualization.portfolio_nav_visualization --no-show          # 仅保存不显示
    python -m visualization.portfolio_nav_visualization --strategy NAME    # 指定策略名称，如 2025_7d_for_once_ver1.1
    python -m visualization.portfolio_nav_visualization -s 2025_7d_for_once_ver1.1 -o custom.png  # 指定策略和输出文件
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Tuple, Optional

import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from matplotlib import font_manager
import numpy as np

# 导入数据获取模块
sys.path.insert(0, str(Path(__file__).parent.parent))
from dataflow.kline_data import KLineDataFetcher
from dataflow.config import DATA_SOURCES


# 设置中文字体
plt.rcParams['font.sans-serif'] = ['SimHei', 'Microsoft YaHei', 'Arial Unicode MS', 'DejaVu Sans']
plt.rcParams['axes.unicode_minus'] = False


def parse_position_percentage(position_str: str) -> float:
    """解析仓位百分比字符串，如 '8.18%' -> 8.18"""
    if isinstance(position_str, (int, float)):
        return float(position_str)
    if isinstance(position_str, str):
        return float(position_str.replace("%", ""))
    return 0.0


def read_portfolio_history(data_dir: str, strategy_name: str = "2025_7d_for_once") -> List[Dict]:
    """读取所有历史 portfolio 数据，包含仓位信息
    
    Args:
        data_dir: 数据根目录
        strategy_name: 策略名称，默认为 "2025_7d_for_once"
    """
    history = []
    portfolio_dir = Path(data_dir) / "artifacts" / "decision" / strategy_name / "portfolio"
    
    if not portfolio_dir.exists():
        raise FileNotFoundError(f"Portfolio 目录不存在: {portfolio_dir}")
    
    for date_dir in sorted(portfolio_dir.iterdir()):
        if date_dir.is_dir():
            result_file = date_dir / "result.json"
            if result_file.exists():
                with open(result_file, "r", encoding="utf-8") as f:
                    data = json.load(f)
                    portfolio_table = data.get("portfolio_table", [])
                    
                    # 计算仓位（非现金仓位 = 总仓位 - 现金仓位）
                    cash_position = 0.0
                    stock_position = 0.0
                    
                    for asset in portfolio_table:
                        position_str = asset.get("仓位", "0%")
                        position = parse_position_percentage(position_str)
                        asset_type = asset.get("资产类型", "")
                        asset_name = asset.get("资产名称", "")
                        
                        if asset_type == "其他" or asset_name == "待投资现金":
                            cash_position = position
                        else:
                            stock_position += position
                    
                    history.append({
                        "trade_date": data.get("trade_date"),
                        "total_capital": data.get("meta", {}).get("total_capital", 0),
                        "initial_capital": data.get("meta", {}).get("initial_capital", 0),
                        "stock_position": stock_position,
                        "cash_position": cash_position,
                        "total_position": stock_position + cash_position,
                    })
    
    return sorted(history, key=lambda x: x["trade_date"])


def calculate_nav(history: List[Dict]) -> Tuple[List[datetime], List[float], List[float]]:
    """计算净值曲线
    
    返回:
        dates: 日期列表
        nav_values: 净值列表（以初始资金为1）
        total_capitals: 总资金列表
    """
    dates = []
    nav_values = []
    total_capitals = []
    
    if not history:
        return dates, nav_values, total_capitals
    
    # 使用第一个记录的 initial_capital 作为基准
    base_capital = history[0].get("initial_capital", history[0].get("total_capital", 1))
    if base_capital == 0:
        base_capital = 1
    
    for record in history:
        date_str = record["trade_date"]
        date = datetime.strptime(date_str, "%Y%m%d")
        total_capital = record.get("total_capital", 0)
        
        dates.append(date)
        total_capitals.append(total_capital)
        nav_values.append(total_capital / base_capital)
    
    return dates, nav_values, total_capitals


def load_index_data(ts_code: str, start_date: str = None, end_date: str = None) -> Dict[str, float]:
    """加载指数数据
    
    从 Tushare 获取真实的指数日线数据
    
    Args:
        ts_code: 指数代码，如 000001.SH（上证指数）、000300.SH（沪深300）
        start_date: 开始日期，格式 YYYYMMDD（可选，默认从投资组合最早日期）
        end_date: 结束日期，格式 YYYYMMDD（可选，默认到投资组合最晚日期）
    
    返回: {日期字符串: 收盘点位} 的字典
    """
    # 如果未启用 Tushare，返回空字典（后续会处理为None显示警告）
    if not DATA_SOURCES.get('tushare', {}).get('enabled', False):
        print(f"警告: Tushare 未启用，无法获取 {ts_code} 数据")
        return {}
    
    try:
        fetcher = KLineDataFetcher()
        
        # 获取指数日线数据
        df = fetcher.fetch_index_daily_data(
            ts_code=ts_code,
            start_date=start_date,
            end_date=end_date
        )
        
        if df.empty:
            print(f"警告: 未获取到 {ts_code} 数据")
            return {}
        
        # 转换为字典 {日期: 收盘点位}
        result = {}
        for _, row in df.iterrows():
            date_str = str(row['trade_date'])
            close_price = float(row['close'])
            result[date_str] = close_price
        
        index_name = "上证指数" if ts_code == "000001.SH" else ("沪深300" if ts_code == "000300.SH" else ts_code)
        print(f"成功获取 {index_name} 数据: {len(result)} 条记录")
        if result:
            dates = sorted(result.keys())
            print(f"  数据范围: {dates[0]} ~ {dates[-1]}")
        
        return result
        
    except Exception as e:
        print(f"获取 {ts_code} 数据失败: {e}")
        return {}


# 为了保持向后兼容，保留旧函数名作为别名
def load_sh_index_data(start_date: str = None, end_date: str = None) -> Dict[str, float]:
    """加载上证指数数据（向后兼容的别名）"""
    return load_index_data("000001.SH", start_date, end_date)


def align_sh_index(
    portfolio_dates: List[datetime], 
    portfolio_nav: List[float],
    sh_index_data: Dict[str, float]
) -> Tuple[List[float], List[float]]:
    """对齐上证指数数据与组合净值数据
    
    以上证指数首个交易日的值为基准，计算相对涨跌幅
    """
    sh_values = []
    sh_nav = []
    
    # 找到第一个有效数据点作为基准
    base_value = None
    for date in portfolio_dates:
        date_str = date.strftime("%Y%m%d")
        if date_str in sh_index_data:
            base_value = sh_index_data[date_str]
            break
    
    if base_value is None or base_value == 0:
        base_value = 3300.0  # 默认值
    
    for date in portfolio_dates:
        date_str = date.strftime("%Y%m%d")
        if date_str in sh_index_data:
            value = sh_index_data[date_str]
            sh_values.append(value)
            sh_nav.append(value / base_value)
        else:
            # 如果当天没有数据，使用前一个值或None
            if sh_nav:
                sh_nav.append(sh_nav[-1])
            else:
                sh_nav.append(1.0)
    
    return sh_values, sh_nav


def calculate_performance_metrics(
    dates: List[datetime],
    nav_values: List[float],
    sh_nav: List[float],
    hs300_nav: Optional[List[float]] = None
) -> Dict:
    """计算业绩指标
    
    Args:
        dates: 日期列表
        nav_values: 投资组合净值列表
        sh_nav: 上证指数净值列表
        hs300_nav: 沪深300净值列表（可选）
    
    Returns:
        包含各项业绩指标的字典
    """
    metrics = {}
    
    if len(nav_values) < 2:
        return metrics
    
    # 总收益率
    metrics["total_return"] = (nav_values[-1] - 1) * 100
    
    # 上证指数收益率
    metrics["sh_total_return"] = (sh_nav[-1] - 1) * 100 if sh_nav else 0
    
    # 沪深300收益率
    if hs300_nav:
        metrics["hs300_total_return"] = (hs300_nav[-1] - 1) * 100
    else:
        metrics["hs300_total_return"] = 0
    
    # 超额收益（相对上证指数）
    metrics["excess_return"] = metrics["total_return"] - metrics["sh_total_return"]
    
    # 超额收益（相对沪深300）
    metrics["excess_return_hs300"] = metrics["total_return"] - metrics["hs300_total_return"]
    
    # 计算日收益率序列
    daily_returns = []
    for i in range(1, len(nav_values)):
        daily_returns.append((nav_values[i] - nav_values[i-1]) / nav_values[i-1])
    
    # 年化收益率（简化计算）
    days = (dates[-1] - dates[0]).days
    if days > 0:
        metrics["annualized_return"] = ((nav_values[-1] / nav_values[0]) ** (365.0 / days) - 1) * 100
    
    # 最大回撤
    peak = nav_values[0]
    max_drawdown = 0
    for nav in nav_values:
        if nav > peak:
            peak = nav
        drawdown = (peak - nav) / peak
        max_drawdown = max(max_drawdown, drawdown)
    metrics["max_drawdown"] = max_drawdown * 100
    
    # 波动率（年化）
    if daily_returns:
        volatility = np.std(daily_returns) * np.sqrt(252) * 100
        metrics["volatility"] = volatility
    
    # 夏普比率（简化，假设无风险利率为2%）
    if "annualized_return" in metrics and "volatility" in metrics:
        risk_free_rate = 2.0
        if metrics["volatility"] > 0:
            metrics["sharpe_ratio"] = (metrics["annualized_return"] - risk_free_rate) / metrics["volatility"]
    
    return metrics


def plot_combined_chart(
    dates_nav: List[datetime],
    nav_values: List[float],
    sh_nav: List[float],
    dates_position: List[datetime],
    stock_positions: List[float],
    cash_positions: List[float],
    total_capitals: List[float],
    metrics: Dict,
    output_path: Optional[str] = None,
    show_plot: bool = True,
    hs300_nav: Optional[List[float]] = None,
    strategy_name: str = "default"
):
    """绘制组合图表：上面收益对比，下面仓位变化
    
    Args:
        dates_nav: 净值日期列表
        nav_values: 净值列表
        sh_nav: 上证指数净值列表
        dates_position: 仓位日期列表
        stock_positions: 股票仓位列表（%）
        cash_positions: 现金仓位列表（%）
        total_capitals: 总资金列表
        metrics: 业绩指标字典
        output_path: 图表保存路径
        show_plot: 是否显示图表（在后台模式下设为False）
        hs300_nav: 沪深300净值列表（可选）
        strategy_name: 策略名称，用于图表标题
    """
    # 在无图形界面环境下使用非交互式后端
    if not show_plot:
        plt.switch_backend('Agg')
    
    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(14, 10), gridspec_kw={'height_ratios': [2, 1]})
    
    # ========== 子图1: 收益对比（净值）==========
    ax1.plot(dates_nav, nav_values, 'b-', linewidth=2.5, label='投资组合净值', marker='o', markersize=5)
    if sh_nav:
        ax1.plot(dates_nav, sh_nav, 'r--', linewidth=1.5, label='上证指数', marker='s', markersize=3, alpha=0.7)
    if hs300_nav:
        ax1.plot(dates_nav, hs300_nav, 'g--', linewidth=1.5, label='沪深300', marker='^', markersize=3, alpha=0.7)
    
    ax1.set_ylabel('净值（初始=1）', fontsize=12)
    title = f'[{strategy_name}] 投资组合净值走势 vs 上证指数'
    if hs300_nav:
        title = f'[{strategy_name}] 投资组合净值走势 vs 上证指数/沪深300'
    ax1.set_title(title, fontsize=14, fontweight='bold')
    ax1.legend(loc='upper left', fontsize=10)
    ax1.grid(True, alpha=0.3)
    ax1.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
    ax1.xaxis.set_major_locator(mdates.WeekdayLocator(interval=2))
    plt.setp(ax1.xaxis.get_majorticklabels(), rotation=45, ha='right')
    
    # 添加业绩指标文本框（移到右下角，避免与图例重叠）
    metrics_text = f"""业绩指标:
组合收益: {metrics.get('total_return', 0):.2f}%
上证收益: {metrics.get('sh_total_return', 0):.2f}%
沪深300收益: {metrics.get('hs300_total_return', 0):.2f}%
超额收益(相对上证): {metrics.get('excess_return', 0):.2f}%
超额收益(相对沪深300): {metrics.get('excess_return_hs300', 0):.2f}%
最大回撤: {metrics.get('max_drawdown', 0):.2f}%
年化收益: {metrics.get('annualized_return', 0):.2f}%
夏普比率: {metrics.get('sharpe_ratio', 0):.2f}"""
    
    ax1.text(0.98, 0.02, metrics_text, transform=ax1.transAxes, fontsize=9,
             verticalalignment='bottom', horizontalalignment='right',
             bbox=dict(boxstyle='round', facecolor='wheat', alpha=0.5))
    
    # ========== 子图2: 仓位变化 ==========
    ax2.fill_between(dates_position, stock_positions, alpha=0.3, color='blue', label='股票仓位')
    ax2.plot(dates_position, stock_positions, 'b-', linewidth=2, marker='o', markersize=4)
    ax2.plot(dates_position, cash_positions, 'g--', linewidth=1.5, label='现金仓位', marker='s', markersize=3, alpha=0.7)
    
    # 添加仓位区域标注
    ax2.fill_between(dates_position, 0, stock_positions, alpha=0.1, color='blue')
    
    ax2.set_ylabel('仓位 (%)', fontsize=12)
    ax2.set_xlabel('日期', fontsize=12)
    ax2.set_title(f'[{strategy_name}] 仓位变化趋势', fontsize=12)
    ax2.legend(loc='upper right', fontsize=9)
    ax2.grid(True, alpha=0.3)
    ax2.set_ylim(0, 100)
    ax2.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
    ax2.xaxis.set_major_locator(mdates.WeekdayLocator(interval=2))
    plt.setp(ax2.xaxis.get_majorticklabels(), rotation=45, ha='right')
    
    # 添加关键仓位水平线
    ax2.axhline(y=50, color='gray', linestyle='--', alpha=0.5, linewidth=0.8)
    ax2.axhline(y=70, color='orange', linestyle='--', alpha=0.5, linewidth=0.8)
    ax2.text(dates_position[-1], 50, ' 50%', fontsize=8, color='gray', va='center')
    ax2.text(dates_position[-1], 70, ' 70%', fontsize=8, color='orange', va='center')
    
    # 添加最新仓位标注
    if stock_positions:
        ax2.annotate(f'股票: {stock_positions[-1]:.1f}%', 
                    xy=(dates_position[-1], stock_positions[-1]),
                    xytext=(-50, 15), textcoords='offset points',
                    fontsize=9, color='blue',
                    arrowprops=dict(arrowstyle='->', color='blue', lw=1.5))
        ax2.annotate(f'现金: {cash_positions[-1]:.1f}%', 
                    xy=(dates_position[-1], cash_positions[-1]),
                    xytext=(-50, -20), textcoords='offset points',
                    fontsize=9, color='green',
                    arrowprops=dict(arrowstyle='->', color='green', lw=1.5))
    
    # 添加资金总额文本标注
    if total_capitals:
        capital_text = f"资金: {total_capitals[0]/10000:.1f}万 → {total_capitals[-1]/10000:.1f}万"
        ax2.text(0.02, 0.98, capital_text, transform=ax2.transAxes, fontsize=9,
                 verticalalignment='top', horizontalalignment='left',
                 bbox=dict(boxstyle='round', facecolor='lightblue', alpha=0.5))
    
    plt.tight_layout()
    
    if output_path:
        plt.savefig(output_path, dpi=150, bbox_inches='tight')
        print(f"图表已保存: {output_path}")
    
    if show_plot:
        plt.show()
    else:
        plt.close(fig)


def main(show_plot: bool = True, strategy_name: str = "2025_7d_for_once", output_path: Optional[str] = None):
    """主函数
    
    Args:
        show_plot: 是否显示图表窗口（在后台模式下设为False）
        strategy_name: 策略名称，默认为 "2025_7d_for_once"
        output_path: 输出文件路径（可选，默认自动生成包含策略名的文件名）
    """
    # 确定数据目录
    script_dir = Path(__file__).parent
    data_dir = script_dir.parent / "data"
    
    print("=" * 60)
    print(f"投资组合可视化 - 策略: {strategy_name}")
    print("=" * 60)
    
    # 读取历史数据
    print(f"\n读取 {strategy_name} 历史数据...")
    try:
        history = read_portfolio_history(str(data_dir), strategy_name)
    except FileNotFoundError as e:
        print(f"错误: {e}")
        print("请确认数据目录路径正确")
        return
    
    if not history:
        print(f"未找到任何 {strategy_name} 数据")
        return
    
    print(f"成功读取 {len(history)} 条历史记录")
    
    # 显示数据概览
    print("\n数据概览:")
    print(f"  起始日期: {history[0]['trade_date']}")
    print(f"  结束日期: {history[-1]['trade_date']}")
    print(f"  初始资金: {history[0].get('initial_capital', 'N/A'):,.2f} 元")
    print(f"  最新资金: {history[-1]['total_capital']:,.2f} 元")
    print(f"  最新股票仓位: {history[-1].get('stock_position', 0):.2f}%")
    print(f"  最新现金仓位: {history[-1].get('cash_position', 0):.2f}%")
    
    # 提取仓位数据
    dates_position = []
    stock_positions = []
    cash_positions = []
    total_capitals = []
    
    for record in history:
        date_str = record["trade_date"]
        dates_position.append(datetime.strptime(date_str, "%Y%m%d"))
        stock_positions.append(record.get("stock_position", 0))
        cash_positions.append(record.get("cash_position", 0))
        total_capitals.append(record.get("total_capital", 0))
    
    # 计算净值用于业绩指标
    dates_nav, nav_values, _ = calculate_nav(history)
    
    # 加载指数数据（使用投资组合的日期范围）
    start_date = history[0]['trade_date'] if history else None
    end_date = history[-1]['trade_date'] if history else None
    
    print("\n加载指数数据...")
    sh_index_data = load_index_data("000001.SH", start_date=start_date, end_date=end_date)
    sh_values, sh_nav = align_sh_index(dates_nav, nav_values, sh_index_data)
    
    # 加载沪深300数据
    hs300_data = load_index_data("000300.SH", start_date=start_date, end_date=end_date)
    _, hs300_nav = align_sh_index(dates_nav, nav_values, hs300_data)
    
    # 计算业绩指标
    metrics = calculate_performance_metrics(dates_nav, nav_values, sh_nav, hs300_nav)
    
    # 打印业绩指标
    print("\n业绩指标:")
    print(f"  组合总收益率: {metrics.get('total_return', 0):.2f}%")
    print(f"  上证总收益率: {metrics.get('sh_total_return', 0):.2f}%")
    print(f"  沪深300收益率: {metrics.get('hs300_total_return', 0):.2f}%")
    print(f"  超额收益(相对上证): {metrics.get('excess_return', 0):.2f}%")
    print(f"  超额收益(相对沪深300): {metrics.get('excess_return_hs300', 0):.2f}%")
    print(f"  最大回撤: {metrics.get('max_drawdown', 0):.2f}%")
    if 'annualized_return' in metrics:
        print(f"  年化收益率: {metrics['annualized_return']:.2f}%")
    if 'sharpe_ratio' in metrics:
        print(f"  夏普比率: {metrics['sharpe_ratio']:.2f}")
    
    # 绘制组合图表
    print("\n生成可视化图表...")
    
    # 根据策略名称生成输出文件名
    if output_path:
        final_output_path = Path(output_path)
    else:
        # 默认保存到 data/visualization，避免与脚本目录混杂
        safe_strategy_name = strategy_name.replace(".", "_")
        final_output_path = data_dir / "visualization" / f"portfolio_{safe_strategy_name}_chart.png"
    final_output_path.parent.mkdir(parents=True, exist_ok=True)
    
    plot_combined_chart(
        dates_nav, nav_values, sh_nav,
        dates_position, stock_positions, cash_positions,
        total_capitals,
        metrics, str(final_output_path), show_plot=show_plot,
        hs300_nav=hs300_nav, strategy_name=strategy_name
    )
    
    print(f"图表文件: {final_output_path}")
    print("\n完成!")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="投资组合净值可视化")
    parser.add_argument("--no-show", action="store_true", help="不显示图表，仅保存到文件")
    parser.add_argument("--output", "-o", type=str, default=None, help="输出文件路径")
    parser.add_argument("--strategy", "-s", type=str, default="2025_7d_for_once", 
                        help="策略名称 (默认: 2025_7d_for_once)")
    args = parser.parse_args()
    
    main(show_plot=not args.no_show, strategy_name=args.strategy, output_path=args.output)
