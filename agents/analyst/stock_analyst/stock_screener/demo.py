"""
Stock Screener（股票筛选分析师）Demo

    python -m agents.analyst.stock_analyst.stock_screener.demo
    python -m agents.analyst.stock_analyst.stock_screener.demo 20260328

筛选条件可在 main() 中修改，支持：
- sectors: 板块列表（支持多板块；未传时自动读取 sector_manager 输出）
- min_market_cap/max_market_cap: 市值范围（元）
- min_pe/max_pe: 市盈率范围
- min_pb/max_pb: 市净率范围
- max_stocks: 返回股票数量
- sort_by: total_mv、circ_mv、股本、pe/pe_ttm、pb、ps/ps_ttm、close、dv_ratio/dv_ttm、换手、量比等（同 daily_basic）
- sort_order: 排序方向，asc/正序/升序 或 desc/倒序/降序（默认 desc）
"""
import argparse
import logging
import re
from datetime import datetime, timedelta
from typing import Optional

from .graph import create_stock_screener_graph


def _parse_trade_date(s: str) -> datetime:
    s = s.strip()
    if re.fullmatch(r"\d{8}", s):
        return datetime.strptime(s, "%Y%m%d")
    raise ValueError(f"无效日期格式: {s}，请使用 YYYYMMDD")


def _resolve_trade_date(target_date: Optional[datetime]) -> str:
    today = datetime.now()
    if target_date is not None:
        return target_date.strftime("%Y%m%d")
    # 返回最近一个交易日
    for i in range(7):
        check = today - timedelta(days=i)
        if check.weekday() < 5:
            return check.strftime("%Y%m%d")
    return today.strftime("%Y%m%d")


def main():
    logging.basicConfig(level=logging.INFO)

    parser = argparse.ArgumentParser(description="Stock Screener Demo：股票筛选")
    parser.add_argument("date", nargs="?", help="交易日期 YYYYMMDD")
    args = parser.parse_args()

    target_date = _parse_trade_date(args.date) if args.date else None
    trade_date = _resolve_trade_date(target_date)

    print(f"交易日 {trade_date}，运行 Stock Screener...")
    print("-" * 60)

    graph = create_stock_screener_graph()

    # ========== 筛选条件示例 ==========
    # 示例1: 使用 sector_manager 动态板块（推荐；由筛选模板决策节点自动选择每板块策略）
    criteria = {
        "trade_date": trade_date,
        "min_market_cap": 80e8,  # 统一底线约束；具体板块可被模板细化
        "exclude_st": True,
        "max_stocks": 12,
    }


    result = graph.invoke(criteria)

    screener_result = result.get("screener_result", {})

    if screener_result.get("error"):
        print(f"错误: {screener_result['error']}")
        return

    print(f"\n筛选条件: {screener_result.get('applied_filters', [])}")
    print(f"{screener_result.get('filter_summary', 'N/A')}")

    # 板块分布
    sector_dist = screener_result.get("sector_distribution", {})
    if sector_dist:
        print(f"\n板块分布:")
        for sector, count in sorted(sector_dist.items(), key=lambda x: x[1], reverse=True)[:5]:
            print(f"  {sector}: {count}只")

    # 股票列表（daily_basic：总/流通市值为万元，这里展示为亿元人民币）
    print(f"\n股票列表:")
    hdr = (
        f"{'代码':<12} {'名称':<10} {'行业':<10} {'收盘':>8} {'PE':>8} {'PB':>8} "
        f"{'股息率%':>8} {'总股本(万)':>10} {'总市值(亿)':>10} {'流通市(亿)':>10}"
    )
    print(hdr)
    print("-" * max(75, len(hdr)))
    for stock in screener_result.get("filtered_stocks", []):
        pe = stock.get("pe") or 0
        pb = stock.get("pb") or 0
        close_p = stock.get("close") or 0
        dv_raw = stock.get("dv_ratio")
        try:
            dv_pct = float(dv_raw) if dv_raw is not None else float("nan")
        except (TypeError, ValueError):
            dv_pct = float("nan")
        dv_s = f"{dv_pct:>8.2f}" if dv_pct == dv_pct else f"{'-':>8}"  # 无数据时占位
        share = stock.get("total_share") or 0
        tmv = stock.get("total_mv") or 0
        cmv = stock.get("circ_mv") or 0
        mv_b = float(tmv) / 10000.0 if tmv else 0.0
        cv_b = float(cmv) / 10000.0 if cmv else 0.0
        print(
            f"{stock['ts_code']:<12} {str(stock.get('name') or ''):<10} "
            f"{str(stock.get('industry') or '未知'):<10} "
            f"{float(close_p):>8.2f} {float(pe):>8.2f} {float(pb):>8.2f} "
            f"{dv_s} {float(share):>10.2f} {mv_b:>10.2f} {cv_b:>10.2f}"
        )

    print("\n" + "=" * 60)
    print(f"共筛选出 {screener_result.get('total_count', 0)} 只股票")


if __name__ == "__main__":
    main()
