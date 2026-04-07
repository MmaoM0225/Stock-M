"""
Stock Screener（股票筛选分析师）Demo

    python -m agents.analyst.stock_screener.demo
    python -m agents.analyst.stock_screener.demo 20260328

筛选条件可在 main() 中修改，支持：
- sectors: 行业列表
- min_market_cap/max_market_cap: 市值范围（元）
- min_pe/max_pe: 市盈率范围
- min_pb/max_pb: 市净率范围
- max_stocks: 返回股票数量
- sort_by: 排序字段（total_share/pe/pb）
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
    from pprint import pprint

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
    # # 示例1: 大盘股 (100亿以上)
    # criteria = {
    #     "trade_date": trade_date,
    #     "min_market_cap": 100e8,  # 100亿以上
    #     "exclude_st": True,
    #     "max_stocks": 20,
    #     "sort_by": "total_share",
    # }

    # 示例2: 低估值 (PE < 20, PB < 3)
    # criteria = {
    #     "trade_date": trade_date,
    #     "max_pe": 20,
    #     "max_pb": 3,
    #     "exclude_st": True,
    #     "max_stocks": 30,
    #     "sort_by": "pe",
    # }

    # 示例3: 结合板块筛选
    criteria = {
        "trade_date": trade_date,
        "sectors": ["半导体", "食品"],
        "exclude_st": True,
        "max_stocks": 50,
        "sort_by": "total_share",
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

    # 股票列表（展示关键指标）
    print(f"\n股票列表:")
    print(f"{'代码':<12} {'名称':<10} {'行业':<8} {'PE':>8} {'PB':>8} {'总股本':>8} {'总资产':>10} {'流动资产':>10}")
    print("-" * 75)
    for stock in screener_result.get("filtered_stocks", [])[:20]:
        pe = stock.get("pe", 0) or 0
        pb = stock.get("pb", 0) or 0
        share = stock.get("total_share", 0) or 0
        assets = stock.get("total_assets", 0) or 0
        liquid = stock.get("liquid_assets", 0) or 0
        print(f"{stock['ts_code']:<12} {stock['name']:<10} {stock.get('industry', '未知'):<8} "
              f"{pe:>8.2f} {pb:>8.2f} {share:>8.2f} {assets:>10.2f} {liquid:>10.2f}")

    print("\n" + "=" * 60)
    print(f"共筛选出 {screener_result.get('total_count', 0)} 只股票")


if __name__ == "__main__":
    main()
