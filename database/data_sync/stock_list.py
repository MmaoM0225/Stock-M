"""
股票列表表同步
从 Tushare API 拉取股票列表，全量替换写入数据库

运行方式:
  python -m database.data_sync.stock_list           # 同步股票列表（Tushare）
"""
import argparse
import logging

import pandas as pd
from dataflow.market_data import MarketDataFetcher

from database import StockList
from database.config import get_db_session

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


def sync_stock_list() -> int:
    """
    拉取股票列表并写入数据库（全量替换）

    Returns:
        int: 写入的记录数
    """
    logger.info("=" * 50)
    logger.info("开始同步股票列表")
    logger.info("=" * 50)

    fetcher = MarketDataFetcher()
    df = fetcher.fetch_stock_basic(
        exchange="",
        list_status="L",
        fields="ts_code,symbol,name,area,industry,market,list_date",
    )

    if df.empty:
        logger.warning("未获取到股票列表")
        return 0

    with get_db_session() as session:
        deleted = session.query(StockList).delete()
        logger.info(f"已清空旧数据: {deleted} 条")

        records = []
        for _, row in df.iterrows():
            records.append(
                StockList(
                    ts_code=str(row.get("ts_code", "")),
                    symbol=str(row.get("symbol", "")) if pd.notna(row.get("symbol")) else None,
                    name=str(row.get("name", "")) if pd.notna(row.get("name")) else None,
                    area=str(row.get("area", "")) if pd.notna(row.get("area")) else None,
                    industry=str(row.get("industry", "")) if pd.notna(row.get("industry")) else None,
                    market=str(row.get("market", "")) if pd.notna(row.get("market")) else None,
                    list_date=int(row["list_date"]) if pd.notna(row.get("list_date")) else None,
                )
            )

        session.add_all(records)
        count = len(records)

    logger.info(f"股票列表同步完成，共 {count} 条")
    return count


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="股票列表同步")
    args = parser.parse_args()

    sync_stock_list()
