"""
股票列表表同步
从 Tushare API 拉取股票列表，全量替换写入数据库
从东财补充个股信息（流通股本、市值等）

运行方式:
  python -m database.data_sync.stock_list           # 同步股票列表（Tushare）
  python -m database.data_sync.stock_list --em     # 同步东财个股信息（流通股本、市值等）
"""
import argparse
import logging
import time
from typing import List, Optional

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


# 东财 item 名称到 stock_list 字段的映射
_EM_ITEM_TO_FIELD = {
    "总股本": "total_share",
    "流通股": "float_share",
    "总市值": "total_mv",
    "流通市值": "float_mv",
}


def _parse_individual_info_df(df: pd.DataFrame) -> dict:
    """将东财 item/value 格式 DataFrame 解析为字段字典"""
    if df is None or df.empty or "item" not in df.columns or "value" not in df.columns:
        return {}
    result = {}
    for _, row in df.iterrows():
        item = str(row.get("item", "")).strip()
        if item in _EM_ITEM_TO_FIELD:
            field = _EM_ITEM_TO_FIELD[item]
            val = row.get("value")
            if pd.notna(val):
                try:
                    result[field] = float(val)
                except (TypeError, ValueError):
                    pass
    return result


def sync_stock_individual_info_em(
    symbols: Optional[List[str]] = None,
    delay_seconds: float = 0.5,
) -> int:
    """
    从东财拉取个股信息（流通股本、总市值、流通市值等）并更新 stock_list 表

    Args:
        symbols: 要更新的股票代码列表，6位或 ts_code 格式。为 None 时从数据库读取全部
        delay_seconds: 每次请求间隔秒数，避免限流

    Returns:
        int: 成功更新的记录数
    """
    logger.info("=" * 50)
    logger.info("开始同步东财个股信息")
    logger.info("=" * 50)

    fetcher = MarketDataFetcher()
    updated = 0

    with get_db_session() as session:
        if symbols is None:
            rows = session.query(StockList).all()
            to_fetch = [r.symbol or r.ts_code.split(".")[0] for r in rows if r.ts_code]
        else:
            to_fetch = []
            for s in symbols:
                code = str(s).strip()
                if "." in code:
                    code = code.split(".")[0]
                if len(code) == 6 and code.isdigit():
                    to_fetch.append(code)

        total = len(to_fetch)
        logger.info(f"待更新 {total} 只股票")
        for i, code in enumerate(to_fetch):
            if (i + 1) % 100 == 0 or i == 0:
                logger.info(f"进度: {i + 1}/{total}")
            try:
                df = fetcher.fetch_stock_individual_info_em(symbol=code)
                parsed = _parse_individual_info_df(df)
                if not parsed:
                    continue

                # 按 symbol 或 ts_code 查找记录
                stock = (
                    session.query(StockList)
                    .filter(
                        (StockList.symbol == code)
                        | (StockList.ts_code.like(f"{code}.%"))
                    )
                    .first()
                )
                if stock:
                    for k, v in parsed.items():
                        setattr(stock, k, v)
                    updated += 1

            except Exception as e:
                logger.warning(f"获取 {code} 失败: {e}")
            finally:
                if i < total - 1 and delay_seconds > 0:
                    time.sleep(delay_seconds)

    logger.info(f"东财个股信息同步完成，共更新 {updated} 条")
    return updated


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="股票列表同步")
    parser.add_argument(
        "--em",
        "-e",
        action="store_true",
        help="同步东财个股信息（流通股本、市值等），默认同步 Tushare 股票列表",
    )
    parser.add_argument(
        "--delay",
        type=float,
        default=0.5,
        help="东财请求间隔秒数（仅 --em 时生效），默认 0.5",
    )
    args = parser.parse_args()

    if args.em:
        sync_stock_individual_info_em(delay_seconds=args.delay)
    else:
        sync_stock_list()
