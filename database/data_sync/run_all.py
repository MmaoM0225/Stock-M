"""
一键运行全部 data_sync 任务。

运行方式:
  python -m database.data_sync.run_all
  python database/data_sync/run_all.py
"""
import logging
import sys
from pathlib import Path
from typing import Callable, List, Tuple

# 允许直接运行: python database/data_sync/run_all.py
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from database.data_sync.breakfast_news import sync_breakfast_news
from database.data_sync.commodity_analyst import sync_commodity_analyst
from database.data_sync.industry import sync_industry
from database.data_sync.macro_economist import sync_macro_economist
from database.data_sync.macro_manager import sync_macro_manager
from database.data_sync.market_sentiment_analyst import sync_market_sentiment_analyst
from database.data_sync.news_analyst import sync_news_analyst
from database.data_sync.news_finlight import sync_finlight_news
from database.data_sync.portfolio_decision import sync_portfolio_decision
from database.data_sync.sector_capital_flow_analyst import sync_sector_capital_flow_analyst
from database.data_sync.sector_manager import sync_sector_manager
from database.data_sync.sector_trend_analyst import sync_sector_trend_analyst
from database.data_sync.stock_fundamental_analyst import sync_stock_fundamental_analyst
from database.data_sync.stock_list import sync_stock_list
from database.data_sync.stock_manager import sync_stock_manager
from database.data_sync.stock_pool_manager import sync_stock_pool_manager
from database.data_sync.stock_screener import sync_stock_screener
from database.data_sync.stock_technical_analyst import sync_stock_technical_analyst
from database.data_sync.ths_index import sync_ths_index

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

SyncTask = Tuple[str, Callable[[], int]]
SYNC_TASKS: List[SyncTask] = [
    ("stock_list", sync_stock_list),
    ("ths_index", sync_ths_index),
    ("industry", sync_industry),
    ("breakfast_news", sync_breakfast_news),
    ("news_finlight", sync_finlight_news),
    ("macro_manager", sync_macro_manager),
    ("sector_manager", sync_sector_manager),
    ("stock_manager", sync_stock_manager),
    ("stock_pool_manager", sync_stock_pool_manager),
    ("portfolio_decision", sync_portfolio_decision),
    ("stock_screener", sync_stock_screener),
    ("stock_technical_analyst", sync_stock_technical_analyst),
    ("stock_fundamental_analyst", sync_stock_fundamental_analyst),
    ("sector_trend_analyst", sync_sector_trend_analyst),
    ("sector_capital_flow_analyst", sync_sector_capital_flow_analyst),
    ("news_analyst", sync_news_analyst),
    ("market_sentiment_analyst", sync_market_sentiment_analyst),
    ("macro_economist", sync_macro_economist),
    ("commodity_analyst", sync_commodity_analyst),
]


def run_all_data_sync() -> int:
    """
    顺序运行所有 data_sync 任务。

    Returns:
        int: 失败任务数（为 0 表示全部成功）
    """
    logger.info("=" * 60)
    logger.info("开始运行全部 data_sync 任务，共 %d 个", len(SYNC_TASKS))
    logger.info("=" * 60)

    failed = 0
    for task_name, task_fn in SYNC_TASKS:
        logger.info("-" * 60)
        logger.info("开始任务: %s", task_name)
        try:
            count = task_fn()
            logger.info("任务完成: %s（写入/更新 %s 条）", task_name, count)
        except Exception as exc:
            failed += 1
            logger.exception("任务失败: %s, 错误: %s", task_name, exc)

    succeeded = len(SYNC_TASKS) - failed
    logger.info("=" * 60)
    logger.info("全部任务执行结束：成功 %d，失败 %d", succeeded, failed)
    logger.info("=" * 60)
    return failed


if __name__ == "__main__":
    sys.exit(run_all_data_sync())
