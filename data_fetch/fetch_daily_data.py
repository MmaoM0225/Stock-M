"""
每日数据获取脚本
用于获取需要每天更新的数据，包括新闻舆情等
"""
import logging
from datetime import datetime

from dataflow.news_sentiment import NewsSentimentFetcher

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def fetch_breakfast_news():
    """获取财经早餐数据"""
    try:
        logger.info("=" * 50)
        logger.info("开始获取财经早餐数据")
        logger.info("=" * 50)
        
        fetcher = NewsSentimentFetcher()
        df = fetcher.fetch_eastmoney_breakfast_news()
        
        if not df.empty:
            logger.info(f"成功获取 {len(df)} 条财经早餐数据")
            logger.info("数据已保存到 data/breakfast_news.json")
        else:
            logger.warning("未获取到任何财经早餐数据")
        
        return df
        
    except Exception as e:
        logger.error(f"获取财经早餐数据失败: {str(e)}")
        raise


def fetch_all_daily_data():
    """获取所有每日数据"""
    logger.info("=" * 50)
    logger.info(f"开始执行每日数据获取任务 - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info("=" * 50)
    
    results = {}
    
    try:
        results['breakfast_news'] = fetch_breakfast_news()
    except Exception as e:
        logger.error(f"获取财经早餐数据时出错: {str(e)}")
        results['breakfast_news'] = None
    
    logger.info("=" * 50)
    logger.info("每日数据获取任务完成")
    logger.info("=" * 50)
    
    return results


if __name__ == "__main__":
    fetch_all_daily_data()
