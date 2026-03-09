"""
财经早餐表同步
从东方财富 API 拉取财经早餐列表，upsert 写入数据库（按 pub_date）
可单独运行: python -m database.data_sync.breakfast_news
"""
import logging
from typing import Optional

import pandas as pd
from dataflow.news_sentiment import NewsSentimentFetcher

from database import BreakfastNews
from database.config import get_db_session

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


def _get_column(df: pd.DataFrame, *candidates: str) -> Optional[str]:
    """从 DataFrame 中查找列名（支持中文/英文）"""
    for c in candidates:
        if c in df.columns:
            return c
    return None


def sync_breakfast_news() -> int:
    """
    拉取财经早餐列表并写入数据库（按 pub_date upsert）

    Returns:
        int: 写入/更新的记录数
    """
    logger.info("=" * 50)
    logger.info("开始同步财经早餐")
    logger.info("=" * 50)

    fetcher = NewsSentimentFetcher()
    df = fetcher.fetch_eastmoney_breakfast_news()

    if df.empty:
        logger.warning("未获取到财经早餐数据")
        return 0

    # 列名映射（akshare 可能返回中文列名）
    title_col = _get_column(df, "标题", "title")
    summary_col = _get_column(df, "摘要", "summary")
    date_col = _get_column(df, "发布时间", "时间", "pub_date", "date")
    url_col = _get_column(df, "链接", "url", "detail_url")

    if not date_col:
        logger.warning("未找到日期列，跳过同步")
        return 0

    # 统一日期格式为 YYYYMMDD
    if date_col and pd.api.types.is_datetime64_any_dtype(df[date_col]):
        df = df.copy()
        df[date_col] = df[date_col].dt.strftime("%Y%m%d")
    elif date_col:
        df = df.copy()
        df[date_col] = pd.to_datetime(df[date_col], errors="coerce").dt.strftime("%Y%m%d")

    # 按日期升序，保证 id 与日期正相关（旧日期 id 小）
    df = df.sort_values(by=date_col, ascending=True).reset_index(drop=True)

    count = 0
    with get_db_session() as session:
        for _, row in df.iterrows():
            pub_date = str(row.get(date_col, "")).strip() if pd.notna(row.get(date_col)) else None
            if not pub_date:
                continue

            existing = session.query(BreakfastNews).filter(BreakfastNews.pub_date == pub_date).first()
            title = str(row.get(title_col, "")) if title_col and pd.notna(row.get(title_col)) else None
            summary = str(row.get(summary_col, "")) if summary_col and pd.notna(row.get(summary_col)) else None
            detail_url = str(row.get(url_col, "")) if url_col and pd.notna(row.get(url_col)) else None

            if existing:
                existing.title = title
                existing.summary = summary
                existing.detail_url = detail_url
                count += 1
            else:
                session.add(
                    BreakfastNews(
                        title=title,
                        summary=summary,
                        pub_date=pub_date,
                        detail_url=detail_url,
                    )
                )
                count += 1

    logger.info(f"财经早餐同步完成，共 {count} 条")
    return count


if __name__ == "__main__":
    sync_breakfast_news()
