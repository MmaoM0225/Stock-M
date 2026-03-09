"""
行业分类表同步
从 Tushare API 拉取一级、二级行业分类，全量替换写入数据库
可单独运行: python -m database.data_sync.industry
"""
import logging

import pandas as pd
from dataflow.industry_data import fetch_industry_list

from database import Industry
from database.config import get_db_session

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


def _row_to_industry(row) -> Industry:
    """将 DataFrame 行转换为 Industry 模型"""
    return Industry(
        index_code=str(row.get("index_code", "")),
        industry_name=str(row.get("industry_name", "")) if pd.notna(row.get("industry_name")) else None,
        level=str(row.get("level", "")) if pd.notna(row.get("level")) else None,
        industry_code=int(row["industry_code"]) if pd.notna(row.get("industry_code")) else None,
        parent_code=int(row["parent_code"]) if pd.notna(row.get("parent_code")) else 0,
        is_pub=int(row["is_pub"]) if pd.notna(row.get("is_pub")) else 1,
        src=str(row.get("src", "")) if pd.notna(row.get("src")) else None,
    )


def sync_industry() -> int:
    """
    拉取行业分类（L1+L2）并写入数据库（全量替换）

    Returns:
        int: 写入的记录数
    """
    logger.info("=" * 50)
    logger.info("开始同步行业分类")
    logger.info("=" * 50)

    df_l1 = fetch_industry_list(level="L1")
    df_l2 = fetch_industry_list(level="L2")

    if df_l1.empty and df_l2.empty:
        logger.warning("未获取到行业分类")
        return 0

    df = pd.concat([df_l1, df_l2], ignore_index=True)
    df = df.drop_duplicates(subset=["index_code"], keep="first")

    with get_db_session() as session:
        deleted = session.query(Industry).delete()
        logger.info(f"已清空旧数据: {deleted} 条")

        records = [_row_to_industry(row) for _, row in df.iterrows()]
        session.add_all(records)
        count = len(records)

    logger.info(f"行业分类同步完成，共 {count} 条（L1: {len(df_l1)}, L2: {len(df_l2)}）")
    return count


if __name__ == "__main__":
    sync_industry()
