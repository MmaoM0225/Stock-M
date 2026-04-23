"""
同花顺板块指数表同步
从 Tushare API 拉取同花顺概念/行业/地域/特色/风格/主题/宽基指数，写入数据库

支持按指数类型同步或全量同步：
  python -m database.data_sync.ths_index              # 全量同步
  python -m database.data_sync.ths_index --type N     # 仅同步概念指数
  python -m database.data_sync.ths_index --type I     # 仅同步行业指数
"""
import argparse
import logging
from typing import Optional

import pandas as pd
from dataflow.industry_data import fetch_ths_index

from database import ThsIndex
from database.config import get_db_session

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

# 指数类型代码 -> 中文名（用于日志）
THS_INDEX_TYPE_NAMES = {
    "N": "概念指数",
    "I": "行业指数",
    "R": "地域指数",
    "S": "同花顺特色指数",
    "ST": "同花顺风格指数",
    "TH": "同花顺主题指数",
    "BB": "同花顺宽基指数",
}

# 支持同步的指数类型列表（与 dataflow fetch_ths_index 的 index_type 一致）
THS_INDEX_TYPES = list(THS_INDEX_TYPE_NAMES)
DEFAULT_ALLOWED_INDEX_TYPES = {"I", "N"}


def _row_to_ths_index(row: pd.Series) -> ThsIndex:
    """将 DataFrame 行转换为 ThsIndex 模型"""
    return ThsIndex(
        ts_code=str(row.get("ts_code", "")),
        name=str(row.get("name", "")) if pd.notna(row.get("name")) else None,
        count=int(row["count"]) if pd.notna(row.get("count")) else None,
        exchange=str(row.get("exchange", "")) if pd.notna(row.get("exchange")) else None,
        list_date=str(row.get("list_date", "")) if pd.notna(row.get("list_date")) else None,
        index_type=str(row.get("type", "")) if pd.notna(row.get("type")) else None,
    )


def sync_ths_index(index_type: Optional[str] = None) -> int:
    """
    拉取同花顺板块指数并写入数据库。

    Args:
        index_type: 指数类型。为 None 时拉取全部并全量替换表；
                    为 'N'/'I'/'R'/'S'/'ST'/'TH'/'BB' 时仅拉取该类型并只替换该类型数据。
                    N-概念 I-行业 R-地域 S-特色 ST-风格 TH-主题 BB-宽基

    Returns:
        int: 写入的记录数
    """
    logger.info("=" * 50)
    if index_type is None:
        logger.info("开始同步同花顺板块指数（全量）")
    else:
        type_name = THS_INDEX_TYPE_NAMES.get(index_type, index_type)
        logger.info("开始同步同花顺板块指数：%s（%s）", type_name, index_type)
    logger.info("=" * 50)

    df = fetch_ths_index(index_type=index_type)
    if df.empty:
        logger.warning("未获取到同花顺板块指数")
        return 0

    # 全量同步时仅保留 I/N，避免写入非目标类型板块
    if index_type is None and "type" in df.columns:
        raw_count = len(df)
        df["type"] = df["type"].astype(str).str.strip().str.upper()
        df = df[df["type"].isin(DEFAULT_ALLOWED_INDEX_TYPES)].copy()
        logger.info(
            "按 index_type in %s 过滤: %d -> %d",
            sorted(DEFAULT_ALLOWED_INDEX_TYPES),
            raw_count,
            len(df),
        )
        if df.empty:
            logger.warning("按 index_type 过滤后无数据，已终止写库")
            return 0

    # 仅保留 A 股口径板块，避免混入非 A 市场导致后续成分映射错误
    raw_count = len(df)
    if "exchange" not in df.columns:
        logger.warning("ths_index 数据缺少 exchange 字段，跳过 A 股口径过滤")
    else:
        df["exchange"] = df["exchange"].astype(str).str.strip().str.upper()
        df = df[df["exchange"] == "A"].copy()
        logger.info("按 exchange='A' 过滤: %d -> %d", raw_count, len(df))
        if df.empty:
            logger.warning("按 exchange='A' 过滤后无数据，已终止写库")
            return 0

    # 过滤无有效成分数量的板块（count<=0 或缺失）
    if "count" not in df.columns:
        logger.warning("ths_index 数据缺少 count 字段，跳过 count>0 过滤")
    else:
        raw_count = len(df)
        count_numeric = pd.to_numeric(df["count"], errors="coerce")
        df = df[count_numeric > 0].copy()
        logger.info("按 count>0 过滤: %d -> %d", raw_count, len(df))
        if df.empty:
            logger.warning("按 count>0 过滤后无数据，已终止写库")
            return 0

    with get_db_session() as session:
        if index_type is None:
            deleted = session.query(ThsIndex).delete()
            logger.info("已清空旧数据: %d 条", deleted)
        else:
            deleted = session.query(ThsIndex).filter(ThsIndex.index_type == index_type).delete()
            logger.info("已删除该类型旧数据: %d 条", deleted)

        records = [_row_to_ths_index(row) for _, row in df.iterrows()]
        session.add_all(records)
        count = len(records)

    if index_type is None:
        logger.info("同花顺板块指数同步完成，共 %d 条", count)
    else:
        type_name = THS_INDEX_TYPE_NAMES.get(index_type, index_type)
        logger.info("同花顺板块指数同步完成 [%s]，共 %d 条", type_name, count)
    return count


def sync_ths_index_by_type(index_type: str) -> int:
    """
    按指定指数类型同步（供其他模块调用）。

    Args:
        index_type: 指数类型代码，如 'N', 'I', 'R', 'S', 'ST', 'TH', 'BB'

    Returns:
        int: 写入的记录数
    """
    if index_type not in THS_INDEX_TYPES:
        logger.warning("未知指数类型 %s，已支持: %s", index_type, THS_INDEX_TYPES)
        return 0
    return sync_ths_index(index_type=index_type)


def sync_ths_index_all_types() -> dict:
    """
    按每种指数类型分别拉取并同步（每种类型只更新该类型数据）。

    Returns:
        dict: 各类型及其写入条数，如 {'N': 271, 'I': 80, ...}
    """
    result = {}
    for t in THS_INDEX_TYPES:
        result[t] = sync_ths_index(index_type=t)
    return result


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="同花顺板块指数同步")
    parser.add_argument(
        "--type",
        "-t",
        choices=THS_INDEX_TYPES,
        default=None,
        metavar="TYPE",
        help="仅同步指定类型：N概念 I行业 R地域 S特色 ST风格 TH主题 BB宽基；不传则全量同步",
    )
    parser.add_argument(
        "--each",
        action="store_true",
        help="按每种类型分别调用接口同步（共 7 次），仅更新各类型子集",
    )
    args = parser.parse_args()

    if args.each:
        sync_ths_index_all_types()
    else:
        sync_ths_index(index_type=args.type)
