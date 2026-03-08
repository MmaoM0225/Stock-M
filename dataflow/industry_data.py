"""
行业数据获取模块
支持本地 JSON 文件及 Tushare API 获取行业分类数据
"""
import json
import logging
from pathlib import Path
from typing import List, Optional

import pandas as pd
import tushare as ts

from .config import DATA_SOURCES
from .utils import clean_dataframe, DataFlowException

logger = logging.getLogger(__name__)

INDUSTRY_L1_PATH = Path("data/industry_list_simple.json")
INDUSTRY_L2_PATH = Path("data/industry_list_l2_simple.json")


def _get_ts_pro():
    """获取 Tushare pro 实例。"""
    if not DATA_SOURCES["tushare"]["enabled"]:
        raise DataFlowException("Tushare 未配置或未启用")
    ts.set_token(DATA_SOURCES["tushare"]["token"])
    return ts.pro_api()


def fetch_industry_list(
    level: str = "L1",
    src: str = "SW2021",
) -> pd.DataFrame:
    """
    从 Tushare 获取行业分类列表。

    Args:
        level: 行业级别，'L1'(一级), 'L2'(二级), 'L3'(三级)
        src: 分类来源，默认 'SW2021'(申万2021版)

    Returns:
        行业分类 DataFrame
    """
    try:
        ts_pro = _get_ts_pro()
        logger.info(f"获取行业分类: level={level}, src={src}")
        df = ts_pro.index_classify(level=level, src=src)
        if df.empty:
            logger.warning(f"未获取到行业分类: level={level}, src={src}")
            return pd.DataFrame()
        df = clean_dataframe(df)
        level_name = {"L1": "一级", "L2": "二级", "L3": "三级"}.get(level, level)
        logger.info(f"成功获取 {len(df)} 条申万{level_name}行业")
        return df
    except Exception as e:
        logger.error(f"获取行业分类失败: {e}")
        raise DataFlowException(f"获取行业分类失败: {e}") from e


def fetch_industry_members(
    l1_code: Optional[str] = None,
    l2_code: Optional[str] = None,
    l3_code: Optional[str] = None,
    is_new: str = "Y",
) -> pd.DataFrame:
    """
    从 Tushare 获取行业成分股。

    Args:
        l1_code: 一级行业代码
        l2_code: 二级行业代码
        l3_code: 三级行业代码
        is_new: 是否最新，默认 'Y'

    Returns:
        行业成分股 DataFrame
    """
    try:
        ts_pro = _get_ts_pro()
        logger.info(f"获取行业成分股: l1={l1_code}, l2={l2_code}, l3={l3_code}")
        df = ts_pro.index_member_all(
            l1_code=l1_code,
            l2_code=l2_code,
            l3_code=l3_code,
            is_new=is_new,
        )
        if df.empty:
            logger.warning("未获取到行业成分股")
            return pd.DataFrame()
        df = clean_dataframe(df)
        logger.info(f"成功获取 {len(df)} 条成分股")
        return df
    except Exception as e:
        logger.error(f"获取行业成分股失败: {e}")
        raise DataFlowException(f"获取行业成分股失败: {e}") from e


def fetch_stock_industry(
    ts_code: str,
    is_new: str = "Y",
) -> pd.DataFrame:
    """
    从 Tushare 查询个股所属行业。

    Args:
        ts_code: 股票代码
        is_new: 是否最新，默认 'Y'

    Returns:
        个股所属行业 DataFrame
    """
    try:
        ts_pro = _get_ts_pro()
        logger.info(f"查询个股所属行业: {ts_code}")
        df = ts_pro.index_member_all(ts_code=ts_code, is_new=is_new)
        if df.empty:
            logger.warning(f"未获取到 {ts_code} 的行业信息")
            return pd.DataFrame()
        df = clean_dataframe(df)
        logger.info(f"成功获取 {ts_code} 的行业分类")
        return df
    except Exception as e:
        logger.error(f"查询个股所属行业失败: {e}")
        raise DataFlowException(f"查询个股所属行业失败: {e}") from e


def _load_from_json(l1_path: Path, l2_path: Path) -> List[str]:
    """从本地 JSON 文件加载行业名称列表。"""
    names = set()
    for path in (l1_path, l2_path):
        try:
            with open(path, "r", encoding="utf-8") as f:
                data = json.load(f)
            for item in data if isinstance(data, list) else []:
                if isinstance(item, dict) and "industry_name" in item:
                    names.add(item["industry_name"])
        except FileNotFoundError:
            logger.debug(f"未找到行业文件: {path}")
        except Exception as e:
            logger.warning(f"加载行业文件失败 {path}: {e}")
    return sorted(names)


def get_all_industry_names(
    l1_path: Path = None,
    l2_path: Path = None,
) -> List[str]:
    """
    获取完整行业名称列表。
    优先从本地 JSON 加载；若未找到或为空，则调用 fetch_industry_list 从 Tushare 获取。

    Returns:
        去重排序后的行业名称列表
    """
    l1_path = l1_path or INDUSTRY_L1_PATH
    l2_path = l2_path or INDUSTRY_L2_PATH

    names = _load_from_json(l1_path, l2_path)

    if not names:
        logger.info("本地行业文件未找到或为空，尝试从 Tushare 获取")
        try:
            for level in ("L1", "L2"):
                df = fetch_industry_list(level=level)
                if not df.empty and "industry_name" in df.columns:
                    names.extend(df["industry_name"].dropna().unique().tolist())
            names = sorted(set(names))
        except Exception as e:
            logger.warning(f"从 Tushare 获取行业列表失败: {e}")
            return []

    logger.info(f"已加载 {len(names)} 个行业分类")
    return names
