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
from .utils import clean_dataframe, DataFlowException, with_retry

logger = logging.getLogger(__name__)

INDUSTRY_L1_PATH = Path("data/industry_list_simple.json")
INDUSTRY_L2_PATH = Path("data/industry_list_l2_simple.json")


def _get_ts_pro():
    """获取 Tushare pro 实例。"""
    if not DATA_SOURCES["tushare"]["enabled"]:
        raise DataFlowException("Tushare 未配置或未启用")
    ts.set_token(DATA_SOURCES["tushare"]["token"])
    return ts.pro_api()


@with_retry(max_retries=3, retry_on_empty=True)
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


@with_retry(max_retries=3, retry_on_empty=True)
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


@with_retry(max_retries=3, retry_on_empty=True)
def fetch_ths_index(
    ts_code: Optional[str] = None,
    exchange: Optional[str] = None,
    index_type: Optional[str] = None,
) -> pd.DataFrame:
    """
    获取同花顺板块指数列表（概念、行业、地域、特色、风格、主题、宽基等）。

    数据版权归属同花顺，商业用途需联系同花顺。
    接口需 6000 积分，单次最大 5000 行，一次可提取全部，请勿循环提取。

    Args:
        ts_code: 指数代码，可选
        exchange: 市场类型，A-a股 HK-港股 US-美股
        index_type: 指数类型
            N - 概念指数, I - 行业指数, R - 地域指数,
            S - 同花顺特色指数, ST - 同花顺风格指数,
            TH - 同花顺主题指数, BB - 同花顺宽基指数

    Returns:
        列含 ts_code, name, count, exchange, list_date, type 的 DataFrame
    """
    try:
        ts_pro = _get_ts_pro()
        kwargs = {}
        if ts_code is not None:
            kwargs["ts_code"] = ts_code
        if exchange is not None:
            kwargs["exchange"] = exchange
        if index_type is not None:
            kwargs["type"] = index_type
        logger.info(f"获取同花顺板块指数: {kwargs}")
        df = ts_pro.ths_index(**kwargs)
        if df.empty:
            logger.warning("未获取到同花顺板块指数")
            return pd.DataFrame()
        df = clean_dataframe(df)
        logger.info(f"成功获取 {len(df)} 条同花顺板块指数")
        return df
    except Exception as e:
        logger.error(f"获取同花顺板块指数失败: {e}")
        raise DataFlowException(f"获取同花顺板块指数失败: {e}") from e


@with_retry(max_retries=3, retry_on_empty=True)
def fetch_moneyflow_cnt_ths(
    ts_code: Optional[str] = None,
    trade_date: Optional[str] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
) -> pd.DataFrame:
    """
    获取同花顺概念板块每日资金流向。

    数据版权归属同花顺。接口需 6000 积分，单次最大 5000 条；
    若按日期区间拉取且超过单次限量，可配合 fetch_moneyflow_cnt_ths_range 按日循环。

    Args:
        ts_code: 板块代码，可选
        trade_date: 交易日期 YYYYMMDD，可选
        start_date: 开始日期 YYYYMMDD，可选
        end_date: 结束日期 YYYYMMDD，可选

    Returns:
        列含 trade_date, ts_code, name, lead_stock, close_price, pct_change,
        industry_index, company_num, pct_change_stock, net_buy_amount,
        net_sell_amount, net_amount 的 DataFrame
    """
    try:
        ts_pro = _get_ts_pro()
        kwargs = {}
        if ts_code is not None:
            kwargs["ts_code"] = ts_code
        if trade_date is not None:
            kwargs["trade_date"] = trade_date.replace("-", "")[:8]
        if start_date is not None:
            kwargs["start_date"] = start_date.replace("-", "")[:8]
        if end_date is not None:
            kwargs["end_date"] = end_date.replace("-", "")[:8]
        if not kwargs:
            raise DataFlowException("moneyflow_cnt_ths 至少需要提供 ts_code、trade_date、start_date/end_date 之一")
        logger.info("获取同花顺概念板块资金流向: %s", kwargs)
        df = ts_pro.moneyflow_cnt_ths(**kwargs)
        if df.empty:
            logger.warning("未获取到同花顺概念板块资金流向")
            return pd.DataFrame()
        df = clean_dataframe(df)
        logger.info("成功获取 %d 条同花顺概念板块资金流向", len(df))
        return df
    except DataFlowException:
        raise
    except Exception as e:
        logger.error("获取同花顺概念板块资金流向失败: %s", e)
        raise DataFlowException(f"获取同花顺概念板块资金流向失败: {e}") from e


def fetch_moneyflow_cnt_ths_range(
    start_date: str,
    end_date: str,
    ts_code: Optional[str] = None,
) -> pd.DataFrame:
    """
    按日循环获取同花顺概念板块资金流向，避免单次超过 5000 条限量。

    Args:
        start_date: 开始日期 YYYYMMDD
        end_date: 结束日期 YYYYMMDD
        ts_code: 板块代码，可选，不传则拉取全部板块

    Returns:
        合并后的 DataFrame
    """
    from datetime import datetime, timedelta

    start = start_date.replace("-", "")[:8]
    end = end_date.replace("-", "")[:8]
    if start > end:
        raise DataFlowException("start_date 不能晚于 end_date")
    d = datetime.strptime(start, "%Y%m%d")
    end_d = datetime.strptime(end, "%Y%m%d")
    frames: List[pd.DataFrame] = []
    while d <= end_d:
        # 跳过周末等明显非交易日，避免无意义请求与警告日志
        if d.weekday() >= 5:
            d += timedelta(days=1)
            continue
        day = d.strftime("%Y%m%d")
        kwargs: dict = {"trade_date": day}
        if ts_code is not None:
            kwargs["ts_code"] = ts_code
        try:
            df = fetch_moneyflow_cnt_ths(**kwargs)
            if not df.empty:
                frames.append(df)
        except Exception as e:
            logger.warning("获取 %s 资金流向失败: %s", day, e)
        d += timedelta(days=1)
    if not frames:
        return pd.DataFrame()
    out = pd.concat(frames, ignore_index=True)
    logger.info("资金流向区间 %s～%s 共 %d 条", start, end, len(out))
    return out


@with_retry(max_retries=3, retry_on_empty=True)
def fetch_sw_daily(
    ts_code: Optional[str] = None,
    trade_date: Optional[str] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    fields: Optional[str] = None,
) -> pd.DataFrame:
    """
    获取申万行业日线行情（默认申万2021版）。

    数据版权归属申万 / Tushare。接口需 5000 积分，
    单次最大 4000 行，可按指数代码和日期参数循环提取。

    Args:
        ts_code: 行业代码，可选，如 801010.SI
        trade_date: 交易日期 YYYYMMDD，可选
        start_date: 开始日期 YYYYMMDD，可选
        end_date: 结束日期 YYYYMMDD，可选
        fields: Tushare 字段列表字符串，可选；不传则取全部字段

    Returns:
        包含 ts_code, trade_date, name, open, high, low, close, change,
        pct_change, vol, amount, pe, pb, float_mv, total_mv 等列的 DataFrame
    """
    try:
        ts_pro = _get_ts_pro()
        kwargs: dict = {}
        if ts_code is not None:
            kwargs["ts_code"] = ts_code
        if trade_date is not None:
            kwargs["trade_date"] = trade_date.replace("-", "")[:8]
        if start_date is not None:
            kwargs["start_date"] = start_date.replace("-", "")[:8]
        if end_date is not None:
            kwargs["end_date"] = end_date.replace("-", "")[:8]
        if fields is not None:
            kwargs["fields"] = fields
        if not kwargs:
            raise DataFlowException("sw_daily 至少需要提供 ts_code、trade_date、start_date/end_date 之一")

        logger.info("获取申万行业日线行情: %s", kwargs)
        df = ts_pro.sw_daily(**kwargs)
        if df.empty:
            logger.warning("未获取到申万行业日线行情")
            return pd.DataFrame()
        df = clean_dataframe(df)
        logger.info("成功获取 %d 条申万行业日线行情", len(df))
        return df
    except DataFlowException:
        raise
    except Exception as e:
        logger.error("获取申万行业日线行情失败: %s", e)
        raise DataFlowException(f"获取申万行业日线行情失败: {e}") from e


def fetch_sw_daily_range(
    start_date: str,
    end_date: str,
    ts_code: Optional[str] = None,
    fields: Optional[str] = None,
) -> pd.DataFrame:
    """
    按日循环获取申万行业日线行情，避免单次超过 4000 行限量。

    Args:
        start_date: 开始日期 YYYYMMDD
        end_date: 结束日期 YYYYMMDD
        ts_code: 行业代码，可选，不传则拉取全部行业
        fields: Tushare 字段列表字符串，可选

    Returns:
        合并后的 DataFrame
    """
    from datetime import datetime, timedelta

    start = start_date.replace("-", "")[:8]
    end = end_date.replace("-", "")[:8]
    if start > end:
        raise DataFlowException("start_date 不能晚于 end_date")

    d = datetime.strptime(start, "%Y%m%d")
    end_d = datetime.strptime(end, "%Y%m%d")
    frames: List[pd.DataFrame] = []
    while d <= end_d:
        day = d.strftime("%Y%m%d")
        kwargs: dict = {"trade_date": day}
        if ts_code is not None:
            kwargs["ts_code"] = ts_code
        if fields is not None:
            kwargs["fields"] = fields
        try:
            df = fetch_sw_daily(**kwargs)
            if not df.empty:
                frames.append(df)
        except Exception as e:
            logger.warning("获取 %s 申万行业行情失败: %s", day, e)
        d += timedelta(days=1)

    if not frames:
        return pd.DataFrame()
    out = pd.concat(frames, ignore_index=True)
    logger.info("申万行业行情区间 %s～%s 共 %d 条", start, end, len(out))
    return out


@with_retry(max_retries=3, retry_on_empty=True)
def fetch_ths_daily(
    ts_code: Optional[str] = None,
    trade_date: Optional[str] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    fields: Optional[str] = None,
) -> pd.DataFrame:
    """
    获取同花顺板块指数行情。

    数据版权归属同花顺。接口需 6000 积分，
    单次最大 3000 行数据，可根据指数代码、日期参数循环提取。

    Args:
        ts_code: 指数代码，可选，如 865001.TI
        trade_date: 交易日期 YYYYMMDD，可选
        start_date: 开始日期 YYYYMMDD，可选
        end_date: 结束日期 YYYYMMDD，可选
        fields: Tushare 字段列表字符串，可选；不传则取全部字段

    Returns:
        包含 ts_code, trade_date, close, open, high, low, pre_close,
        avg_price, change, pct_change, vol, turnover_rate, total_mv,
        float_mv 等列的 DataFrame
    """
    try:
        ts_pro = _get_ts_pro()
        kwargs: dict = {}
        if ts_code is not None:
            kwargs["ts_code"] = ts_code
        if trade_date is not None:
            kwargs["trade_date"] = trade_date.replace("-", "")[:8]
        if start_date is not None:
            kwargs["start_date"] = start_date.replace("-", "")[:8]
        if end_date is not None:
            kwargs["end_date"] = end_date.replace("-", "")[:8]
        if fields is not None:
            kwargs["fields"] = fields
        if not kwargs:
            raise DataFlowException("ths_daily 至少需要提供 ts_code、trade_date、start_date/end_date 之一")

        logger.info("获取同花顺板块指数行情: %s", kwargs)
        df = ts_pro.ths_daily(**kwargs)
        if df.empty:
            logger.warning("未获取到同花顺板块指数行情")
            return pd.DataFrame()
        df = clean_dataframe(df)
        logger.info("成功获取 %d 条同花顺板块指数行情", len(df))
        return df
    except DataFlowException:
        raise
    except Exception as e:
        logger.error("获取同花顺板块指数行情失败: %s", e)
        raise DataFlowException(f"获取同花顺板块指数行情失败: {e}") from e


def fetch_ths_daily_range(
    start_date: str,
    end_date: str,
    ts_code: Optional[str] = None,
    fields: Optional[str] = None,
) -> pd.DataFrame:
    """
    按日循环获取同花顺板块指数行情，避免单次超过 3000 行限量。

    Args:
        start_date: 开始日期 YYYYMMDD
        end_date: 结束日期 YYYYMMDD
        ts_code: 指数代码，可选，不传则拉取全部板块指数
        fields: Tushare 字段列表字符串，可选

    Returns:
        合并后的 DataFrame
    """
    from datetime import datetime, timedelta

    start = start_date.replace("-", "")[:8]
    end = end_date.replace("-", "")[:8]
    if start > end:
        raise DataFlowException("start_date 不能晚于 end_date")

    d = datetime.strptime(start, "%Y%m%d")
    end_d = datetime.strptime(end, "%Y%m%d")
    frames: List[pd.DataFrame] = []
    while d <= end_d:
        day = d.strftime("%Y%m%d")
        kwargs: dict = {"trade_date": day}
        if ts_code is not None:
            kwargs["ts_code"] = ts_code
        if fields is not None:
            kwargs["fields"] = fields
        try:
            df = fetch_ths_daily(**kwargs)
            if not df.empty:
                frames.append(df)
        except Exception as e:
            logger.warning("获取 %s 同花顺板块指数行情失败: %s", day, e)
        d += timedelta(days=1)

    if not frames:
        return pd.DataFrame()
    out = pd.concat(frames, ignore_index=True)
    logger.info("同花顺板块指数行情区间 %s～%s 共 %d 条", start, end, len(out))
    return out


@with_retry(max_retries=3, retry_on_empty=False)
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
