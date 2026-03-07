"""
市场数据获取模块
包括资金流向、融资融券、龙虎榜、机构数据等
"""
import pandas as pd
import tushare as ts
import akshare as ak
from typing import Dict, List, Optional, Any
from datetime import datetime, date
import logging

from .config import DATA_SOURCES
from .utils import (
    format_date, validate_stock_code,
    clean_dataframe, DataFlowException
)

logger = logging.getLogger(__name__)


class MarketDataFetcher:
    """市场数据获取器"""
    
    def __init__(self):
        """初始化"""
        self.tushare_enabled = DATA_SOURCES['tushare']['enabled']
        if self.tushare_enabled:
            ts.set_token(DATA_SOURCES['tushare']['token'])
            self.ts_pro = ts.pro_api()
        
        self.akshare_enabled = DATA_SOURCES['akshare']['enabled']
    
    def get_stock_basic(
        self,
        ts_code: str = None,
        name: str = None,
        market: str = None,
        list_status: str = 'L',
        exchange: str = None,
        is_hs: str = None,
        fields: str = None
    ) -> pd.DataFrame:
        """
        获取基础信息数据，包括股票代码、名称、上市日期、退市日期等
        
        Args:
            ts_code: TS股票代码
            name: 名称
            market: 市场类别（主板/创业板/科创板/CDR/北交所）
            list_status: 上市状态 L上市 D退市 P暂停上市 G过会未交易，默认是L
            exchange: 交易所 SSE上交所 SZSE深交所 BSE北交所
            is_hs: 是否沪深港通标的，N否 H沪股通 S深股通
            fields: 需要获取的字段，如 'ts_code,symbol,name,area,industry,list_date'
        
        Returns:
            基础信息DataFrame
        """
        if not self.tushare_enabled:
            raise DataFlowException("Tushare未配置或未启用")
        
        try:
            logger.info(f"获取股票基础信息: ts_code={ts_code}, list_status={list_status}")
            
            params = {}
            if ts_code:
                params['ts_code'] = ts_code
            if name:
                params['name'] = name
            if market:
                params['market'] = market
            if list_status:
                params['list_status'] = list_status
            if exchange:
                params['exchange'] = exchange
            if is_hs:
                params['is_hs'] = is_hs
            if fields:
                params['fields'] = fields
            
            df = self.ts_pro.stock_basic(**params)
            
            if df.empty:
                logger.warning("未获取到股票基础信息")
                return pd.DataFrame()
            
            df = clean_dataframe(df)
            
            logger.info(f"成功获取 {len(df)} 条股票基础信息")
            return df
            
        except Exception as e:
            logger.error(f"获取股票基础信息失败: {e}")
            raise DataFlowException(f"获取股票基础信息失败: {e}")
    
    def get_money_flow(
        self,
        ts_code: str,
        start_date: str,
        end_date: str
    ) -> pd.DataFrame:
        """
        获取个股资金流向数据
        
        Args:
            ts_code: 股票代码
            start_date: 开始日期
            end_date: 结束日期
        
        Returns:
            资金流向DataFrame
        """
        if not self.tushare_enabled:
            raise DataFlowException("Tushare未配置或未启用")
        
        try:
            # 格式化日期
            start_date_fmt = format_date(start_date, 'tushare')
            end_date_fmt = format_date(end_date, 'tushare')
            
            logger.info(f"获取资金流向: {ts_code}, {start_date_fmt} - {end_date_fmt}")
            
            # 获取资金流向数据
            df = self.ts_pro.moneyflow(
                ts_code=ts_code,
                start_date=start_date_fmt,
                end_date=end_date_fmt
            )
            
            if df.empty:
                logger.warning(f"未获取到资金流向数据: {ts_code}")
                return pd.DataFrame()
            
            # 数据处理
            df = clean_dataframe(df)
            df = df.sort_values('trade_date').reset_index(drop=True)
            
            logger.info(f"成功获取 {len(df)} 条资金流向数据")
            return df
            
        except Exception as e:
            logger.error(f"获取资金流向失败: {e}")
            raise DataFlowException(f"获取资金流向失败: {e}")
    
    def get_margin_detail(
        self,
        trade_date: str,
        ts_code: str = None
    ) -> pd.DataFrame:
        """
        获取融资融券交易明细
        
        Args:
            trade_date: 交易日期
            ts_code: 股票代码（可选）
        
        Returns:
            融资融券明细DataFrame
        """
        if not self.tushare_enabled:
            raise DataFlowException("Tushare未配置或未启用")
        
        try:
            # 格式化日期
            trade_date_fmt = format_date(trade_date, 'tushare')
            
            logger.info(f"获取融资融券明细: {trade_date_fmt}, {ts_code or '全市场'}")
            
            # 获取融资融券明细
            df = self.ts_pro.margin_detail(
                trade_date=trade_date_fmt,
                ts_code=ts_code
            )
            
            if df.empty:
                logger.warning(f"未获取到融资融券明细: {trade_date_fmt}")
                return pd.DataFrame()
            
            # 数据处理
            df = clean_dataframe(df)
            
            logger.info(f"成功获取 {len(df)} 条融资融券明细")
            return df
            
        except Exception as e:
            logger.error(f"获取融资融券明细失败: {e}")
            raise DataFlowException(f"获取融资融券明细失败: {e}")
    
    def get_margin_target(self, ts_code: str = None) -> pd.DataFrame:
        """
        获取融资融券标的
        
        Args:
            ts_code: 股票代码（可选）
        
        Returns:
            融资融券标的DataFrame
        """
        if not self.tushare_enabled:
            raise DataFlowException("Tushare未配置或未启用")
        
        try:
            logger.info(f"获取融资融券标的: {ts_code or '全部'}")
            
            # 获取融资融券标的
            df = self.ts_pro.margin_target(ts_code=ts_code)
            
            if df.empty:
                logger.warning("未获取到融资融券标的")
                return pd.DataFrame()
            
            # 数据处理
            df = clean_dataframe(df)
            
            logger.info(f"成功获取 {len(df)} 条融资融券标的")
            return df
            
        except Exception as e:
            logger.error(f"获取融资融券标的失败: {e}")
            raise DataFlowException(f"获取融资融券标的失败: {e}")
    
    def get_industry_list(
        self,
        level: str = 'L1',
        src: str = 'SW2021'
    ) -> pd.DataFrame:
        """
        获取行业分类列表
        
        Args:
            level: 行业级别，可选值：'L1'(一级行业), 'L2'(二级行业), 'L3'(三级行业)
            src: 行业分类来源，默认为'SW2021'(申万2021版)
        
        Returns:
            行业分类列表DataFrame
        """
        if not self.tushare_enabled:
            raise DataFlowException("Tushare未配置或未启用")
        
        try:
            logger.info(f"获取行业分类列表: level={level}, src={src}")
            
            # 获取行业分类列表
            df = self.ts_pro.index_classify(level=level, src=src)
            
            if df.empty:
                logger.warning(f"未获取到行业分类列表: level={level}, src={src}")
                return pd.DataFrame()
            
            # 数据处理
            df = clean_dataframe(df)
            
            level_name = {'L1': '一级行业', 'L2': '二级行业', 'L3': '三级行业'}.get(level, level)
            logger.info(f"成功获取 {len(df)} 条申万{level_name}分类数据")
            return df
            
        except Exception as e:
            logger.error(f"获取行业分类列表失败: {e}")
            raise DataFlowException(f"获取行业分类列表失败: {e}")
    
    def get_industry_members(
        self,
        l1_code: str = None,
        l2_code: str = None,
        l3_code: str = None,
        is_new: str = 'Y'
    ) -> pd.DataFrame:
        """
        获取行业成分股
        
        Args:
            l1_code: 一级行业代码
            l2_code: 二级行业代码
            l3_code: 三级行业代码
            is_new: 是否最新，默认为'Y'（是）
        
        Returns:
            行业成分股DataFrame
        """
        if not self.tushare_enabled:
            raise DataFlowException("Tushare未配置或未启用")
        
        try:
            logger.info(f"获取行业成分股: l1_code={l1_code}, l2_code={l2_code}, l3_code={l3_code}")
            
            # 获取行业成分股
            df = self.ts_pro.index_member_all(
                l1_code=l1_code,
                l2_code=l2_code,
                l3_code=l3_code,
                is_new=is_new
            )
            
            if df.empty:
                logger.warning(f"未获取到行业成分股: l1_code={l1_code}, l2_code={l2_code}, l3_code={l3_code}")
                return pd.DataFrame()
            
            # 数据处理
            df = clean_dataframe(df)
            
            logger.info(f"成功获取 {len(df)} 条行业成分股数据")
            return df
            
        except Exception as e:
            logger.error(f"获取行业成分股失败: {e}")
            raise DataFlowException(f"获取行业成分股失败: {e}")
    
    def get_stock_industry(
        self,
        ts_code: str,
        is_new: str = 'Y'
    ) -> pd.DataFrame:
        """
        查询个股所属行业
        
        Args:
            ts_code: 股票代码
            is_new: 是否最新，默认为'Y'（是）
        
        Returns:
            个股所属行业DataFrame
        """
        if not self.tushare_enabled:
            raise DataFlowException("Tushare未配置或未启用")
        
        try:
            logger.info(f"查询个股所属行业: {ts_code}")
            
            # 查询个股所属行业
            df = self.ts_pro.index_member_all(
                ts_code=ts_code,
                is_new=is_new
            )
            
            if df.empty:
                logger.warning(f"未获取到个股所属行业: {ts_code}")
                return pd.DataFrame()
            
            # 数据处理
            df = clean_dataframe(df)
            
            logger.info(f"成功获取 {ts_code} 的行业分类信息")
            return df
            
        except Exception as e:
            logger.error(f"查询个股所属行业失败: {e}")
            raise DataFlowException(f"查询个股所属行业失败: {e}")
    
    def get_shibor_lpr(
        self,
        start_date: str,
        end_date: str,
        fields: str = None
    ) -> pd.DataFrame:
        """
        获取贷款市场报价利率(LPR)数据
        
        Args:
            start_date: 开始日期
            end_date: 结束日期
            fields: 需要获取的字段，如 'date,1y'，默认获取全部字段
        
        Returns:
            LPR利率DataFrame
        """
        if not self.tushare_enabled:
            raise DataFlowException("Tushare未配置或未启用")
        
        try:
            # 格式化日期
            start_date_fmt = format_date(start_date, 'tushare')
            end_date_fmt = format_date(end_date, 'tushare')
            
            logger.info(f"获取LPR利率数据: {start_date_fmt} - {end_date_fmt}")
            
            # 获取LPR利率数据
            if fields:
                df = self.ts_pro.shibor_lpr(
                    start_date=start_date_fmt,
                    end_date=end_date_fmt,
                    fields=fields
                )
            else:
                df = self.ts_pro.shibor_lpr(
                    start_date=start_date_fmt,
                    end_date=end_date_fmt
                )
            
            if df.empty:
                logger.warning(f"未获取到LPR利率数据: {start_date_fmt} - {end_date_fmt}")
                return pd.DataFrame()
            
            # 数据处理
            df = clean_dataframe(df)
            df = df.sort_values('date').reset_index(drop=True)
            
            logger.info(f"成功获取 {len(df)} 条LPR利率数据")
            return df
            
        except Exception as e:
            logger.error(f"获取LPR利率数据失败: {e}")
            raise DataFlowException(f"获取LPR利率数据失败: {e}")
    
    def get_cpi(
        self,
        start_m: str = None,
        end_m: str = None,
        m: str = None,
        fields: str = None
    ) -> pd.DataFrame:
        """
        获取居民消费价格指数(CPI)数据
        
        Args:
            start_m: 开始月份（YYYYMM）
            end_m: 结束月份（YYYYMM）
            m: 指定月份（YYYYMM），支持多个月份同时输入，逗号分隔
            fields: 需要获取的字段，如 'month,nt_val,nt_yoy'，默认获取全部字段
        
        Returns:
            CPI数据DataFrame，包含全国、城市和农村的CPI数据
        """
        if not self.tushare_enabled:
            raise DataFlowException("Tushare未配置或未启用")
        
        try:
            logger.info(f"获取CPI数据: start_m={start_m}, end_m={end_m}, m={m}")
            
            # 获取CPI数据
            if fields:
                df = self.ts_pro.cn_cpi(
                    start_m=start_m,
                    end_m=end_m,
                    m=m,
                    fields=fields
                )
            else:
                df = self.ts_pro.cn_cpi(
                    start_m=start_m,
                    end_m=end_m,
                    m=m
                )
            
            if df.empty:
                logger.warning(f"未获取到CPI数据")
                return pd.DataFrame()
            
            # 数据处理
            df = clean_dataframe(df)
            df = df.sort_values('month').reset_index(drop=True)
            
            logger.info(f"成功获取 {len(df)} 条CPI数据")
            return df
            
        except Exception as e:
            logger.error(f"获取CPI数据失败: {e}")
            raise DataFlowException(f"获取CPI数据失败: {e}")
    
    def get_sf_month(
        self,
        start_m: str = None,
        end_m: str = None,
        m: str = None
    ) -> pd.DataFrame:
        """
        获取月度社会融资数据
        
        Args:
            start_m: 开始月份（YYYYMM）
            end_m: 结束月份（YYYYMM）
            m: 指定月份（YYYYMM），支持多个月份同时输入，逗号分隔
        
        Returns:
            社融数据DataFrame，包含社融增量和存量数据
        """
        if not self.tushare_enabled:
            raise DataFlowException("Tushare未配置或未启用")
        
        try:
            logger.info(f"获取社融数据: start_m={start_m}, end_m={end_m}, m={m}")
            
            # 获取社融数据
            df = self.ts_pro.sf_month(
                start_m=start_m,
                end_m=end_m,
                m=m
            )
            
            if df.empty:
                logger.warning(f"未获取到社融数据")
                return pd.DataFrame()
            
            # 数据处理
            df = clean_dataframe(df)
            df = df.sort_values('month').reset_index(drop=True)
            
            logger.info(f"成功获取 {len(df)} 条社融数据")
            return df
            
        except Exception as e:
            logger.error(f"获取社融数据失败: {e}")
            raise DataFlowException(f"获取社融数据失败: {e}")
    

# 便捷函数
def get_stock_basic(
    ts_code: str = None,
    name: str = None,
    market: str = None,
    list_status: str = 'L',
    exchange: str = None,
    is_hs: str = None,
    fields: str = None
) -> pd.DataFrame:
    """
    获取股票基础信息的便捷函数
    
    Args:
        ts_code: TS股票代码
        name: 名称
        market: 市场类别（主板/创业板/科创板/CDR/北交所）
        list_status: 上市状态 L上市 D退市 P暂停上市 G过会未交易，默认是L
        exchange: 交易所 SSE上交所 SZSE深交所 BSE北交所
        is_hs: 是否沪深港通标的，N否 H沪股通 S深股通
        fields: 需要获取的字段，如 'ts_code,symbol,name,area,industry,list_date'
    
    Returns:
        基础信息DataFrame
    
    Example:
        >>> df = get_stock_basic(exchange='', list_status='L', fields='ts_code,symbol,name,area,industry,list_date')
        >>> df = get_stock_basic(ts_code='000001.SZ')
    """
    fetcher = MarketDataFetcher()
    return fetcher.get_stock_basic(ts_code, name, market, list_status, exchange, is_hs, fields)


def get_money_flow(
    ts_code: str,
    start_date: str,
    end_date: str
) -> pd.DataFrame:
    """
    获取资金流向的便捷函数
    """
    fetcher = MarketDataFetcher()
    return fetcher.get_money_flow(ts_code, start_date, end_date)


def get_margin_detail(
    trade_date: str,
    ts_code: str = None
) -> pd.DataFrame:
    """
    获取融资融券明细的便捷函数
    """
    fetcher = MarketDataFetcher()
    return fetcher.get_margin_detail(trade_date, ts_code)


def get_dragon_tiger_list(
    trade_date: str,
    ts_code: str = None
) -> pd.DataFrame:
    """
    获取龙虎榜的便捷函数
    """
    fetcher = MarketDataFetcher()
    return fetcher.get_dragon_tiger_list(trade_date, ts_code)


def get_top10_holders(
    ts_code: str,
    period: str,
    ann_date: str = None
) -> pd.DataFrame:
    """
    获取前十大股东的便捷函数
    """
    fetcher = MarketDataFetcher()
    return fetcher.get_top10_holders(ts_code, period, ann_date)


def get_block_trade(
    ts_code: str,
    start_date: str,
    end_date: str
) -> pd.DataFrame:
    """
    获取大宗交易的便捷函数
    """
    fetcher = MarketDataFetcher()
    return fetcher.get_block_trade(ts_code, start_date, end_date)


def get_industry_list(
    level: str = 'L1',
    src: str = 'SW2021'
) -> pd.DataFrame:
    """
    获取行业分类列表的便捷函数
    
    Args:
        level: 行业级别，可选值：'L1'(一级行业), 'L2'(二级行业), 'L3'(三级行业)
        src: 行业分类来源，默认为'SW2021'(申万2021版)
    
    Returns:
        行业分类列表DataFrame
    """
    fetcher = MarketDataFetcher()
    return fetcher.get_industry_list(level, src)


def get_industry_members(
    l1_code: str = None,
    l2_code: str = None,
    l3_code: str = None,
    is_new: str = 'Y'
) -> pd.DataFrame:
    """
    获取行业成分股的便捷函数
    
    Args:
        l1_code: 一级行业代码
        l2_code: 二级行业代码
        l3_code: 三级行业代码
        is_new: 是否最新，默认为'Y'（是）
    
    Returns:
        行业成分股DataFrame
    """
    fetcher = MarketDataFetcher()
    return fetcher.get_industry_members(l1_code, l2_code, l3_code, is_new)


def get_stock_industry(
    ts_code: str,
    is_new: str = 'Y'
) -> pd.DataFrame:
    """
    查询个股所属行业的便捷函数
    
    Args:
        ts_code: 股票代码
        is_new: 是否最新，默认为'Y'（是）
    
    Returns:
        个股所属行业DataFrame
    """
    fetcher = MarketDataFetcher()
    return fetcher.get_stock_industry(ts_code, is_new)


def get_shibor_lpr(
    start_date: str,
    end_date: str,
    fields: str = None
) -> pd.DataFrame:
    """
    获取贷款市场报价利率(LPR)数据的便捷函数
    
    Args:
        start_date: 开始日期
        end_date: 结束日期
        fields: 需要获取的字段，如 'date,1y'，默认获取全部字段
    
    Returns:
        LPR利率DataFrame
    
    Example:
        >>> df = get_shibor_lpr('20180101', '20181130', 'date,1y')
        >>> df = get_shibor_lpr('20180101', '20181130')
    """
    fetcher = MarketDataFetcher()
    return fetcher.get_shibor_lpr(start_date, end_date, fields)


def get_cpi(
    start_m: str = None,
    end_m: str = None,
    m: str = None,
    fields: str = None
) -> pd.DataFrame:
    """
    获取居民消费价格指数(CPI)数据的便捷函数
    
    Args:
        start_m: 开始月份（YYYYMM）
        end_m: 结束月份（YYYYMM）
        m: 指定月份（YYYYMM），支持多个月份同时输入，逗号分隔
        fields: 需要获取的字段，如 'month,nt_val,nt_yoy'，默认获取全部字段
    
    Returns:
        CPI数据DataFrame，包含全国、城市和农村的CPI数据
    
    Example:
        >>> df = get_cpi(start_m='201801', end_m='201903')
        >>> df = get_cpi(start_m='201801', end_m='201903', fields='month,nt_val,nt_yoy')
        >>> df = get_cpi(m='201801,201802,201803')
    """
    fetcher = MarketDataFetcher()
    return fetcher.get_cpi(start_m, end_m, m, fields)


def get_sf_month(
    start_m: str = None,
    end_m: str = None,
    m: str = None
) -> pd.DataFrame:
    """
    获取月度社会融资数据的便捷函数
    
    Args:
        start_m: 开始月份（YYYYMM）
        end_m: 结束月份（YYYYMM）
        m: 指定月份（YYYYMM），支持多个月份同时输入，逗号分隔
    
    Returns:
        社融数据DataFrame，包含社融增量和存量数据
    
    Example:
        >>> df = get_sf_month(start_m='201901', end_m='202307')
        >>> df = get_sf_month(m='201901,201902,201903')
    """
    fetcher = MarketDataFetcher()
    return fetcher.get_sf_month(start_m, end_m, m)

