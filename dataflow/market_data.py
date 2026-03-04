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
    
    def get_top10_holders(
        self,
        ts_code: str,
        period: str,
        ann_date: str = None
    ) -> pd.DataFrame:
        """
        获取前十大股东
        
        Args:
            ts_code: 股票代码
            period: 报告期
            ann_date: 公告日期
        
        Returns:
            前十大股东DataFrame
        """
        if not self.tushare_enabled:
            raise DataFlowException("Tushare未配置或未启用")
        
        try:
            # 格式化日期
            period_fmt = format_date(period, 'tushare')
            ann_date_fmt = format_date(ann_date, 'tushare') if ann_date else None
            
            logger.info(f"获取前十大股东: {ts_code}, {period_fmt}")
            
            # 获取前十大股东
            df = self.ts_pro.top10_holders(
                ts_code=ts_code,
                period=period_fmt,
                ann_date=ann_date_fmt
            )
            
            if df.empty:
                logger.warning(f"未获取到前十大股东: {ts_code}")
                return pd.DataFrame()
            
            # 数据处理
            df = clean_dataframe(df)
            
            logger.info(f"成功获取 {len(df)} 条前十大股东数据")
            return df
            
        except Exception as e:
            logger.error(f"获取前十大股东失败: {e}")
            raise DataFlowException(f"获取前十大股东失败: {e}")
    
    def get_top10_floatholders(
        self,
        ts_code: str,
        period: str,
        ann_date: str = None
    ) -> pd.DataFrame:
        """
        获取前十大流通股东
        
        Args:
            ts_code: 股票代码
            period: 报告期
            ann_date: 公告日期
        
        Returns:
            前十大流通股东DataFrame
        """
        if not self.tushare_enabled:
            raise DataFlowException("Tushare未配置或未启用")
        
        try:
            # 格式化日期
            period_fmt = format_date(period, 'tushare')
            ann_date_fmt = format_date(ann_date, 'tushare') if ann_date else None
            
            logger.info(f"获取前十大流通股东: {ts_code}, {period_fmt}")
            
            # 获取前十大流通股东
            df = self.ts_pro.top10_floatholders(
                ts_code=ts_code,
                period=period_fmt,
                ann_date=ann_date_fmt
            )
            
            if df.empty:
                logger.warning(f"未获取到前十大流通股东: {ts_code}")
                return pd.DataFrame()
            
            # 数据处理
            df = clean_dataframe(df)
            
            logger.info(f"成功获取 {len(df)} 条前十大流通股东数据")
            return df
            
        except Exception as e:
            logger.error(f"获取前十大流通股东失败: {e}")
            raise DataFlowException(f"获取前十大流通股东失败: {e}")
    
    def get_dragon_tiger_list(
        self,
        trade_date: str,
        ts_code: str = None
    ) -> pd.DataFrame:
        """
        获取龙虎榜数据
        
        Args:
            trade_date: 交易日期
            ts_code: 股票代码（可选）
        
        Returns:
            龙虎榜DataFrame
        """
        if not self.tushare_enabled:
            raise DataFlowException("Tushare未配置或未启用")
        
        try:
            # 格式化日期
            trade_date_fmt = format_date(trade_date, 'tushare')
            
            logger.info(f"获取龙虎榜: {trade_date_fmt}, {ts_code or '全市场'}")
            
            # 获取龙虎榜数据
            df = self.ts_pro.top_list(
                trade_date=trade_date_fmt,
                ts_code=ts_code
            )
            
            if df.empty:
                logger.warning(f"未获取到龙虎榜数据: {trade_date_fmt}")
                return pd.DataFrame()
            
            # 数据处理
            df = clean_dataframe(df)
            
            logger.info(f"成功获取 {len(df)} 条龙虎榜数据")
            return df
            
        except Exception as e:
            logger.error(f"获取龙虎榜失败: {e}")
            raise DataFlowException(f"获取龙虎榜失败: {e}")
    
    def get_dragon_tiger_institutions(
        self,
        trade_date: str,
        ts_code: str = None
    ) -> pd.DataFrame:
        """
        获取龙虎榜机构交易明细
        
        Args:
            trade_date: 交易日期
            ts_code: 股票代码（可选）
        
        Returns:
            龙虎榜机构明细DataFrame
        """
        if not self.tushare_enabled:
            raise DataFlowException("Tushare未配置或未启用")
        
        try:
            # 格式化日期
            trade_date_fmt = format_date(trade_date, 'tushare')
            
            logger.info(f"获取龙虎榜机构明细: {trade_date_fmt}, {ts_code or '全市场'}")
            
            # 获取龙虎榜机构明细
            df = self.ts_pro.top_inst(
                trade_date=trade_date_fmt,
                ts_code=ts_code
            )
            
            if df.empty:
                logger.warning(f"未获取到龙虎榜机构明细: {trade_date_fmt}")
                return pd.DataFrame()
            
            # 数据处理
            df = clean_dataframe(df)
            
            logger.info(f"成功获取 {len(df)} 条龙虎榜机构明细")
            return df
            
        except Exception as e:
            logger.error(f"获取龙虎榜机构明细失败: {e}")
            raise DataFlowException(f"获取龙虎榜机构明细失败: {e}")
    
    def get_block_trade(
        self,
        ts_code: str,
        start_date: str,
        end_date: str
    ) -> pd.DataFrame:
        """
        获取大宗交易数据
        
        Args:
            ts_code: 股票代码
            start_date: 开始日期
            end_date: 结束日期
        
        Returns:
            大宗交易DataFrame
        """
        if not self.tushare_enabled:
            raise DataFlowException("Tushare未配置或未启用")
        
        try:
            # 格式化日期
            start_date_fmt = format_date(start_date, 'tushare')
            end_date_fmt = format_date(end_date, 'tushare')
            
            logger.info(f"获取大宗交易: {ts_code}, {start_date_fmt} - {end_date_fmt}")
            
            # 获取大宗交易数据
            df = self.ts_pro.block_trade(
                ts_code=ts_code,
                start_date=start_date_fmt,
                end_date=end_date_fmt
            )
            
            if df.empty:
                logger.warning(f"未获取到大宗交易数据: {ts_code}")
                return pd.DataFrame()
            
            # 数据处理
            df = clean_dataframe(df)
            df = df.sort_values('trade_date').reset_index(drop=True)
            
            logger.info(f"成功获取 {len(df)} 条大宗交易数据")
            return df
            
        except Exception as e:
            logger.error(f"获取大宗交易失败: {e}")
            raise DataFlowException(f"获取大宗交易失败: {e}")
    
    def get_stk_holdernumber(
        self,
        ts_code: str,
        start_date: str,
        end_date: str
    ) -> pd.DataFrame:
        """
        获取股东人数数据
        
        Args:
            ts_code: 股票代码
            start_date: 开始日期
            end_date: 结束日期
        
        Returns:
            股东人数DataFrame
        """
        if not self.tushare_enabled:
            raise DataFlowException("Tushare未配置或未启用")
        
        try:
            # 格式化日期
            start_date_fmt = format_date(start_date, 'tushare')
            end_date_fmt = format_date(end_date, 'tushare')
            
            logger.info(f"获取股东人数: {ts_code}, {start_date_fmt} - {end_date_fmt}")
            
            # 获取股东人数数据
            df = self.ts_pro.stk_holdernumber(
                ts_code=ts_code,
                start_date=start_date_fmt,
                end_date=end_date_fmt
            )
            
            if df.empty:
                logger.warning(f"未获取到股东人数数据: {ts_code}")
                return pd.DataFrame()
            
            # 数据处理
            df = clean_dataframe(df)
            df = df.sort_values('end_date').reset_index(drop=True)
            
            logger.info(f"成功获取 {len(df)} 条股东人数数据")
            return df
            
        except Exception as e:
            logger.error(f"获取股东人数失败: {e}")
            raise DataFlowException(f"获取股东人数失败: {e}")
    
    def get_concept_detail(self, id: str) -> pd.DataFrame:
        """
        获取概念股分类明细
        
        Args:
            id: 概念分类ID
        
        Returns:
            概念股明细DataFrame
        """
        if not self.tushare_enabled:
            raise DataFlowException("Tushare未配置或未启用")
        
        try:
            logger.info(f"获取概念股明细: {id}")
            
            # 获取概念股明细
            df = self.ts_pro.concept_detail(id=id)
            
            if df.empty:
                logger.warning(f"未获取到概念股明细: {id}")
                return pd.DataFrame()
            
            # 数据处理
            df = clean_dataframe(df)
            
            logger.info(f"成功获取 {len(df)} 条概念股明细")
            return df
            
        except Exception as e:
            logger.error(f"获取概念股明细失败: {e}")
            raise DataFlowException(f"获取概念股明细失败: {e}")
    
    def get_index_weight(
        self,
        index_code: str,
        start_date: str,
        end_date: str
    ) -> pd.DataFrame:
        """
        获取指数成分和权重
        
        Args:
            index_code: 指数代码
            start_date: 开始日期
            end_date: 结束日期
        
        Returns:
            指数权重DataFrame
        """
        if not self.tushare_enabled:
            raise DataFlowException("Tushare未配置或未启用")
        
        try:
            # 格式化日期
            start_date_fmt = format_date(start_date, 'tushare')
            end_date_fmt = format_date(end_date, 'tushare')
            
            logger.info(f"获取指数权重: {index_code}, {start_date_fmt} - {end_date_fmt}")
            
            # 获取指数权重
            df = self.ts_pro.index_weight(
                index_code=index_code,
                start_date=start_date_fmt,
                end_date=end_date_fmt
            )
            
            if df.empty:
                logger.warning(f"未获取到指数权重: {index_code}")
                return pd.DataFrame()
            
            # 数据处理
            df = clean_dataframe(df)
            df = df.sort_values('trade_date').reset_index(drop=True)
            
            logger.info(f"成功获取 {len(df)} 条指数权重数据")
            return df
            
        except Exception as e:
            logger.error(f"获取指数权重失败: {e}")
            raise DataFlowException(f"获取指数权重失败: {e}")
    
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

