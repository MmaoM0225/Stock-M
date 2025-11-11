"""
市场数据获取模块
包括资金流向、融资融券、龙虎榜、机构数据等
"""
import asyncio
import aiohttp
import pandas as pd
import tushare as ts
from typing import Dict, List, Optional, Any
from datetime import datetime, date
import logging

from .config import DATA_SOURCES
from .utils import (
    format_date, validate_stock_code, async_request,
    clean_dataframe, tushare_limiter, DataFlowException
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
        
        self.session: Optional[aiohttp.ClientSession] = None
    
    async def __aenter__(self):
        """异步上下文管理器入口"""
        self.session = aiohttp.ClientSession()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """异步上下文管理器出口"""
        if self.session:
            await self.session.close()
    
    async def get_money_flow(
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
            
            # 限频
            await tushare_limiter.acquire()
            
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
    
    async def get_margin_detail(
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
            
            # 限频
            await tushare_limiter.acquire()
            
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
    
    async def get_margin_target(self, ts_code: str = None) -> pd.DataFrame:
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
            # 限频
            await tushare_limiter.acquire()
            
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
    
    async def get_top10_holders(
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
            
            # 限频
            await tushare_limiter.acquire()
            
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
    
    async def get_top10_floatholders(
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
            
            # 限频
            await tushare_limiter.acquire()
            
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
    
    async def get_dragon_tiger_list(
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
            
            # 限频
            await tushare_limiter.acquire()
            
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
    
    async def get_dragon_tiger_institutions(
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
            
            # 限频
            await tushare_limiter.acquire()
            
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
    
    async def get_block_trade(
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
            
            # 限频
            await tushare_limiter.acquire()
            
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
    
    async def get_stk_holdernumber(
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
            
            # 限频
            await tushare_limiter.acquire()
            
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
    
    async def get_concept_detail(self, id: str) -> pd.DataFrame:
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
            # 限频
            await tushare_limiter.acquire()
            
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
    
    async def get_index_weight(
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
            
            # 限频
            await tushare_limiter.acquire()
            
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


# 便捷函数
async def get_money_flow(
    ts_code: str,
    start_date: str,
    end_date: str
) -> pd.DataFrame:
    """
    获取资金流向的便捷函数
    """
    async with MarketDataFetcher() as fetcher:
        return await fetcher.get_money_flow(ts_code, start_date, end_date)


async def get_margin_detail(
    trade_date: str,
    ts_code: str = None
) -> pd.DataFrame:
    """
    获取融资融券明细的便捷函数
    """
    async with MarketDataFetcher() as fetcher:
        return await fetcher.get_margin_detail(trade_date, ts_code)


async def get_dragon_tiger_list(
    trade_date: str,
    ts_code: str = None
) -> pd.DataFrame:
    """
    获取龙虎榜的便捷函数
    """
    async with MarketDataFetcher() as fetcher:
        return await fetcher.get_dragon_tiger_list(trade_date, ts_code)


async def get_top10_holders(
    ts_code: str,
    period: str,
    ann_date: str = None
) -> pd.DataFrame:
    """
    获取前十大股东的便捷函数
    """
    async with MarketDataFetcher() as fetcher:
        return await fetcher.get_top10_holders(ts_code, period, ann_date)


async def get_block_trade(
    ts_code: str,
    start_date: str,
    end_date: str
) -> pd.DataFrame:
    """
    获取大宗交易的便捷函数
    """
    async with MarketDataFetcher() as fetcher:
        return await fetcher.get_block_trade(ts_code, start_date, end_date)
