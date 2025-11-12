"""
K线数据获取模块
支持日线、周线、月线行情数据获取
"""
import asyncio
import aiohttp
import pandas as pd
import tushare as ts
from typing import Dict, List, Optional, Any
from datetime import datetime, date, timedelta
import logging

from .config import DATA_SOURCES
from .utils import (
    format_date, validate_stock_code, async_request, 
    clean_dataframe, tushare_limiter, DataFlowException
)

logger = logging.getLogger(__name__)


class KLineDataFetcher:
    """K线数据获取器"""
    
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
    
    async def get_daily_data(
        self,
        ts_code: str,
        start_date: str,
        end_date: str,
        adj: str = 'qfq'
    ) -> pd.DataFrame:
        """
        获取股票日线行情数据
        
        Args:
            ts_code: 股票代码，支持多个股票同时提取，逗号分隔 (如: 000001.SZ 或 000001.SZ,600000.SH)
            start_date: 开始日期，格式为 YYYYMMDD 或 YYYY-MM-DD (如: 20180701)
            end_date: 结束日期，格式为 YYYYMMDD 或 YYYY-MM-DD (如: 20180718)
            adj: 复权类型 ('qfq':前复权, 'hfq':后复权, None:不复权)
        
        Returns:
            pd.DataFrame: 包含以下字段的日线行情数据
                - ts_code (str): 股票代码
                - trade_date (str): 交易日期
                - open (float): 开盘价
                - high (float): 最高价
                - low (float): 最低价
                - close (float): 收盘价
                - pre_close (float): 昨收价【除权价，前复权】
                - change (float): 涨跌额
                - pct_chg (float): 涨跌幅【基于除权后的昨收计算：（今收-除权昨收）/除权昨收】
                - vol (float): 成交量（手）
                - amount (float): 成交额（千元）
                - adj_factor (float): 复权因子（如果启用复权）
        
        Raises:
            DataFlowException: 当 Tushare 未配置、股票代码无效或数据获取失败时
        
        Note:
            - 本接口是未复权行情，停牌期间不提供数据
            - 交易日每天15点～16点之间入库
            - 基础积分每分钟内可调取500次，每次6000条数据
        """
        if not self.tushare_enabled:
            raise DataFlowException("Tushare未配置或未启用")
        
        if not validate_stock_code(ts_code, 'cn'):
            raise DataFlowException(f"无效的股票代码: {ts_code}")
        
        try:
            # 格式化日期
            start_date_fmt = format_date(start_date, 'tushare')
            end_date_fmt = format_date(end_date, 'tushare')
            
            # 限频
            await tushare_limiter.acquire()
            
            # 获取数据
            logger.info(f"获取日线数据: {ts_code}, {start_date_fmt} - {end_date_fmt}")
            
            if adj:
                # 复权数据
                df = self.ts_pro.adj_factor(
                    ts_code=ts_code,
                    start_date=start_date_fmt,
                    end_date=end_date_fmt
                )
                if not df.empty:
                    # 获取基础行情数据
                    await tushare_limiter.acquire()
                    basic_df = self.ts_pro.daily(
                        ts_code=ts_code,
                        start_date=start_date_fmt,
                        end_date=end_date_fmt
                    )
                    
                    if not basic_df.empty:
                        # 合并复权因子
                        df = pd.merge(basic_df, df, on=['ts_code', 'trade_date'], how='left')
                        df['adj_factor'] = df['adj_factor'].fillna(1.0)
                        
                        # 计算复权价格
                        if adj == 'qfq':  # 前复权
                            latest_factor = df['adj_factor'].iloc[0]
                            df['adj_factor'] = latest_factor / df['adj_factor']
                        
                        for col in ['open', 'high', 'low', 'close', 'pre_close']:
                            if col in df.columns:
                                df[col] = df[col] * df['adj_factor']
                    else:
                        df = basic_df
            else:
                # 不复权数据
                df = self.ts_pro.daily(
                    ts_code=ts_code,
                    start_date=start_date_fmt,
                    end_date=end_date_fmt
                )
            
            if df.empty:
                logger.warning(f"未获取到数据: {ts_code}")
                return pd.DataFrame()
            
            # 数据清理和处理
            df = clean_dataframe(df)
            
            # 按日期排序
            df = df.sort_values('trade_date').reset_index(drop=True)
            
            logger.info(f"成功获取 {len(df)} 条日线数据")
            return df
            
        except Exception as e:
            logger.error(f"获取日线数据失败: {e}")
            raise DataFlowException(f"获取日线数据失败: {e}")
    
    async def get_weekly_data(
        self,
        ts_code: str,
        start_date: str,
        end_date: str,
        adj: str = 'qfq'
    ) -> pd.DataFrame:
        """
        获取股票周线行情数据
        
        Args:
            ts_code: 股票代码，支持多个股票同时提取，逗号分隔 (如: 000001.SZ)
            start_date: 开始日期，格式为 YYYYMMDD 或 YYYY-MM-DD (如: 20180701)
            end_date: 结束日期，格式为 YYYYMMDD 或 YYYY-MM-DD (如: 20180718)
            adj: 复权类型 ('qfq':前复权, 'hfq':后复权, None:不复权)
        
        Returns:
            pd.DataFrame: 包含以下字段的周线行情数据
                - ts_code (str): 股票代码
                - trade_date (str): 交易日期（周的最后一个交易日）
                - open (float): 开盘价（周的第一个交易日开盘价）
                - high (float): 最高价（周内最高价）
                - low (float): 最低价（周内最低价）
                - close (float): 收盘价（周的最后一个交易日收盘价）
                - pre_close (float): 上周收盘价
                - change (float): 涨跌额
                - pct_chg (float): 涨跌幅
                - vol (float): 成交量（手）
                - amount (float): 成交额（千元）
        
        Raises:
            DataFlowException: 当 Tushare 未配置、股票代码无效或数据获取失败时
        """
        if not self.tushare_enabled:
            raise DataFlowException("Tushare未配置或未启用")
        
        try:
            # 格式化日期
            start_date_fmt = format_date(start_date, 'tushare')
            end_date_fmt = format_date(end_date, 'tushare')
            
            # 限频
            await tushare_limiter.acquire()
            
            logger.info(f"获取周线数据: {ts_code}, {start_date_fmt} - {end_date_fmt}")
            
            # 获取周线数据
            df = self.ts_pro.weekly(
                ts_code=ts_code,
                start_date=start_date_fmt,
                end_date=end_date_fmt
            )
            
            if df.empty:
                logger.warning(f"未获取到周线数据: {ts_code}")
                return pd.DataFrame()
            
            # 数据处理
            df = clean_dataframe(df)
            df = df.sort_values('trade_date').reset_index(drop=True)
            
            logger.info(f"成功获取 {len(df)} 条周线数据")
            return df
            
        except Exception as e:
            logger.error(f"获取周线数据失败: {e}")
            raise DataFlowException(f"获取周线数据失败: {e}")
    
    async def get_monthly_data(
        self,
        ts_code: str,
        start_date: str,
        end_date: str,
        adj: str = 'qfq'
    ) -> pd.DataFrame:
        """
        获取股票月线行情数据
        
        Args:
            ts_code: 股票代码，支持多个股票同时提取，逗号分隔 (如: 000001.SZ)
            start_date: 开始日期，格式为 YYYYMMDD 或 YYYY-MM-DD (如: 20180701)
            end_date: 结束日期，格式为 YYYYMMDD 或 YYYY-MM-DD (如: 20180718)
            adj: 复权类型 ('qfq':前复权, 'hfq':后复权, None:不复权)
        
        Returns:
            pd.DataFrame: 包含以下字段的月线行情数据
                - ts_code (str): 股票代码
                - trade_date (str): 交易日期（月的最后一个交易日）
                - open (float): 开盘价（月的第一个交易日开盘价）
                - high (float): 最高价（月内最高价）
                - low (float): 最低价（月内最低价）
                - close (float): 收盘价（月的最后一个交易日收盘价）
                - pre_close (float): 上月收盘价
                - change (float): 涨跌额
                - pct_chg (float): 涨跌幅
                - vol (float): 成交量（手）
                - amount (float): 成交额（千元）
        
        Raises:
            DataFlowException: 当 Tushare 未配置、股票代码无效或数据获取失败时
        """
        if not self.tushare_enabled:
            raise DataFlowException("Tushare未配置或未启用")
        
        try:
            # 格式化日期
            start_date_fmt = format_date(start_date, 'tushare')
            end_date_fmt = format_date(end_date, 'tushare')
            
            # 限频
            await tushare_limiter.acquire()
            
            logger.info(f"获取月线数据: {ts_code}, {start_date_fmt} - {end_date_fmt}")
            
            # 获取月线数据
            df = self.ts_pro.monthly(
                ts_code=ts_code,
                start_date=start_date_fmt,
                end_date=end_date_fmt
            )
            
            if df.empty:
                logger.warning(f"未获取到月线数据: {ts_code}")
                return pd.DataFrame()
            
            # 数据处理
            df = clean_dataframe(df)
            df = df.sort_values('trade_date').reset_index(drop=True)
            
            logger.info(f"成功获取 {len(df)} 条月线数据")
            return df
            
        except Exception as e:
            logger.error(f"获取月线数据失败: {e}")
            raise DataFlowException(f"获取月线数据失败: {e}")
    
