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
    clean_dataframe, calculate_technical_indicators,
    tushare_limiter, DataFlowException
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
        adj: str = 'qfq',
        with_indicators: bool = True
    ) -> pd.DataFrame:
        """
        获取日线行情数据
        
        Args:
            ts_code: 股票代码 (如: 000001.SZ)
            start_date: 开始日期 (YYYYMMDD 或 YYYY-MM-DD)
            end_date: 结束日期 (YYYYMMDD 或 YYYY-MM-DD)
            adj: 复权类型 ('qfq':前复权, 'hfq':后复权, None:不复权)
            with_indicators: 是否计算技术指标
        
        Returns:
            日线数据DataFrame
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
            
            # 计算技术指标
            if with_indicators:
                df = calculate_technical_indicators(df)
            
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
        adj: str = 'qfq',
        with_indicators: bool = True
    ) -> pd.DataFrame:
        """
        获取周线行情数据
        
        Args:
            ts_code: 股票代码
            start_date: 开始日期
            end_date: 结束日期
            adj: 复权类型
            with_indicators: 是否计算技术指标
        
        Returns:
            周线数据DataFrame
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
            
            if with_indicators:
                df = calculate_technical_indicators(df)
            
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
        adj: str = 'qfq',
        with_indicators: bool = True
    ) -> pd.DataFrame:
        """
        获取月线行情数据
        
        Args:
            ts_code: 股票代码
            start_date: 开始日期
            end_date: 结束日期
            adj: 复权类型
            with_indicators: 是否计算技术指标
        
        Returns:
            月线数据DataFrame
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
            
            if with_indicators:
                df = calculate_technical_indicators(df)
            
            logger.info(f"成功获取 {len(df)} 条月线数据")
            return df
            
        except Exception as e:
            logger.error(f"获取月线数据失败: {e}")
            raise DataFlowException(f"获取月线数据失败: {e}")
    
    async def get_minute_data(
        self,
        ts_code: str,
        trade_date: str,
        freq: str = '1min'
    ) -> pd.DataFrame:
        """
        获取分钟级行情数据
        
        Args:
            ts_code: 股票代码
            trade_date: 交易日期
            freq: 频率 ('1min', '5min', '15min', '30min', '60min')
        
        Returns:
            分钟数据DataFrame
        """
        if not self.tushare_enabled:
            raise DataFlowException("Tushare未配置或未启用")
        
        try:
            # 格式化日期
            trade_date_fmt = format_date(trade_date, 'tushare')
            
            # 限频
            await tushare_limiter.acquire()
            
            logger.info(f"获取分钟数据: {ts_code}, {trade_date_fmt}, {freq}")
            
            # 获取分钟数据
            df = ts.get_hist_data(
                ts_code.split('.')[0],  # 去掉后缀
                start=trade_date_fmt,
                end=trade_date_fmt,
                ktype=freq
            )
            
            if df is None or df.empty:
                logger.warning(f"未获取到分钟数据: {ts_code}")
                return pd.DataFrame()
            
            # 重置索引，将日期时间作为列
            df = df.reset_index()
            df['ts_code'] = ts_code
            df['trade_date'] = trade_date_fmt
            
            # 重命名列
            column_mapping = {
                'date': 'trade_time',
                'volume': 'vol'
            }
            df = df.rename(columns=column_mapping)
            
            # 数据处理
            df = clean_dataframe(df)
            
            logger.info(f"成功获取 {len(df)} 条分钟数据")
            return df
            
        except Exception as e:
            logger.error(f"获取分钟数据失败: {e}")
            raise DataFlowException(f"获取分钟数据失败: {e}")
    
    async def get_realtime_data(self, ts_codes: List[str]) -> pd.DataFrame:
        """
        获取实时行情数据
        
        Args:
            ts_codes: 股票代码列表
        
        Returns:
            实时行情DataFrame
        """
        if not self.tushare_enabled:
            raise DataFlowException("Tushare未配置或未启用")
        
        try:
            # 限频
            await tushare_limiter.acquire()
            
            logger.info(f"获取实时数据: {len(ts_codes)} 只股票")
            
            # 获取实时数据
            df = ts.get_realtime_quotes(ts_codes)
            
            if df is None or df.empty:
                logger.warning("未获取到实时数据")
                return pd.DataFrame()
            
            # 数据处理
            df = clean_dataframe(df)
            
            logger.info(f"成功获取 {len(df)} 条实时数据")
            return df
            
        except Exception as e:
            logger.error(f"获取实时数据失败: {e}")
            raise DataFlowException(f"获取实时数据失败: {e}")
    
    async def get_stock_list(self, market: str = 'all') -> pd.DataFrame:
        """
        获取股票列表
        
        Args:
            market: 市场类型 ('all', 'main', 'sme', 'gem', 'sci', 'bj')
        
        Returns:
            股票列表DataFrame
        """
        if not self.tushare_enabled:
            raise DataFlowException("Tushare未配置或未启用")
        
        try:
            # 限频
            await tushare_limiter.acquire()
            
            logger.info(f"获取股票列表: {market}")
            
            # 获取股票列表
            if market == 'all':
                df = self.ts_pro.stock_basic(exchange='', list_status='L')
            else:
                df = self.ts_pro.stock_basic(market=market, list_status='L')
            
            if df.empty:
                logger.warning("未获取到股票列表")
                return pd.DataFrame()
            
            # 数据处理
            df = clean_dataframe(df)
            
            logger.info(f"成功获取 {len(df)} 只股票信息")
            return df
            
        except Exception as e:
            logger.error(f"获取股票列表失败: {e}")
            raise DataFlowException(f"获取股票列表失败: {e}")
    
    async def get_trading_calendar(
        self,
        start_date: str,
        end_date: str,
        exchange: str = 'SSE'
    ) -> pd.DataFrame:
        """
        获取交易日历
        
        Args:
            start_date: 开始日期
            end_date: 结束日期
            exchange: 交易所 ('SSE', 'SZSE')
        
        Returns:
            交易日历DataFrame
        """
        if not self.tushare_enabled:
            raise DataFlowException("Tushare未配置或未启用")
        
        try:
            # 格式化日期
            start_date_fmt = format_date(start_date, 'tushare')
            end_date_fmt = format_date(end_date, 'tushare')
            
            # 限频
            await tushare_limiter.acquire()
            
            logger.info(f"获取交易日历: {start_date_fmt} - {end_date_fmt}")
            
            # 获取交易日历
            df = self.ts_pro.trade_cal(
                exchange=exchange,
                start_date=start_date_fmt,
                end_date=end_date_fmt
            )
            
            if df.empty:
                logger.warning("未获取到交易日历")
                return pd.DataFrame()
            
            # 数据处理
            df = clean_dataframe(df)
            
            logger.info(f"成功获取 {len(df)} 条交易日历数据")
            return df
            
        except Exception as e:
            logger.error(f"获取交易日历失败: {e}")
            raise DataFlowException(f"获取交易日历失败: {e}")


# 便捷函数
async def get_daily_kline(
    ts_code: str,
    start_date: str,
    end_date: str,
    adj: str = 'qfq',
    with_indicators: bool = True
) -> pd.DataFrame:
    """
    获取日线K线数据的便捷函数
    
    Args:
        ts_code: 股票代码
        start_date: 开始日期
        end_date: 结束日期
        adj: 复权类型
        with_indicators: 是否计算技术指标
    
    Returns:
        日线数据DataFrame
    """
    async with KLineDataFetcher() as fetcher:
        return await fetcher.get_daily_data(ts_code, start_date, end_date, adj, with_indicators)


async def get_weekly_kline(
    ts_code: str,
    start_date: str,
    end_date: str,
    adj: str = 'qfq',
    with_indicators: bool = True
) -> pd.DataFrame:
    """
    获取周线K线数据的便捷函数
    """
    async with KLineDataFetcher() as fetcher:
        return await fetcher.get_weekly_data(ts_code, start_date, end_date, adj, with_indicators)


async def get_monthly_kline(
    ts_code: str,
    start_date: str,
    end_date: str,
    adj: str = 'qfq',
    with_indicators: bool = True
) -> pd.DataFrame:
    """
    获取月线K线数据的便捷函数
    """
    async with KLineDataFetcher() as fetcher:
        return await fetcher.get_monthly_data(ts_code, start_date, end_date, adj, with_indicators)


async def get_stock_list(market: str = 'all') -> pd.DataFrame:
    """
    获取股票列表的便捷函数
    """
    async with KLineDataFetcher() as fetcher:
        return await fetcher.get_stock_list(market)
