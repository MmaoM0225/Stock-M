"""
基本面数据获取模块
包括公司基本信息、财务报表数据等
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


class FundamentalDataFetcher:
    """基本面数据获取器"""
    
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
    
    async def get_company_info(self, ts_code: str) -> pd.DataFrame:
        """
        获取公司基本信息
        
        Args:
            ts_code: 股票代码
        
        Returns:
            公司基本信息DataFrame
        """
        if not self.tushare_enabled:
            raise DataFlowException("Tushare未配置或未启用")
        
        if not validate_stock_code(ts_code, 'cn'):
            raise DataFlowException(f"无效的股票代码: {ts_code}")
        
        try:
            # 限频
            await tushare_limiter.acquire()
            
            logger.info(f"获取公司基本信息: {ts_code}")
            
            # 获取股票基本信息
            basic_info = self.ts_pro.stock_basic(ts_code=ts_code)
            
            if basic_info.empty:
                logger.warning(f"未获取到基本信息: {ts_code}")
                return pd.DataFrame()
            
            # 获取公司详细信息
            await tushare_limiter.acquire()
            company_info = self.ts_pro.stock_company(ts_code=ts_code)
            
            # 合并信息
            if not company_info.empty:
                result = pd.merge(basic_info, company_info, on='ts_code', how='left')
            else:
                result = basic_info
            
            result = clean_dataframe(result)
            
            logger.info(f"成功获取公司基本信息: {ts_code}")
            return result
            
        except Exception as e:
            logger.error(f"获取公司基本信息失败: {e}")
            raise DataFlowException(f"获取公司基本信息失败: {e}")
    
    async def get_income_statement(
        self,
        ts_code: str,
        start_date: str,
        end_date: str,
        report_type: str = '1'
    ) -> pd.DataFrame:
        """
        获取利润表数据
        
        Args:
            ts_code: 股票代码
            start_date: 开始日期
            end_date: 结束日期
            report_type: 报告类型 ('1':合并报表, '2':单季合并, '3':调整单季合并表, '4':调整合并报表, '5':调整前合并报表)
        
        Returns:
            利润表DataFrame
        """
        if not self.tushare_enabled:
            raise DataFlowException("Tushare未配置或未启用")
        
        try:
            # 格式化日期
            start_date_fmt = format_date(start_date, 'tushare')
            end_date_fmt = format_date(end_date, 'tushare')
            
            # 限频
            await tushare_limiter.acquire()
            
            logger.info(f"获取利润表: {ts_code}, {start_date_fmt} - {end_date_fmt}")
            
            # 获取利润表数据
            df = self.ts_pro.income(
                ts_code=ts_code,
                start_date=start_date_fmt,
                end_date=end_date_fmt,
                report_type=report_type
            )
            
            if df.empty:
                logger.warning(f"未获取到利润表数据: {ts_code}")
                return pd.DataFrame()
            
            # 数据处理
            df = clean_dataframe(df)
            df = df.sort_values('end_date').reset_index(drop=True)
            
            logger.info(f"成功获取 {len(df)} 条利润表数据")
            return df
            
        except Exception as e:
            logger.error(f"获取利润表失败: {e}")
            raise DataFlowException(f"获取利润表失败: {e}")
    
    async def get_balance_sheet(
        self,
        ts_code: str,
        start_date: str,
        end_date: str,
        report_type: str = '1'
    ) -> pd.DataFrame:
        """
        获取资产负债表数据
        
        Args:
            ts_code: 股票代码
            start_date: 开始日期
            end_date: 结束日期
            report_type: 报告类型
        
        Returns:
            资产负债表DataFrame
        """
        if not self.tushare_enabled:
            raise DataFlowException("Tushare未配置或未启用")
        
        try:
            # 格式化日期
            start_date_fmt = format_date(start_date, 'tushare')
            end_date_fmt = format_date(end_date, 'tushare')
            
            # 限频
            await tushare_limiter.acquire()
            
            logger.info(f"获取资产负债表: {ts_code}, {start_date_fmt} - {end_date_fmt}")
            
            # 获取资产负债表数据
            df = self.ts_pro.balancesheet(
                ts_code=ts_code,
                start_date=start_date_fmt,
                end_date=end_date_fmt,
                report_type=report_type
            )
            
            if df.empty:
                logger.warning(f"未获取到资产负债表数据: {ts_code}")
                return pd.DataFrame()
            
            # 数据处理
            df = clean_dataframe(df)
            df = df.sort_values('end_date').reset_index(drop=True)
            
            logger.info(f"成功获取 {len(df)} 条资产负债表数据")
            return df
            
        except Exception as e:
            logger.error(f"获取资产负债表失败: {e}")
            raise DataFlowException(f"获取资产负债表失败: {e}")
    
    async def get_cashflow_statement(
        self,
        ts_code: str,
        start_date: str,
        end_date: str,
        report_type: str = '1'
    ) -> pd.DataFrame:
        """
        获取现金流量表数据
        
        Args:
            ts_code: 股票代码
            start_date: 开始日期
            end_date: 结束日期
            report_type: 报告类型
        
        Returns:
            现金流量表DataFrame
        """
        if not self.tushare_enabled:
            raise DataFlowException("Tushare未配置或未启用")
        
        try:
            # 格式化日期
            start_date_fmt = format_date(start_date, 'tushare')
            end_date_fmt = format_date(end_date, 'tushare')
            
            # 限频
            await tushare_limiter.acquire()
            
            logger.info(f"获取现金流量表: {ts_code}, {start_date_fmt} - {end_date_fmt}")
            
            # 获取现金流量表数据
            df = self.ts_pro.cashflow(
                ts_code=ts_code,
                start_date=start_date_fmt,
                end_date=end_date_fmt,
                report_type=report_type
            )
            
            if df.empty:
                logger.warning(f"未获取到现金流量表数据: {ts_code}")
                return pd.DataFrame()
            
            # 数据处理
            df = clean_dataframe(df)
            df = df.sort_values('end_date').reset_index(drop=True)
            
            logger.info(f"成功获取 {len(df)} 条现金流量表数据")
            return df
            
        except Exception as e:
            logger.error(f"获取现金流量表失败: {e}")
            raise DataFlowException(f"获取现金流量表失败: {e}")
    
    async def get_financial_indicators(
        self,
        ts_code: str,
        start_date: str,
        end_date: str
    ) -> pd.DataFrame:
        """
        获取财务指标数据
        
        Args:
            ts_code: 股票代码
            start_date: 开始日期
            end_date: 结束日期
        
        Returns:
            财务指标DataFrame
        """
        if not self.tushare_enabled:
            raise DataFlowException("Tushare未配置或未启用")
        
        try:
            # 格式化日期
            start_date_fmt = format_date(start_date, 'tushare')
            end_date_fmt = format_date(end_date, 'tushare')
            
            # 限频
            await tushare_limiter.acquire()
            
            logger.info(f"获取财务指标: {ts_code}, {start_date_fmt} - {end_date_fmt}")
            
            # 获取财务指标数据
            df = self.ts_pro.fina_indicator(
                ts_code=ts_code,
                start_date=start_date_fmt,
                end_date=end_date_fmt
            )
            
            if df.empty:
                logger.warning(f"未获取到财务指标数据: {ts_code}")
                return pd.DataFrame()
            
            # 数据处理
            df = clean_dataframe(df)
            df = df.sort_values('end_date').reset_index(drop=True)
            
            logger.info(f"成功获取 {len(df)} 条财务指标数据")
            return df
            
        except Exception as e:
            logger.error(f"获取财务指标失败: {e}")
            raise DataFlowException(f"获取财务指标失败: {e}")
    
    async def get_dividend_data(
        self,
        ts_code: str,
        start_date: str,
        end_date: str
    ) -> pd.DataFrame:
        """
        获取分红送股数据
        
        Args:
            ts_code: 股票代码
            start_date: 开始日期
            end_date: 结束日期
        
        Returns:
            分红送股DataFrame
        """
        if not self.tushare_enabled:
            raise DataFlowException("Tushare未配置或未启用")
        
        try:
            # 格式化日期
            start_date_fmt = format_date(start_date, 'tushare')
            end_date_fmt = format_date(end_date, 'tushare')
            
            # 限频
            await tushare_limiter.acquire()
            
            logger.info(f"获取分红送股数据: {ts_code}, {start_date_fmt} - {end_date_fmt}")
            
            # 获取分红送股数据
            df = self.ts_pro.dividend(
                ts_code=ts_code,
                start_date=start_date_fmt,
                end_date=end_date_fmt
            )
            
            if df.empty:
                logger.warning(f"未获取到分红送股数据: {ts_code}")
                return pd.DataFrame()
            
            # 数据处理
            df = clean_dataframe(df)
            df = df.sort_values('div_proc').reset_index(drop=True)
            
            logger.info(f"成功获取 {len(df)} 条分红送股数据")
            return df
            
        except Exception as e:
            logger.error(f"获取分红送股数据失败: {e}")
            raise DataFlowException(f"获取分红送股数据失败: {e}")
    
    async def get_forecast_data(
        self,
        ts_code: str,
        start_date: str,
        end_date: str
    ) -> pd.DataFrame:
        """
        获取业绩预告数据
        
        Args:
            ts_code: 股票代码
            start_date: 开始日期
            end_date: 结束日期
        
        Returns:
            业绩预告DataFrame
        """
        if not self.tushare_enabled:
            raise DataFlowException("Tushare未配置或未启用")
        
        try:
            # 格式化日期
            start_date_fmt = format_date(start_date, 'tushare')
            end_date_fmt = format_date(end_date, 'tushare')
            
            # 限频
            await tushare_limiter.acquire()
            
            logger.info(f"获取业绩预告: {ts_code}, {start_date_fmt} - {end_date_fmt}")
            
            # 获取业绩预告数据
            df = self.ts_pro.forecast(
                ts_code=ts_code,
                start_date=start_date_fmt,
                end_date=end_date_fmt
            )
            
            if df.empty:
                logger.warning(f"未获取到业绩预告数据: {ts_code}")
                return pd.DataFrame()
            
            # 数据处理
            df = clean_dataframe(df)
            df = df.sort_values('end_date').reset_index(drop=True)
            
            logger.info(f"成功获取 {len(df)} 条业绩预告数据")
            return df
            
        except Exception as e:
            logger.error(f"获取业绩预告失败: {e}")
            raise DataFlowException(f"获取业绩预告失败: {e}")
    
    async def get_express_data(
        self,
        ts_code: str,
        start_date: str,
        end_date: str
    ) -> pd.DataFrame:
        """
        获取业绩快报数据
        
        Args:
            ts_code: 股票代码
            start_date: 开始日期
            end_date: 结束日期
        
        Returns:
            业绩快报DataFrame
        """
        if not self.tushare_enabled:
            raise DataFlowException("Tushare未配置或未启用")
        
        try:
            # 格式化日期
            start_date_fmt = format_date(start_date, 'tushare')
            end_date_fmt = format_date(end_date, 'tushare')
            
            # 限频
            await tushare_limiter.acquire()
            
            logger.info(f"获取业绩快报: {ts_code}, {start_date_fmt} - {end_date_fmt}")
            
            # 获取业绩快报数据
            df = self.ts_pro.express(
                ts_code=ts_code,
                start_date=start_date_fmt,
                end_date=end_date_fmt
            )
            
            if df.empty:
                logger.warning(f"未获取到业绩快报数据: {ts_code}")
                return pd.DataFrame()
            
            # 数据处理
            df = clean_dataframe(df)
            df = df.sort_values('end_date').reset_index(drop=True)
            
            logger.info(f"成功获取 {len(df)} 条业绩快报数据")
            return df
            
        except Exception as e:
            logger.error(f"获取业绩快报失败: {e}")
            raise DataFlowException(f"获取业绩快报失败: {e}")
    
    async def get_all_financial_data(
        self,
        ts_code: str,
        start_date: str,
        end_date: str,
        report_type: str = '1'
    ) -> Dict[str, pd.DataFrame]:
        """
        获取所有财务数据
        
        Args:
            ts_code: 股票代码
            start_date: 开始日期
            end_date: 结束日期
            report_type: 报告类型
        
        Returns:
            包含所有财务数据的字典
        """
        try:
            logger.info(f"获取所有财务数据: {ts_code}")
            
            # 并发获取所有财务数据
            tasks = [
                self.get_income_statement(ts_code, start_date, end_date, report_type),
                self.get_balance_sheet(ts_code, start_date, end_date, report_type),
                self.get_cashflow_statement(ts_code, start_date, end_date, report_type),
                self.get_financial_indicators(ts_code, start_date, end_date),
                self.get_dividend_data(ts_code, start_date, end_date),
                self.get_forecast_data(ts_code, start_date, end_date),
                self.get_express_data(ts_code, start_date, end_date)
            ]
            
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            # 组织结果
            financial_data = {
                'income_statement': results[0] if not isinstance(results[0], Exception) else pd.DataFrame(),
                'balance_sheet': results[1] if not isinstance(results[1], Exception) else pd.DataFrame(),
                'cashflow_statement': results[2] if not isinstance(results[2], Exception) else pd.DataFrame(),
                'financial_indicators': results[3] if not isinstance(results[3], Exception) else pd.DataFrame(),
                'dividend_data': results[4] if not isinstance(results[4], Exception) else pd.DataFrame(),
                'forecast_data': results[5] if not isinstance(results[5], Exception) else pd.DataFrame(),
                'express_data': results[6] if not isinstance(results[6], Exception) else pd.DataFrame()
            }
            
            # 记录异常
            for i, result in enumerate(results):
                if isinstance(result, Exception):
                    logger.error(f"获取财务数据失败 (任务{i}): {result}")
            
            logger.info(f"成功获取财务数据: {ts_code}")
            return financial_data
            
        except Exception as e:
            logger.error(f"获取所有财务数据失败: {e}")
            raise DataFlowException(f"获取所有财务数据失败: {e}")


# 便捷函数
async def get_company_basic_info(ts_code: str) -> pd.DataFrame:
    """
    获取公司基本信息的便捷函数
    
    Args:
        ts_code: 股票代码
    
    Returns:
        公司基本信息DataFrame
    """
    async with FundamentalDataFetcher() as fetcher:
        return await fetcher.get_company_info(ts_code)


async def get_income_statement(
    ts_code: str,
    start_date: str,
    end_date: str,
    report_type: str = '1'
) -> pd.DataFrame:
    """
    获取利润表的便捷函数
    """
    async with FundamentalDataFetcher() as fetcher:
        return await fetcher.get_income_statement(ts_code, start_date, end_date, report_type)


async def get_balance_sheet(
    ts_code: str,
    start_date: str,
    end_date: str,
    report_type: str = '1'
) -> pd.DataFrame:
    """
    获取资产负债表的便捷函数
    """
    async with FundamentalDataFetcher() as fetcher:
        return await fetcher.get_balance_sheet(ts_code, start_date, end_date, report_type)


async def get_cashflow_statement(
    ts_code: str,
    start_date: str,
    end_date: str,
    report_type: str = '1'
) -> pd.DataFrame:
    """
    获取现金流量表的便捷函数
    """
    async with FundamentalDataFetcher() as fetcher:
        return await fetcher.get_cashflow_statement(ts_code, start_date, end_date, report_type)


async def get_financial_indicators(
    ts_code: str,
    start_date: str,
    end_date: str
) -> pd.DataFrame:
    """
    获取财务指标的便捷函数
    """
    async with FundamentalDataFetcher() as fetcher:
        return await fetcher.get_financial_indicators(ts_code, start_date, end_date)


async def get_all_financial_data(
    ts_code: str,
    start_date: str,
    end_date: str,
    report_type: str = '1'
) -> Dict[str, pd.DataFrame]:
    """
    获取所有财务数据的便捷函数
    """
    async with FundamentalDataFetcher() as fetcher:
        return await fetcher.get_all_financial_data(ts_code, start_date, end_date, report_type)
