"""
数据管理器 - 统一的数据获取接口
"""
import asyncio
import pandas as pd
from typing import Dict, List, Optional, Any, Union
from datetime import datetime, date, timedelta
import logging

from .kline_data import KLineDataFetcher
from .fundamental_data import FundamentalDataFetcher
from .market_data import MarketDataFetcher
from .news_sentiment import NewsSentimentFetcher
from .utils import DataFlowException, validate_stock_code, format_date

logger = logging.getLogger(__name__)


class DataManager:
    """统一数据管理器"""
    
    def __init__(self):
        """初始化数据管理器"""
        self.kline_fetcher = None
        self.fundamental_fetcher = None
        self.market_fetcher = None
        self.news_fetcher = None
    
    async def __aenter__(self):
        """异步上下文管理器入口"""
        self.kline_fetcher = await KLineDataFetcher().__aenter__()
        self.fundamental_fetcher = await FundamentalDataFetcher().__aenter__()
        self.market_fetcher = await MarketDataFetcher().__aenter__()
        self.news_fetcher = await NewsSentimentFetcher().__aenter__()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """异步上下文管理器出口"""
        if self.kline_fetcher:
            await self.kline_fetcher.__aexit__(exc_type, exc_val, exc_tb)
        if self.fundamental_fetcher:
            await self.fundamental_fetcher.__aexit__(exc_type, exc_val, exc_tb)
        if self.market_fetcher:
            await self.market_fetcher.__aexit__(exc_type, exc_val, exc_tb)
        if self.news_fetcher:
            await self.news_fetcher.__aexit__(exc_type, exc_val, exc_tb)
    
    # K线数据相关方法
    async def get_kline_data(
        self,
        ts_code: str,
        start_date: str,
        end_date: str,
        freq: str = 'daily',
        adj: str = 'qfq',
        with_indicators: bool = True
    ) -> pd.DataFrame:
        """
        获取K线数据
        
        Args:
            ts_code: 股票代码
            start_date: 开始日期
            end_date: 结束日期
            freq: 频率 ('daily', 'weekly', 'monthly', '1min', '5min', '15min', '30min', '60min')
            adj: 复权类型 ('qfq', 'hfq', None)
            with_indicators: 是否计算技术指标
        
        Returns:
            K线数据DataFrame
        """
        try:
            if freq == 'daily':
                return await self.kline_fetcher.get_daily_data(
                    ts_code, start_date, end_date, adj, with_indicators
                )
            elif freq == 'weekly':
                return await self.kline_fetcher.get_weekly_data(
                    ts_code, start_date, end_date, adj, with_indicators
                )
            elif freq == 'monthly':
                return await self.kline_fetcher.get_monthly_data(
                    ts_code, start_date, end_date, adj, with_indicators
                )
            elif freq in ['1min', '5min', '15min', '30min', '60min']:
                # 分钟数据只支持单日获取
                return await self.kline_fetcher.get_minute_data(ts_code, start_date, freq)
            else:
                raise DataFlowException(f"不支持的频率: {freq}")
        
        except Exception as e:
            logger.error(f"获取K线数据失败: {e}")
            raise
    
    async def get_stock_list(self, market: str = 'all') -> pd.DataFrame:
        """获取股票列表"""
        return await self.kline_fetcher.get_stock_list(market)
    
    async def get_trading_calendar(
        self,
        start_date: str,
        end_date: str,
        exchange: str = 'SSE'
    ) -> pd.DataFrame:
        """获取交易日历"""
        return await self.kline_fetcher.get_trading_calendar(start_date, end_date, exchange)
    
    # 基本面数据相关方法
    async def get_company_info(self, ts_code: str) -> pd.DataFrame:
        """获取公司基本信息"""
        return await self.fundamental_fetcher.get_company_info(ts_code)
    
    async def get_financial_statements(
        self,
        ts_code: str,
        start_date: str,
        end_date: str,
        statement_type: str = 'all',
        report_type: str = '1'
    ) -> Union[pd.DataFrame, Dict[str, pd.DataFrame]]:
        """
        获取财务报表数据
        
        Args:
            ts_code: 股票代码
            start_date: 开始日期
            end_date: 结束日期
            statement_type: 报表类型 ('income', 'balance', 'cashflow', 'all')
            report_type: 报告类型
        
        Returns:
            财务报表数据
        """
        try:
            if statement_type == 'income':
                return await self.fundamental_fetcher.get_income_statement(
                    ts_code, start_date, end_date, report_type
                )
            elif statement_type == 'balance':
                return await self.fundamental_fetcher.get_balance_sheet(
                    ts_code, start_date, end_date, report_type
                )
            elif statement_type == 'cashflow':
                return await self.fundamental_fetcher.get_cashflow_statement(
                    ts_code, start_date, end_date, report_type
                )
            elif statement_type == 'all':
                return await self.fundamental_fetcher.get_all_financial_data(
                    ts_code, start_date, end_date, report_type
                )
            else:
                raise DataFlowException(f"不支持的报表类型: {statement_type}")
        
        except Exception as e:
            logger.error(f"获取财务报表失败: {e}")
            raise
    
    async def get_financial_indicators(
        self,
        ts_code: str,
        start_date: str,
        end_date: str
    ) -> pd.DataFrame:
        """获取财务指标"""
        return await self.fundamental_fetcher.get_financial_indicators(
            ts_code, start_date, end_date
        )
    
    # 市场数据相关方法
    async def get_money_flow(
        self,
        ts_code: str,
        start_date: str,
        end_date: str
    ) -> pd.DataFrame:
        """获取资金流向"""
        return await self.market_fetcher.get_money_flow(ts_code, start_date, end_date)
    
    async def get_margin_data(
        self,
        trade_date: str,
        ts_code: str = None,
        data_type: str = 'detail'
    ) -> pd.DataFrame:
        """
        获取融资融券数据
        
        Args:
            trade_date: 交易日期
            ts_code: 股票代码
            data_type: 数据类型 ('detail', 'target')
        
        Returns:
            融资融券数据DataFrame
        """
        try:
            if data_type == 'detail':
                return await self.market_fetcher.get_margin_detail(trade_date, ts_code)
            elif data_type == 'target':
                return await self.market_fetcher.get_margin_target(ts_code)
            else:
                raise DataFlowException(f"不支持的数据类型: {data_type}")
        
        except Exception as e:
            logger.error(f"获取融资融券数据失败: {e}")
            raise
    
    async def get_dragon_tiger_data(
        self,
        trade_date: str,
        ts_code: str = None,
        include_institutions: bool = False
    ) -> Dict[str, pd.DataFrame]:
        """
        获取龙虎榜数据
        
        Args:
            trade_date: 交易日期
            ts_code: 股票代码
            include_institutions: 是否包含机构明细
        
        Returns:
            龙虎榜数据字典
        """
        try:
            result = {}
            
            # 获取龙虎榜基础数据
            result['list'] = await self.market_fetcher.get_dragon_tiger_list(trade_date, ts_code)
            
            # 获取机构明细
            if include_institutions:
                result['institutions'] = await self.market_fetcher.get_dragon_tiger_institutions(
                    trade_date, ts_code
                )
            
            return result
        
        except Exception as e:
            logger.error(f"获取龙虎榜数据失败: {e}")
            raise
    
    async def get_holders_data(
        self,
        ts_code: str,
        period: str,
        ann_date: str = None,
        holder_type: str = 'all'
    ) -> Dict[str, pd.DataFrame]:
        """
        获取股东数据
        
        Args:
            ts_code: 股票代码
            period: 报告期
            ann_date: 公告日期
            holder_type: 股东类型 ('top10', 'float', 'all')
        
        Returns:
            股东数据字典
        """
        try:
            result = {}
            
            if holder_type in ['top10', 'all']:
                result['top10_holders'] = await self.market_fetcher.get_top10_holders(
                    ts_code, period, ann_date
                )
            
            if holder_type in ['float', 'all']:
                result['top10_floatholders'] = await self.market_fetcher.get_top10_floatholders(
                    ts_code, period, ann_date
                )
            
            return result
        
        except Exception as e:
            logger.error(f"获取股东数据失败: {e}")
            raise
    
    # 新闻舆情相关方法
    async def get_news_data(
        self,
        start_date: str,
        end_date: str,
        src: str = None,
        ts_code: str = None,
        include_sentiment: bool = False
    ) -> pd.DataFrame:
        """
        获取新闻数据
        
        Args:
            start_date: 开始日期
            end_date: 结束日期
            src: 新闻来源
            ts_code: 股票代码（获取相关公告）
            include_sentiment: 是否包含情绪分析
        
        Returns:
            新闻数据DataFrame
        """
        try:
            if ts_code:
                # 获取股票相关公告
                news_df = await self.news_fetcher.get_announcements(
                    ts_code, start_date, end_date
                )
            else:
                # 获取通用新闻
                news_df = await self.news_fetcher.get_news(start_date, end_date, src)
            
            if include_sentiment and not news_df.empty:
                # 进行情绪分析
                texts = []
                for _, row in news_df.iterrows():
                    title = str(row.get('title', ''))
                    content = str(row.get('content', '') or row.get('summary', ''))
                    text = title + ' ' + content
                    texts.append(text)
                
                sentiments = await self.news_fetcher.analyze_sentiment(texts)
                
                # 添加情绪分析结果
                for i, sentiment in enumerate(sentiments):
                    if i < len(news_df):
                        news_df.loc[i, 'sentiment'] = sentiment['sentiment']
                        news_df.loc[i, 'sentiment_score'] = sentiment['score']
                        news_df.loc[i, 'sentiment_confidence'] = sentiment['confidence']
            
            return news_df
        
        except Exception as e:
            logger.error(f"获取新闻数据失败: {e}")
            raise
    
    async def get_research_data(
        self,
        ts_code: str,
        start_date: str,
        end_date: str
    ) -> pd.DataFrame:
        """获取机构调研数据"""
        return await self.news_fetcher.get_research_reports(ts_code, start_date, end_date)
    
    # 综合数据获取方法
    async def get_stock_comprehensive_data(
        self,
        ts_code: str,
        start_date: str,
        end_date: str,
        include_kline: bool = True,
        include_financial: bool = True,
        include_market: bool = True,
        include_news: bool = True
    ) -> Dict[str, Any]:
        """
        获取股票综合数据
        
        Args:
            ts_code: 股票代码
            start_date: 开始日期
            end_date: 结束日期
            include_kline: 是否包含K线数据
            include_financial: 是否包含财务数据
            include_market: 是否包含市场数据
            include_news: 是否包含新闻数据
        
        Returns:
            综合数据字典
        """
        try:
            logger.info(f"获取股票综合数据: {ts_code}")
            
            result = {
                'ts_code': ts_code,
                'start_date': start_date,
                'end_date': end_date,
                'update_time': datetime.now().isoformat()
            }
            
            # 并发获取各类数据
            tasks = []
            task_names = []
            
            if include_kline:
                tasks.append(self.get_kline_data(ts_code, start_date, end_date))
                task_names.append('kline')
            
            if include_financial:
                tasks.append(self.get_financial_statements(ts_code, start_date, end_date, 'all'))
                task_names.append('financial')
            
            if include_market:
                tasks.append(self.get_money_flow(ts_code, start_date, end_date))
                task_names.append('money_flow')
            
            if include_news:
                tasks.append(self.get_news_data(start_date, end_date, ts_code=ts_code, include_sentiment=True))
                task_names.append('news')
            
            # 执行并发任务
            if tasks:
                results = await asyncio.gather(*tasks, return_exceptions=True)
                
                # 处理结果
                for i, (task_result, task_name) in enumerate(zip(results, task_names)):
                    if isinstance(task_result, Exception):
                        logger.error(f"获取{task_name}数据失败: {task_result}")
                        result[task_name] = None
                    else:
                        result[task_name] = task_result
            
            logger.info(f"成功获取股票综合数据: {ts_code}")
            return result
        
        except Exception as e:
            logger.error(f"获取股票综合数据失败: {e}")
            raise
    
    async def get_market_overview(
        self,
        date: str,
        include_news: bool = True
    ) -> Dict[str, Any]:
        """
        获取市场概览数据
        
        Args:
            date: 日期
            include_news: 是否包含新闻
        
        Returns:
            市场概览数据字典
        """
        try:
            logger.info(f"获取市场概览: {date}")
            
            result = {
                'date': date,
                'update_time': datetime.now().isoformat()
            }
            
            # 并发获取市场数据
            tasks = [
                self.get_dragon_tiger_data(date, include_institutions=True),
                self.get_margin_data(date, data_type='detail')
            ]
            task_names = ['dragon_tiger', 'margin']
            
            if include_news:
                tasks.append(self.get_news_data(date, date, include_sentiment=True))
                task_names.append('news')
            
            # 执行任务
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            # 处理结果
            for task_result, task_name in zip(results, task_names):
                if isinstance(task_result, Exception):
                    logger.error(f"获取{task_name}数据失败: {task_result}")
                    result[task_name] = None
                else:
                    result[task_name] = task_result
            
            logger.info(f"成功获取市场概览: {date}")
            return result
        
        except Exception as e:
            logger.error(f"获取市场概览失败: {e}")
            raise


# 便捷函数
async def get_stock_data(
    ts_code: str,
    start_date: str,
    end_date: str,
    data_types: List[str] = None
) -> Dict[str, Any]:
    """
    获取股票数据的便捷函数
    
    Args:
        ts_code: 股票代码
        start_date: 开始日期
        end_date: 结束日期
        data_types: 数据类型列表 ['kline', 'financial', 'market', 'news']
    
    Returns:
        股票数据字典
    """
    if data_types is None:
        data_types = ['kline', 'financial', 'market', 'news']
    
    async with DataManager() as manager:
        return await manager.get_stock_comprehensive_data(
            ts_code=ts_code,
            start_date=start_date,
            end_date=end_date,
            include_kline='kline' in data_types,
            include_financial='financial' in data_types,
            include_market='market' in data_types,
            include_news='news' in data_types
        )


async def get_market_data(
    date: str,
    include_news: bool = True
) -> Dict[str, Any]:
    """
    获取市场数据的便捷函数
    
    Args:
        date: 日期
        include_news: 是否包含新闻
    
    Returns:
        市场数据字典
    """
    async with DataManager() as manager:
        return await manager.get_market_overview(date, include_news)
