"""
数据流工具函数
"""
import time
import asyncio
import aiohttp
import pandas as pd
from typing import Dict, Any, Optional, List
from datetime import datetime, date
import logging

logger = logging.getLogger(__name__)


class DataFlowException(Exception):
    """数据流异常"""
    pass


def format_date(date_input: Any, format_type: str = 'tushare') -> str:
    """
    格式化日期
    
    Args:
        date_input: 日期输入
        format_type: 格式类型 ('tushare': YYYYMMDD, 'yahoo': YYYY-MM-DD)
    
    Returns:
        格式化后的日期字符串
    """
    if isinstance(date_input, str):
        # 清理输入字符串
        clean_date = date_input.replace('-', '').replace('/', '')
        if format_type == 'tushare':
            return clean_date
        elif format_type == 'yahoo':
            if len(clean_date) == 8:
                return f"{clean_date[:4]}-{clean_date[4:6]}-{clean_date[6:8]}"
    elif isinstance(date_input, (date, datetime)):
        if format_type == 'tushare':
            return date_input.strftime('%Y%m%d')
        elif format_type == 'yahoo':
            return date_input.strftime('%Y-%m-%d')
    
    raise ValueError(f"不支持的日期格式: {date_input}")


def validate_stock_code(stock_code: str, market: str = 'cn') -> bool:
    """
    验证股票代码格式
    
    Args:
        stock_code: 股票代码
        market: 市场类型 ('cn', 'hk', 'us')
    
    Returns:
        是否有效
    """
    if not stock_code:
        return False
    
    if market == 'cn':
        # A股代码格式: 000001.SZ, 600000.SH, 430001.BJ
        if '.' not in stock_code:
            return False
        symbol, suffix = stock_code.split('.')
        return (len(symbol) == 6 and symbol.isdigit() and 
                suffix in ['SH', 'SZ', 'BJ'])
    elif market == 'hk':
        # 港股代码格式: 00001.HK
        if '.' not in stock_code:
            return False
        symbol, suffix = stock_code.split('.')
        return (len(symbol) == 5 and symbol.isdigit() and suffix == 'HK')
    elif market == 'us':
        # 美股代码格式: AAPL, TSLA
        return stock_code.isalpha() and len(stock_code) <= 5
    
    return False


async def async_request(
    session: aiohttp.ClientSession,
    method: str,
    url: str,
    params: Optional[Dict] = None,
    data: Optional[Dict] = None,
    headers: Optional[Dict] = None,
    timeout: int = 30,
    max_retries: int = 3
) -> Dict[str, Any]:
    """
    异步HTTP请求
    
    Args:
        session: aiohttp会话
        method: 请求方法
        url: 请求URL
        params: 查询参数
        data: 请求数据
        headers: 请求头
        timeout: 超时时间
        max_retries: 最大重试次数
    
    Returns:
        响应数据
    """
    for attempt in range(max_retries + 1):
        try:
            async with session.request(
                method=method,
                url=url,
                params=params,
                json=data,
                headers=headers,
                timeout=aiohttp.ClientTimeout(total=timeout)
            ) as response:
                if response.status == 200:
                    return await response.json()
                else:
                    logger.warning(f"请求失败，状态码: {response.status}, URL: {url}")
                    if attempt < max_retries:
                        await asyncio.sleep(2 ** attempt)  # 指数退避
                    else:
                        raise DataFlowException(f"请求失败，状态码: {response.status}")
        except asyncio.TimeoutError:
            logger.warning(f"请求超时，尝试 {attempt + 1}/{max_retries + 1}")
            if attempt < max_retries:
                await asyncio.sleep(2 ** attempt)
            else:
                raise DataFlowException("请求超时")
        except Exception as e:
            logger.error(f"请求异常: {e}")
            if attempt < max_retries:
                await asyncio.sleep(2 ** attempt)
            else:
                raise DataFlowException(f"请求异常: {e}")


def clean_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """
    清理DataFrame数据
    
    Args:
        df: 原始DataFrame
    
    Returns:
        清理后的DataFrame
    """
    if df.empty:
        return df
    
    # 移除空行
    df = df.dropna(how='all')
    
    # 重置索引
    df = df.reset_index(drop=True)
    
    # 转换数值列
    numeric_columns = df.select_dtypes(include=['object']).columns
    for col in numeric_columns:
        if col not in ['ts_code', 'symbol', 'name', 'trade_date']:
            try:
                df[col] = pd.to_numeric(df[col], errors='ignore')
            except:
                pass
    
    return df


def calculate_technical_indicators(df: pd.DataFrame) -> pd.DataFrame:
    """
    计算基础技术指标
    
    Args:
        df: 包含OHLCV数据的DataFrame
    
    Returns:
        添加技术指标的DataFrame
    """
    if df.empty or 'close' not in df.columns:
        return df
    
    df = df.copy()
    
    # 移动平均线
    df['ma5'] = df['close'].rolling(window=5).mean()
    df['ma10'] = df['close'].rolling(window=10).mean()
    df['ma20'] = df['close'].rolling(window=20).mean()
    df['ma60'] = df['close'].rolling(window=60).mean()
    
    # 涨跌幅
    df['pct_change'] = df['close'].pct_change() * 100
    
    # 成交额（如果没有的话）
    if 'amount' not in df.columns and 'volume' in df.columns:
        df['amount'] = df['close'] * df['volume']
    
    return df


class RateLimiter:
    """请求频率限制器"""
    
    def __init__(self, max_requests: int, time_window: int):
        self.max_requests = max_requests
        self.time_window = time_window
        self.requests = []
    
    async def acquire(self):
        """获取请求许可"""
        now = time.time()
        
        # 清理过期请求
        self.requests = [req_time for req_time in self.requests 
                        if now - req_time < self.time_window]
        
        # 检查是否超过限制
        if len(self.requests) >= self.max_requests:
            sleep_time = self.time_window - (now - self.requests[0])
            if sleep_time > 0:
                await asyncio.sleep(sleep_time)
                return await self.acquire()
        
        self.requests.append(now)


# 全局限频器实例
tushare_limiter = RateLimiter(max_requests=200, time_window=60)  # 每分钟200次
alpha_vantage_limiter = RateLimiter(max_requests=5, time_window=60)  # 每分钟5次
