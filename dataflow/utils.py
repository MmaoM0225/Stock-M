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

from .config import TECHNICAL_INDICATORS_CONFIG

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

# 技术指标计算

def calculate_ma(df: pd.DataFrame, periods: list = None) -> pd.DataFrame:
    """
    计算移动平均线
    
    Args:
        df: 包含收盘价的DataFrame
        periods: 移动平均周期列表，如果为None则使用配置文件中的默认值
    
    Returns:
        添加移动平均线的DataFrame
    """
    if df.empty or 'close' not in df.columns:
        return df
    
    df = df.copy()
    
    # 使用配置文件中的默认参数
    if periods is None:
        periods = TECHNICAL_INDICATORS_CONFIG['ma']['periods']
    
    # 确保数据按日期排序
    if 'trade_date' in df.columns:
        df = df.sort_values('trade_date').reset_index(drop=True)
    
    # 计算移动平均线
    for period in periods:
        df[f'ma{period}'] = df['close'].rolling(window=period, min_periods=1).mean()
    
    # 成交量移动平均
    if 'vol' in df.columns:
        vol_periods = TECHNICAL_INDICATORS_CONFIG['ma']['volume_periods']
        for period in vol_periods:
            df[f'vol_ma{period}'] = df['vol'].rolling(window=period, min_periods=1).mean()
    
    # 涨跌幅
    df['pct_change'] = df['close'].pct_change() * 100
    
    return df


def calculate_rsi(df: pd.DataFrame, periods: list = None) -> pd.DataFrame:
    """
    计算RSI相对强弱指标
    
    Args:
        df: 包含收盘价的DataFrame
        periods: RSI计算周期列表，如果为None则使用配置文件中的默认值
    
    Returns:
        添加RSI指标的DataFrame
    """
    if df.empty or 'close' not in df.columns:
        return df
    
    df = df.copy()
    
    # 使用配置文件中的默认参数
    if periods is None:
        periods = TECHNICAL_INDICATORS_CONFIG['rsi']['periods']
    
    # 确保数据按日期排序
    if 'trade_date' in df.columns:
        df = df.sort_values('trade_date').reset_index(drop=True)
    
    # 计算价格变化
    delta = df['close'].diff()
    
    for period in periods:
        # 分别计算上涨和下跌
        gain = delta.where(delta > 0, 0)
        loss = -delta.where(delta < 0, 0)
        
        # 计算平均收益和平均损失
        avg_gain = gain.rolling(window=period, min_periods=1).mean()
        avg_loss = loss.rolling(window=period, min_periods=1).mean()
        
        # 计算RSI
        rs = avg_gain / avg_loss
        rsi = 100 - (100 / (1 + rs))
        
        df[f'rsi{period}'] = rsi
    
    return df


def calculate_kdj(df: pd.DataFrame, period: int = None, k_period: int = None, d_period: int = None) -> pd.DataFrame:
    """
    计算KDJ随机指标
    
    Args:
        df: 包含高低收价格的DataFrame
        period: KDJ计算周期，如果为None则使用配置文件中的默认值
        k_period: K值平滑周期，如果为None则使用配置文件中的默认值
        d_period: D值平滑周期，如果为None则使用配置文件中的默认值
    
    Returns:
        添加KDJ指标的DataFrame
    """
    if df.empty or not all(col in df.columns for col in ['high', 'low', 'close']):
        return df
    
    df = df.copy()
    
    # 使用配置文件中的默认参数
    kdj_config = TECHNICAL_INDICATORS_CONFIG['kdj']
    if period is None:
        period = kdj_config['period']
    if k_period is None:
        k_period = kdj_config['k_period']
    if d_period is None:
        d_period = kdj_config['d_period']
    
    # 确保数据按日期排序
    if 'trade_date' in df.columns:
        df = df.sort_values('trade_date').reset_index(drop=True)
    
    # 计算最高价和最低价
    high_n = df['high'].rolling(window=period, min_periods=1).max()
    low_n = df['low'].rolling(window=period, min_periods=1).min()
    
    # 计算RSV
    rsv = (df['close'] - low_n) / (high_n - low_n) * 100
    rsv = rsv.fillna(50)  # 填充NaN值
    
    # 计算K值
    k = rsv.ewm(alpha=1/k_period, adjust=False).mean()
    
    # 计算D值
    d = k.ewm(alpha=1/d_period, adjust=False).mean()
    
    # 计算J值
    j = 3 * k - 2 * d
    
    df['k'] = k
    df['d'] = d
    df['j'] = j
    
    return df


def calculate_bollinger_bands(df: pd.DataFrame, period: int = None, std_dev: float = None) -> pd.DataFrame:
    """
    计算布林带指标
    
    Args:
        df: 包含收盘价的DataFrame
        period: 移动平均周期，如果为None则使用配置文件中的默认值
        std_dev: 标准差倍数，如果为None则使用配置文件中的默认值
    
    Returns:
        添加布林带指标的DataFrame
    """
    if df.empty or 'close' not in df.columns:
        return df
    
    df = df.copy()
    
    # 使用配置文件中的默认参数
    boll_config = TECHNICAL_INDICATORS_CONFIG['bollinger_bands']
    if period is None:
        period = boll_config['period']
    if std_dev is None:
        std_dev = boll_config['std_dev']
    
    # 确保数据按日期排序
    if 'trade_date' in df.columns:
        df = df.sort_values('trade_date').reset_index(drop=True)
    
    # 计算中轨（移动平均线）
    df['boll_mid'] = df['close'].rolling(window=period, min_periods=1).mean()
    
    # 计算标准差
    std = df['close'].rolling(window=period, min_periods=1).std()
    
    # 计算上轨和下轨
    df['boll_upper'] = df['boll_mid'] + (std * std_dev)
    df['boll_lower'] = df['boll_mid'] - (std * std_dev)
    
    return df


def calculate_macd(df: pd.DataFrame, fast_period: int = None, slow_period: int = None, signal_period: int = None) -> pd.DataFrame:
    """
    计算MACD指标
    
    Args:
        df: 包含收盘价的DataFrame
        fast_period: 快速EMA周期，如果为None则使用配置文件中的默认值
        slow_period: 慢速EMA周期，如果为None则使用配置文件中的默认值
        signal_period: 信号线EMA周期，如果为None则使用配置文件中的默认值
    
    Returns:
        添加MACD指标的DataFrame
    """
    if df.empty or 'close' not in df.columns:
        return df
    
    df = df.copy()
    
    # 使用配置文件中的默认参数
    macd_config = TECHNICAL_INDICATORS_CONFIG['macd']
    if fast_period is None:
        fast_period = macd_config['fast_period']
    if slow_period is None:
        slow_period = macd_config['slow_period']
    if signal_period is None:
        signal_period = macd_config['signal_period']
    
    # 确保数据按日期排序
    if 'trade_date' in df.columns:
        df = df.sort_values('trade_date').reset_index(drop=True)
    
    # 计算快速和慢速EMA
    ema_fast = df['close'].ewm(span=fast_period, adjust=False).mean()
    ema_slow = df['close'].ewm(span=slow_period, adjust=False).mean()
    
    # 计算DIF线（快线）
    dif = ema_fast - ema_slow
    
    # 计算DEA线（慢线，信号线）
    dea = dif.ewm(span=signal_period, adjust=False).mean()
    
    # 计算MACD柱状图
    macd = (dif - dea) * 2
    
    df['macd_dif'] = dif
    df['macd_dea'] = dea
    df['macd_macd'] = macd
    
    return df