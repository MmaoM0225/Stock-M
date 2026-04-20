"""
数据流工具函数
"""
import time
import asyncio
import aiohttp
import pandas as pd
from typing import Dict, Any, Optional, List, Union, Callable, Tuple
from datetime import datetime, date
from functools import wraps
import logging

from .config import TECHNICAL_INDICATORS_CONFIG, MAX_RETRIES, RETRY_DELAY

logger = logging.getLogger(__name__)


class DataFlowException(Exception):
    """数据流异常"""
    pass


class RetryExhaustedException(DataFlowException):
    """重试次数耗尽异常"""
    def __init__(self, message: str, last_error: Optional[Exception] = None):
        super().__init__(message)
        self.last_error = last_error


def with_retry(
    max_retries: int = None,
    delay: float = None,
    backoff: bool = True,
    jitter: bool = True,
    retry_on_empty: bool = False,
    empty_result_checker: Optional[Callable] = None
):
    """
    数据获取重试装饰器，支持指数退避和抖动
    
    Args:
        max_retries: 最大重试次数，默认使用配置文件中的 MAX_RETRIES
        delay: 初始延迟秒数，默认使用配置文件中的 RETRY_DELAY
        backoff: 是否使用指数退避（每次重试延迟翻倍）
        jitter: 是否添加随机抖动（避免所有请求同时重试）
        retry_on_empty: 当返回结果为空时是否重试
        empty_result_checker: 自定义空结果检查函数，接收返回值，返回True表示为空
    
    Example:
        @with_retry(max_retries=5)
        def fetch_data(ts_code: str) -> pd.DataFrame:
            return api.get_data(ts_code)
    """
    max_retries = max_retries or MAX_RETRIES
    delay = delay or RETRY_DELAY
    
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs) -> Any:
            last_error = None
            
            for attempt in range(max_retries):
                try:
                    result = func(*args, **kwargs)
                    
                    # 检查是否返回空结果
                    is_empty = False
                    if retry_on_empty:
                        if empty_result_checker:
                            is_empty = empty_result_checker(result)
                        elif result is None:
                            is_empty = True
                        elif hasattr(result, 'empty') and result.empty:
                            is_empty = True
                        elif hasattr(result, '__len__') and len(result) == 0:
                            is_empty = True
                    
                    if is_empty and attempt < max_retries - 1:
                        wait_time = delay * (2 ** attempt) if backoff else delay
                        if jitter:
                            import random
                            wait_time += random.uniform(0, 0.5)
                        
                        logger.warning(
                            f"{func.__name__} 返回空结果，将在 {wait_time:.2f}s 后重试 "
                            f"({attempt + 1}/{max_retries})"
                        )
                        time.sleep(wait_time)
                        continue
                    
                    return result
                    
                except Exception as e:
                    last_error = e
                    error_msg = str(e).lower()
                    
                    # 判断是否为可重试的错误
                    # 不可重试的错误：认证失败、无效参数、权限不足
                    non_retryable_keywords = [
                        'auth', 'token', 'permission', 'invalid', 'unauthorized',
                        '认证', '权限', '无效', 'unauthorized'
                    ]
                    
                    is_non_retryable = any(kw in error_msg for kw in non_retryable_keywords)
                    
                    if is_non_retryable:
                        logger.error(f"{func.__name__} 遇到不可重试错误: {e}")
                        raise
                    
                    if attempt < max_retries - 1:
                        wait_time = delay * (2 ** attempt) if backoff else delay
                        if jitter:
                            import random
                            wait_time += random.uniform(0, 0.5)
                        
                        logger.warning(
                            f"{func.__name__} 失败: {e}，将在 {wait_time:.2f}s 后重试 "
                            f"({attempt + 1}/{max_retries})"
                        )
                        time.sleep(wait_time)
                    else:
                        logger.error(
                            f"{func.__name__} 在 {max_retries} 次尝试后仍然失败: {e}"
                        )
            
            # 所有重试都失败了
            raise RetryExhaustedException(
                f"{func.__name__} 重试 {max_retries} 次后仍然失败",
                last_error=last_error
            )
        
        return wrapper
    return decorator


def with_retry_and_fallback(
    fallback_func: Optional[Callable] = None,
    max_retries: int = None,
    delay: float = None,
    return_default_on_failure: Any = None
):
    """
    带降级功能的数据获取重试装饰器
    
    当所有重试都失败时，可以选择：
    1. 调用降级函数 fallback_func
    2. 返回默认值 return_default_on_failure
    
    Args:
        fallback_func: 降级函数，在主函数失败后被调用
        max_retries: 最大重试次数
        delay: 初始延迟秒数
        return_default_on_failure: 所有重试失败后的默认返回值
    
    Example:
        @with_retry_and_fallback(
            fallback_func=lambda ts_code: load_from_cache(ts_code),
            return_default_on_failure=pd.DataFrame()
        )
        def fetch_company_info(ts_code: str) -> pd.DataFrame:
            return api.get_company(ts_code)
    """
    max_retries = max_retries or MAX_RETRIES
    delay = delay or RETRY_DELAY
    
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs) -> Any:
            last_error = None
            
            for attempt in range(max_retries):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    last_error = e
                    error_msg = str(e).lower()
                    
                    # 不可重试的错误
                    non_retryable_keywords = ['auth', 'token', 'permission', 'invalid', '认证', '权限']
                    if any(kw in error_msg for kw in non_retryable_keywords):
                        break
                    
                    if attempt < max_retries - 1:
                        wait_time = delay * (2 ** attempt)
                        logger.warning(
                            f"{func.__name__} 失败: {e}，将在 {wait_time:.2f}s 后重试 "
                            f"({attempt + 1}/{max_retries})"
                        )
                        time.sleep(wait_time)
                    else:
                        logger.error(f"{func.__name__} 在 {max_retries} 次尝试后仍然失败: {e}")
            
            # 尝试降级
            if fallback_func:
                try:
                    logger.info(f"{func.__name__} 尝试调用降级函数")
                    return fallback_func(*args, **kwargs)
                except Exception as fallback_error:
                    logger.error(f"降级函数也失败了: {fallback_error}")
            
            # 返回默认值
            if return_default_on_failure is not None:
                logger.info(f"{func.__name__} 返回默认值")
                return return_default_on_failure
            
            # 无法降级，抛出异常
            raise RetryExhaustedException(
                f"{func.__name__} 重试 {max_retries} 次且降级失败",
                last_error=last_error
            )
        
        return wrapper
    return decorator


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


def normalize_cn_ts_code(raw: str) -> str:
    """
    将 A 股代码规范为 Tushare 格式 XXXXXX.SH / .SZ / .BJ。

    已带合法后缀时仅统一后缀大写；仅为 6 位数字时按常见规则补后缀：
    6 开头 → .SH；0 / 3 开头 → .SZ；200 开头 → .SZ（深 B）；900 开头 → .SH（沪 B）；
    4 / 8 / 92 开头 → .BJ；其余 9 开头 → .BJ（如 920xxx）。
    """
    if raw is None:
        raise ValueError("股票代码为空")
    s = str(raw).strip().upper().replace(" ", "")
    if not s:
        raise ValueError("股票代码为空")

    if "." in s:
        parts = s.split(".")
        if len(parts) != 2:
            raise ValueError(f"无效股票代码格式: {raw}")
        sym, suf = parts[0], parts[1]
        if len(sym) != 6 or not sym.isdigit():
            raise ValueError(f"无效股票代码: {raw}")
        if suf not in ("SH", "SZ", "BJ"):
            raise ValueError(f"不支持的后缀 .{suf}，请使用 SH / SZ / BJ")
        return f"{sym}.{suf}"

    if len(s) == 6 and s.isdigit():
        if s.startswith("6"):
            return f"{s}.SH"
        if s.startswith("0") or s.startswith("3"):
            return f"{s}.SZ"
        if s.startswith("200"):
            return f"{s}.SZ"
        if s.startswith("900"):
            return f"{s}.SH"
        if s.startswith("4") or s.startswith("8") or s.startswith("92"):
            return f"{s}.BJ"
        if s.startswith("9"):
            return f"{s}.BJ"

    raise ValueError(
        f"无法识别股票代码: {raw}，请使用 6 位数字或完整格式如 000001.SZ"
    )


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
                df[col] = pd.to_numeric(df[col])
            except (ValueError, TypeError):
                # 如果转换失败，保持原始数据类型
                pass
    
    return df

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