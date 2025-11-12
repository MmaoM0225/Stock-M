"""
数据流配置文件
"""
import os
from typing import Dict, Any
from dotenv import load_dotenv

# 加载环境变量
load_dotenv()

# Tushare配置
TUSHARE_TOKEN = os.getenv('TUSHARE_TOKEN', '')

# 美股数据配置
ALPHA_VANTAGE_API_KEY = os.getenv('ALPHA_VANTAGE_API_KEY', '')
YAHOO_FINANCE_ENABLED = True

# 新闻数据配置
NEWS_API_KEY = os.getenv('NEWS_API_KEY', '')

# 请求配置
REQUEST_TIMEOUT = 30
MAX_RETRIES = 3
RETRY_DELAY = 1

# 数据源配置
DATA_SOURCES = {
    'tushare': {
        'enabled': bool(TUSHARE_TOKEN),
        'token': TUSHARE_TOKEN,
        'base_url': 'http://api.waditu.com'
    },
    'alpha_vantage': {
        'enabled': bool(ALPHA_VANTAGE_API_KEY),
        'api_key': ALPHA_VANTAGE_API_KEY,
        'base_url': 'https://www.alphavantage.co/query'
    },
    'yahoo_finance': {
        'enabled': YAHOO_FINANCE_ENABLED,
        'base_url': 'https://query1.finance.yahoo.com'
    }
}

# 缓存配置
CACHE_CONFIG = {
    'enabled': True,
    'ttl': 3600,  # 1小时
    'max_size': 1000
}

# 技术指标配置
TECHNICAL_INDICATORS_CONFIG = {
    # 移动平均线配置
    'ma': {
        'periods': [5, 10, 20, 60],  # 移动平均周期
        'volume_periods': [5, 10]    # 成交量移动平均周期
    },
    
    # RSI相对强弱指标配置
    'rsi': {
        'periods': [6, 12, 24]  # RSI计算周期
    },
    
    # KDJ随机指标配置
    'kdj': {
        'period': 9,        # KDJ计算周期
        'k_period': 3,      # K值平滑周期
        'd_period': 3       # D值平滑周期
    },
    
    # 布林带配置
    'bollinger_bands': {
        'period': 20,       # 移动平均周期
        'std_dev': 2.0      # 标准差倍数
    },
    
    # MACD指标配置
    'macd': {
        'fast_period': 12,    # 快速EMA周期
        'slow_period': 26,    # 慢速EMA周期
        'signal_period': 9    # 信号线EMA周期
    }
}