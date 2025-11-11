"""
数据流配置文件
"""
import os
from typing import Dict, Any

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
