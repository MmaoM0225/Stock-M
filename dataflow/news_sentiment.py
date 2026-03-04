"""
新闻舆情数据获取模块
包括新闻数据、公告数据、社交媒体情绪等
"""
import pandas as pd
import tushare as ts
import requests
import akshare as ak
from typing import Dict, List, Optional, Any
from datetime import datetime, date
import logging
import json

from .config import DATA_SOURCES, NEWS_API_KEY
from .utils import (
    format_date, validate_stock_code,
    clean_dataframe, DataFlowException
)

logger = logging.getLogger(__name__)


class NewsSentimentFetcher:
    """新闻舆情数据获取器"""
    
    def __init__(self):
        """初始化"""
        self.tushare_enabled = DATA_SOURCES['tushare']['enabled']
        if self.tushare_enabled:
            ts.set_token(DATA_SOURCES['tushare']['token'])
            self.ts_pro = ts.pro_api()
        
        self.news_api_key = NEWS_API_KEY
    
    def get_eastmoney_breakfast_news(self) -> pd.DataFrame:
        """获取东方财富财经早餐数据
        
        Returns:
            pd.DataFrame: 包含标题、摘要、发布时间、链接的新闻数据
            
        Raises:
            DataFlowException: 当数据获取失败时抛出异常
        """
        try:
            logger.info("开始获取东方财富财经早餐数据")
            df = ak.stock_info_cjzc_em()
            
            if df.empty:
                logger.warning("获取到的财经早餐数据为空")
                return pd.DataFrame(columns=['标题', '摘要', '发布时间', '链接'])
            
            logger.info(f"成功获取 {len(df)} 条财经早餐数据")
            return df
            
        except Exception as e:
            logger.error(f"获取东方财富财经早餐数据失败: {str(e)}")
            raise DataFlowException(f"获取东方财富财经早餐数据失败: {str(e)}")
    
    

