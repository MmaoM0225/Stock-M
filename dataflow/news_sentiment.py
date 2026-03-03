"""
新闻舆情数据获取模块
包括新闻数据、公告数据、社交媒体情绪等
"""
import pandas as pd
import tushare as ts
import requests
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
    
    def get_news(
        self,
        start_date: str,
        end_date: str,
        src: str = None
    ) -> pd.DataFrame:
        """
        获取新闻数据
        
        Args:
            start_date: 开始日期
            end_date: 结束日期
            src: 新闻来源
        
        Returns:
            新闻数据DataFrame
        """
        if not self.tushare_enabled:
            raise DataFlowException("Tushare未配置或未启用")
        
        try:
            # 格式化日期
            start_date_fmt = format_date(start_date, 'tushare')
            end_date_fmt = format_date(end_date, 'tushare')
            
            logger.info(f"获取新闻数据: {start_date_fmt} - {end_date_fmt}, 来源: {src or '全部'}")
            
            # 获取新闻数据
            df = self.ts_pro.news(
                start_date=start_date_fmt,
                end_date=end_date_fmt,
                src=src
            )
            
            if df.empty:
                logger.warning(f"未获取到新闻数据: {start_date_fmt} - {end_date_fmt}")
                return pd.DataFrame()
            
            # 数据处理
            df = clean_dataframe(df)
            df = df.sort_values('datetime').reset_index(drop=True)
            
            logger.info(f"成功获取 {len(df)} 条新闻数据")
            return df
            
        except Exception as e:
            logger.error(f"获取新闻数据失败: {e}")
            raise DataFlowException(f"获取新闻数据失败: {e}")
    
    def get_cctv_news(
        self,
        date: str
    ) -> pd.DataFrame:
        """
        获取新闻联播文字稿
        
        Args:
            date: 日期
        
        Returns:
            新闻联播DataFrame
        """
        if not self.tushare_enabled:
            raise DataFlowException("Tushare未配置或未启用")
        
        try:
            # 格式化日期
            date_fmt = format_date(date, 'tushare')
            
            logger.info(f"获取新闻联播: {date_fmt}")
            
            # 获取新闻联播数据
            df = self.ts_pro.cctv_news(date=date_fmt)
            
            if df.empty:
                logger.warning(f"未获取到新闻联播数据: {date_fmt}")
                return pd.DataFrame()
            
            # 数据处理
            df = clean_dataframe(df)
            
            logger.info(f"成功获取 {len(df)} 条新闻联播数据")
            return df
            
        except Exception as e:
            logger.error(f"获取新闻联播失败: {e}")
            raise DataFlowException(f"获取新闻联播失败: {e}")
    
    def get_announcements(
        self,
        ts_code: str,
        start_date: str,
        end_date: str,
        ann_type: str = None
    ) -> pd.DataFrame:
        """
        获取上市公司公告
        
        Args:
            ts_code: 股票代码
            start_date: 开始日期
            end_date: 结束日期
            ann_type: 公告类型
        
        Returns:
            公告数据DataFrame
        """
        if not self.tushare_enabled:
            raise DataFlowException("Tushare未配置或未启用")
        
        try:
            # 格式化日期
            start_date_fmt = format_date(start_date, 'tushare')
            end_date_fmt = format_date(end_date, 'tushare')
            
            logger.info(f"获取公告: {ts_code}, {start_date_fmt} - {end_date_fmt}")
            
            # 获取公告数据
            df = self.ts_pro.anns(
                ts_code=ts_code,
                start_date=start_date_fmt,
                end_date=end_date_fmt,
                ann_type=ann_type
            )
            
            if df.empty:
                logger.warning(f"未获取到公告数据: {ts_code}")
                return pd.DataFrame()
            
            # 数据处理
            df = clean_dataframe(df)
            df = df.sort_values('ann_date').reset_index(drop=True)
            
            logger.info(f"成功获取 {len(df)} 条公告数据")
            return df
            
        except Exception as e:
            logger.error(f"获取公告失败: {e}")
            raise DataFlowException(f"获取公告失败: {e}")
    
    def get_sz_interactions(
        self,
        ts_code: str,
        start_date: str,
        end_date: str
    ) -> pd.DataFrame:
        """
        获取深证易互动问答
        
        Args:
            ts_code: 股票代码
            start_date: 开始日期
            end_date: 结束日期
        
        Returns:
            互动问答DataFrame
        """
        if not self.tushare_enabled:
            raise DataFlowException("Tushare未配置或未启用")
        
        try:
            # 格式化日期
            start_date_fmt = format_date(start_date, 'tushare')
            end_date_fmt = format_date(end_date, 'tushare')
            
            logger.info(f"获取深证易互动: {ts_code}, {start_date_fmt} - {end_date_fmt}")
            
            # 获取互动问答数据
            df = self.ts_pro.sz_sse_summary(
                ts_code=ts_code,
                start_date=start_date_fmt,
                end_date=end_date_fmt
            )
            
            if df.empty:
                logger.warning(f"未获取到互动问答数据: {ts_code}")
                return pd.DataFrame()
            
            # 数据处理
            df = clean_dataframe(df)
            df = df.sort_values('q_date').reset_index(drop=True)
            
            logger.info(f"成功获取 {len(df)} 条互动问答数据")
            return df
            
        except Exception as e:
            logger.error(f"获取互动问答失败: {e}")
            raise DataFlowException(f"获取互动问答失败: {e}")
    
    def get_research_reports(
        self,
        ts_code: str,
        start_date: str,
        end_date: str
    ) -> pd.DataFrame:
        """
        获取机构调研数据
        
        Args:
            ts_code: 股票代码
            start_date: 开始日期
            end_date: 结束日期
        
        Returns:
            调研数据DataFrame
        """
        if not self.tushare_enabled:
            raise DataFlowException("Tushare未配置或未启用")
        
        try:
            # 格式化日期
            start_date_fmt = format_date(start_date, 'tushare')
            end_date_fmt = format_date(end_date, 'tushare')
            
            logger.info(f"获取机构调研: {ts_code}, {start_date_fmt} - {end_date_fmt}")
            
            # 获取机构调研数据
            df = self.ts_pro.stk_surv(
                ts_code=ts_code,
                start_date=start_date_fmt,
                end_date=end_date_fmt
            )
            
            if df.empty:
                logger.warning(f"未获取到机构调研数据: {ts_code}")
                return pd.DataFrame()
            
            # 数据处理
            df = clean_dataframe(df)
            df = df.sort_values('survey_date').reset_index(drop=True)
            
            logger.info(f"成功获取 {len(df)} 条机构调研数据")
            return df
            
        except Exception as e:
            logger.error(f"获取机构调研失败: {e}")
            raise DataFlowException(f"获取机构调研失败: {e}")
    
    def get_external_news(
        self,
        query: str,
        from_date: str,
        to_date: str,
        language: str = 'zh',
        sort_by: str = 'publishedAt'
    ) -> List[Dict[str, Any]]:
        """
        获取外部新闻数据（通过News API）
        
        Args:
            query: 搜索关键词
            from_date: 开始日期 (YYYY-MM-DD)
            to_date: 结束日期 (YYYY-MM-DD)
            language: 语言
            sort_by: 排序方式
        
        Returns:
            新闻数据列表
        """
        if not self.news_api_key:
            logger.warning("News API Key未配置，跳过外部新闻获取")
            return []
        
        try:
            # 格式化日期
            from_date_fmt = format_date(from_date, 'yahoo')
            to_date_fmt = format_date(to_date, 'yahoo')
            
            logger.info(f"获取外部新闻: {query}, {from_date_fmt} - {to_date_fmt}")
            
            # 构建请求参数
            params = {
                'q': query,
                'from': from_date_fmt,
                'to': to_date_fmt,
                'language': language,
                'sortBy': sort_by,
                'apiKey': self.news_api_key
            }
            
            # 发送请求
            url = 'https://newsapi.org/v2/everything'
            response = requests.get(url, params=params, timeout=30)
            
            if response.status_code == 200:
                data = response.json()
                articles = data.get('articles', [])
                logger.info(f"成功获取 {len(articles)} 条外部新闻")
                return articles
            else:
                logger.error(f"外部新闻API错误: 状态码 {response.status_code}")
                return []
            
        except Exception as e:
            logger.error(f"获取外部新闻失败: {e}")
            return []
    
    def analyze_sentiment(
        self,
        texts: List[str],
        method: str = 'simple'
    ) -> List[Dict[str, Any]]:
        """
        分析文本情绪
        
        Args:
            texts: 文本列表
            method: 分析方法 ('simple', 'advanced')
        
        Returns:
            情绪分析结果列表
        """
        try:
            logger.info(f"分析文本情绪: {len(texts)} 条文本")
            
            results = []
            
            if method == 'simple':
                # 简单情绪分析（基于关键词）
                positive_keywords = ['上涨', '利好', '增长', '盈利', '突破', '买入', '推荐']
                negative_keywords = ['下跌', '利空', '亏损', '风险', '下调', '卖出', '减持']
                
                for text in texts:
                    if not text:
                        results.append({'sentiment': 'neutral', 'score': 0.0, 'confidence': 0.0})
                        continue
                    
                    positive_count = sum(1 for keyword in positive_keywords if keyword in text)
                    negative_count = sum(1 for keyword in negative_keywords if keyword in text)
                    
                    if positive_count > negative_count:
                        sentiment = 'positive'
                        score = min(positive_count / len(positive_keywords), 1.0)
                    elif negative_count > positive_count:
                        sentiment = 'negative'
                        score = -min(negative_count / len(negative_keywords), 1.0)
                    else:
                        sentiment = 'neutral'
                        score = 0.0
                    
                    confidence = abs(score) if score != 0 else 0.5
                    
                    results.append({
                        'sentiment': sentiment,
                        'score': score,
                        'confidence': confidence
                    })
            
            else:
                # 高级情绪分析（可以集成更复杂的NLP模型）
                logger.warning("高级情绪分析暂未实现，使用简单方法")
                return self.analyze_sentiment(texts, 'simple')
            
            logger.info(f"完成情绪分析: {len(results)} 条结果")
            return results
            
        except Exception as e:
            logger.error(f"情绪分析失败: {e}")
            return [{'sentiment': 'neutral', 'score': 0.0, 'confidence': 0.0}] * len(texts)
    
    def get_hot_topics(
        self,
        date: str,
        limit: int = 50
    ) -> pd.DataFrame:
        """
        获取热门话题（基于新闻频次）
        
        Args:
            date: 日期
            limit: 返回数量限制
        
        Returns:
            热门话题DataFrame
        """
        try:
            logger.info(f"获取热门话题: {date}")
            
            # 获取当日新闻
            news_df = self.get_news(date, date)
            
            if news_df.empty:
                return pd.DataFrame()
            
            # 简单的关键词提取和统计
            # 这里可以集成更复杂的NLP处理
            keywords_count = {}
            
            for _, row in news_df.iterrows():
                title = str(row.get('title', ''))
                content = str(row.get('content', ''))
                text = title + ' ' + content
                
                # 简单的关键词提取（可以改进）
                words = text.split()
                for word in words:
                    if len(word) > 1:  # 过滤单字符
                        keywords_count[word] = keywords_count.get(word, 0) + 1
            
            # 排序并返回热门话题
            sorted_topics = sorted(keywords_count.items(), key=lambda x: x[1], reverse=True)
            top_topics = sorted_topics[:limit]
            
            # 构建DataFrame
            topics_df = pd.DataFrame(top_topics, columns=['topic', 'frequency'])
            topics_df['date'] = date
            
            logger.info(f"成功获取 {len(topics_df)} 个热门话题")
            return topics_df
            
        except Exception as e:
            logger.error(f"获取热门话题失败: {e}")
            return pd.DataFrame()


# 便捷函数
def get_news(
    start_date: str,
    end_date: str,
    src: str = None
) -> pd.DataFrame:
    """
    获取新闻数据的便捷函数
    """
    fetcher = NewsSentimentFetcher()
    return fetcher.get_news(start_date, end_date, src)


def get_announcements(
    ts_code: str,
    start_date: str,
    end_date: str,
    ann_type: str = None
) -> pd.DataFrame:
    """
    获取公告数据的便捷函数
    """
    fetcher = NewsSentimentFetcher()
    return fetcher.get_announcements(ts_code, start_date, end_date, ann_type)


def get_research_reports(
    ts_code: str,
    start_date: str,
    end_date: str
) -> pd.DataFrame:
    """
    获取机构调研的便捷函数
    """
    fetcher = NewsSentimentFetcher()
    return fetcher.get_research_reports(ts_code, start_date, end_date)


def analyze_news_sentiment(
    news_texts: List[str],
    method: str = 'simple'
) -> List[Dict[str, Any]]:
    """
    分析新闻情绪的便捷函数
    """
    fetcher = NewsSentimentFetcher()
    return fetcher.analyze_sentiment(news_texts, method)


def get_stock_news_with_sentiment(
    ts_code: str,
    start_date: str,
    end_date: str
) -> pd.DataFrame:
    """
    获取股票相关新闻并分析情绪的便捷函数
    
    Args:
        ts_code: 股票代码
        start_date: 开始日期
        end_date: 结束日期
    
    Returns:
        包含情绪分析的新闻DataFrame
    """
    fetcher = NewsSentimentFetcher()
    # 获取公告数据
    announcements = fetcher.get_announcements(ts_code, start_date, end_date)
    
    if announcements.empty:
        return pd.DataFrame()
    
    # 提取文本内容
    texts = []
    for _, row in announcements.iterrows():
        title = str(row.get('title', ''))
        summary = str(row.get('summary', ''))
        text = title + ' ' + summary
        texts.append(text)
    
    # 情绪分析
    sentiments = fetcher.analyze_sentiment(texts)
    
    # 合并结果
    for i, sentiment in enumerate(sentiments):
        if i < len(announcements):
            announcements.loc[i, 'sentiment'] = sentiment['sentiment']
            announcements.loc[i, 'sentiment_score'] = sentiment['score']
            announcements.loc[i, 'sentiment_confidence'] = sentiment['confidence']
    
    return announcements
