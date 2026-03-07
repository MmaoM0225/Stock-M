"""
Agents工具模块
提供各种数据获取工具，包括新闻、基本面、技术指标等
"""
import json
import os
from typing import Dict, Any, Optional, List
from langchain_core.tools import tool
import logging

from dataflow.news_sentiment import NewsSentimentFetcher

logger = logging.getLogger(__name__)


class NewsToolkit:
    """新闻数据工具包"""
    
    def __init__(self, config: Dict[str, Any] = None):
        """
        初始化新闻工具包
        
        Args:
            config: 配置字典，包含online_tools等配置
        """
        self.config = config or {}
        self.fetcher = NewsSentimentFetcher()
        self._industry_data = self._load_industry_data()
    
    def _load_industry_data(self) -> Dict[str, Any]:
        """
        加载行业数据
        
        Returns:
            Dict: 行业数据字典
        """
        industry_data = {}
        
        # 加载一级行业
        try:
            with open('data/industry_list_simple.json', 'r', encoding='utf-8') as f:
                l1_data = json.load(f)
                for item in l1_data:
                    industry_data[item['industry_name']] = {
                        'level': 1,
                        'index_code': item['index_code']
                    }
        except FileNotFoundError:
            logger.warning("未找到一级行业数据文件")
        
        # 加载二级行业
        try:
            with open('data/industry_list_l2_simple.json', 'r', encoding='utf-8') as f:
                l2_data = json.load(f)
                for item in l2_data:
                    industry_data[item['industry_name']] = {
                        'level': 2,
                        'index_code': item['index_code']
                    }
        except FileNotFoundError:
            logger.warning("未找到二级行业数据文件")
        
        logger.info(f"已加载 {len(industry_data)} 个行业分类")
        return industry_data
    
    def _search_industry(self, keywords: List[str]) -> List[str]:
        """
        根据关键词搜索相关行业
        
        Args:
            keywords: 关键词列表
            
        Returns:
            List: 匹配的行业名称列表
        """
        matched_industries = []
        
        for industry_name, info in self._industry_data.items():
            for keyword in keywords:
                if keyword.lower() in industry_name.lower():
                    matched_industries.append(industry_name)
                    break
        
        return matched_industries
    
    @tool
    def get_news_by_date(self, date_str: str) -> Dict[str, Any]:
        """
        从本地获取指定日期的新闻数据
        
        Args:
            date_str: 日期字符串，格式为YYYYMMDD，例如"20260305"
            
        Returns:
            Dict: 新闻数据字典，包含标题、内容、sections等信息
            如果本地没有该日期的数据，返回None
            
        Examples:
            >>> toolkit = NewsToolkit()
            >>> news = toolkit.get_news_by_date("20260305")
            >>> if news:
            ...     print(f"获取到 {len(news['sections'])} 条新闻")
        """
        try:
            logger.info(f"尝试从本地获取日期 {date_str} 的新闻数据")
            news_data = self.fetcher.get_news_by_date(date_str)
            
            if news_data:
                logger.info(f"成功从本地获取到日期 {date_str} 的新闻数据")
                return news_data
            else:
                logger.info(f"本地没有日期 {date_str} 的新闻数据")
                return None
                
        except Exception as e:
            logger.error(f"从本地获取新闻数据失败: {str(e)}")
            return None
    
    @tool
    def fetch_news_by_date(self, date_str: str) -> Dict[str, Any]:
        """
        从网络获取指定日期的新闻数据并保存到本地
        
        Args:
            date_str: 日期字符串，格式为YYYYMMDD，例如"20260305"
            
        Returns:
            Dict: 新闻数据字典，包含标题、内容、sections等信息
            如果获取失败，返回None
            
        Examples:
            >>> toolkit = NewsToolkit()
            >>> news = toolkit.fetch_news_by_date("20260305")
            >>> if news:
            ...     print(f"成功获取并保存了 {len(news['sections'])} 条新闻")
        """
        try:
            logger.info(f"开始从网络获取日期 {date_str} 的新闻数据")
            news_data = self.fetcher.fetch_eastmoney_news_by_date(date_str)
            
            if news_data and news_data.get('sections'):
                logger.info(f"成功从网络获取到日期 {date_str} 的新闻数据，共 {len(news_data['sections'])} 条")
                return news_data
            else:
                logger.warning(f"从网络获取日期 {date_str} 的新闻数据失败或数据为空")
                return None
                
        except Exception as e:
            logger.error(f"从网络获取新闻数据失败: {str(e)}")
            return None
    
    @tool
    def get_or_fetch_news_by_date(self, date_str: str) -> Dict[str, Any]:
        """
        获取指定日期的新闻数据（优先从本地获取，若没有则从网络获取）
        
        这是推荐使用的工具，它会自动处理本地和网络获取的逻辑：
        1. 首先尝试从本地获取
        2. 如果本地没有，则从网络获取并保存到本地
        3. 返回获取到的新闻数据
        
        Args:
            date_str: 日期字符串，格式为YYYYMMDD，例如"20260305"
            
        Returns:
            Dict: 新闻数据字典，包含标题、内容、sections等信息
            如果获取失败，返回None
            
        Examples:
            >>> toolkit = NewsToolkit()
            >>> news = toolkit.get_or_fetch_news_by_date("20260305")
            >>> if news:
            ...     print(f"获取到 {len(news['sections'])} 条新闻")
        """
        try:
            logger.info(f"获取日期 {date_str} 的新闻数据（优先本地）")
            
            # 首先尝试从本地获取
            news_data = self.get_news_by_date(date_str)
            
            if news_data:
                logger.info(f"从本地成功获取日期 {date_str} 的新闻数据")
                return news_data
            
            # 本地没有，从网络获取
            logger.info(f"本地没有日期 {date_str} 的新闻数据，尝试从网络获取")
            news_data = self.fetch_news_by_date(date_str)
            
            if news_data:
                logger.info(f"从网络成功获取并保存了日期 {date_str} 的新闻数据")
                return news_data
            else:
                logger.error(f"无法获取日期 {date_str} 的新闻数据")
                return None
                
        except Exception as e:
            logger.error(f"获取新闻数据失败: {str(e)}")
            return None
    
    @tool
    def get_news_sections_by_date(self, date_str: str) -> list:
        """
        获取指定日期的新闻sections列表
        
        Args:
            date_str: 日期字符串，格式为YYYYMMDD，例如"20260305"
            
        Returns:
            list: 新闻sections列表，每个section包含title和content
            如果获取失败，返回空列表
            
        Examples:
            >>> toolkit = NewsToolkit()
            >>> sections = toolkit.get_news_sections_by_date("20260305")
            >>> for section in sections:
            ...     print(f"{section['title']}: {section['content'][:50]}...")
        """
        try:
            news_data = self.get_or_fetch_news_by_date(date_str)
            
            if news_data and 'sections' in news_data:
                return news_data['sections']
            else:
                logger.warning(f"日期 {date_str} 的新闻数据中没有sections")
                return []
                
        except Exception as e:
            logger.error(f"获取新闻sections失败: {str(e)}")
            return []
    
    @tool
    def get_latest_news(self) -> Dict[str, Any]:
        """
        获取最新的新闻数据
        
        尝试获取最近一个工作日的新闻数据
        
        Returns:
            Dict: 新闻数据字典，包含标题、内容、sections等信息
            如果获取失败，返回None
            
        Examples:
            >>> toolkit = NewsToolkit()
            >>> news = toolkit.get_latest_news()
            >>> if news:
            ...     print(f"获取到最新新闻，共 {len(news['sections'])} 条")
        """
        try:
            from datetime import datetime, timedelta
            
            # 从今天开始往前查找最近的工作日
            today = datetime.now()
            for i in range(7):  # 最多查找7天
                check_date = today - timedelta(days=i)
                date_str = check_date.strftime("%Y%m%d")
                
                # 跳过周末（周六=5，周日=6）
                if check_date.weekday() >= 5:
                    continue
                
                news_data = self.get_or_fetch_news_by_date(date_str)
                if news_data:
                    logger.info(f"获取到最新新闻数据，日期: {date_str}")
                    return news_data
            
            logger.warning("未找到最近7天内的新闻数据")
            return None
            
        except Exception as e:
            logger.error(f"获取最新新闻失败: {str(e)}")
            return None
    
    @tool
    def get_industry_info(self, keywords: str) -> Dict[str, Any]:
        """
        根据关键词获取行业信息
        
        Args:
            keywords: 关键词，可以是行业名称或相关词汇
            
        Returns:
            Dict: 包含匹配的行业信息
                {
                    "matched_industries": ["行业1", "行业2"],
                    "industry_count": 2,
                    "all_industries": ["所有行业1", "所有行业2"]
                }
            
        Examples:
            >>> toolkit = NewsToolkit()
            >>> info = toolkit.get_industry_info("AI")
            >>> print(info)
            {
                "matched_industries": ["计算机", "半导体"],
                "industry_count": 2,
                "all_industries": ["农林牧渔", "基础化工", ...]
            }
        """
        try:
            logger.info(f"搜索行业关键词: {keywords}")
            
            # 提取关键词
            keyword_list = [kw.strip() for kw in keywords.split(',') if kw.strip()]
            
            # 搜索匹配的行业
            matched_industries = []
            for keyword in keyword_list:
                matched = self._search_industry([keyword])
                matched_industries.extend(matched)
            
            # 去重
            matched_industries = list(set(matched_industries))
            
            result = {
                "matched_industries": matched_industries,
                "industry_count": len(matched_industries),
                "all_industries": list(self._industry_data.keys())
            }
            
            logger.info(f"找到 {len(matched_industries)} 个匹配的行业")
            return result
            
        except Exception as e:
            logger.error(f"获取行业信息失败: {str(e)}")
            return {
                "matched_industries": [],
                "industry_count": 0,
                "all_industries": []
            }
    
    @tool
    def get_all_industries(self) -> Dict[str, Any]:
        """
        获取所有行业分类
        
        Returns:
            Dict: 包含所有行业分类信息
                {
                    "industries": ["行业1", "行业2"],
                    "count": 2
                }
            
        Examples:
            >>> toolkit = NewsToolkit()
            >>> info = toolkit.get_all_industries()
            >>> print(info)
            {
                "industries": ["农林牧渔", "基础化工", ...],
                "count": 126
            }
        """
        try:
            logger.info("获取所有行业分类")
            
            result = {
                "industries": list(self._industry_data.keys()),
                "count": len(self._industry_data)
            }
            
            logger.info(f"共 {result['count']} 个行业分类")
            return result
            
        except Exception as e:
            logger.error(f"获取行业分类失败: {str(e)}")
            return {
                "industries": [],
                "count": 0
            }


def create_news_toolkit(config: Dict[str, Any] = None) -> NewsToolkit:
    """
    创建新闻工具包实例
    
    Args:
        config: 配置字典
        
    Returns:
        NewsToolkit: 新闻工具包实例
    """
    return NewsToolkit(config)
