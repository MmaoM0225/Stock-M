"""
新闻舆情数据获取模块
包括新闻数据、公告数据、社交媒体情绪等
"""
import pandas as pd
import tushare as ts
import requests
import akshare as ak
from typing import Dict, List, Optional, Any, Union
from datetime import datetime, date
import logging
import json
from bs4 import BeautifulSoup

from .config import DATA_SOURCES, NEWS_API_KEY
from .utils import (
    format_date, validate_stock_code,
    clean_dataframe, DataFlowException
)
from .finlight_data import FinlightDataFetcher

logger = logging.getLogger(__name__)


class NewsSentimentFetcher:
    """新闻舆情数据获取器"""
    
    def __init__(self, finlight_api_key: Optional[str] = None):
        """初始化
        
        Args:
            finlight_api_key: Finlight API Key（可选），如未提供则从环境变量读取
        """
        self.tushare_enabled = DATA_SOURCES['tushare']['enabled']
        if self.tushare_enabled:
            ts.set_token(DATA_SOURCES['tushare']['token'])
            self.ts_pro = ts.pro_api()
        
        self.news_api_key = NEWS_API_KEY
        
        # 初始化 Finlight 获取器
        self.finlight_fetcher = FinlightDataFetcher(api_key=finlight_api_key)
    
    def fetch_eastmoney_breakfast_news(self) -> pd.DataFrame:
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
    
    def fetch_eastmoney_news_by_date(self, date_str: str, auto_save: bool = True) -> Dict[str, Any]:
        """根据日期爬取东方财富新闻页面内容（详情 URL 来自数据库 breakfast_news 表）

        Args:
            date_str: 日期字符串，格式为 YYYYMMDD
            auto_save: 是否将正文保存到 data/news/news_{date}.json

        Returns:
            Dict: 包含标题、内容、发布时间等信息的字典

        Raises:
            DataFlowException: 当数据获取失败时抛出异常
        """
        try:
            from database import BreakfastNews
            from database.config import get_db_session

            with get_db_session() as session:
                row = session.query(BreakfastNews).filter(BreakfastNews.pub_date == date_str).first()
                url = row.detail_url if row else None

            if not url:
                raise DataFlowException(
                    f"未找到日期 {date_str} 的财经早餐详情链接，请先运行同步: python -m database.data_sync.breakfast_news"
                )
            
            logger.info(f"找到日期 {date_str} 对应的URL: {url}")
            
            use_old_format = int(date_str) <= 20250603
            
            news_data = self.fetch_eastmoney_news_page(url, use_old_format)
            
            if auto_save:
                self._save_news_to_json(news_data, date_str)
            
            return news_data

        except DataFlowException:
            raise
        except Exception as e:
            logger.error(f"根据日期获取新闻失败: {str(e)}")
            raise DataFlowException(f"根据日期获取新闻失败: {str(e)}")

    def get_news_by_date(self, date_str: str, file_dir: str = "data/news") -> Optional[Dict[str, Any]]:
        """从本地读取指定日期的新闻数据
        
        Args:
            date_str: 日期字符串，格式为YYYYMMDD
            file_dir: 新闻文件存储目录
            
        Returns:
            Dict: 新闻数据字典，如果文件不存在则返回None
            
        Raises:
            DataFlowException: 当文件读取失败时抛出异常
        """
        try:
            import os
            file_path = os.path.join(file_dir, f"news_{date_str}.json")
            
            if not os.path.exists(file_path):
                logger.info(f"文件不存在: {file_path}")
                return None
            
            with open(file_path, 'r', encoding='utf-8') as f:
                news_data = json.load(f)
            
            logger.info(f"成功读取本地文件: {file_path}")
            return news_data
            
        except json.JSONDecodeError as e:
            logger.error(f"JSON文件解析失败: {str(e)}")
            raise DataFlowException(f"JSON文件解析失败: {str(e)}")
        except Exception as e:
            logger.error(f"读取新闻文件失败: {str(e)}")
            raise DataFlowException(f"读取新闻文件失败: {str(e)}")

    def _save_news_to_json(self, news_data: Dict[str, Any], date_str: str, file_dir: str = "data/news"):
        """保存新闻数据到JSON文件
        
        Args:
            news_data: 新闻数据
            date_str: 日期字符串，格式为YYYYMMDD
            file_dir: 存储目录
        """
        try:
            import os
            os.makedirs(file_dir, exist_ok=True)
            
            file_path = os.path.join(file_dir, f"news_{date_str}.json")
            
            with open(file_path, 'w', encoding='utf-8') as f:
                json.dump(news_data, f, ensure_ascii=False, indent=2)
            
            logger.info(f"数据已保存到 {file_path}")
            
        except Exception as e:
            logger.error(f"保存新闻数据失败: {str(e)}")

    def fetch_eastmoney_news_page(self, url: str, use_old_format: bool = False) -> Dict[str, Any]:
        """爬取东方财富新闻页面内容
        
        Args:
            url: 新闻页面URL
            use_old_format: 是否使用旧版解析方式
            
        Returns:
            Dict: 包含标题、内容、发布时间等信息的字典
            
        Raises:
            DataFlowException: 当数据获取失败时抛出异常
        """
        try:
            logger.info(f"开始爬取东方财富新闻页面: {url}")
            logger.info(f"使用{'旧版' if use_old_format else '新版'}解析方式")
            
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
                'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
                'Accept-Encoding': 'gzip, deflate, br',
                'Connection': 'keep-alive',
                'Upgrade-Insecure-Requests': '1'
            }
            
            response = requests.get(url, headers=headers, timeout=30)
            response.raise_for_status()
            response.encoding = 'utf-8'
            
            soup = BeautifulSoup(response.text, 'html.parser')
            
            result = {
                'url': url,
                'title': '',
                'content': '',
                'sections': [],
                'fetch_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            }
            
            title_elem = soup.find('h1') or soup.find('title')
            if title_elem:
                result['title'] = title_elem.get_text(strip=True)
            
            if not use_old_format:
                # 20250603 以后：每日精选为两个p标签一组，其他区块为一个p标签一组，标题与旧版一样命名
                h3_sections = soup.find_all('h3', class_='emh3')
                section_index = 1  # 用于旧版标题命名 title1, title2, ...
                
                for h3 in h3_sections:
                    section_title = h3.get_text(strip=True)
                    
                    if '交易提示' in section_title:
                        continue
                    
                    next_sibling = h3.find_next_sibling()
                    p_list = []
                    while next_sibling:
                        if next_sibling.name == 'h3' and next_sibling.get('class') == ['emh3']:
                            break
                        
                        if next_sibling.name == 'p':
                            p_text = next_sibling.get_text(strip=True)
                            if p_text:
                                p_list.append(p_text)
                        
                        next_sibling = next_sibling.find_next_sibling()
                    
                    if '每日精选' in section_title or '热点题材' in section_title:
                        # 每日精选、热点题材：两个p标签为一组（第一个p为title，第二个p为content）
                        for i in range(0, len(p_list), 2):
                            if i + 1 < len(p_list):
                                result['sections'].append({
                                    'title': p_list[i],
                                    'content': p_list[i + 1]
                                })
                    else:
                        # 其他区块：一个p标签为一组，标题与旧版一样命名（title1, title2...）
                        for p_text in p_list:
                            result['sections'].append({
                                'title': f'title{section_index}',
                                'content': p_text
                            })
                            section_index += 1
            else:
                logger.info("使用旧格式解析（从txtinfos div中提取p标签）")
                txtinfos_div = soup.find('div', class_='txtinfos')
                if txtinfos_div:
                    paragraphs = txtinfos_div.find_all('p')
                    section_index = 1
                    for p in paragraphs:
                        p_text = p.get_text(strip=True)
                        p_html = str(p)
                        
                        if p_text and '.jpg' not in p_html.lower() and '.png' not in p_html.lower() and '<a' not in p_html:
                            result['sections'].append({
                                'title': f'title{section_index}',
                                'content': p_text
                            })
                            section_index += 1
                else:
                    logger.warning("未找到txtinfos容器")
            
            content_parts = []
            for section in result['sections']:
                content_parts.append(section['content'])
                content_parts.append('')
            
            result['content'] = '\n'.join(content_parts).strip()
            
            if result['title'] or result['content']:
                logger.info(f"成功爬取新闻: {result['title'][:50] if result['title'] else '无标题'}...")
                logger.info(f"共解析到 {len(result['sections'])} 个章节")
                return result
            else:
                logger.warning("未能提取到有效的新闻内容")
                return result
                
        except requests.RequestException as e:
            logger.error(f"请求新闻页面失败: {str(e)}")
            raise DataFlowException(f"请求新闻页面失败: {str(e)}")
        except Exception as e:
            logger.error(f"解析新闻页面失败: {str(e)}")
            raise DataFlowException(f"解析新闻页面失败: {str(e)}")

    def get_breakfast_news_by_date_range(
        self, start_date: str = None, end_date: str = None
    ) -> pd.DataFrame:
        """根据时间范围从数据库 breakfast_news 表获取财经早餐列表摘要

        Args:
            start_date: 开始日期，格式为 'YYYY-MM-DD'，不指定则从最早数据开始
            end_date: 结束日期，格式为 'YYYY-MM-DD'，不指定则到最新数据

        Returns:
            pd.DataFrame: 列为 title, summary, pub_date, detail_url，pub_date 已转为 datetime

        Raises:
            DataFlowException: 当数据库读取失败时抛出异常
        """
        try:
            from database import BreakfastNews
            from database.config import get_db_session

            with get_db_session() as session:
                rows = (
                    session.query(BreakfastNews)
                    .filter(BreakfastNews.pub_date.isnot(None))
                    .order_by(BreakfastNews.pub_date)
                    .all()
                )

            if not rows:
                logger.warning("数据库中无财经早餐记录")
                return pd.DataFrame()

            df = pd.DataFrame(
                [
                    {
                        "title": r.title,
                        "summary": r.summary,
                        "pub_date": r.pub_date,
                        "detail_url": r.detail_url,
                    }
                    for r in rows
                ]
            )
            time_column = "pub_date"
            df[time_column] = pd.to_datetime(df[time_column], format="%Y%m%d", errors="coerce")
            df = df.dropna(subset=[time_column])

            if start_date:
                start_datetime = pd.to_datetime(start_date)
                df = df[df[time_column] >= start_datetime]

            if end_date:
                end_datetime = pd.to_datetime(end_date)
                df = df[df[time_column] <= end_datetime]

            df = df.sort_values(by=time_column, ascending=False).reset_index(drop=True)

            logger.info(f"根据时间范围筛选得到 {len(df)} 条数据")
            return df

        except Exception as e:
            logger.error(f"读取财经早餐数据失败: {str(e)}")
            raise DataFlowException(f"读取财经早餐数据失败: {str(e)}")
    
    def fetch_finlight_news(
        self,
        query: Optional[str] = None,
        countries: Optional[List[str]] = None,
        categories: Optional[List[str]] = None,
        page_size: int = 50,
        as_sections: bool = True,
        use_cache: bool = True,
    ) -> Union[Dict[str, Any], List[Dict[str, str]]]:
        """通过 Finlight API 获取全球金融新闻（优先使用本地缓存）
        
        可作为东方财富新闻的补充源，特别适用于：
        - 地缘政治风险评估
        - 国际市场情绪监测
        - 大宗商品和能源新闻
        
        Args:
            query: 搜索查询，如 "country:CN"
            countries: 国家代码列表，如 ["CN", "US"]
            categories: 分类列表，如 ["geopolitics", "energy"]
            page_size: 每页数量
            as_sections: 是否转换为 sections 格式（供 news_analyst 使用）
            use_cache: 是否优先使用本地缓存，默认 True
            
        Returns:
            如果 as_sections=True: 返回 sections 列表
            如果 as_sections=False: 返回原始 API 响应
            
        Raises:
            DataFlowException: 当 API 调用失败时抛出异常
        """
        try:
            logger.info(f"开始获取 Finlight 新闻: query={query}, categories={categories}, use_cache={use_cache}")
            
            result = self.finlight_fetcher.fetch_articles(
                query=query,
                countries=countries,
                categories=categories,
                page_size=page_size,
                use_cache=use_cache,
            )
            
            if as_sections:
                sections = self.finlight_fetcher.convert_to_sections_format(result)
                logger.info(f"成功获取并转换 {len(sections)} 条 Finlight 新闻")
                return sections
            
            logger.info(f"成功获取 Finlight 新闻，共 {len(result.get('articles', []))} 条")
            return result
            
        except Exception as e:
            logger.error(f"获取 Finlight 新闻失败: {str(e)}")
            raise DataFlowException(f"获取 Finlight 新闻失败: {str(e)}")
    
    def fetch_finlight_china_news(
        self,
        categories: Optional[List[str]] = None,
        page_size: int = 50,
        as_sections: bool = True,
        use_cache: bool = True,
    ) -> Union[Dict[str, Any], List[Dict[str, str]]]:
        """便捷方法：获取中国相关新闻（优先使用本地缓存）
        
        Args:
            categories: 分类筛选
            page_size: 每页数量
            as_sections: 是否转换为 sections 格式
            use_cache: 是否优先使用本地缓存，默认 True
            
        Returns:
            sections 列表或原始响应
        """
        return self.fetch_finlight_news(
            query="country:CN",
            categories=categories,
            page_size=page_size,
            as_sections=as_sections,
            use_cache=use_cache,
        )
    
    def get_combined_news_sections(
        self,
        date_str: str,
        use_finlight: bool = True,
        finlight_categories: Optional[List[str]] = None,
        use_finlight_cache: bool = True,
    ) -> List[Dict[str, str]]:
        """获取组合新闻 sections（东方财富 + Finlight，优先使用本地缓存）
        
        Args:
            date_str: 日期字符串，格式 YYYYMMDD
            use_finlight: 是否同时获取 Finlight 新闻
            finlight_categories: Finlight 新闻分类筛选
            use_finlight_cache: 是否优先使用 Finlight 本地缓存，默认 True
            
        Returns:
            List[Dict]: 合并后的 sections 列表
        """
        sections = []
        
        # 1. 获取东方财富新闻（原有逻辑）
        try:
            eastmoney_data = self.get_news_by_date(date_str)
            if eastmoney_data:
                sections.extend(eastmoney_data.get("sections", []))
                logger.info(f"东方财富新闻: {len(eastmoney_data.get('sections', []))} 条")
        except Exception as e:
            logger.warning(f"获取东方财富新闻失败: {e}")
        
        # 2. 获取 Finlight 新闻（新增，优先使用缓存）
        if use_finlight:
            try:
                finlight_sections = self.fetch_finlight_china_news(
                    categories=finlight_categories,
                    page_size=30,
                    as_sections=True,
                    use_cache=use_finlight_cache,
                )
                if isinstance(finlight_sections, list):
                    sections.extend(finlight_sections)
                    logger.info(f"Finlight 新闻: {len(finlight_sections)} 条")
            except Exception as e:
                logger.warning(f"获取 Finlight 新闻失败: {e}")
        
        logger.info(f"组合新闻总计: {len(sections)} 条")
        return sections


