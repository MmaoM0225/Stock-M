"""
Finlight API 数据获取模块
提供全球金融新闻、市场数据等
"""
import os
import json
import logging
from typing import Dict, List, Optional, Any, Union
from datetime import datetime, date, timedelta
from pathlib import Path

from .config import FINLIGHT_API_KEY, REQUEST_TIMEOUT, MAX_RETRIES, RETRY_DELAY
from .utils import format_date, DataFlowException

logger = logging.getLogger(__name__)


class FinlightDataFetcher:
    """Finlight API 数据获取器
    
    用于获取全球金融新闻数据，支持按国家、分类、时间等维度筛选。
    """
    
    def __init__(self, api_key: Optional[str] = None):
        """初始化 Finlight 数据获取器
        
        Args:
            api_key: Finlight API Key，如未提供则从环境变量读取
        """
        self.api_key = api_key or FINLIGHT_API_KEY
        if not self.api_key:
            logger.warning("未配置 Finlight API Key，请设置 FINLIGHT_API_KEY 环境变量")
        
        self._client = None
        self._config = None
    
    def _get_client(self):
        """懒加载 Finlight API 客户端"""
        if self._client is None:
            try:
                from finlight_client import FinlightApi, ApiConfig
                self._config = ApiConfig(api_key=self.api_key)
                self._client = FinlightApi(config=self._config)
                logger.info("Finlight API 客户端初始化成功")
            except ImportError:
                raise DataFlowException(
                    "finlight_client 模块未安装，请运行: pip install finlight-client"
                )
            except Exception as e:
                raise DataFlowException(f"Finlight API 客户端初始化失败: {str(e)}")
        return self._client
    
    def fetch_articles(
        self,
        query: Optional[str] = None,
        countries: Optional[List[str]] = None,
        categories: Optional[List[str]] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        from_: Optional[str] = None,
        to: Optional[str] = None,
        language: str = "en",
        page: int = 1,
        page_size: int = 50,
        use_cache: bool = True,
        cache_dir: str = "data/news_finlight",
    ) -> Dict[str, Any]:
        """获取新闻文章列表（优先使用本地缓存）
        
        如果本地缓存存在且未过期，直接返回缓存数据；
        否则请求 API 并保存到本地缓存。
        
        Args:
            query: 搜索查询，如 "country:CN" 或 "category:technology"
            countries: 国家代码列表，如 ["CN", "US"]
            categories: 分类列表，如 ["technology", "business"]
            start_date: 开始日期 (YYYY-MM-DD)
            end_date: 结束日期 (YYYY-MM-DD)
            from_: 开始时间（ISO 8601 格式，如 "2026-04-20T00:00"）
            to: 结束时间（ISO 8601 格式，如 "2026-04-20T12:00"）
            language: 语言代码，默认 "en"
            page: 页码，默认 1
            page_size: 每页数量，默认 50
            use_cache: 是否使用本地缓存，默认 True
            cache_dir: 缓存目录
            
        Returns:
            Dict: 包含 articles, total, page, page_size 的响应数据
            
        Raises:
            DataFlowException: 当 API 调用失败时抛出异常
        """
        # 1. 生成缓存 key
        cache_key = self._generate_cache_key(
            query=query,
            countries=countries,
            categories=categories,
            start_date=start_date,
            end_date=end_date,
            from_=from_,
            to=to,
            language=language,
            page=page,
            page_size=page_size,
        )
        
        # 2. 优先读取本地缓存
        if use_cache:
            cached_data = self._load_from_cache(cache_key, cache_dir)
            if cached_data is not None:
                logger.info(f"使用本地缓存数据: {cache_key}")
                return cached_data
        
        # 3. 本地无缓存，请求 API
        try:
            from finlight_client.models import GetArticlesParams
            
            client = self._get_client()
            
            # 构建查询参数
            params_dict = {"page": page, "pageSize": page_size, "language": language}
            
            if query:
                params_dict["query"] = query
            if countries:
                params_dict["countries"] = countries
            if categories:
                params_dict["categories"] = categories
            if start_date:
                params_dict["startDate"] = start_date
            if end_date:
                params_dict["endDate"] = end_date
            if from_:
                params_dict["from"] = from_
            if to:
                params_dict["to"] = to
            
            params = GetArticlesParams(**params_dict)
            
            logger.info(f"正在请求 Finlight API: query={query}, page={page}")
            
            response = client.articles.fetch_articles(params=params)
            
            # 打印原始响应类型以便调试
            logger.debug(f"原始响应类型: {type(response)}")
            if hasattr(response, 'articles'):
                articles = response.articles
                logger.debug(f"articles 类型: {type(articles)}")
                if articles and len(articles) > 0:
                    logger.debug(f"第一条 article 类型: {type(articles[0])}")
                    logger.debug(f"第一条 article 属性: {dir(articles[0]) if hasattr(articles[0], '__dict__') else 'N/A'}")
            
            # 转换为字典格式
            result = self._response_to_dict(response)
            
            articles_count = len(result.get("articles", []))
            logger.info(f"API 请求成功，获取 {articles_count} 条新闻")
            
            # 4. 保存到本地缓存
            if use_cache and result.get("articles"):
                self._save_to_cache(result, cache_key, cache_dir)
            
            return result
            
        except Exception as e:
            logger.error(f"获取 Finlight 新闻失败: {str(e)}")
            raise DataFlowException(f"获取 Finlight 新闻失败: {str(e)}")
    
    def _generate_cache_key(
        self,
        query: Optional[str] = None,
        countries: Optional[List[str]] = None,
        categories: Optional[List[str]] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        from_: Optional[str] = None,
        to: Optional[str] = None,
        language: str = "en",
        page: int = 1,
        page_size: int = 50,
    ) -> str:
        """根据查询参数生成缓存文件名
        
        缓存 key 格式: news_{date}.json
        """
        # 优先使用 from/to 时间范围作为日期标识
        date_info = ""
        if from_ and to:
            # 从 from_ 提取日期部分
            from_date = from_.split("T")[0].replace("-", "")
            to_date = to.split("T")[0].replace("-", "")
            if from_date == to_date:
                date_info = from_date
            else:
                date_info = f"{from_date}_{to_date}"
        elif start_date and end_date and start_date == end_date:
            # 单日查询
            date_info = start_date.replace("-", "")
        elif start_date or end_date:
            date_info = f"{start_date or 'na'}_{end_date or 'na'}"
        elif from_:
            date_info = from_.split("T")[0].replace("-", "")
        else:
            # 无日期信息，使用今日日期
            date_info = datetime.now().strftime("%Y%m%d")
        
        return f"news_{date_info}"
    
    def _load_from_cache(self, cache_key: str, cache_dir: str) -> Optional[Dict[str, Any]]:
        """从本地缓存读取数据
        
        缓存有效期：当天数据不过期，历史数据永久有效
        """
        try:
            cache_path = Path(cache_dir) / f"{cache_key}.json"
            
            if not cache_path.exists():
                return None
            
            # 检查缓存是否过期（仅当日数据需要检查）
            file_mtime = cache_path.stat().st_mtime
            file_date = datetime.fromtimestamp(file_mtime).strftime("%Y%m%d")
            today = datetime.now().strftime("%Y%m%d")
            
            # 如果是今天创建的缓存，检查是否在6小时内
            if file_date == today:
                file_age_hours = (datetime.now().timestamp() - file_mtime) / 3600
                if file_age_hours > 6:  # 当日缓存6小时过期
                    logger.info(f"当日缓存已过期（{file_age_hours:.1f}小时），将重新请求")
                    return None
            
            with open(cache_path, "r", encoding="utf-8") as f:
                data = json.load(f)
            
            logger.debug(f"从缓存读取: {cache_path}")
            return data
            
        except json.JSONDecodeError:
            logger.warning(f"缓存文件损坏: {cache_key}")
            return None
        except Exception as e:
            logger.warning(f"读取缓存失败: {e}")
            return None
    
    def _ensure_json_serializable(self, obj: Any) -> Any:
        """确保对象是可 JSON 序列化的
        
        递归处理字典、列表和对象，转换为可序列化的格式。
        """
        if obj is None:
            return None
        elif isinstance(obj, (str, int, float, bool)):
            return obj
        elif isinstance(obj, list):
            return [self._ensure_json_serializable(item) for item in obj]
        elif isinstance(obj, dict):
            return {key: self._ensure_json_serializable(value) for key, value in obj.items()}
        else:
            # 对象类型，尝试转换为字典
            return self._article_to_dict(obj)
    
    def _save_to_cache(
        self, 
        data: Dict[str, Any], 
        cache_key: str, 
        cache_dir: str
    ) -> None:
        """保存数据到本地缓存"""
        try:
            cache_path = Path(cache_dir)
            cache_path.mkdir(parents=True, exist_ok=True)
            
            file_path = cache_path / f"{cache_key}.json"
            
            # 确保数据可 JSON 序列化
            serializable_data = self._ensure_json_serializable(data)
            
            # 添加元数据
            data_with_meta = {
                "_meta": {
                    "cache_key": cache_key,
                    "cached_at": datetime.now().isoformat(),
                    "source": "finlight",
                },
                **serializable_data,
            }
            
            with open(file_path, "w", encoding="utf-8") as f:
                json.dump(data_with_meta, f, ensure_ascii=False, indent=2)
            
            logger.info(f"数据已缓存: {file_path}")
            
        except Exception as e:
            logger.warning(f"保存缓存失败: {e}")
            import traceback
            logger.debug(traceback.format_exc())
    
    def fetch_china_news(
        self,
        categories: Optional[List[str]] = None,
        page: int = 1,
        page_size: int = 50,
        use_cache: bool = True,
    ) -> Dict[str, Any]:
        """获取中国相关新闻（便捷方法，优先使用本地缓存）
        
        Args:
            categories: 分类筛选，如 ["economy", "technology", "geopolitics"]
            page: 页码
            page_size: 每页数量
            use_cache: 是否使用本地缓存，默认 True
            
        Returns:
            Dict: 新闻数据
        """
        return self.fetch_articles(
            query="country:CN",
            categories=categories,
            page=page,
            page_size=page_size,
            use_cache=use_cache,
        )
    
    def fetch_news_by_date(
        self,
        date_str: str,
        countries: Optional[List[str]] = None,
        categories: Optional[List[str]] = None,
        use_cache: bool = True,
    ) -> Dict[str, Any]:
        """获取指定日期的新闻（优先使用本地缓存）
        
        Args:
            date_str: 日期字符串，格式 YYYYMMDD 或 YYYY-MM-DD
            countries: 国家代码列表
            categories: 分类列表
            use_cache: 是否使用本地缓存，默认 True
            
        Returns:
            Dict: 新闻数据
        """
        # 统一日期格式为 YYYY-MM-DD
        if len(date_str) == 8 and date_str.isdigit():
            formatted_date = f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:]}"
        else:
            formatted_date = date_str
        
        return self.fetch_articles(
            countries=countries,
            categories=categories,
            start_date=formatted_date,
            end_date=formatted_date,
            page_size=100,  # 单日获取更多
            use_cache=use_cache,
        )
    
    def fetch_news_by_time_range(
        self,
        from_: str,
        to: str,
        countries: Optional[List[str]] = None,
        categories: Optional[List[str]] = None,
        page_size: int = 100,
        use_cache: bool = True,
    ) -> Dict[str, Any]:
        """获取指定时间范围的新闻（优先使用本地缓存）
        
        适用于获取昨日00:00到今日中午12:00等跨天时间范围的新闻。
        
        Args:
            from_: 开始时间（ISO 8601 格式，如 "2026-04-20T00:00"）
            to: 结束时间（ISO 8601 格式，如 "2026-04-20T12:00"）
            countries: 国家代码列表，默认 ["CN"]
            categories: 分类列表
            page_size: 每页数量，默认 100
            use_cache: 是否使用本地缓存，默认 True
            
        Returns:
            Dict: 新闻数据
            
        Example:
            # 获取昨日00:00到今日12:00的新闻
            from datetime import datetime, timedelta
            
            today = datetime.now()
            yesterday = today - timedelta(days=1)
            
            from_time = yesterday.strftime("%Y-%m-%dT00:00")
            to_time = today.strftime("%Y-%m-%dT12:00")
            
            result = fetcher.fetch_news_by_time_range(
                from_=from_time,
                to=to_time,
                countries=["CN"]
            )
        """
        if countries is None:
            countries = ["CN"]
        
        return self.fetch_articles(
            query=f"country:{','.join(countries)}" if countries else None,
            countries=countries,
            categories=categories,
            from_=from_,
            to=to,
            page_size=page_size,
            use_cache=use_cache,
        )
    
    def fetch_yesterday_to_today_news(
        self,
        end_hour: int = 12,
        countries: Optional[List[str]] = None,
        categories: Optional[List[str]] = None,
        page_size: int = 100,
        use_cache: bool = True,
    ) -> Dict[str, Any]:
        """获取昨日00:00到今日指定时间的新闻（便捷方法）
        
        Args:
            end_hour: 今日结束小时，默认 12（中午12点）
            countries: 国家代码列表，默认 ["CN"]
            categories: 分类列表
            page_size: 每页数量，默认 100
            use_cache: 是否使用本地缓存，默认 True
            
        Returns:
            Dict: 新闻数据
        """
        today = datetime.now()
        yesterday = today - timedelta(days=1)
        
        from_time = yesterday.strftime("%Y-%m-%dT00:00")
        to_time = today.strftime(f"%Y-%m-%dT{end_hour:02d}:00")
        
        logger.info(f"获取昨日 {from_time} 到今日 {to_time} 的新闻")
        
        return self.fetch_news_by_time_range(
            from_=from_time,
            to=to_time,
            countries=countries,
            categories=categories,
            page_size=page_size,
            use_cache=use_cache,
        )
    
    def _response_to_dict(self, response) -> Dict[str, Any]:
        """将 API 响应转换为字典格式
        
        Finlight API 返回的可能是对象或字典，统一转换为字典。
        递归处理嵌套的 Article 对象列表。
        """
        # 如果是字典，直接返回（但需处理内部可能的 Article 对象）
        if isinstance(response, dict):
            result = {}
            for key, value in response.items():
                if isinstance(value, list) and value and not isinstance(value[0], (str, int, float, bool)):
                    # 处理列表中的 Article 对象（跳过基本类型列表）
                    try:
                        result[key] = [self._article_to_dict(item) for item in value]
                    except Exception as e:
                        logger.warning(f"转换列表 {key} 失败: {e}")
                        result[key] = value
                else:
                    result[key] = value
            return result
        
        # 如果是列表（直接返回 Article 列表的情况）
        if isinstance(response, list):
            return {"articles": [self._article_to_dict(item) for item in response]}
        
        # 如果是单个对象，使用 model_dump 或 dict 方法
        if hasattr(response, "model_dump"):
            try:
                return response.model_dump()
            except Exception:
                pass
        if hasattr(response, "dict"):
            try:
                return response.dict()
            except Exception:
                pass
        if hasattr(response, "__dict__"):
            try:
                return response.__dict__
            except Exception:
                pass
        
        # 尝试 JSON 序列化再解析
        try:
            return json.loads(json.dumps(response, default=str))
        except Exception:
            return {"raw_response": str(response)}
    
    def _article_to_dict(self, article) -> Dict[str, Any]:
        """将单个 Article 对象转换为字典
        
        Args:
            article: Article 对象或字典
            
        Returns:
            Dict: 文章字典
        """
        if isinstance(article, dict):
            return article
        
        # Pydantic v2
        if hasattr(article, "model_dump"):
            try:
                return article.model_dump()
            except Exception:
                pass
        
        # Pydantic v1
        if hasattr(article, "dict"):
            try:
                return article.dict()
            except Exception:
                pass
        
        # 提取已知属性（适用于普通 Python 对象/dataclass）
        result = {}
        known_attrs = [
            "link", "source", "title", "summary", "publishDate",
            "language", "categories", "countries", "images"
        ]
        
        for attr in known_attrs:
            if hasattr(article, attr):
                value = getattr(article, attr)
                # 确保值是可序列化的
                if value is None:
                    result[attr] = None
                elif isinstance(value, (str, int, float, bool, list, dict)):
                    result[attr] = value
                else:
                    result[attr] = str(value)
        
        if result:
            return result
        
        # 最后尝试 __dict__
        if hasattr(article, "__dict__"):
            try:
                return dict(article.__dict__)
            except Exception:
                pass
        
        # 其他类型，使用 str 转换
        return {"raw_data": str(article)}
    
    def _get_article_attr(self, article, key: str, default: Any = "") -> Any:
        """统一获取 Article 属性（支持对象和字典）
        
        Args:
            article: Article 对象或字典
            key: 属性名
            default: 默认值
            
        Returns:
            属性值
        """
        if isinstance(article, dict):
            return article.get(key, default)
        
        # 尝试作为属性访问
        if hasattr(article, key):
            return getattr(article, key, default)
        
        # 尝试常见的大小写变体
        key_mapping = {
            "publishDate": ["publish_date", "publishDate"],
            "publish_date": ["publish_date", "publishDate"],
        }
        
        for alt_key in key_mapping.get(key, [key]):
            if hasattr(article, alt_key):
                return getattr(article, alt_key, default)
        
        return default
    
    def convert_to_sections_format(
        self, articles_data: Dict[str, Any]
    ) -> List[Dict[str, str]]:
        """将 Finlight 新闻格式转换为系统内部的 sections 格式
        
        将英文新闻转换为兼容现有 news_analyst 的格式
        
        Args:
            articles_data: fetch_articles 返回的数据
            
        Returns:
            List[Dict]: sections 列表，每个元素包含 title 和 content
        """
        # 确保数据已转换为字典格式
        if not isinstance(articles_data, dict):
            articles_data = self._response_to_dict(articles_data)
        
        articles = articles_data.get("articles", [])
        sections = []
        
        for article in articles:
            # 如果还是对象，先转换为字典
            if not isinstance(article, dict):
                article = self._article_to_dict(article)
            
            title = article.get("title", "") if isinstance(article, dict) else getattr(article, "title", "")
            summary = article.get("summary", "") if isinstance(article, dict) else getattr(article, "summary", "")
            source = article.get("source", "") if isinstance(article, dict) else getattr(article, "source", "")
            link = article.get("link", "") if isinstance(article, dict) else getattr(article, "link", "")
            categories = article.get("categories", []) if isinstance(article, dict) else getattr(article, "categories", [])
            countries = article.get("countries", []) if isinstance(article, dict) else getattr(article, "countries", [])
            publish_date = article.get("publishDate", "") if isinstance(article, dict) else getattr(article, "publishDate", "")
            
            # 构建 content：整合摘要、来源、链接和元数据
            content_parts = []
            if summary:
                content_parts.append(summary)
            
            metadata_parts = []
            if source:
                metadata_parts.append(f"来源: {source}")
            if categories:
                metadata_parts.append(f"分类: {', '.join(categories) if isinstance(categories, list) else str(categories)}")
            if countries:
                metadata_parts.append(f"涉及国家: {', '.join(countries) if isinstance(countries, list) else str(countries)}")
            if link:
                metadata_parts.append(f"链接: {link}")
            
            if metadata_parts:
                content_parts.append("\n" + " | ".join(metadata_parts))
            
            content = "\n".join(content_parts) if content_parts else title
            
            sections.append({
                "title": title,
                "content": content,
                "source": source,
                "link": link,
                "categories": categories if isinstance(categories, list) else [],
                "countries": countries if isinstance(countries, list) else [],
                "publish_date": publish_date,
            })
        
        logger.info(f"成功转换 {len(sections)} 条新闻为 sections 格式")
        return sections
    
    def save_articles_to_json(
        self,
        articles_data: Dict[str, Any],
        date_str: str,
        file_dir: str = "data/news_finlight",
    ) -> str:
        """保存新闻数据到 JSON 文件
        
        Args:
            articles_data: 新闻数据
            date_str: 日期字符串，格式 YYYYMMDD
            file_dir: 存储目录
            
        Returns:
            str: 保存的文件路径
        """
        try:
            os.makedirs(file_dir, exist_ok=True)
            
            # 统一日期格式
            if len(date_str) == 8 and date_str.isdigit():
                formatted_date = date_str
            else:
                formatted_date = date_str.replace("-", "")
            
            file_path = os.path.join(file_dir, f"finlight_{formatted_date}.json")
            
            # 添加元数据
            data_to_save = {
                "source": "finlight",
                "fetch_time": datetime.now().isoformat(),
                "date": formatted_date,
                **articles_data,
            }
            
            with open(file_path, "w", encoding="utf-8") as f:
                json.dump(data_to_save, f, ensure_ascii=False, indent=2)
            
            logger.info(f"Finlight 新闻已保存到 {file_path}")
            return file_path
            
        except Exception as e:
            logger.error(f"保存 Finlight 新闻失败: {str(e)}")
            raise DataFlowException(f"保存 Finlight 新闻失败: {str(e)}")
    
    def load_articles_from_json(
        self,
        date_str: str,
        file_dir: str = "data/news_finlight",
    ) -> Optional[Dict[str, Any]]:
        """从本地 JSON 文件读取新闻数据
        
        Args:
            date_str: 日期字符串，格式 YYYYMMDD
            file_dir: 文件目录
            
        Returns:
            Dict: 新闻数据，文件不存在则返回 None
        """
        try:
            # 统一日期格式
            if len(date_str) == 8 and date_str.isdigit():
                formatted_date = date_str
            else:
                formatted_date = date_str.replace("-", "")
            
            file_path = os.path.join(file_dir, f"finlight_{formatted_date}.json")
            
            if not os.path.exists(file_path):
                logger.info(f"本地文件不存在: {file_path}")
                return None
            
            with open(file_path, "r", encoding="utf-8") as f:
                data = json.load(f)
            
            logger.info(f"成功读取本地文件: {file_path}")
            return data
            
        except json.JSONDecodeError as e:
            logger.error(f"JSON 解析失败: {str(e)}")
            return None
        except Exception as e:
            logger.error(f"读取文件失败: {str(e)}")
            return None


# 便捷函数，供外部直接调用
def fetch_finlight_china_news(
    categories: Optional[List[str]] = None,
    page_size: int = 50,
    api_key: Optional[str] = None,
) -> Dict[str, Any]:
    """获取中国相关新闻（便捷函数）
    
    Args:
        categories: 分类筛选
        page_size: 每页数量
        api_key: API Key（可选）
        
    Returns:
        Dict: 新闻数据
    """
    fetcher = FinlightDataFetcher(api_key=api_key)
    return fetcher.fetch_china_news(categories=categories, page_size=page_size)


def fetch_finlight_articles(
    query: Optional[str] = None,
    countries: Optional[List[str]] = None,
    categories: Optional[List[str]] = None,
    api_key: Optional[str] = None,
) -> Dict[str, Any]:
    """获取 Finlight 新闻（便捷函数）
    
    Args:
        query: 搜索查询
        countries: 国家代码列表
        categories: 分类列表
        api_key: API Key（可选）
        
    Returns:
        Dict: 新闻数据
    """
    fetcher = FinlightDataFetcher(api_key=api_key)
    return fetcher.fetch_articles(
        query=query,
        countries=countries,
        categories=categories,
    )
