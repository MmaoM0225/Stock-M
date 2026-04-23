from langchain_core.prompts import ChatPromptTemplate

from langchain_core.runnables import RunnableConfig

from ....utils import extract_json_text, is_trading_day
from langgraph.constants import Send
from langgraph.graph import END
import os
import json
import logging
from typing import Dict, List, Any, Optional
from datetime import datetime, timedelta
from pathlib import Path
from pydantic import BaseModel, Field, ConfigDict
from enum import Enum

logger = logging.getLogger(__name__)

NEWS_ARTIFACT_ROOT = Path("data") / "artifacts" / "analyst" / "macro_analyst" / "news_analyst"


def _normalize_text(value: str) -> str:
    """统一文本用于去重比较。"""
    return "".join(str(value or "").strip().lower().split())


def _deduplicate_sections(sections: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """按 link 优先、title+content 兜底去重。"""
    unique_sections: List[Dict[str, Any]] = []
    seen_keys: set[str] = set()
    duplicate_count = 0

    for section in sections:
        link = _normalize_text(section.get("link", ""))
        title = _normalize_text(section.get("title", ""))
        content = _normalize_text(section.get("content", ""))

        # 修正：东方财富一篇早餐文章会拆成多个 section，link 相同但内容不同。
        # 若仅按 link 去重会把大量有效 section 错误合并为 1 条。
        # 这里改为优先使用 link + title + content 的复合键，仅在文本缺失时回退 link。
        if link and (title or content):
            key = f"link_text:{link}|{title}|{content}"
        elif link:
            key = f"link:{link}"
        else:
            key = f"text:{title}|{content}"
        if key in seen_keys:
            duplicate_count += 1
            continue

        seen_keys.add(key)
        unique_sections.append(section)

    if duplicate_count:
        logger.info("新闻去重完成：移除重复 %d 条，保留 %d 条", duplicate_count, len(unique_sections))
    return unique_sections


def _filter_sections_by_publish_range(
    sections: List[Dict[str, Any]],
    from_time: datetime,
    to_time: datetime,
) -> List[Dict[str, Any]]:
    """
    按 publish_date 过滤新闻，无法解析时间的条目保留（避免过度丢弃）。
    """
    filtered: List[Dict[str, Any]] = []
    dropped = 0

    for section in sections:
        publish_date = str(section.get("publish_date", "") or "").strip()
        if not publish_date:
            filtered.append(section)
            continue

        parsed_dt: Optional[datetime] = None
        for fmt in ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M"):
            try:
                parsed_dt = datetime.strptime(publish_date, fmt)
                break
            except ValueError:
                continue

        if parsed_dt is None:
            try:
                parsed_dt = datetime.fromisoformat(publish_date.replace("Z", "+00:00")).replace(tzinfo=None)
            except Exception:
                filtered.append(section)
                continue

        if from_time <= parsed_dt <= to_time:
            filtered.append(section)
        else:
            dropped += 1

    if dropped:
        logger.info("时间窗过滤完成：移除越界新闻 %d 条，保留 %d 条", dropped, len(filtered))
    return filtered


def _filter_sections_by_publish_before(
    sections: List[Dict[str, Any]],
    to_time: datetime,
) -> List[Dict[str, Any]]:
    """
    仅保留发布时间 <= to_time 的新闻（用于历史日期向前回退）。
    无法解析时间的条目保留，避免误伤。
    """
    filtered: List[Dict[str, Any]] = []
    dropped = 0

    for section in sections:
        publish_date = str(section.get("publish_date", "") or "").strip()
        if not publish_date:
            filtered.append(section)
            continue

        parsed_dt: Optional[datetime] = None
        for fmt in ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M"):
            try:
                parsed_dt = datetime.strptime(publish_date, fmt)
                break
            except ValueError:
                continue

        if parsed_dt is None:
            try:
                parsed_dt = datetime.fromisoformat(publish_date.replace("Z", "+00:00")).replace(tzinfo=None)
            except Exception:
                filtered.append(section)
                continue

        if parsed_dt <= to_time:
            filtered.append(section)
        else:
            dropped += 1

    if dropped:
        logger.info("历史回退时间过滤：移除未来新闻 %d 条，保留 %d 条", dropped, len(filtered))
    return filtered


class EventType(str, Enum):
    policy = "policy"
    geopolitics = "geopolitics"
    macro = "macro"
    company = "company"
    industry = "industry"
    market = "market"
    other = "other"


class Sentiment(str, Enum):
    positive = "positive"
    negative = "negative"
    neutral = "neutral"


class NewsEvent(BaseModel):
    source: str = Field(default="未知", description="新闻来源，如：东方财富、新浪财经、财联社等")
    type: EventType = Field(alias="event_type", description="事件类型")
    summary: str = Field(description="事件摘要（简明扼要，不超过50字）")
    industry: List[str] = Field(default_factory=list, description="行业标签列表，如：['银行业', '金融业']")
    sentiment: Sentiment = Field(description="情绪倾向")
    impact_level: int = Field(description="影响等级(1-5)", ge=1, le=5)

    model_config = ConfigDict(populate_by_name=True, use_enum_values=True)


class SectorSentiment(str, Enum):
    bullish = "bullish"
    bearish = "bearish"
    neutral = "neutral"


class Liquidity(str, Enum):
    tight = "tight"
    neutral = "neutral"
    loose = "loose"


class PolicyBias(str, Enum):
    supportive = "supportive"
    neutral = "neutral"
    restrictive = "restrictive"


class GlobalRisk(str, Enum):
    low = "low"
    medium = "medium"
    high = "high"


class SectorImpact(BaseModel):
    sentiment: SectorSentiment = Field(description="板块情绪")
    confidence: float = Field(description="置信度 (0-1)", ge=0, le=1)
    reason: List[str] = Field(description="影响原因列表")

    model_config = ConfigDict(use_enum_values=True)


class MacroEnvironment(BaseModel):
    liquidity: Liquidity = Field(description="流动性")
    policy_bias: PolicyBias = Field(description="政策倾向")
    global_risk: GlobalRisk = Field(description="全球风险")
    market_sentiment: SectorSentiment = Field(description="市场情绪")

    model_config = ConfigDict(use_enum_values=True)


class SectorImpacts(BaseModel):
    sector_impacts: Dict[str, SectorImpact] = Field(description="各板块影响分析")
    macro_environment: MacroEnvironment = Field(description="宏观环境评估")

    model_config = ConfigDict(use_enum_values=True)


def _get_news_sections_by_date(
    date_str: str, finlight_fetcher: Any, use_cache: bool = True
) -> tuple:
    """
    按日期获取 Finlight 新闻 sections。

    获取指定日期的前一天00:00到当天12:00的新闻数据（跨天时间范围）。
    如果历史日期没有新闻，则回退到“该日期之前最近新闻”（不会使用全局最新新闻）。

    Args:
        date_str: 日期字符串，格式 YYYYMMDD（指定日期）
        finlight_fetcher: FinlightDataFetcher 实例
        use_cache: 是否使用本地缓存

    Returns:
        (sections, source): sections 列表，source 为 "finlight_local"（本地缓存）或 "finlight_api"（API 请求）
    """
    try:
        # 解析传入的日期，计算前一天
        current_date = datetime.strptime(date_str, "%Y%m%d")
        previous_date = current_date - timedelta(days=1)

        # 构建时间范围：前一天00:00 到 当天12:00
        from_time = previous_date.strftime("%Y-%m-%dT00:00")
        to_time = current_date.strftime("%Y-%m-%dT12:00")

        logger.info(f"获取 {date_str} 新闻：{from_time} 到 {to_time}")

        # 使用 Finlight API 获取指定时间范围的新闻
        result = finlight_fetcher.fetch_news_by_time_range(
            from_=from_time,
            to=to_time,
            countries=["CN"],
            page_size=50,
            use_cache=use_cache,
        )

        # 转换为 sections 格式
        sections = finlight_fetcher.convert_to_sections_format(result)
        sections = _filter_sections_by_publish_range(sections, previous_date, current_date.replace(hour=12, minute=0, second=0))
        sections = _deduplicate_sections(sections)

        if sections:
            # 判断数据来源：检查是否有缓存文件
            # 缓存文件使用当天日期命名（如 news_20260310.json）
            cache_dir = "data/news_finlight"
            cache_file = f"{cache_dir}/news_{date_str}.json"

            import os
            source = "finlight_local" if os.path.exists(cache_file) and use_cache else "finlight_api"

            prev_date_str = previous_date.strftime("%Y%m%d")
            logger.info(f"获取 {date_str} 新闻成功（{prev_date_str}00:00到{date_str}12:00），共 {len(sections)} 条 section（来源: {source}）")
            return (sections, source)
        else:
            # 历史日期没有新闻时，向前回退到“该日期之前最近新闻”
            fallback_to = current_date.replace(hour=12, minute=0, second=0)
            fallback_to_str = fallback_to.strftime("%Y-%m-%dT%H:%M")
            logger.warning("历史日期 %s 无新闻，回退获取截止 %s 的最近新闻", date_str, fallback_to_str)

            fallback_result = finlight_fetcher.fetch_articles(
                query="country:CN",
                countries=["CN"],
                to=fallback_to_str,
                page_size=50,
                use_cache=use_cache,
            )
            fallback_sections = finlight_fetcher.convert_to_sections_format(fallback_result)
            fallback_sections = _filter_sections_by_publish_before(fallback_sections, fallback_to)
            fallback_sections = _deduplicate_sections(fallback_sections)

            if fallback_sections:
                logger.info("历史日期 %s 回退成功，获取到 %d 条截止当日的最近新闻", date_str, len(fallback_sections))
                return (fallback_sections, "finlight_api")

            logger.warning("历史日期 %s 回退后仍无可用新闻，返回空结果", date_str)
            return ([], "finlight_api")

    except Exception as e:
        logger.warning(f"获取历史日期新闻失败: {e}，返回空结果")
        import traceback
        logger.debug(traceback.format_exc())
        # 出错时不回退最新新闻，避免历史日期污染
        return ([], "finlight_api")


def _get_news_sections_latest(
    finlight_fetcher: Any, 
    use_cache: bool = True,
    page_size: int = 50
) -> tuple:
    """
    获取最新新闻 sections（不指定日期，获取最近的新闻）
    
    Args:
        finlight_fetcher: FinlightDataFetcher 实例
        use_cache: 是否使用本地缓存
        page_size: 获取新闻数量
        
    Returns:
        (sections, source): sections 列表
    """
    try:
        # 使用 Finlight API 获取最新中国新闻
        result = finlight_fetcher.fetch_articles(
            query="country:CN",
            page_size=page_size,
            use_cache=use_cache,
        )
        
        # 转换为 sections 格式
        sections = finlight_fetcher.convert_to_sections_format(result)
        sections = _deduplicate_sections(sections)
        
        if sections:
            logger.info(f"获取最新新闻成功，共 {len(sections)} 条 section")
            return (sections, "finlight_api")
        else:
            logger.warning("获取最新新闻为空")
            return ([], "finlight_api")
            
    except Exception as e:
        logger.warning(f"获取最新新闻失败: {e}")
        import traceback
        logger.debug(traceback.format_exc())
        return ([], "finlight_api")


def _convert_eastmoney_news_to_sections(news_data: Dict[str, Any]) -> List[Dict[str, Any]]:
    """将东方财富新闻 JSON 转换为内部 sections 格式。"""
    sections = news_data.get("sections", []) if isinstance(news_data, dict) else []
    converted: List[Dict[str, Any]] = []
    for sec in sections:
        title = str(sec.get("title", "") or "").strip()
        content = str(sec.get("content", "") or "").strip()
        if not title and not content:
            continue
        converted.append(
            {
                "title": title or "东方财富快讯",
                "content": content or title,
                "source": "东方财富",
                "link": str(news_data.get("url", "") or "").strip(),
                "publish_date": str(news_data.get("fetch_time", "") or "").strip(),
            }
        )
    return converted


def _get_eastmoney_sections_by_date(date_str: str) -> tuple:
    """按日期读取东方财富新闻并转成 sections。

    优先读取本地缓存 `data/news/news_{date}.json`。
    若本地不存在，则自动触发抓取并落盘，再回读并转换。
    """
    try:
        from dataflow.news_sentiment import NewsSentimentFetcher

        fetcher = NewsSentimentFetcher()
        fetched_now = False
        news_data = fetcher.get_news_by_date(date_str, file_dir="data/news")

        # 本地无缓存时，自动抓取并落盘，避免手工准备 data/news/news_{date}.json
        if not news_data:
            logger.info("东方财富本地缓存缺失，尝试自动抓取并落盘: %s", date_str)
            try:
                fetcher.fetch_eastmoney_news_by_date(date_str, auto_save=True)
                fetched_now = True
            except Exception as fetch_err:
                logger.warning("自动抓取东方财富新闻失败: %s", fetch_err)

            # 二次回读，确认是否已落盘成功
            news_data = fetcher.get_news_by_date(date_str, file_dir="data/news")

        if not news_data:
            return ([], "eastmoney_none")
        sections = _convert_eastmoney_news_to_sections(news_data)
        if not sections:
            return ([], "eastmoney_none")
        logger.info("东方财富新闻读取成功：%s，共 %d 条 section", date_str, len(sections))
        source = "eastmoney_fetched" if fetched_now else "eastmoney_local"
        return (sections, source)
    except Exception as e:
        logger.warning("读取东方财富新闻失败: %s", e)
        return ([], "eastmoney_error")


def _get_industry_and_concept_lists() -> tuple:
    """
    获取同花顺行业(I)列表与同花顺概念(N)列表，供 LLM 在事件 industry 与 sector_impacts 中选用。
    优先从数据库读取，否则从 dataflow 拉取。
    """
    industry_list: List[str] = []
    concept_list: List[str] = []
    try:
        from database import ThsIndex
        from database.config import get_db_session

        with get_db_session() as session:
            industry_list = [
                r[0] for r in session.query(ThsIndex.name).filter(
                    ThsIndex.index_type == "I"
                ).all() if r[0]
            ]
            concept_list = [
                r[0] for r in session.query(ThsIndex.name).filter(
                    ThsIndex.index_type == "N"
                ).all() if r[0]
            ]
    except Exception as e:
        logger.debug("从数据库读取同花顺行业(I)/概念(N)列表失败，改用 dataflow: %s", e)

    if not industry_list or not concept_list:
        try:
            from dataflow.industry_data import fetch_ths_index

            if not industry_list:
                df = fetch_ths_index(index_type="I")
                if not df.empty and "name" in df.columns:
                    industry_list = df["name"].dropna().unique().tolist()
            if not concept_list:
                df = fetch_ths_index(index_type="N")
                if not df.empty and "name" in df.columns:
                    concept_list = df["name"].dropna().unique().tolist()
        except Exception as e:
            logger.warning("从 dataflow 获取同花顺行业(I)/概念(N)列表失败: %s", e)

    industry_list = sorted(set(str(x).strip() for x in industry_list if x))
    concept_list = sorted(set(str(x).strip() for x in concept_list if x))
    return industry_list, concept_list


def create_news_fetch_node(finlight_fetcher: Optional[Any] = None):
    """
    构建新闻获取节点（融合东方财富 + Finlight）。

    流程：
    1. 判断 trade_date 是否为交易日，否则返回空 sections 结束图
    2. 从东方财富本地文件读取新闻（如存在）
    3. 从 Finlight API 获取新闻（优先本地缓存）
    4. 合并两路新闻并去重
    5. 获取完整行业列表与同花顺概念列表（DB 优先，否则 dataflow）
    6. 输出 sections、all_industries、ths_concept_list 供 extract / reduce 节点使用

    Args:
        finlight_fetcher: FinlightDataFetcher 实例（可选，如未提供则自动创建）
    """
    # 自动创建 fetcher（如果未提供）
    if finlight_fetcher is None:
        try:
            from dataflow.finlight_data import FinlightDataFetcher
            finlight_fetcher = FinlightDataFetcher()
            logger.info("自动创建 FinlightDataFetcher 实例")
        except Exception as e:
            logger.warning(f"自动创建 FinlightDataFetcher 失败: {e}")

    def news_fetch_node(state):
        current_date = state.get("trade_date", datetime.now().strftime("%Y%m%d"))

        # 1. 非交易日则跳过
        if not is_trading_day(current_date):
            logger.info(f"{current_date} 非交易日，跳过新闻分析")
            return {
                "messages": [{"type": "skip", "reason": "非交易日", "trade_date": current_date}],
                "trade_date": current_date,
                "news_sections": [],
                "news_source": "trading_day_skip",
            }

        # 2. 检查 fetcher
        if finlight_fetcher is None:
            logger.warning("未提供 finlight_fetcher，无法获取新闻")
            return {
                "messages": [{"type": "skip", "reason": "未配置 fetcher", "trade_date": current_date}],
                "trade_date": current_date,
                "news_sections": [],
                "news_source": "fetcher_missing",
            }

        # 3. 获取东方财富本地新闻（可缺省，不阻塞主流程）
        eastmoney_sections, eastmoney_source = _get_eastmoney_sections_by_date(current_date)

        # 4. 从 Finlight API 获取新闻（优先本地缓存）
        try:
            # 判断是否为今日，如果是则获取最新新闻（不限定日期）
            today = datetime.now().strftime("%Y%m%d")
            if current_date == today:
                # 获取最新新闻（不限定具体日期）
                logger.info(f"获取今日最新新闻: {current_date}")
                finlight_sections, finlight_source = _get_news_sections_latest(finlight_fetcher, use_cache=True)
            else:
                # 获取指定日期的新闻
                finlight_sections, finlight_source = _get_news_sections_by_date(current_date, finlight_fetcher, use_cache=True)
        except Exception as e:
            logger.warning(f"获取新闻失败: {e}")
            import traceback
            logger.debug(traceback.format_exc())
            finlight_sections, finlight_source = [], "finlight_api"

        # 4.1 Finlight 有数据时，立即同步到 finlight_news 表（按日期 upsert）
        if finlight_sections:
            try:
                from database.data_sync.news_finlight import sync_finlight_news

                sync_finlight_news()
            except Exception as sync_err:
                logger.warning("Finlight 新闻数据库同步失败: %s", sync_err)

        # 5. 合并两路数据并去重
        sections = _deduplicate_sections([*eastmoney_sections, *finlight_sections])
        source_tokens = [eastmoney_source, finlight_source]
        news_source = "+".join([x for x in source_tokens if x and not x.endswith("_none")]) or "unknown"
        logger.info(
            "新闻聚合完成：东方财富 %d 条，Finlight %d 条，合并后 %d 条，来源=%s",
            len(eastmoney_sections),
            len(finlight_sections),
            len(sections),
            news_source,
        )

        if not sections:
            return {
                "messages": [{"type": "skip", "reason": "获取新闻失败", "trade_date": current_date}],
                "trade_date": current_date,
                "news_sections": [],
                "news_source": news_source,
            }

        # 6. 获取行业列表与同花顺概念列表，供 extract / reduce 节点（LLM 从中选择）
        all_industries, ths_concept_list = _get_industry_and_concept_lists()
        logger.info(
            "新闻分析：已注入行业列表 %d 条、同花顺概念列表 %d 条供 LLM 选用",
            len(all_industries),
            len(ths_concept_list),
        )
        return {
            "messages": [],
            "trade_date": current_date,
            "news_sections": sections,
            "news_source": news_source,
            "all_industries": all_industries,
            "ths_concept_list": ths_concept_list,
        }

    return news_fetch_node


def map_sections_to_extract(state):
    sections = state.get("news_sections", [])
    if not sections:
        return END
    all_industries = state.get("all_industries", [])
    return [Send("news_extract", {"section": section, "all_industries": all_industries}) for section in sections]


def reduce_events(state, update):
    events = state.get("events", [])
    new_events = update.get("events", [])
    if new_events:
        events.extend(new_events)
        return {"events": events}


def create_news_extract_node(llm):
    """构建新闻抽取节点。all_industries 由 fetch 节点预先写入，LLM 从中选择相关行业。"""
    def news_extract_node(state, config: Optional[RunnableConfig] = None):
        section = state.get("section", {})
        
        title = section.get("title", "")
        content = section.get("content", "")
        
        if not content:
            return {
                "events": []
            }
        
        # 使用 fetch 节点预先获取的行业列表与同花顺概念列表（LLM 从中选择）
        all_industries: List[str] = state.get("all_industries", [])
        ths_concept_list: List[str] = state.get("ths_concept_list", [])
        industry_info = ""
        if all_industries or ths_concept_list:
            parts = []
            if all_industries:
                parts.append(
                    "【行业列表】industry 中的行业名必须从以下列表中选取：\n"
                    f"{', '.join(all_industries)}"
                )
            if ths_concept_list:
                parts.append(
                    "\n\n【概念列表】若新闻涉及概念板块，可从以下列表中选取并一并填入 industry：\n"
                    f"{', '.join(ths_concept_list)}"
                )
            parts.append("\n\nindustry 字段必须是上述两个列表的子集，不要自造名称。")
            industry_info = "\n".join(parts)
        
        system_message = (
            "您是一位专业的金融新闻分析师。您的任务是分析新闻内容，提取关键信息，"
            "包括新闻来源、事件类型、影响行业、情绪倾向、影响等级等。\n\n"
            "【语言要求】\n"
            "新闻内容可能是英文，但您的分析结果必须用中文输出。\n"
            "- summary（事件摘要）必须是中文\n"
            "- source（新闻来源）可以保留原文或使用中文名\n"
            "- 其他字段保持英文枚举值\n\n"
            "请按照以下要求进行分析，并返回严格的 JSON 格式结构化结果（不要输出多余文字）：\n\n"
            "【必需字段】\n"
            "1. source: 新闻来源（如：东方财富、新浪财经、财联社等，如果无法确定则填\"未知\"）\n"
            "2. event_type: 事件类型，必须是以下之一：\n"
            "   - policy: 政策法规\n"
            "   - geopolitics: 地缘政治\n"
            "   - macro: 宏观经济\n"
            "   - company: 公司公告\n"
            "   - industry: 行业动态\n"
            "   - market: 市场行情\n"
            "   - other: 其他\n"
            "3. summary: 事件摘要（简明扼要，不超过30字，必须用中文）\n"
            "4. industry: 行业标签列表（如：['银行业', '金融业']，至少包含一个行业标签，必须用中文）\n"
            "5. sentiment: 情绪倾向，必须是以下之一：\n"
            "   - positive: 正面利好\n"
            "   - negative: 负面利空\n"
            "   - neutral: 中性\n"
            "6. impact_level: 影响等级（1-5分，1为最低，5为最高）\n\n"
            "【重要提示】\n"
            "- 必须返回所有6个字段，不能遗漏\n"
            "- event_type 必须使用指定的英文枚举值\n"
            "- sentiment 必须使用指定的英文枚举值\n"
            "- impact_level 必须是 1-5 的整数\n"
            "- industry 必须是列表格式，且使用中文\n"
            "- summary 必须是中文\n"
            "- 只输出一段 JSON，不能有任何解释或多余文本"
            + industry_info
        )
        
        prompt = ChatPromptTemplate.from_messages(
            [
                (
                    "system",
                    system_message
                ),
                (
                    "human",
                    "新闻标题：{title}\n\n"
                    "新闻内容：{content}\n\n"
                    "请分析这条新闻并返回结构化结果。"
                ),
            ]
        )
        
        try:
            logger.info("正在处理 新闻事件抽取：抽取单条新闻事件")
            chain = prompt | llm
            raw = chain.invoke(
                {"title": title, "content": content},
                config={**(config or {}), "run_name": "新闻事件抽取"},
            )

            data = extract_json_text(raw)
            event = NewsEvent.model_validate(data)

            return {
                "events": [event.model_dump()]
            }
        except Exception as e:
            print(f"提取事件信息时出错: {str(e)}")
            return {
                "events": [
                    NewsEvent(
                        source="未知",
                        type=EventType.other,
                        summary=content[:50] if len(content) > 50 else content,
                        industry=[],
                        sentiment=Sentiment.neutral,
                        impact_level=2,
                    ).model_dump()
                ]
            }
    
    return news_extract_node


def create_news_reduce_node(llm):
    def news_reduce_node(state, config: Optional[RunnableConfig] = None):
        current_date = state.get("trade_date", datetime.now().strftime("%Y%m%d"))
        events = state.get("events", [])
        
        if not events:
            return {
                "messages": [],
                "news_analysis": None
            }
        
        system_message = (
            "您是一位资深的市场分析师。基于以下事件列表，请分析各板块的影响和宏观环境。\n\n"
            "【语言要求】\n"
            "输入的事件列表可能是英文，但您的分析结果必须用中文输出。\n"
            "- sector_impacts 中的 key 必须从下方【行业列表】或【概念列表】中选取（使用中文）\n"
            "- reason（影响原因）必须用中文描述\n"
            "- 其他字段保持英文枚举值\n\n"
            "请返回严格的 JSON 结构化结果，包含以下两部分：\n\n"
            "【板块影响分析 sector_impacts】\n"
            "字典结构，key 必须从下方提供的【行业列表】或【概念列表】中选取（行业板块与概念板块均可，必须使用中文），value 为对象，包含：\n"
            "- sentiment: 板块情绪 (bullish/bearish/neutral)\n"
            "  - bullish: 看涨，预期上涨\n"
            "  - bearish: 看跌，预期下跌\n"
            "  - neutral: 中性，无明显趋势\n"
            "- confidence: 置信度 (0-1)\n"
            "- reason: 影响原因列表（必须用中文描述）\n\n"
            "【宏观环境评估 macro_environment】\n"
            "对象结构，包含以下四个字段：\n"
            "- liquidity: 流动性 (tight/neutral/loose)\n"
            "  - tight: 流动性紧缩\n"
            "  - neutral: 流动性中性\n"
            "  - loose: 流动性宽松\n"
            "- policy_bias: 政策倾向 (supportive/neutral/restrictive)\n"
            "  - supportive: 政策支持\n"
            "  - neutral: 政策中性\n"
            "  - restrictive: 政策收紧\n"
            "- global_risk: 全球风险 (low/medium/high)\n"
            "- market_sentiment: 市场情绪 (bullish/bearish/neutral)\n\n"
            "【重要要求】\n"
            "- sector_impacts 中的 key 必须从下方「行业列表」或「概念列表」中选取，不能自造名称，且必须使用中文；\n"
            "- reason 字段必须用中文描述；\n"
            "- 至少输出 3 个有代表性的板块影响（可混合行业与概念）；\n"
            "- 只输出一段 JSON，不能有任何解释文字。"
        )
        
        prompt = ChatPromptTemplate.from_messages(
            [
                (
                    "system",
                    system_message
                ),
                (
                    "human",
                    "事件列表：\n{events}\n\n"
                    "【行业列表】（sector_impacts 的 key 可从中选取）：\n{industry_list}\n\n"
                    "【概念列表】（sector_impacts 的 key 可从中选取）：\n{ths_concept_list}\n\n"
                    "请基于以上事件和两个列表，分析板块影响和宏观环境，并返回 JSON 结果。"
                ),
            ]
        )
        
        sector_impacts = {}
        macro_environment = MacroEnvironment(
            liquidity=Liquidity.neutral,
            policy_bias=PolicyBias.neutral,
            global_risk=GlobalRisk.medium,
            market_sentiment=SectorSentiment.neutral,
        ).model_dump()

        try:
            events_text = "\n".join(
                [
                    f"- {event['summary']} (类型: {event.get('event_type', event.get('type', 'unknown'))}, 情绪: {event['sentiment']}, 影响: {event['impact_level']}, 行业: {', '.join(event.get('industry', []))})"
                    for event in events
                ]
            )

            # 使用 fetch 节点注入的完整行业列表与同花顺概念列表，供 LLM 选取 sector_impacts 的 key
            industry_list = state.get("all_industries", [])
            ths_concept_list = state.get("ths_concept_list", [])
            industry_list_str = ", ".join(industry_list) if industry_list else "（无）"
            ths_concept_list_str = ", ".join(ths_concept_list) if ths_concept_list else "（无）"

            logger.info("正在处理 新闻汇总：事件列表 → 板块影响与宏观环境")
            chain = prompt | llm
            raw = chain.invoke(
                {
                    "events": events_text,
                    "industry_list": industry_list_str,
                    "ths_concept_list": ths_concept_list_str,
                },
                config={**(config or {}), "run_name": "新闻汇总"},
            )

            data = extract_json_text(raw)
            result = SectorImpacts.model_validate(data)

            sector_impacts = {
                sector: {
                    "sentiment": impact.sentiment,
                    "confidence": impact.confidence,
                    "reason": impact.reason,
                }
                for sector, impact in result.sector_impacts.items()
            }

            macro_environment = result.macro_environment.model_dump()

        except Exception as e:
            logger.warning("新闻汇总(reduce)时出错: %s", e)

        analysis_result = {
            "date": current_date,
            "events": events,
            "sector_impacts": sector_impacts,
            "macro_environment": macro_environment,
        }

        return {
            "messages": [],
            "news_analysis": analysis_result,
        }
    
    return news_reduce_node


def _write_json_atomic(path: Path, payload: Dict[str, Any]) -> None:
    """原子写入 JSON，避免中途中断留下半成品。"""
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_suffix(path.suffix + ".tmp")
    with open(tmp_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
    os.replace(tmp_path, path)


def create_news_result_persist_node():
    """将最终输出键 news_analysis 持久化到本地 artifacts，并同步数据库。"""

    def news_result_persist_node(
        state: Dict[str, Any],
        config: Optional[RunnableConfig] = None,
    ) -> Dict[str, Any]:
        _ = config
        analysis_result = state.get("news_analysis")
        if not analysis_result:
            return state

        trade_date = str(
            analysis_result.get("date")
            or state.get("trade_date")
            or datetime.now().strftime("%Y%m%d")
        ).replace("-", "")[:8]
        artifact_dir = NEWS_ARTIFACT_ROOT / trade_date
        result_path = artifact_dir / "result.json"
        manifest_path = artifact_dir / "manifest.json"

        try:
            _write_json_atomic(result_path, analysis_result)
            _write_json_atomic(
                manifest_path,
                {
                    "artifact_type": "news_analysis",
                    "module": "agents.analyst.macro_analyst.news_analyst",
                    "trade_date": trade_date,
                    "created_at": datetime.now().astimezone().isoformat(),
                    "status": "success",
                    "source": state.get("news_source"),
                    "result_path": result_path.as_posix(),
                },
            )
            # 每次运行成功落盘后，立即 upsert 到关键表（仅同步摘要字段）。
            try:
                from database.data_sync.news_analyst import sync_single_result

                sync_single_result(result_path)
            except Exception as sync_err:
                logger.warning("news_analyst 数据库同步失败: %s", sync_err)
            logger.info("news_analysis 已写入本地 artifacts: %s", result_path)
            return {
                **state,
                "news_analysis_artifact_path": result_path.as_posix(),
                "news_analysis_manifest_path": manifest_path.as_posix(),
            }
        except Exception as e:
            logger.warning("写入 news_analysis artifacts 失败: %s", e)
            return state

    return news_result_persist_node

