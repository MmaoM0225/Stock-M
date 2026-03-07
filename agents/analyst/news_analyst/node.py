from langchain_core.prompts import ChatPromptTemplate, MessagesPlaceholder
from langgraph.constants import Send
from langgraph.graph import END
import json
import logging
from typing import Dict, List, Any, Optional
from datetime import datetime
from pydantic import BaseModel, Field, ConfigDict
from enum import Enum

logger = logging.getLogger(__name__)


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


def _extract_json_text(raw) -> Dict[str, Any]:
    """
    从 LLM 原始返回中提取 JSON 文本并解析。
    兼容 BaseMessage / dict / 纯字符串，以及 ```json 包裹的情况。
    """
    if isinstance(raw, dict) and "content" in raw:
        text = raw["content"]
    else:
        text = getattr(raw, "content", str(raw))

    text = text.strip()

    # 处理 ```json ... ``` 或 ``` ... ``` 包裹的输出
    if text.startswith("```"):
        lines = text.splitlines()
        # 去掉首尾 ``` 或 ```json
        if lines and lines[0].startswith("```"):
            lines = lines[1:]
        if lines and lines[-1].startswith("```"):
            lines = lines[:-1]
        text = "\n".join(lines).strip()

    if not text:
        raise ValueError("LLM 输出为空，无法解析为 JSON")

    return json.loads(text)


def _is_trading_day(date_str: str) -> bool:
    """判断是否为交易日（周一至周五，暂不考虑节假日）。"""
    try:
        dt = datetime.strptime(date_str, "%Y%m%d")
        return dt.weekday() < 5  # 0=Mon, 4=Fri
    except ValueError:
        return False


def create_news_fetch_node(fetcher: Optional[Any] = None):
    """
    构建新闻获取节点。

    流程：
    1. 判断 trade_date 是否为交易日，否则返回空 sections 结束图
    2. 若 state 已有 news_data，直接使用
    3. 若有 fetcher：先尝试本地文件 (get_news_by_date)，无则 fetch (fetch_eastmoney_news_by_date)
    4. 提取 sections 供后续分析

    Args:
        fetcher: 可选的 NewsSentimentFetcher，用于本地读取或远程抓取新闻
    """

    def news_fetch_node(state):
        current_date = state.get("trade_date", datetime.now().strftime("%Y%m%d"))
        news_data = state.get("news_data")

        # 1. 非交易日则跳过
        if not _is_trading_day(current_date):
            logger.info(f"{current_date} 非交易日，跳过新闻分析")
            return {
                "messages": [{"type": "skip", "reason": "非交易日", "trade_date": current_date}],
                "trade_date": current_date,
                "news_sections": [],
            }

        # 2. 已有 news_data 则直接使用
        if news_data and news_data.get("sections"):
            sections = news_data.get("sections", [])
            return {
                "messages": [],
                "trade_date": current_date,
                "news_sections": sections,
            }

        # 3. 需要 fetcher 来获取新闻
        if fetcher is None:
            logger.warning("未提供 fetcher 且 state 无 news_data，无法获取新闻")
            return {
                "messages": [{"type": "skip", "reason": "无新闻数据且未配置 fetcher", "trade_date": current_date}],
                "trade_date": current_date,
                "news_sections": [],
            }

        # 3a. 先尝试本地文件
        news_data = fetcher.get_news_by_date(current_date)
        if news_data and news_data.get("sections"):
            logger.info(f"从本地读取 {current_date} 新闻，共 {len(news_data['sections'])} 条")
            return {
                "messages": [],
                "trade_date": current_date,
                "news_sections": news_data.get("sections", []),
            }

        # 3b. 本地无则 fetch
        try:
            news_data = fetcher.fetch_eastmoney_news_by_date(current_date)
            if news_data and news_data.get("sections"):
                logger.info(f"抓取 {current_date} 新闻成功，共 {len(news_data['sections'])} 条")
                return {
                    "messages": [],
                    "trade_date": current_date,
                    "news_sections": news_data.get("sections", []),
                }
        except Exception as e:
            logger.warning(f"抓取 {current_date} 新闻失败: {e}")

        # 抓取失败
        return {
            "messages": [{"type": "skip", "reason": "获取新闻失败", "trade_date": current_date}],
            "trade_date": current_date,
            "news_sections": [],
        }

    return news_fetch_node


def map_sections_to_extract(state):
    sections = state.get("news_sections", [])
    if not sections:
        return END
    return [Send("news_extract", {"section": section}) for section in sections]


def reduce_events(state, update):
    events = state.get("events", [])
    new_events = update.get("events", [])
    if new_events:
        events.extend(new_events)
    return {"events": events}


def create_news_extract_node(llm, toolkit=None):
    def news_extract_node(state):
        section = state.get("section", {})
        
        title = section.get("title", "")
        content = section.get("content", "")
        
        if not content:
            return {
                "events": []
            }
        
        industry_info = ""
        matched_industries: List[str] = []
        if toolkit:
            try:
                info = toolkit.get_industry_info(content)
                if info and info.get("matched_industries"):
                    matched_industries = info["matched_industries"]
                    industry_info = (
                        "\n\n【可用行业列表】\n"
                        "系统已经基于新闻内容识别出如下候选行业，请务必只从这些行业中选择：\n"
                        f"{', '.join(matched_industries)}\n\n"
                        "industry 字段必须是上述列表的子集，不要发明新的行业名称。"
                    )
            except Exception as e:
                print(f"获取行业信息失败: {str(e)}")
        
        system_message = (
            "您是一位专业的金融新闻分析师。您的任务是分析新闻内容，提取关键信息，"
            "包括新闻来源、事件类型、影响行业、情绪倾向、影响等级等。\n\n"
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
            "3. summary: 事件摘要（简明扼要，不超过30字）\n"
            "4. industry: 行业标签列表（如：['银行业', '金融业']，至少包含一个行业标签）\n"
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
            "- industry 必须是列表格式\n"
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
            chain = prompt | llm
            raw = chain.invoke(
                {
                    "title": title,
                    "content": content,
                }
            )

            data = _extract_json_text(raw)
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
    def news_reduce_node(state):
        current_date = state.get("trade_date", datetime.now().strftime("%Y%m%d"))
        events = state.get("events", [])
        
        if not events:
            return {
                "messages": [],
                "news_analysis": None
            }
        
        system_message = (
            "您是一位资深的市场分析师。基于以下事件列表，请分析各板块的影响和宏观环境。\n\n"
            "请返回严格的 JSON 结构化结果，包含以下两部分：\n\n"
            "【板块影响分析 sector_impacts】\n"
            "字典结构，key 必须是给定行业列表中的行业名称，value 为对象，包含：\n"
            "- sentiment: 板块情绪 (bullish/bearish/neutral)\n"
            "  - bullish: 看涨，预期上涨\n"
            "  - bearish: 看跌，预期下跌\n"
            "  - neutral: 中性，无明显趋势\n"
            "- confidence: 置信度 (0-1)\n"
            "- reason: 影响原因列表\n\n"
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
            "- sector_impacts 中的 key 必须从提供的行业列表中选择，不能发明新行业；\n"
            "- 至少输出 3 个有代表性的行业影响；\n"
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
                    "可用行业列表（只能从中选择作为 sector_impacts 的 key）：\n{industries}\n\n"
                    "请基于这些事件和行业列表，分析板块影响和宏观环境，并返回 JSON 结果。"
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

            # 从事件中抽取去重后的行业列表，作为 reduce 阶段的显式输入
            industries = sorted(
                {
                    industry
                    for event in events
                    for industry in event.get("industry", [])
                    if industry
                }
            )

            chain = prompt | llm
            raw = chain.invoke({"events": events_text, "industries": ", ".join(industries)})

            data = _extract_json_text(raw)
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
            print(f"汇总分析时出错: {str(e)}")

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

