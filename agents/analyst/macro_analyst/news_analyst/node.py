from langchain_core.prompts import ChatPromptTemplate

from langchain_core.runnables import RunnableConfig

from ....utils import extract_json_text, is_trading_day
from langgraph.constants import Send
from langgraph.graph import END
import os
import json
import logging
from typing import Dict, List, Any, Optional
from datetime import datetime
from pathlib import Path
from pydantic import BaseModel, Field, ConfigDict
from enum import Enum

logger = logging.getLogger(__name__)

NEWS_ARTIFACT_ROOT = Path("data") / "artifacts" / "analyst" / "macro_analyst" / "news_analyst"


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
    date_str: str, fetcher: Any, news_dir: str = "data/news", _sync_attempted: bool = False
) -> tuple:
    """
    按日期获取新闻 sections，优先从数据库 breakfast_news 表。
    1. 查 DB：若有 json_file_path 且文件存在，直接读取
    2. 若有 detail_url：抓取页面，保存到 JSON，更新 DB 的 json_file_path
    3. 若无 DB 记录且未 sync 过：先 sync_breakfast_news，再重试

    Returns:
        (sections, source): sections 列表，source 为 "local"（本地）或 "fetch"（远程抓取）
    """
    import os
    from pathlib import Path

    from database import BreakfastNews
    from database.config import get_db_session

    json_path = Path(news_dir) / f"news_{date_str}.json"
    json_path_str = json_path.as_posix()  # 统一使用正斜杠，如 data/news/news_20260309.json

    with get_db_session() as session:
        row = session.query(BreakfastNews).filter(BreakfastNews.pub_date == date_str).first()
        # 在 session 关闭前提取属性，避免 detached instance 错误
        row_json_path = row.json_file_path if row else None
        row_detail_url = row.detail_url if row else None

    if row:
        # 1. 优先从 json_file_path 读取，若无则尝试默认路径 data/news/news_{date}.json
        for path in [row_json_path, json_path_str]:
            if path and os.path.exists(path):
                try:
                    with open(path, "r", encoding="utf-8") as f:
                        data = json.load(f)
                    sections = data.get("sections", [])
                    if sections:
                        if path == json_path_str and not row_json_path:
                            with get_db_session() as session:
                                r = session.query(BreakfastNews).filter(BreakfastNews.pub_date == date_str).first()
                                if r:
                                    r.json_file_path = json_path_str
                        return (sections, "local")
                except Exception as e:
                    logger.warning(f"读取 JSON 失败 {path}: {e}")

        # 2. 有 detail_url 则抓取
        if row_detail_url and fetcher:
            try:
                use_old_format = int(date_str) <= 20250603
                news_data = fetcher.fetch_eastmoney_news_page(row_detail_url, use_old_format)
                if news_data and news_data.get("sections"):
                    json_path.parent.mkdir(parents=True, exist_ok=True)
                    with open(json_path, "w", encoding="utf-8") as f:
                        json.dump(news_data, f, ensure_ascii=False, indent=2)
                    with get_db_session() as session:
                        r = session.query(BreakfastNews).filter(BreakfastNews.pub_date == date_str).first()
                        if r:
                            r.json_file_path = json_path_str
                    return (news_data.get("sections", []), "fetch")
            except Exception as e:
                logger.warning(f"抓取新闻失败: {e}")
        return ([], "fetch")

    # 3. 无 DB 记录，先同步早餐列表再重试（仅尝试一次，避免死循环）
    if not _sync_attempted:
        try:
            from database.data_sync.breakfast_news import sync_breakfast_news
            sync_breakfast_news()
            return _get_news_sections_by_date(date_str, fetcher, news_dir, _sync_attempted=True)
        except Exception as e:
            logger.warning(f"同步财经早餐失败: {e}")

    return ([], "fetch")


def _get_industry_and_concept_lists() -> tuple:
    """
    获取行业名称列表与同花顺概念名称列表，供 LLM 在事件 industry 与 sector_impacts 中选用。
    优先从数据库读取，否则从 dataflow 拉取。
    """
    industry_list: List[str] = []
    concept_list: List[str] = []
    try:
        from database import Industry, ThsIndex
        from database.config import get_db_session

        with get_db_session() as session:
            industry_list = [
                r[0] for r in session.query(Industry.industry_name).filter(
                    Industry.industry_name.isnot(None)
                ).distinct().all() if r[0]
            ]
            concept_list = [
                r[0] for r in session.query(ThsIndex.name).filter(
                    ThsIndex.index_type == "N"
                ).all() if r[0]
            ]
    except Exception as e:
        logger.debug("从数据库读取行业/概念列表失败，改用 dataflow: %s", e)

    if not industry_list or not concept_list:
        try:
            from dataflow.industry_data import fetch_ths_index, get_all_industry_names

            if not industry_list:
                industry_list = get_all_industry_names()
            if not concept_list:
                df = fetch_ths_index(index_type="N")
                if not df.empty and "name" in df.columns:
                    concept_list = df["name"].dropna().unique().tolist()
        except Exception as e:
            logger.warning("从 dataflow 获取行业/概念列表失败: %s", e)

    industry_list = sorted(set(str(x).strip() for x in industry_list if x))
    concept_list = sorted(set(str(x).strip() for x in concept_list if x))
    return industry_list, concept_list


def create_news_fetch_node(fetcher: Optional[Any] = None):
    """
    构建新闻获取节点。

    流程：
    1. 判断 trade_date 是否为交易日，否则返回空 sections 结束图
    2. 从数据库 breakfast_news 表获取新闻：优先 json_file_path，无则按 detail_url 抓取
    3. 获取完整行业列表与同花顺概念列表（DB 优先，否则 dataflow）
    4. 输出 sections、all_industries、ths_concept_list 供 extract / reduce 节点使用

    Args:
        fetcher: NewsSentimentFetcher，用于远程抓取新闻详情（必须提供）
    """

    def news_fetch_node(state):
        current_date = state.get("trade_date", datetime.now().strftime("%Y%m%d"))

        # 1. 非交易日则跳过
        if not is_trading_day(current_date):
            logger.info(f"{current_date} 非交易日，跳过新闻分析")
            return {
                "messages": [{"type": "skip", "reason": "非交易日", "trade_date": current_date}],
                "trade_date": current_date,
                "news_sections": [],
            }

        # 2. 需要 fetcher 来抓取新闻（当 DB 无 json_file_path 时）
        if fetcher is None:
            logger.warning("未提供 fetcher，无法抓取新闻")
            return {
                "messages": [{"type": "skip", "reason": "未配置 fetcher", "trade_date": current_date}],
                "trade_date": current_date,
                "news_sections": [],
            }

        # 3. 从数据库获取新闻 sections（DB 优先，json_file_path -> detail_url 抓取）
        try:
            sections, news_source = _get_news_sections_by_date(current_date, fetcher)
            if sections:
                logger.info(f"获取 {current_date} 新闻成功，共 {len(sections)} 条 section（来源: {'本地' if news_source == 'local' else '远程抓取'}）")
        except Exception as e:
            logger.warning(f"获取新闻失败: {e}")
            sections, news_source = [], "fetch"

        if not sections:
            return {
                "messages": [{"type": "skip", "reason": "获取新闻失败", "trade_date": current_date}],
                "trade_date": current_date,
                "news_sections": [],
            }

        # 4. 获取行业列表与同花顺概念列表，供 extract / reduce 节点（LLM 从中选择）
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
            "请返回严格的 JSON 结构化结果，包含以下两部分：\n\n"
            "【板块影响分析 sector_impacts】\n"
            "字典结构，key 必须从下方提供的【行业列表】或【概念列表】中选取（行业板块与概念板块均可），value 为对象，包含：\n"
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
            "- sector_impacts 中的 key 必须从下方「行业列表」或「概念列表」中选取，不能自造名称；\n"
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
    """将最终输出键 news_analysis 持久化到本地 artifacts。"""

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

