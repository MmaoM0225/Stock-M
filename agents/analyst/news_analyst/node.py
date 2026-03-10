import os

from langchain_core.prompts import ChatPromptTemplate, MessagesPlaceholder

from langchain_core.runnables import RunnableConfig

from ...utils import extract_json_text, is_trading_day, get_news_config
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


def create_news_fetch_node(fetcher: Optional[Any] = None):
    """
    构建新闻获取节点。

    流程：
    1. 判断 trade_date 是否为交易日，否则返回空 sections 结束图
    2. 从数据库 breakfast_news 表获取新闻：优先 json_file_path，无则按 detail_url 抓取
    3. 从数据库 industry 表获取完整行业列表
    4. 输出 sections、all_industries 供 extract 节点使用

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

        # 4. 从数据库获取完整行业列表，供 extract 节点（LLM 从中选择）
        try:
            from database import Industry
            from database.config import get_db_session
            with get_db_session() as session:
                rows = session.query(Industry.industry_name).filter(
                    Industry.industry_name.isnot(None)
                ).distinct().all()
                all_industries = sorted({r[0] for r in rows if r[0]})
        except Exception as e:
            logger.warning(f"获取行业列表失败: {e}")
            all_industries = []
        return {
            "messages": [],
            "trade_date": current_date,
            "news_sections": sections,
            "news_source": news_source,
            "all_industries": all_industries,
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


def _build_news_analysis_md_programmatic(analysis_result: Dict) -> str:
    """程序化拼接新闻分析 Markdown。"""
    trade_date = analysis_result.get("date", datetime.now().strftime("%Y%m%d"))
    events = analysis_result.get("events", [])
    sector_impacts = analysis_result.get("sector_impacts", {})
    macro = analysis_result.get("macro_environment", {})

    lines = [
        f"# 新闻分析报告 {trade_date}",
        "",
        "## 一、宏观环境",
        "",
    ]
    if macro:
        for k, v in macro.items():
            lines.append(f"- **{k}**: {v}")
        lines.append("")
    else:
        lines.append("（无宏观环境数据）\n")

    lines.extend(["---", "", "## 二、板块影响", ""])
    if sector_impacts:
        for sector, info in sector_impacts.items():
            lines.append(f"### {sector}")
            lines.append(f"- 情绪: {info.get('sentiment', '')}")
            lines.append(f"- 置信度: {info.get('confidence', '')}")
            for r in info.get("reason", []):
                lines.append(f"  - {r}")
            lines.append("")
    else:
        lines.append("（无板块影响数据）\n")

    lines.extend(["---", "", "## 三、事件列表", ""])
    for i, ev in enumerate(events, 1):
        lines.append(f"{i}. **{ev.get('summary', '')}**")
        lines.append(f"   - 类型: {ev.get('event_type', ev.get('type', 'unknown'))}, 情绪: {ev.get('sentiment', '')}, 影响: {ev.get('impact_level', '')}")
        lines.append(f"   - 行业: {', '.join(ev.get('industry', []))}")
        lines.append("")

    lines.extend([
        "---",
        "",
        "## 报告元数据",
        "",
        f"- 报告日期: {trade_date}",
        f"- 生成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
        "",
    ])
    return "\n".join(lines)


def _build_news_analysis_md_with_llm(analysis_result: Dict, llm, trade_date: str) -> Optional[str]:
    """用 LLM 总结生成新闻分析 Markdown，风格为分析式报告（与程序化拼接的列表式区分）。"""
    try:
        system_msg = (
            "你是一位金融新闻分析师。请根据提供的结构化分析数据，撰写一份**分析式、可读性强的日报**，"
            "不要照抄或罗列原始字段，而是写成给人看的解读报告。\n\n"
            "【风格要求】\n"
            "1. 开头用 2～4 句话写「今日要点」或「核心结论」，概括宏观环境与市场情绪。\n"
            "2. 宏观环境：用一两段话解读流动性、政策倾向、全球风险、市场情绪的含义与组合影响，不要只写「liquidity: neutral」这种键值。\n"
            "3. 板块与事件：按重要性或逻辑分块（如「地缘与能源」「产业与政策」「市场与资金」等），每块用段落+要点简述，突出因果与对投资的含义。\n"
            "4. 事件不必逐条罗列，可归纳为几类并挑重点事件说明。\n"
            "5. 全文 Markdown：适当用二级、三级标题和加粗，结尾注明「报告日期」和「生成时间」。\n"
            "6. 标题用「# 新闻分析报告 {date}」，其余结构由你组织，不必与数据字段一一对应。直接输出完整 Markdown，不要输出其他说明。"
        ).format(date=trade_date)
        prompt = ChatPromptTemplate.from_messages([
            ("system", system_msg),
            ("human", "当日新闻分析数据（JSON）：\n{data}\n\n请据此撰写一份分析式 Markdown 报告。"),
        ])
        logger.info("news_markdown: LLM 生成新闻报告")
        chain = prompt | llm
        raw = chain.invoke({"data": json.dumps(analysis_result, ensure_ascii=False, indent=2, default=str)})
        text = raw.content if hasattr(raw, "content") else str(raw)
        if "报告日期" not in text and "生成时间" not in text:
            text += f"\n\n---\n\n## 报告元数据\n\n- 报告日期: {trade_date}\n- 生成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
        return text.strip()
    except Exception as e:
        logger.warning("LLM 生成新闻 Markdown 失败: %s，回退到程序化输出", e)
        return None


def _write_news_analysis_md(
    analysis_result: Dict, trade_date: str, llm=None, use_llm_for_md: bool = False
) -> Optional[str]:
    """写入 data/analysis/YYYYMMDD_news_analysis.md。use_llm_for_md=True 时用 LLM 润色。"""
    try:
        md_content = None
        if use_llm_for_md and llm:
            md_content = _build_news_analysis_md_with_llm(analysis_result, llm, trade_date)
        if md_content is None:
            md_content = _build_news_analysis_md_programmatic(analysis_result)
        out_dir = os.path.join("data", "analysis")
        os.makedirs(out_dir, exist_ok=True)
        path = os.path.join(out_dir, f"{trade_date}_news_analysis.md")
        with open(path, "w", encoding="utf-8") as f:
            f.write(md_content)
        logger.info("新闻分析报告已写入: %s", path)
        return path
    except Exception as e:
        logger.warning("写入新闻分析 MD 失败: %s", e)
        return None


def create_news_markdown_write_node(llm=None):
    """构建新闻 Markdown 报告写入节点。由 configurable.news_config.generate_markdown 控制是否执行（由 graph 条件边决定）。"""
    from ...config import NEWS_USE_LLM_FOR_MARKDOWN

    def news_markdown_write_node(state, config: Optional[RunnableConfig] = None) -> Dict:
        analysis_result = state.get("news_analysis")
        if not analysis_result:
            return {}
        trade_date = analysis_result.get("date", datetime.now().strftime("%Y%m%d"))
        cfg = get_news_config(config) or {}
        use_llm = cfg.get("use_llm_for_markdown", NEWS_USE_LLM_FOR_MARKDOWN)
        _write_news_analysis_md(analysis_result, trade_date, llm=llm, use_llm_for_md=use_llm)
        return {}

    return news_markdown_write_node


def create_news_extract_node(llm):
    """构建新闻抽取节点。all_industries 由 fetch 节点预先写入，LLM 从中选择相关行业。"""
    def news_extract_node(state):
        section = state.get("section", {})
        
        title = section.get("title", "")
        content = section.get("content", "")
        
        if not content:
            return {
                "events": []
            }
        
        # 使用 fetch 节点预先获取的完整行业列表（LLM 做判断，从中选择相关行业）
        all_industries: List[str] = state.get("all_industries", [])
        industry_info = ""
        if all_industries:
            industry_info = (
                "\n\n【可用行业列表】\n"
                "请从以下完整行业列表中选择与本条新闻相关的行业：\n"
                f"{', '.join(all_industries)}\n\n"
                "industry 字段必须是上述列表的子集，不要发明新的行业名称。"
            )
        
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
            logger.info("news_extract: 抽取单条新闻事件")
            chain = prompt | llm
            raw = chain.invoke(
                {
                    "title": title,
                    "content": content,
                }
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

            logger.info("news_reduce: 事件列表 → 板块影响与宏观环境")
            chain = prompt | llm
            raw = chain.invoke({"events": events_text, "industries": ", ".join(industries)})

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

