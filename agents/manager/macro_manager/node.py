"""
Macro Manager（宏观管理器）- 节点

run_analysts：并行调用多个分析师子图（带最大并发限制），合并结果到 state。
write_macro_report：根据 state 汇总所有分析师结果，生成一篇 Markdown 报告写入 data/analysis。
"""
import json
import logging
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

from langchain_core.prompts import ChatPromptTemplate
from langchain_core.runnables import RunnableConfig

from ...config import MACRO_GENERATE_MARKDOWN, MACRO_USE_LLM_FOR_MARKDOWN
from ...utils import extract_json_text

logger = logging.getLogger(__name__)


def get_macro_config(config: Optional[RunnableConfig] = None) -> Dict[str, Any]:
    """从 configurable 或 agents.config 默认值读取宏观管理器相关配置。"""
    out = {
        "generate_markdown": MACRO_GENERATE_MARKDOWN,
        "use_llm_for_markdown": MACRO_USE_LLM_FOR_MARKDOWN,
    }
    if config and isinstance(config.get("configurable"), dict):
        cfg = config["configurable"]
        if "macro_generate_markdown" in cfg:
            out["generate_markdown"] = cfg["macro_generate_markdown"]
        if "macro_use_llm_for_markdown" in cfg:
            out["use_llm_for_markdown"] = cfg["macro_use_llm_for_markdown"]
    return out

# 报告输出目录（与新闻分析等一致）
MACRO_REPORT_DIR = os.path.join("data", "analysis")
MACRO_REPORT_FILENAME_TEMPLATE = "{trade_date}_macro_report.md"

_MACRO_MANAGER_SUMMARY_DEFAULT: Dict[str, Any] = {
    "market_regime": "unknown",
    "market_direction": "unknown",
    "target_position": "unknown",
    "focus_sectors": [],
    "avoid_sectors": [],
    "macro_themes": [],
    "risk_factors": [],
    "confidence": 0.0,
    "macro_summary": "",
}


def _fmt_list_inline(items: List[Any], max_show: int = 20) -> str:
    """列表转为「A、B、C」或短 bullet，避免整块 JSON。"""
    if not items:
        return "（无）"
    if all(isinstance(x, (str, int, float, bool)) or x is None for x in items):
        tail = " …" if len(items) > max_show else ""
        return "、".join(str(x) for x in items[:max_show]) + tail
    return "\n".join(f"- {_fmt_value_short(x)}" for x in items[:max_show])


def _fmt_value_short(v: Any) -> str:
    """单值或短 dict 一行摘要。"""
    if v is None:
        return "—"
    if isinstance(v, (str, int, float, bool)):
        return str(v)
    if isinstance(v, dict):
        if v.get("error"):
            return f"错误: {v['error']}"
        # 常见短字段优先
        for key in ("summary", "conclusion", "macro_summary", "sentiment_summary", "name", "title"):
            if key in v and v[key]:
                return str(v[key])[:120]
        return json.dumps(v, ensure_ascii=False, default=str)[:100] + ("…" if len(str(v)) > 100 else "")
    if isinstance(v, list):
        return "、".join(str(x) for x in v[:5]) + (" …" if len(v) > 5 else "")
    return str(v)


def _format_manager_summary(d: Dict[str, Any]) -> str:
    """总体结论（Manager）专用：结构化字段排成易读段落，少用 JSON。"""
    if not d or d.get("error"):
        return "（无结论）" if not d else f"**错误**: {d['error']}"
    lines = []
    if d.get("macro_summary"):
        lines.append(d["macro_summary"])
        lines.append("")
    lines.append(f"- **市场状态**: {d.get('market_regime', '—')}  |  **方向**: {d.get('market_direction', '—')}  |  **建议仓位**: {d.get('target_position', '—')}")
    lines.append(f"- **置信度**: {d.get('confidence', 0)}")
    for label, key in [("关注板块", "focus_sectors"), ("规避板块", "avoid_sectors"), ("宏观主题", "macro_themes"), ("风险因素", "risk_factors")]:
        val = d.get(key)
        if isinstance(val, list) and val:
            lines.append(f"- **{label}**: {_fmt_list_inline(val)}")
        elif val:
            lines.append(f"- **{label}**: {val}")
    return "\n".join(lines)


def _format_news_analysis(value: Dict[str, Any]) -> str:
    """新闻分析：宏观环境 + 板块影响 + 事件摘要列表（非整块 JSON）。"""
    if not value or value.get("error"):
        return "（无数据）" if not value else f"**错误**: {value['error']}"
    parts = []
    macro = value.get("macro_environment") or {}
    if isinstance(macro, dict) and macro:
        parts.append("**宏观环境**")
        for k, v in macro.items():
            parts.append(f"- {k}: {v}")
        parts.append("")
    sector = value.get("sector_impacts") or {}
    if isinstance(sector, dict) and sector:
        parts.append("**板块影响**")
        for name, info in list(sector.items())[:15]:
            s = info.get("sentiment", "") if isinstance(info, dict) else info
            parts.append(f"- {name}: {s}")
        parts.append("")
    events = value.get("events") or []
    if events:
        parts.append("**事件摘要**")
        for ev in events[:25]:
            if isinstance(ev, dict):
                summary = ev.get("summary", ev.get("title", ""))[:80]
                etype = ev.get("type", ev.get("event_type", ""))
                parts.append(f"- [{etype}] {summary}")
            else:
                parts.append(f"- {ev}")
    return "\n".join(parts) if parts else "（无内容）"


def _format_market_sentiment(value: Dict[str, Any]) -> str:
    """市场情绪：先综合结论，再各指数分块（名称+趋势+成交量/波动率/结论），不用 dict 字符串。"""
    if value.get("error"):
        return f"**错误**: {value['error']}"
    parts = []
    if value.get("sentiment_summary"):
        parts.append(value["sentiment_summary"])
        parts.append("")
    parts.append(f"- **综合趋势**: {value.get('index_trend', '—')}  |  **市场情绪**: {value.get('market_sentiment', '—')}  |  **量能**: {value.get('volume_signal', '—')}  |  **波动**: {value.get('volatility_signal', '—')}")
    parts.append("")
    per_index = value.get("per_index") or {}
    if isinstance(per_index, dict) and per_index:
        parts.append("**各指数**")
        for code, info in per_index.items():
            if not isinstance(info, dict):
                continue
            name = info.get("name", code)
            trend = info.get("index_trend", "—")
            parts.append(f"\n- **{name}** ({code}) · 趋势: {trend}")
            if info.get("turnover_summary"):
                parts.append(f"  - 成交量: {str(info['turnover_summary'])[:150]}{'…' if len(str(info.get('turnover_summary',''))) > 150 else ''}")
            if info.get("volatility_summary"):
                parts.append(f"  - 波动率: {str(info['volatility_summary'])[:150]}{'…' if len(str(info.get('volatility_summary',''))) > 150 else ''}")
            if info.get("market_conclusion"):
                parts.append(f"  - 结论: {info['market_conclusion']}")
    return "\n".join(parts).strip() if parts else "（无内容）"


def _format_commodity(value: Dict[str, Any]) -> str:
    """大宗商品：先总览，再各品种分块（名称+趋势+价格摘要+宏观含义），不用 dict 字符串。"""
    if value.get("error"):
        return f"**错误**: {value['error']}"
    parts = []
    parts.append(f"- **整体趋势**: {value.get('overall_trend', '—')}  |  **商品市场**: {value.get('commodity_market_trend', '—')}")
    if value.get("macro_summary"):
        parts.append(f"- **宏观含义**: {value['macro_summary']}")
    parts.append("")
    per_commodity = value.get("per_commodity") or {}
    if isinstance(per_commodity, dict) and per_commodity:
        parts.append("**各品种**")
        for name_key, info in per_commodity.items():
            if not isinstance(info, dict):
                continue
            name = info.get("name", name_key)
            trend = info.get("trend", "—")
            parts.append(f"\n- **{name}** · 趋势: {trend}")
            if info.get("price_summary"):
                parts.append(f"  - 价格: {str(info['price_summary'])[:200]}{'…' if len(str(info.get('price_summary',''))) > 200 else ''}")
            if info.get("macro_implication"):
                parts.append(f"  - 宏观: {info['macro_implication']}")
    return "\n".join(parts).strip() if parts else "（无内容）"


def _format_analyst_dict(value: Dict[str, Any], section_hint: str) -> str:
    """通用分析师 dict：优先短句与列表行内，避免大段 JSON。"""
    if value.get("error"):
        return f"**错误**: {value['error']}"
    lines = []
    for k, v in value.items():
        if v is None or v == "":
            continue
        if k in ("per_index", "per_commodity"):
            continue
        if isinstance(v, list):
            if not v:
                continue
            if all(isinstance(x, (str, int, float, bool)) for x in v):
                lines.append(f"- **{k}**: {_fmt_list_inline(v)}")
            elif isinstance(v[0], dict) and len(v) <= 10:
                lines.append(f"**{k}**")
                for i, item in enumerate(v[:10], 1):
                    lines.append(f"  {i}. {_fmt_value_short(item)}")
            else:
                lines.append(f"- **{k}**: {_fmt_list_inline(v)}")
        elif isinstance(v, dict):
            lines.append(f"**{k}**")
            for kk, vv in list(v.items())[:12]:
                if vv is None or vv == "":
                    continue
                if isinstance(vv, list) and vv and all(isinstance(x, (str, int, float, bool)) for x in vv):
                    lines.append(f"  - {kk}: {_fmt_list_inline(vv)}")
                else:
                    lines.append(f"  - {kk}: {vv}")
        else:
            lines.append(f"- **{k}**: {v}")
    return "\n".join(lines) if lines else "（无内容）"


def _format_section_content(value: Any, section_title: str = "") -> str:
    """将分析师结果格式化为可读的 Markdown 片段，少用整块 JSON。"""
    if value is None:
        return "（无数据）"
    if isinstance(value, dict):
        if value.get("error"):
            return f"**错误**: {value['error']}"
        if "macro_environment" in value or "sector_impacts" in value or "events" in value:
            return _format_news_analysis(value)
        if "per_index" in value and ("market_sentiment" in value or "sentiment_summary" in value):
            return _format_market_sentiment(value)
        if "per_commodity" in value:
            return _format_commodity(value)
        return _format_analyst_dict(value, section_title)
    if isinstance(value, list):
        return _fmt_list_inline(value)
    return str(value)


def _build_macro_report_md(state: Dict[str, Any]) -> str:
    """根据 state 拼接整篇宏观汇总报告 Markdown（若有 LLM 报告则优先使用）。"""
    trade_date = state.get("trade_date") or datetime.now().strftime("%Y%m%d")

    # 若 LLM 已生成完整报告，则优先使用
    llm_report = state.get("macro_manager_report_markdown")
    if isinstance(llm_report, str) and llm_report.strip():
        return llm_report

    parts = [
        f"# 宏观分析汇总报告 {trade_date}",
        "",
        f"报告日期: {trade_date}  |  生成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
        "",
        "---",
        "",
    ]

    summary = state.get("macro_manager_summary")
    if summary:
        parts.append("## 总体结论（Manager）")
        parts.append("")
        parts.append(_format_manager_summary(summary))
        parts.append("")
        parts.append("---")
        parts.append("")

    sections = [
        ("一、新闻分析", state.get("news_analysis")),
        ("二、市场情绪", state.get("market_sentiment_analyst_summary")),
        ("三、流动性", state.get("liquidity_analyst_summary")),
        ("四、大宗商品", state.get("commodity_analyst_summary")),
        ("五、宏观经济", state.get("macro_economist_analysis")),
    ]
    for title, value in sections:
        parts.append(f"## {title}")
        parts.append("")
        parts.append(_format_section_content(value, title))
        parts.append("")
        parts.append("---")
        parts.append("")

    return "\n".join(parts).strip()


def create_write_macro_report_node():
    """构建写入宏观汇总报告节点：将 state 中所有分析师结果写入 data/analysis/{trade_date}_macro_report.md。"""

    def write_macro_report_node(
        state: Dict[str, Any],
        config: Optional[RunnableConfig] = None,
    ) -> Dict[str, Any]:
        trade_date = state.get("trade_date") or datetime.now().strftime("%Y%m%d")
        os.makedirs(MACRO_REPORT_DIR, exist_ok=True)
        path = os.path.join(MACRO_REPORT_DIR, MACRO_REPORT_FILENAME_TEMPLATE.format(trade_date=trade_date))
        try:
            content = _build_macro_report_md(state)
            with open(path, "w", encoding="utf-8") as f:
                f.write(content)
            logger.info("宏观汇总报告已写入: %s", path)
            return {**state, "macro_report_path": path}
        except Exception as e:
            logger.exception("写入宏观汇总报告失败: %s", e)
            return {**state, "macro_report_path": None}

    return write_macro_report_node


def create_macro_summary_node(llm=None):
    """
    构建宏观 Manager 汇总节点。

    使用各分析师输出，调用 LLM 生成：
    - 结构化决策信息（market_regime、market_direction 等）
    - 自然语言宏观总结与（可选）完整 Markdown 报告
    """

    def macro_summary_node(
        state: Dict[str, Any],
        config: Optional[RunnableConfig] = None,
    ) -> Dict[str, Any]:
        if llm is None:
            logger.warning("macro_summary: 未提供 LLM，跳过汇总分析")
            updates = {
                "macro_manager_summary": {
                    **_MACRO_MANAGER_SUMMARY_DEFAULT,
                    "macro_summary": "未提供 LLM，无法生成宏观汇总结论。",
                },
                "macro_manager_report_markdown": None,
            }
            return {**state, **updates}

        trade_date = state.get("trade_date") or datetime.now().strftime("%Y%m%d")
        macro_cfg = get_macro_config(config)
        use_llm_for_md = macro_cfg.get("use_llm_for_markdown", MACRO_USE_LLM_FOR_MARKDOWN)

        payload = {
            "trade_date": trade_date,
            "news_analysis": state.get("news_analysis"),
            "market_sentiment": state.get("market_sentiment_analyst_summary"),
            "liquidity": state.get("liquidity_analyst_summary"),
            "commodity": state.get("commodity_analyst_summary"),
            "macro_economy": state.get("macro_economist_analysis"),
        }

        if use_llm_for_md:
            system_msg = """你是一位宏观资产配置经理（Macro Portfolio Manager）。

你会收到**某一天**（trade_date）的五个分析师结构化结果：新闻分析、市场情绪、流动性、大宗商品、宏观经济。
你的任务：**仅基于当日这份数据**综合判断，给出该日的市场状态与配置建议，并返回严格 JSON。

重要约束：
- 结论必须针对**当日数据**，不得写与日期无关的泛化表述。macro_summary 中要体现当日各分析师的关键结论或数据。
- **focus_sectors**：从当日 news_analysis.sector_impacts（看多/看空板块）、commodity 强势品种对应行业、macro_economy 结论中提炼，输出 2～6 个具体板块或行业名，无则 []。禁止固定写黄金/能源/高股息。
- **avoid_sectors**：从当日新闻利空板块、宏观/情绪中的谨慎领域提炼，无则 []。禁止固定写高估值科技/可选消费。
- **macro_themes**：从当日新闻主题、商品走势、宏观结论提炼（如当日油价大涨则可有「通胀/能源」），无则 []。禁止固定写通胀交易/防御配置。
- **risk_factors**：从当日新闻事件、宏观/流动性结论中提炼具体风险，无则 []。禁止固定写美联储/地缘政治。
- 不同交易日输入不同，输出必须随当日数据变化。

JSON 结构（以下为字段说明，内容必须全部来自下方当日数据，勿照抄）：
{{
  "market_regime": "从当日 liquidity/macro_economy 等综合得出的状态，如 growth_slowdown_liquidity_loose",
  "market_direction": "neutral | bullish | bearish",
  "target_position": "low | medium | high",
  "focus_sectors": ["从当日数据提炼的板块名"],
  "avoid_sectors": ["从当日数据提炼的规避板块"],
  "macro_themes": ["从当日数据提炼的主题"],
  "risk_factors": ["从当日新闻与宏观提炼的风险"],
  "confidence": 0.0 到 1.0 之间数字,
  "macro_summary": "2～4 句话，概括当日流动性、情绪、商品、宏观中的关键结论，须有当日数据依据",
  "full_report_markdown": "一篇完整的 Markdown 报告，综合所有分析师观点，给出投资解读。"
}}

要求：
- 所有字段必须存在；若无法判断，用 "unknown" 或空列表 []，confidence 用 0.0。
- full_report_markdown 须为合法 Markdown，可直接写入 .md 文件。
- 只输出 JSON，不要任何额外说明文字。"""
        else:
            system_msg = """你是一位宏观资产配置经理（Macro Portfolio Manager）。

你会收到**某一天**（trade_date）的五个分析师结构化结果：新闻分析、市场情绪、流动性、大宗商品、宏观经济。
你的任务：**仅基于当日这份数据**综合判断，给出该日的市场状态与配置建议，并返回严格 JSON。

重要约束：
- 结论必须针对**当日数据**，不得写与日期无关的泛化表述。macro_summary 中要体现当日各分析师的关键结论或数据。
- **focus_sectors**：从当日 news_analysis.sector_impacts（看多/看空板块）、commodity 强势品种对应行业、macro_economy 结论中提炼，输出 2～6 个具体板块或行业名，无则 []。禁止固定写黄金/能源/高股息。
- **avoid_sectors**：从当日新闻利空板块、宏观/情绪中的谨慎领域提炼，无则 []。禁止固定写高估值科技/可选消费。
- **macro_themes**：从当日新闻主题、商品走势、宏观结论提炼，无则 []。禁止固定写通胀交易/防御配置。
- **risk_factors**：从当日新闻事件、宏观/流动性结论中提炼具体风险，无则 []。禁止固定写美联储/地缘政治。
- 不同交易日输入不同，输出必须随当日数据变化。

JSON 结构（以下为字段说明，内容必须全部来自下方当日数据，勿照抄；不要包含 full_report_markdown）：
{{
  "market_regime": "从当日 liquidity/macro_economy 等综合得出的状态",
  "market_direction": "neutral | bullish | bearish",
  "target_position": "low | medium | high",
  "focus_sectors": ["从当日数据提炼的板块名，禁止写黄金、能源、高股息等固定示例"],
  "avoid_sectors": ["从当日数据提炼的规避板块，禁止写高估值科技、可选消费等固定示例"],
  "macro_themes": ["从当日数据提炼的主题，禁止写通胀交易、防御配置等固定示例"],
  "risk_factors": ["从当日新闻与宏观提炼的风险，禁止写美联储、地缘政治等固定示例"],
  "confidence": 0.0 到 1.0 之间数字,
  "macro_summary": "2～4 句话，概括当日流动性、情绪、商品、宏观中的关键结论，须有当日数据依据"
}}

要求：
- 所有字段必须存在；若无法判断，用 "unknown" 或空列表 []，confidence 用 0.0。
- 只输出 JSON，不要任何额外说明文字，不要输出 full_report_markdown。"""

        human_msg = """以下是 **{trade_date}** 当日来自各分析师的结构化结果。请**仅根据下方当日数据**综合判断，给出该日的结论与 JSON，勿输出与当日无关的通用结论。

```json
{data}
```"""

        prompt = ChatPromptTemplate.from_messages(
            [("system", system_msg), ("human", human_msg)]
        )

        logger.info(
            "正在处理 宏观经理汇总分析：综合所有分析师结果 → 决策 JSON%s",
            " + LLM 报告" if use_llm_for_md else "（报告由程序拼接）",
        )
        chain = prompt | llm
        raw = chain.invoke(
            {
                "trade_date": trade_date,
                "data": json.dumps(payload, ensure_ascii=False, indent=2, default=str),
            },
            config={**(config or {}), "run_name": "宏观经理汇总分析"},
        )
        data = extract_json_text(raw)
        for k, v in _MACRO_MANAGER_SUMMARY_DEFAULT.items():
            data.setdefault(k, v)

        report_md = data.get("full_report_markdown") if use_llm_for_md else None
        updates = {
            "macro_manager_summary": {k: v for k, v in data.items() if k != "full_report_markdown"},
            "macro_manager_report_markdown": report_md,
        }
        return {**state, **updates}

    return macro_summary_node


def _invoke_one(
    name: str,
    graph: Any,
    output_key: str,
    invoke_input: Dict[str, Any],
) -> Tuple[str, str, Any]:
    """
    执行单个分析师子图 invoke，返回 (name, output_key, value)。
    若失败则返回 (name, output_key, {"error": "..."})。
    """
    try:
        result = graph.invoke(invoke_input)
        value = result.get(output_key)
        return (name, output_key, value)
    except Exception as e:
        logger.exception("子图 %s 执行失败: %s", name, e)
        return (name, output_key, {"error": str(e)})


def create_run_analysts_node(
    analyst_tasks: List[Tuple[str, Any, str]],
    max_workers: int = 3,
):
    """
    构建 run_analysts 节点：并行运行多个分析师子图，合并结果到 state。

    Args:
        analyst_tasks: 列表项为 (分析师名称, 已编译子图, state 输出键名)
        max_workers: 最大并发子图数量，由 ThreadPoolExecutor 限制

    Returns:
        节点函数，接收 state、config，返回 state 更新（各 output_key 及对应值）
    """

    def run_analysts_node(
        state: Dict[str, Any],
        config: Optional[RunnableConfig] = None,
    ) -> Dict[str, Any]:
        trade_date = state.get("trade_date") or ""
        if not trade_date:
            logger.warning("run_analysts: 缺少 trade_date，跳过所有子图")
            return {
                output_key: {"error": "missing trade_date"}
                for _, _, output_key in analyst_tasks
            }

        logger.info("开始并行运行 %d 个分析师子图（新闻、市场情绪、流动性、大宗商品、宏观经济）", len(analyst_tasks))
        invoke_input: Dict[str, Any] = {"trade_date": trade_date}
        out: Dict[str, Any] = {}

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {
                executor.submit(
                    _invoke_one,
                    name,
                    graph,
                    output_key,
                    invoke_input,
                ): (name, output_key)
                for name, graph, output_key in analyst_tasks
            }
            for future in as_completed(futures):
                name, output_key = futures[future]
                try:
                    _, key, value = future.result()
                    out[key] = value
                except Exception as e:
                    logger.exception("获取子图 %s 结果失败: %s", name, e)
                    out[output_key] = {"error": str(e)}

        # 确保所有 output_key 都有键（未完成的已在 as_completed 里按 key 写入）
        for _, _, output_key in analyst_tasks:
            if output_key not in out:
                out[output_key] = {"error": "no result"}

        logger.info("5 个分析师子图执行完毕，即将进入宏观经理汇总")
        # 显式合并当前 state（含 trade_date）与子图结果，避免框架合并导致下游拿不到键
        return {**state, **out}

    return run_analysts_node
