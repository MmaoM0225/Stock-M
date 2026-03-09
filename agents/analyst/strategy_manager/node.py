"""
策略经理 - 节点函数

包含：strategy_synthesize, strategy_markdown_write
配置通过 RunnableConfig 传入，不纳入 State。
"""
import json
import logging
import os
from datetime import datetime
from typing import Dict, List, Any, Optional

from langchain_core.prompts import ChatPromptTemplate
from langchain_core.runnables import RunnableConfig
from pydantic import BaseModel, Field, ConfigDict

logger = logging.getLogger(__name__)

from ...config import STRATEGY_GENERATE_MARKDOWN, STRATEGY_USE_LLM_FOR_MARKDOWN
from ...utils import get_strategy_config, extract_json_text


# ---------------------------------------------------------------------------
# Pydantic 输出结构
# ---------------------------------------------------------------------------


class SectorAllocation(BaseModel):
    """板块配置建议子结构"""

    allocation: str = Field(
        description="配置建议: overweight/neutral/underweight"
    )
    sentiment: str = Field(
        description="板块情绪: bullish/bearish/neutral"
    )
    confidence: float = Field(
        description="置信度 (0-1)", ge=0, le=1
    )
    reason: List[str] = Field(
        default_factory=list,
        description="配置理由列表"
    )

    model_config = ConfigDict(use_enum_values=True)


class PositionControl(BaseModel):
    """仓位控制建议子结构（百分数 0-100）"""

    position_level: float = Field(
        description="建议仓位百分数 (0-100)，如 60 表示 60%", ge=0, le=100
    )
    position_action: str = Field(
        description="仓位操作: add(加仓)/reduce(减仓)/hold(维持)"
    )
    max_position: float = Field(
        description="建议最大仓位百分数 (0-100)，风控上限", ge=0, le=100
    )
    min_position: float = Field(
        description="建议最小仓位百分数 (0-100)，风控下限", ge=0, le=100
    )
    reason: str = Field(
        description="仓位控制理由"
    )


class StrategyAnalysis(BaseModel):
    """策略分析输出结构"""

    date: str = Field(description="策略基准日 YYYYMMDD")
    market_direction: str = Field(
        description="市场整体方向: bullish/bearish/neutral"
    )
    direction_confidence: float = Field(
        description="方向判断置信度 (0-1)", ge=0, le=1
    )
    direction_reason: str = Field(
        description="方向判断的主要理由"
    )
    position_control: Optional[PositionControl] = Field(
        default=None,
        description="仓位控制建议"
    )
    sector_allocation: Dict[str, SectorAllocation] = Field(
        default_factory=dict,
        description="各板块配置建议"
    )
    key_risks: List[str] = Field(
        default_factory=list,
        description="需关注的主要风险"
    )
    key_opportunities: List[str] = Field(
        default_factory=list,
        description="主要机会点"
    )
    summary: str = Field(
        description="策略日报一句话摘要"
    )


# ---------------------------------------------------------------------------
# strategy_synthesize
# ---------------------------------------------------------------------------


def create_strategy_synthesize_node(llm=None):
    """
    构建策略综合节点。
    综合 macro_analysis 与 news_analysis，生成 strategy_analysis。
    """

    def strategy_synthesize_node(
        state: Dict, config: Optional[RunnableConfig] = None
    ) -> Dict:
        trade_date = (
            state.get("trade_date")
            or datetime.now().strftime("%Y%m%d")
        )
        macro = state.get("macro_analysis") or {}
        news = state.get("news_analysis") or {}

        _default_position = {
            "position_level": 50,
            "position_action": "hold",
            "max_position": 70,
            "min_position": 30,
            "reason": "数据不足，建议维持中性仓位。",
        }

        # 降级：两者都缺失时返回占位结果
        if not macro and not news:
            return {
                "strategy_analysis": {
                    "date": trade_date,
                    "market_direction": "neutral",
                    "direction_confidence": 0.0,
                    "direction_reason": "宏观与新闻分析数据均缺失，无法生成策略建议。",
                    "position_control": _default_position,
                    "sector_allocation": {},
                    "key_risks": ["数据不足"],
                    "key_opportunities": [],
                    "summary": "数据不足，请先运行宏观与新闻分析师。",
                }
            }

        if llm is None:
            return {
                "strategy_analysis": {
                    "date": trade_date,
                    "market_direction": "neutral",
                    "direction_confidence": 0.5,
                    "direction_reason": "未使用 LLM，仅做占位。",
                    "position_control": _default_position,
                    "sector_allocation": {},
                    "key_risks": [],
                    "key_opportunities": [],
                    "summary": "请配置 LLM 以生成策略分析。",
                }
            }

        system_msg = (
            "你是一位资深策略经理，负责综合宏观经济分析与新闻分析，输出可执行的交易策略建议。\n\n"
            "【输入说明】\n"
            "- macro_analysis: 含 date, monetary(货币环境), global(全球环境), market(A股技术面), summary\n"
            "- news_analysis: 含 date, events(事件列表), sector_impacts(板块影响), macro_environment(流动性/政策/风险/市场情绪)\n\n"
            "【输出要求】\n"
            "必须返回严格符合以下结构的 JSON，只输出 JSON，不要任何解释：\n"
            "- date: 策略基准日\n"
            "- market_direction: bullish/bearish/neutral\n"
            "- direction_confidence: 0-1 的浮点数\n"
            "- direction_reason: 1-3 句话说明方向判断理由\n"
            "- position_control: 仓位控制对象，含 position_level(建议仓位百分数0-100)、position_action(add/reduce/hold)、max_position(最大仓位百分数0-100)、min_position(最小仓位百分数0-100)、reason(仓位控制理由)\n"
            "- sector_allocation: 字典，key 为板块/行业名，value 为对象，含 allocation(overweight/neutral/underweight)、sentiment(bullish/bearish/neutral)、confidence(0-1)、reason(字符串数组)\n"
            "- key_risks: 主要风险列表（字符串数组）\n"
            "- key_opportunities: 主要机会列表（字符串数组）\n"
            "- summary: 策略日报一句话摘要\n\n"
            "【重要】\n"
            "- position_control 必须根据市场方向与风险给出：看多时 position_level 可偏高、position_action 可为 add；看空时偏低、可为 reduce；震荡时 hold\n"
            "- sector_allocation 至少 3 个、最多 10 个板块；板块名从 news 的 sector_impacts 或宏观/新闻内容中选取\n"
            "- 当宏观与新闻结论冲突时，说明权衡逻辑并给出综合判断\n"
            "- 置信度需反映不确定性，不要盲目给高置信度"
        )

        prompt = ChatPromptTemplate.from_messages(
            [
                ("system", system_msg),
                (
                    "human",
                    "交易日: {trade_date}\n\n"
                    "【宏观分析】\n{macro}\n\n"
                    "【新闻分析】\n{news}\n\n"
                    "请综合以上信息，输出策略分析 JSON。",
                ),
            ]
        )

        try:
            chain = prompt | llm
            raw = chain.invoke(
                {
                    "trade_date": trade_date,
                    "macro": json.dumps(
                        macro, ensure_ascii=False, indent=2, default=str
                    ),
                    "news": json.dumps(
                        news, ensure_ascii=False, indent=2, default=str
                    ),
                }
            )
            data = extract_json_text(raw)
            data["date"] = trade_date

            # 将 sector_allocation 中的 dict 转为 SectorAllocation 兼容格式
            sa_raw = data.get("sector_allocation") or {}
            sector_allocation = {}
            for k, v in sa_raw.items():
                if isinstance(v, dict):
                    sector_allocation[k] = {
                        "allocation": v.get("allocation", "neutral"),
                        "sentiment": v.get("sentiment", "neutral"),
                        "confidence": float(v.get("confidence", 0.5)),
                        "reason": v.get("reason") or [],
                    }
                else:
                    sector_allocation[k] = v
            data["sector_allocation"] = sector_allocation

            # 解析 position_control（支持 0-1 小数或 0-100 百分数，统一为百分数）
            def _to_pct(v, default=50):
                x = float(v) if v is not None else default
                return x * 100 if 0 < x <= 1 else x

            pc_raw = data.get("position_control")
            if isinstance(pc_raw, dict):
                data["position_control"] = {
                    "position_level": _to_pct(pc_raw.get("position_level"), 50),
                    "position_action": pc_raw.get("position_action", "hold"),
                    "max_position": _to_pct(pc_raw.get("max_position"), 80),
                    "min_position": _to_pct(pc_raw.get("min_position"), 20),
                    "reason": pc_raw.get("reason", ""),
                }
            elif pc_raw is None:
                data["position_control"] = {
                    "position_level": 50,
                    "position_action": "hold",
                    "max_position": 70,
                    "min_position": 30,
                    "reason": "建议维持中性仓位。",
                }

            result = StrategyAnalysis.model_validate(data)
            return {"strategy_analysis": result.model_dump()}
        except Exception as e:
            logger.warning("strategy_synthesize LLM 失败: %s", e)
            return {
                "strategy_analysis": {
                    "date": trade_date,
                    "market_direction": "neutral",
                    "direction_confidence": 0.0,
                    "direction_reason": f"策略综合失败: {e}",
                    "position_control": {
                        "position_level": 50,
                        "position_action": "hold",
                        "max_position": 70,
                        "min_position": 30,
                        "reason": "策略生成失败，建议维持中性仓位。",
                    },
                    "sector_allocation": {},
                    "key_risks": [],
                    "key_opportunities": [],
                    "summary": f"策略生成失败: {e}",
                }
            }

    return strategy_synthesize_node


# ---------------------------------------------------------------------------
# strategy_markdown_write
# ---------------------------------------------------------------------------


def _build_strategy_analysis_md_programmatic(
    analysis_result: Dict,
) -> str:
    """程序化拼接策略分析 Markdown。"""
    trade_date = analysis_result.get(
        "date", datetime.now().strftime("%Y%m%d")
    )
    lines = [
        f"# 策略分析报告 {trade_date}",
        "",
        "## 核心结论",
        "",
        analysis_result.get("summary", "（无摘要）"),
        "",
        "---",
        "",
        "## 市场方向",
        "",
        f"- **方向**: {analysis_result.get('market_direction', 'neutral')}",
        f"- **置信度**: {analysis_result.get('direction_confidence', 0)}",
        f"- **理由**: {analysis_result.get('direction_reason', '')}",
        "",
        "---",
        "",
        "## 仓位控制",
        "",
    ]

    pc = analysis_result.get("position_control") or {}
    if pc:
        level = pc.get("position_level", 50)
        action = pc.get("position_action", "hold")
        action_cn = {"add": "加仓", "reduce": "减仓", "hold": "维持"}.get(action, action)
        lines.append(f"- **建议仓位**: {level:.0f}%")
        lines.append(f"- **操作建议**: {action_cn}")
        lines.append(f"- **仓位区间**: {pc.get('min_position', 30):.0f}% ~ {pc.get('max_position', 80):.0f}%")
        lines.append(f"- **理由**: {pc.get('reason', '')}")
    else:
        lines.append("（无仓位控制建议）")
    lines.append("")
    lines.extend(
        [
            "---",
            "",
            "## 板块配置建议",
            "",
        ]
    )

    sector_allocation = analysis_result.get("sector_allocation") or {}
    if sector_allocation:
        for sector, info in sector_allocation.items():
            alloc = info.get("allocation", "neutral")
            sentiment = info.get("sentiment", "neutral")
            conf = info.get("confidence", 0)
            reasons = info.get("reason") or []
            lines.append(f"### {sector}")
            lines.append(f"- 配置: {alloc} | 情绪: {sentiment} | 置信度: {conf}")
            for r in reasons:
                lines.append(f"  - {r}")
            lines.append("")
    else:
        lines.append("（无板块配置建议）\n")

    lines.extend(
        [
            "---",
            "",
            "## 主要风险",
            "",
        ]
    )
    for r in analysis_result.get("key_risks") or []:
        lines.append(f"- {r}")
    lines.append("")

    lines.extend(
        [
            "---",
            "",
            "## 主要机会",
            "",
        ]
    )
    for o in analysis_result.get("key_opportunities") or []:
        lines.append(f"- {o}")
    lines.append("")

    lines.extend(
        [
            "---",
            "",
            "## 报告元数据",
            "",
            f"- 报告日期: {trade_date}",
            f"- 生成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
            "",
        ]
    )
    return "\n".join(lines)


def _build_strategy_analysis_md_with_llm(
    analysis_result: Dict, llm, trade_date: str
) -> Optional[str]:
    """用 LLM 润色策略分析 Markdown。"""
    try:
        system_msg = (
            "你是一位策略经理。请根据提供的结构化策略数据，撰写一份简洁、可读性强的策略日报。"
            "不要照抄字段，而是写成给人看的解读。"
            "结构：核心结论 → 市场方向 → 仓位控制 → 板块配置 → 风险与机会。"
            "直接输出完整 Markdown，不要输出其他说明。"
        )
        prompt = ChatPromptTemplate.from_messages(
            [
                ("system", system_msg),
                (
                    "human",
                    "策略数据：\n{data}\n\n请据此撰写策略日报 Markdown。",
                ),
            ]
        )
        chain = prompt | llm
        raw = chain.invoke(
            {
                "data": json.dumps(
                    analysis_result,
                    ensure_ascii=False,
                    indent=2,
                    default=str,
                )
            }
        )
        text = raw.content if hasattr(raw, "content") else str(raw)
        if "报告日期" not in text:
            text += (
                f"\n\n---\n\n## 报告元数据\n\n"
                f"- 报告日期: {trade_date}\n"
                f"- 生成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
            )
        return text.strip()
    except Exception as e:
        logger.warning("LLM 生成策略 Markdown 失败: %s，回退到程序化输出", e)
        return None


def _write_strategy_analysis_md(
    analysis_result: Dict,
    trade_date: str,
    llm=None,
    use_llm_for_md: bool = False,
) -> Optional[str]:
    """写入 data/analysis/YYYYMMDD_strategy_analysis.md。"""
    try:
        md_content = None
        if use_llm_for_md and llm:
            md_content = _build_strategy_analysis_md_with_llm(
                analysis_result, llm, trade_date
            )
        if md_content is None:
            md_content = _build_strategy_analysis_md_programmatic(
                analysis_result
            )
        out_dir = os.path.join("data", "analysis")
        os.makedirs(out_dir, exist_ok=True)
        path = os.path.join(out_dir, f"{trade_date}_strategy_analysis.md")
        with open(path, "w", encoding="utf-8") as f:
            f.write(md_content)
        logger.info("策略报告已写入: %s", path)
        return path
    except Exception as e:
        logger.warning("写入策略 Markdown 失败: %s", e)
        return None


def create_strategy_markdown_write_node(llm=None):
    """构建策略 Markdown 写入节点。由 configurable.strategy_config.generate_markdown 控制是否执行。"""

    def strategy_markdown_write_node(
        state: Dict, config: Optional[RunnableConfig] = None
    ) -> Dict:
        analysis_result = state.get("strategy_analysis") or {}
        trade_date = (
            analysis_result.get("date")
            or state.get("trade_date")
            or datetime.now().strftime("%Y%m%d")
        )
        cfg = get_strategy_config(config) or {}
        use_llm = cfg.get(
            "use_llm_for_markdown", STRATEGY_USE_LLM_FOR_MARKDOWN
        )
        _write_strategy_analysis_md(
            analysis_result,
            trade_date,
            llm=llm,
            use_llm_for_md=use_llm,
        )
        return {}  # 不修改 state，仅写文件

    return strategy_markdown_write_node
