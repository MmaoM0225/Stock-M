"""
Macro Manager（宏观管理器）- 节点

run_analysts：并行调用多个分析师子图（带最大并发限制），合并结果到 state。
"""
import json
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

from langchain_core.prompts import ChatPromptTemplate
from langchain_core.runnables import RunnableConfig

from ...utils import extract_json_text

logger = logging.getLogger(__name__)

_MACRO_MANAGER_SUMMARY_DEFAULT: Dict[str, Any] = {
    "market_regime": "unknown",
    "market_direction": "unknown",
    "target_position": "unknown",
    "focus_industry_sectors": [],
    "focus_concept_sectors": [],
    "avoid_sectors": [],
    "macro_themes": [],
    "risk_factors": [],
    "confidence": 0.0,
    "macro_summary": "",
}


def _get_industry_and_concept_lists() -> Tuple[List[str], List[str]]:
    """
    获取行业名称列表与同花顺概念名称列表，供 LLM 输出 focus_industry_sectors / focus_concept_sectors 时选用。
    优先从数据库读取（需先运行 data_sync），否则从 dataflow 拉取。
    """
    industry_list: List[str] = []
    concept_list: List[str] = []
    try:
        from database import Industry, ThsIndex
        from database.config import get_db_session

        with get_db_session() as session:
            industry_list = [
                r.industry_name
                for r in session.query(Industry.industry_name).distinct().all()
                if r.industry_name
            ]
            concept_list = [
                r.name
                for r in session.query(ThsIndex.name).filter(ThsIndex.index_type == "N").all()
                if r.name
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


def create_macro_summary_node(llm=None):
    """构建宏观 Manager 汇总节点，输出结构化宏观结论。"""

    def macro_summary_node(
        state: Dict[str, Any],
        config: Optional[RunnableConfig] = None,
    ) -> Dict[str, Any]:
        if llm is None:
            logger.warning("macro_summary: 未提供 LLM，跳过汇总分析")
            return {
                **state,
                "macro_manager_summary": {
                    **_MACRO_MANAGER_SUMMARY_DEFAULT,
                    "macro_summary": "未提供 LLM，无法生成宏观汇总结论。",
                },
            }

        trade_date = state.get("trade_date") or datetime.now().strftime("%Y%m%d")
        industry_list, ths_concept_list = _get_industry_and_concept_lists()
        logger.info(
            "宏观汇总：已注入行业列表 %d 条、同花顺概念列表 %d 条供 LLM 选用",
            len(industry_list),
            len(ths_concept_list),
        )

        payload = {
            "trade_date": trade_date,
            "industry_list": industry_list,
            "ths_concept_list": ths_concept_list,
            "news_analysis": state.get("news_analysis"),
            "market_sentiment": state.get("market_sentiment_analyst_summary"),
            "liquidity": state.get("liquidity_analyst_summary"),
            "commodity": state.get("commodity_analyst_summary"),
            "macro_economy": state.get("macro_economist_analysis"),
        }

        system_msg = """你是一位宏观资产配置经理（Macro Portfolio Manager）。

你会收到**某一天**（trade_date）的五个分析师结构化结果：新闻分析、市场情绪、流动性、大宗商品、宏观经济。
你的任务：**仅基于当日这份数据**综合判断，给出该日的市场状态与配置建议，并返回严格 JSON。

重要约束：
- 结论必须针对**当日数据**，不得写与日期无关的泛化表述。macro_summary 中要体现当日各分析师的关键结论或数据。
- **focus_industry_sectors**：必须从下方 **industry_list** 中选取。结合当日各分析师结论输出 2～6 个行业名，无则 []。不可自造行业名。
- **focus_concept_sectors**：必须从下方 **ths_concept_list** 中选取。结合当日各分析师结论输出 2～6 个概念名，无则 []。不可自造概念名。
- **avoid_sectors**：从当日新闻利空板块、宏观/情绪中的谨慎领域提炼，无则 []。
- **macro_themes**：从当日新闻主题、商品走势、宏观结论提炼，无则 []。
- **risk_factors**：从当日新闻事件、宏观/流动性结论中提炼具体风险，无则 []。
- 不同交易日输入不同，输出必须随当日数据变化。

JSON 结构（focus_industry_sectors / focus_concept_sectors 必须从下方对应列表中选取）：
{{
  "market_regime": "从当日 liquidity/macro_economy 等综合得出的状态",
  "market_direction": "neutral | bullish | bearish",
  "target_position": "low | medium | high",
  "focus_industry_sectors": ["从 industry_list 中选取的行业名"],
  "focus_concept_sectors": ["从 ths_concept_list 中选取的概念名"],
  "avoid_sectors": ["从当日数据提炼的规避板块"],
  "macro_themes": ["从当日数据提炼的主题"],
  "risk_factors": ["从当日新闻与宏观提炼的风险"],
  "confidence": 0.0 到 1.0 之间数字,
  "macro_summary": "2～4 句话，概括当日流动性、情绪、商品、宏观中的关键结论，须有当日数据依据"
}}

要求：
- 所有字段必须存在；若无法判断，用 "unknown" 或空列表 []，confidence 用 0.0。
- 只输出 JSON，不要任何额外说明文字。"""

        human_msg = """以下是 **{trade_date}** 当日来自各分析师的结构化结果。请**仅根据下方当日数据**综合判断，给出该日的结论与 JSON，勿输出与当日无关的通用结论。

```json
{data}
```"""

        prompt = ChatPromptTemplate.from_messages(
            [("system", system_msg), ("human", human_msg)]
        )
        logger.info("正在处理 宏观经理汇总分析：综合所有分析师结果 → 决策 JSON")
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

        return {**state, "macro_manager_summary": data}

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


__all__ = [
    "create_macro_summary_node",
    "create_run_analysts_node",
]
