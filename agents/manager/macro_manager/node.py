"""
Macro Manager（宏观管理器）- 节点

run_analysts：并行调用多个分析师子图（带最大并发限制），合并结果到 state。
"""
import json
import logging
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from langchain_core.prompts import ChatPromptTemplate
from langchain_core.runnables import RunnableConfig

from ...utils import extract_json_text

logger = logging.getLogger(__name__)

_MACRO_ANALYST_ARTIFACT_ROOT = Path("data") / "artifacts" / "analyst" / "macro_analyst"
_MACRO_MANAGER_ARTIFACT_ROOT = Path("data") / "artifacts" / "manager" / "macro_manager"
_MACRO_ANALYST_ARTIFACT_DIRS: Dict[str, str] = {
    "news": "news_analyst",
    "market_sentiment": "market_sentiment_analyst",
    "commodity": "commodity_analyst",
    "macro_economist": "macro_economist",
}

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


def _build_macro_analyst_result_path(name: str, trade_date: str) -> Optional[Path]:
    """根据分析师名称与交易日，定位本地 artifact 的 result.json 路径。"""
    artifact_dir = _MACRO_ANALYST_ARTIFACT_DIRS.get(name)
    if not artifact_dir:
        return None
    return _MACRO_ANALYST_ARTIFACT_ROOT / artifact_dir / trade_date / "result.json"


def _load_json_file(path: Path) -> Any:
    """读取 JSON 文件并返回解析结果。"""
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def _write_json_atomic(path: Path, payload: Dict[str, Any]) -> None:
    """原子写入 JSON，避免中途中断留下半成品。"""
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_suffix(path.suffix + ".tmp")
    with open(tmp_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
    os.replace(tmp_path, path)


def _persist_macro_manager_summary(
    state: Dict[str, Any],
    summary: Dict[str, Any],
) -> Dict[str, Any]:
    """将 macro_manager_summary 持久化到本地 artifacts，并同步数据库主表。"""
    trade_date = str(state.get("trade_date") or datetime.now().strftime("%Y%m%d")).replace("-", "")[:8]
    artifact_dir = _MACRO_MANAGER_ARTIFACT_ROOT / trade_date
    result_path = artifact_dir / "result.json"
    manifest_path = artifact_dir / "manifest.json"

    _write_json_atomic(result_path, summary)
    _write_json_atomic(
        manifest_path,
        {
            "artifact_type": "macro_manager_summary",
            "module": "agents.manager.macro_manager",
            "trade_date": trade_date,
            "created_at": datetime.now().astimezone().isoformat(),
            "status": "success",
            "result_path": result_path.as_posix(),
        },
    )
    try:
        from database.data_sync.macro_manager import sync_single_result

        sync_single_result(result_path)
    except Exception as sync_err:
        logger.warning("macro_manager 数据库同步失败: %s", sync_err)
    logger.info("macro_manager_summary 已写入本地 artifacts: %s", result_path)
    return {
        **state,
        "macro_manager_summary": summary,
        "macro_manager_artifact_path": result_path.as_posix(),
        "macro_manager_manifest_path": manifest_path.as_posix(),
    }


def create_detect_available_analysts_node(
    analyst_tasks: List[Tuple[str, Any, str]],
):
    """
    检测本地已存在的 analyst artifact。

    - 命中则直接加载到 state 的 output_key
    - 未命中则记录到 missing_analysts
    """

    def detect_available_analysts_node(
        state: Dict[str, Any],
        config: Optional[RunnableConfig] = None,
    ) -> Dict[str, Any]:
        _ = config
        trade_date = str(state.get("trade_date") or "").replace("-", "")[:8]
        available_analysts: List[str] = []
        missing_analysts: List[str] = []
        loaded_artifact_paths: Dict[str, str] = {}
        loaded_values: Dict[str, Any] = {}

        if not trade_date:
            logger.warning("detect_available_analysts: 缺少 trade_date，无法检测本地数据")
            return {
                **state,
                "available_analysts": available_analysts,
                "missing_analysts": [name for name, _, _ in analyst_tasks],
                "loaded_artifact_paths": loaded_artifact_paths,
            }

        for name, _, output_key in analyst_tasks:
            existing_value = state.get(output_key)
            if existing_value:
                available_analysts.append(name)
                loaded_artifact_paths[name] = "<state>"
                continue

            result_path = _build_macro_analyst_result_path(name, trade_date)
            if result_path is None or not result_path.exists():
                missing_analysts.append(name)
                continue

            try:
                payload = _load_json_file(result_path)
                if not payload:
                    logger.warning("detect_available_analysts: %s 本地结果为空，视为缺失", result_path)
                    missing_analysts.append(name)
                    continue
                loaded_values[output_key] = payload
                available_analysts.append(name)
                loaded_artifact_paths[name] = result_path.as_posix()
            except Exception as e:
                logger.warning("detect_available_analysts: 读取 %s 失败: %s", result_path, e)
                missing_analysts.append(name)

        logger.info(
            "macro_manager 本地检测完成: 命中 %d 个，缺失 %d 个",
            len(available_analysts),
            len(missing_analysts),
        )
        return {
            **state,
            **loaded_values,
            "available_analysts": available_analysts,
            "missing_analysts": missing_analysts,
            "loaded_artifact_paths": loaded_artifact_paths,
        }

    return detect_available_analysts_node


def _get_industry_and_concept_lists() -> Tuple[List[str], List[str]]:
    """
    获取同花顺行业(I)列表与同花顺概念(N)列表，供 LLM 输出 focus_industry_sectors / focus_concept_sectors 时选用。
    优先从数据库读取（需先运行 data_sync），否则从 dataflow 拉取。
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


def create_macro_summary_node(llm=None):
    """构建宏观 Manager 汇总节点，输出结构化宏观结论。"""

    def macro_summary_node(
        state: Dict[str, Any],
        config: Optional[RunnableConfig] = None,
    ) -> Dict[str, Any]:
        if llm is None:
            logger.warning("macro_summary: 未提供 LLM，跳过汇总分析")
            summary = {
                **_MACRO_MANAGER_SUMMARY_DEFAULT,
                "macro_summary": "未提供 LLM，无法生成宏观汇总结论。",
            }
            try:
                return _persist_macro_manager_summary(state, summary)
            except Exception as e:
                logger.warning("写入 macro_manager artifacts 失败: %s", e)
                return {
                    **state,
                    "macro_manager_summary": summary,
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
            "commodity": state.get("commodity_analyst_summary"),
            "macro_economy": state.get("macro_economist_analysis"),
        }

        system_msg = """你是一位宏观资产配置经理（Macro Portfolio Manager）。

你会收到**某一天**（trade_date）的四个分析师结构化结果：新闻分析、市场情绪、大宗商品、宏观经济。
你的任务：**仅基于当日这份数据**综合判断，给出该日的市场状态与配置建议，并返回严格 JSON。

重要约束：
- 结论必须针对**当日数据**，不得写与日期无关的泛化表述。macro_summary 中要体现当日各分析师的关键结论或数据。
- **focus_industry_sectors**：必须从下方 **industry_list** 中选取。结合当日各分析师结论输出 2～6 个行业名，无则 []。不可自造行业名。
- **focus_concept_sectors**：必须从下方 **ths_concept_list** 中选取。结合当日各分析师结论输出 2～6 个概念名，无则 []。不可自造概念名。
- **avoid_sectors**：从当日新闻利空板块、宏观/情绪中的谨慎领域提炼，无则 []。
- **macro_themes**：从当日新闻主题、商品走势、宏观结论提炼，无则 []。
- **risk_factors**：从当日新闻事件、宏观结论中提炼具体风险，无则 []。
- 不同交易日输入不同，输出必须随当日数据变化。

JSON 结构（focus_industry_sectors / focus_concept_sectors 必须从下方对应列表中选取）：
{{
  "market_regime": "从当日 macro_economy/market_sentiment/news 等综合得出的状态",
  "market_direction": "neutral | bullish | bearish",
  "target_position": "建议仓位区间，可从以下档位中根据当日数据智能选择，格式如'30%-45%'或'50%-65%'：\n       极端看空：0%-10%\n       谨慎：10%-25%、20%-35%\n       中性偏谨慎：25%-45%、30%-50%\n       中性：40%-60%、45%-65%\n       中性偏积极：50%-70%、55%-75%\n       积极：65%-85%、70%-90%\n       极度乐观：85%-100%",
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
        try:
            return _persist_macro_manager_summary(state, data)
        except Exception as e:
            logger.warning("写入 macro_manager artifacts 失败: %s", e)
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
            error_outputs = {
                output_key: {"error": "missing trade_date"}
                for _, _, output_key in analyst_tasks
            }
            return {
                **state,
                "available_analysts": [],
                "missing_analysts": [name for name, _, _ in analyst_tasks],
                **error_outputs,
            }

        missing_analysts = state.get("missing_analysts")
        if isinstance(missing_analysts, list):
            missing_set = {str(x) for x in missing_analysts}
            tasks_to_run = [
                (name, graph, output_key)
                for name, graph, output_key in analyst_tasks
                if name in missing_set
            ]
        else:
            tasks_to_run = analyst_tasks

        if not tasks_to_run:
            logger.info("macro_manager 所有分析师结果均已命中本地 artifacts，跳过子图执行")
            return {
                **state,
                "missing_analysts": [],
            }

        logger.info(
            "开始并行运行 %d 个缺失的分析师子图（总计 %d 个）",
            len(tasks_to_run),
            len(analyst_tasks),
        )
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
                for name, graph, output_key in tasks_to_run
            }
            for future in as_completed(futures):
                name, output_key = futures[future]
                try:
                    _, key, value = future.result()
                    out[key] = value
                except Exception as e:
                    logger.exception("获取子图 %s 结果失败: %s", name, e)
                    out[output_key] = {"error": str(e)}

        # 确保本次需要补跑的 output_key 都有返回
        for _, _, output_key in tasks_to_run:
            if output_key not in out:
                out[output_key] = {"error": "no result"}

        merged_state = {**state, **out}
        remaining_missing: List[str] = []
        available_analysts: List[str] = []
        for name, _, output_key in analyst_tasks:
            value = merged_state.get(output_key)
            if not value or (isinstance(value, dict) and value.get("error")):
                remaining_missing.append(name)
            else:
                available_analysts.append(name)

        logger.info("缺失分析师补跑结束，即将进入宏观经理汇总")
        # 显式合并当前 state（含 trade_date）与子图结果，避免框架合并导致下游拿不到键
        return {
            **merged_state,
            "available_analysts": available_analysts,
            "missing_analysts": remaining_missing,
        }

    return run_analysts_node


__all__ = [
    "create_detect_available_analysts_node",
    "create_macro_summary_node",
    "create_run_analysts_node",
]
