"""
Liquidity Analyst（流动性分析师）- 节点函数

专注：利率、市场流动性数据（LPR、M2、社融）。
"""
import json
import logging
import os
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional

from langchain_core.prompts import ChatPromptTemplate
from langchain_core.runnables import RunnableConfig

logger = logging.getLogger(__name__)

LIQUIDITY_ARTIFACT_ROOT = (
    Path("data") / "artifacts" / "analyst" / "macro_analyst" / "liquidity_analyst"
)

from ....config import MACRO_DAILY_LOOKBACK, MACRO_MONTH_LOOKBACK
from ....utils import date_offset, to_serializable, extract_json_text


# ---------------------------------------------------------------------------
# liquidity_fetch
# ---------------------------------------------------------------------------


def create_liquidity_fetch_node(market_fetcher=None):
    """
    构建流动性数据拉取节点。
    拉取：LPR（利率）、M2、社融。
    """

    def liquidity_fetch_node(state: Dict, config: Optional[RunnableConfig] = None) -> Dict:
        trade_date = state.get("trade_date") or datetime.now().strftime("%Y%m%d")
        end_date = trade_date.replace("-", "")[:8]
        # 日度数据（若后续扩展其他利率或资金面高频指标，可继续使用）
        start_date = date_offset(end_date, days=MACRO_DAILY_LOOKBACK)
        # 月度数据回溯配置：LPR 实际为月度报价，这里按月份回溯以与宏观经济子图保持一致
        start_m = date_offset(end_date, months=MACRO_MONTH_LOOKBACK)[:6]
        end_m = end_date[:6]
        lpr_start_date = f"{start_m}01"  # LPR 为月度报价，按月份区间回溯

        lpr_data = m2_data = sf_data = None

        market = market_fetcher
        if market is None:
            try:
                from dataflow.market_data import MarketDataFetcher
                market = MarketDataFetcher()
            except Exception as e:
                logger.warning("无法初始化 MarketDataFetcher: %s", e)

        if market:
            try:
                # 使用按月回溯得到的起始日期，避免将 LPR 当作高频日度序列来拉取
                lpr_data = market.fetch_shibor_lpr(lpr_start_date, end_date)
            except Exception as e:
                logger.warning("fetch_shibor_lpr 失败: %s", e)
            try:
                m2_data = market.fetch_m2(start_m=start_m, end_m=end_m)
            except Exception as e:
                logger.warning("fetch_m2 失败: %s", e)
            try:
                sf_data = market.fetch_sf_month(start_m=start_m, end_m=end_m)
            except Exception as e:
                logger.warning("fetch_sf_month 失败: %s", e)

        return {
            "trade_date": trade_date,
            "liquidity_lpr_data": to_serializable(lpr_data),
            "liquidity_m2_data": to_serializable(m2_data),
            "liquidity_sf_data": to_serializable(sf_data),
        }

    return liquidity_fetch_node


# ---------------------------------------------------------------------------
# liquidity_analysis
# ---------------------------------------------------------------------------

_LIQUIDITY_ANALYST_DEFAULT = {
    "lpr_trend": "unknown",
    "m2_trend": "unknown",
    "sf_trend": "unknown",
    "liquidity_signal": "unknown",
    "liquidity_summary": "",
    "conclusion": "",
}


def _format_data_tail(data, max_rows: int = 8) -> str:
    """将数据截取最近 max_rows 行。"""
    if not data:
        return "无数据"
    if isinstance(data, list) and len(data) > max_rows:
        return str(data[-max_rows:])
    return str(data)


def create_liquidity_analysis_node(llm=None):
    """构建流动性分析节点。基于 LPR、M2、社融等分析流动性环境。"""

    def liquidity_analysis_node(state: Dict, config: Optional[RunnableConfig] = None) -> Dict:
        lpr = state.get("liquidity_lpr_data")
        m2 = state.get("liquidity_m2_data")
        sf = state.get("liquidity_sf_data")
        trade_date = state.get("trade_date") or datetime.now().strftime("%Y%m%d")

        if not lpr and not m2 and not sf:
            return {
                "trade_date": trade_date,
                "liquidity_analyst_summary": {
                    **_LIQUIDITY_ANALYST_DEFAULT,
                    "liquidity_summary": "数据缺失",
                    "conclusion": "流动性数据缺失，无法分析",
                }
            }

        system_msg = """你是一位流动性分析师（Liquidity Analyst）。

任务：根据 LPR（利率）、M2、社融等数据，分析流动性环境。

请识别以下维度：
1. 利率环境（LPR 走势）
2. 货币供应（M2 同比、环比）
3. 信用扩张（社融）
4. 整体流动性松紧

返回严格 JSON，只输出 JSON，不要其他文字：

{{
  "lpr_trend": "up | down | stable | unknown",
  "m2_trend": "up | down | stable | unknown",
  "sf_trend": "up | down | stable | unknown",
  "liquidity_signal": "loose | neutral | tight | unknown",
  "liquidity_summary": "一句话解释流动性环境",
  "conclusion": "一句话流动性结论"
}}

无数据的指标填 unknown。"""

        human_msg = """【数据】以下为近期数据（按时间由远及近），请分析流动性环境。

LPR（利率，日度）:
{lpr}

M2（月度）:
{m2}

社融（月度）:
{sf}

请分析并返回上述 JSON。"""

        prompt = ChatPromptTemplate.from_messages(
            [("system", system_msg), ("human", human_msg)]
        )

        logger.info("正在处理 流动性分析：流动性数据 → 分析 JSON")
        chain = prompt | llm
        raw = chain.invoke(
            {"lpr": _format_data_tail(lpr), "m2": _format_data_tail(m2), "sf": _format_data_tail(sf)},
            config={**(config or {}), "run_name": "流动性分析"},
        )
        data = extract_json_text(raw)
        for k, v in _LIQUIDITY_ANALYST_DEFAULT.items():
            data.setdefault(k, v)
        return {"trade_date": trade_date, "liquidity_analyst_summary": data}

    return liquidity_analysis_node


def _write_json_atomic(path: Path, payload: Dict[str, Any]) -> None:
    """原子写入 JSON，避免中途中断留下半成品。"""
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_suffix(path.suffix + ".tmp")
    with open(tmp_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
    os.replace(tmp_path, path)


def create_liquidity_result_persist_node():
    """将最终输出键 liquidity_analyst_summary 持久化到本地 artifacts。"""

    def liquidity_result_persist_node(
        state: Dict[str, Any],
        config: Optional[RunnableConfig] = None,
    ) -> Dict[str, Any]:
        _ = config
        summary = state.get("liquidity_analyst_summary")
        if not summary:
            return state

        trade_date = str(state.get("trade_date") or datetime.now().strftime("%Y%m%d")).replace("-", "")[:8]
        artifact_dir = LIQUIDITY_ARTIFACT_ROOT / trade_date
        result_path = artifact_dir / "result.json"
        manifest_path = artifact_dir / "manifest.json"

        try:
            _write_json_atomic(result_path, summary)
            _write_json_atomic(
                manifest_path,
                {
                    "artifact_type": "liquidity_analyst_summary",
                    "module": "agents.analyst.macro_analyst.liquidity_analyst",
                    "trade_date": trade_date,
                    "created_at": datetime.now().astimezone().isoformat(),
                    "status": "success",
                    "result_path": result_path.as_posix(),
                },
            )
            logger.info("liquidity_analyst_summary 已写入本地 artifacts: %s", result_path)
            return {
                **state,
                "liquidity_artifact_path": result_path.as_posix(),
                "liquidity_manifest_path": manifest_path.as_posix(),
            }
        except Exception as e:
            logger.warning("写入 liquidity artifacts 失败: %s", e)
            return state

    return liquidity_result_persist_node
