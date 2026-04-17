"""
Macro Economist（宏观经济分析师）- 节点函数

专注：GDP、CPI、PMI、利率、M2。
当前：GDP、CPI、LPR（利率）、社融、PMI、M2 已全部接入。
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

MACRO_ECONOMIST_ARTIFACT_ROOT = (
    Path("data") / "artifacts" / "analyst" / "macro_analyst" / "macro_economist"
)

from ....config import MACRO_DAILY_LOOKBACK, MACRO_MONTH_LOOKBACK
from ....utils import date_offset, to_serializable, extract_json_text


# ---------------------------------------------------------------------------
# macro_economist_fetch
# ---------------------------------------------------------------------------


def create_macro_economist_fetch_node(market_fetcher=None):
    """
    构建 Macro Economist 数据拉取节点。
    拉取：GDP、CPI、LPR（利率）、社融、PMI、M2。
    """

    def macro_economist_fetch_node(state: Dict, config: Optional[RunnableConfig] = None) -> Dict:
        trade_date = state.get("trade_date") or datetime.now().strftime("%Y%m%d")
        end_date = trade_date.replace("-", "")[:8]
        start_date = date_offset(end_date, days=MACRO_DAILY_LOOKBACK)
        start_m = date_offset(end_date, months=MACRO_MONTH_LOOKBACK)[:6]
        end_m = end_date[:6]
        lpr_start_date = f"{start_m}01"  # LPR 为月度报价，按一年回溯

        lpr_data = cpi_data = sf_data = pmi_data = m2_data = gdp_data = None

        # GDP 为季度数据，需将月度区间转为季度
        def _month_to_quarter(yyyymm: str) -> str:
            y, m = yyyymm[:4], int(yyyymm[4:6])
            q = (m - 1) // 3 + 1
            return f"{y}Q{q}"
        start_q = _month_to_quarter(start_m)
        end_q = _month_to_quarter(end_m)

        market = market_fetcher
        if market is None:
            try:
                from dataflow.market_data import MarketDataFetcher
                market = MarketDataFetcher()
            except Exception as e:
                logger.warning("无法初始化 MarketDataFetcher: %s", e)

        if market:
            try:
                lpr_data = market.fetch_shibor_lpr(lpr_start_date, end_date)
            except Exception as e:
                logger.warning("fetch_shibor_lpr 失败: %s", e)
            try:
                cpi_data = market.fetch_cpi(start_m=start_m, end_m=end_m)
            except Exception as e:
                logger.warning("fetch_cpi 失败: %s", e)
            try:
                sf_data = market.fetch_sf_month(start_m=start_m, end_m=end_m)
            except Exception as e:
                logger.warning("fetch_sf_month 失败: %s", e)
            try:
                pmi_data = market.fetch_pmi(start_m=start_m, end_m=end_m)
            except Exception as e:
                logger.warning("fetch_pmi 失败: %s", e)
            try:
                m2_data = market.fetch_m2(start_m=start_m, end_m=end_m)
            except Exception as e:
                logger.warning("fetch_m2 失败: %s", e)
            try:
                gdp_data = market.fetch_gdp(start_q=start_q, end_q=end_q)
            except Exception as e:
                logger.warning("fetch_gdp 失败: %s", e)

        return {
            "trade_date": trade_date,
            "macro_economist_lpr_data": to_serializable(lpr_data),
            "macro_economist_cpi_data": to_serializable(cpi_data),
            "macro_economist_sf_data": to_serializable(sf_data),
            "macro_economist_pmi_data": to_serializable(pmi_data),
            "macro_economist_gdp_data": to_serializable(gdp_data),
            "macro_economist_m2_data": to_serializable(m2_data),
        }

    return macro_economist_fetch_node


# ---------------------------------------------------------------------------
# macro_economist_analysis
# ---------------------------------------------------------------------------

# 默认分析结果结构，用于数据缺失或 LLM 失败时的 fallback
_MACRO_ECONOMIST_DEFAULT = {
    "gdp_trend": "unknown",
    "lpr_trend": "unknown",
    "cpi_trend": "unknown",
    "sf_trend": "unknown",
    "m2_trend": "unknown",
    "pmi_status": "unknown",
    "growth_signal": "unknown",
    "inflation_signal": "unknown",
    "liquidity_signal": "unknown",
    "macro_regime": "unknown",
    "equity_market_bias": "unknown",
    "bond_market_bias": "unknown",
    "commodity_bias": "unknown",
    "liquidity_summary": "",
    "conclusion": "",
}


def _format_data_tail(data, max_rows: int = 8) -> str:
    """将数据截取最近 max_rows 行，便于 LLM 聚焦近期趋势。"""
    if not data:
        return "无数据"
    if isinstance(data, list) and len(data) > max_rows:
        return str(data[-max_rows:])
    return str(data)


def create_macro_economist_analysis_node(llm=None):
    """构建宏观经济分析节点。基于 GDP、CPI、LPR、社融、PMI、M2 进行结构化分析。"""

    def macro_economist_analysis_node(state: Dict, config: Optional[RunnableConfig] = None) -> Dict:
        lpr = state.get("macro_economist_lpr_data")
        cpi = state.get("macro_economist_cpi_data")
        sf = state.get("macro_economist_sf_data")
        pmi = state.get("macro_economist_pmi_data")
        gdp = state.get("macro_economist_gdp_data")
        m2 = state.get("macro_economist_m2_data")

        trade_date = state.get("trade_date") or datetime.now().strftime("%Y%m%d")

        if not lpr and not cpi and not sf and not pmi and not gdp and not m2:
            return {
                "trade_date": trade_date,
                "macro_economist_analysis": {
                    **_MACRO_ECONOMIST_DEFAULT,
                    "liquidity_summary": "数据缺失",
                    "conclusion": "宏观经济数据缺失，无法分析",
                }
            }

        data_summary = []
        if gdp:
            data_summary.append(f"GDP: {len(gdp) if isinstance(gdp, list) else 'N'} 条")
        else:
            data_summary.append("GDP: 无")
        if lpr:
            data_summary.append(f"LPR: {len(lpr) if isinstance(lpr, list) else 'N'} 条")
        else:
            data_summary.append("LPR: 无")
        if cpi:
            data_summary.append(f"CPI: {len(cpi) if isinstance(cpi, list) else 'N'} 条")
        else:
            data_summary.append("CPI: 无")
        if sf:
            data_summary.append(f"社融: {len(sf) if isinstance(sf, list) else 'N'} 条")
        else:
            data_summary.append("社融: 无")
        if pmi:
            data_summary.append(f"PMI: {len(pmi) if isinstance(pmi, list) else 'N'} 条")
        else:
            data_summary.append("PMI: 无")
        if m2:
            data_summary.append(f"M2: {len(m2) if isinstance(m2, list) else 'N'} 条")
        else:
            data_summary.append("M2: 无")

        if llm is None:
            return {
                "trade_date": trade_date,
                "macro_economist_analysis": {
                    **_MACRO_ECONOMIST_DEFAULT,
                    "liquidity_summary": "; ".join(data_summary),
                    "conclusion": f"宏观经济数据已拉取({'; '.join(data_summary)})，未使用 LLM 分析。",
                }
            }

        system_msg = """你是一位宏观经济分析师（Macro Economist）。

任务：根据 GDP、LPR（利率）、CPI、社融、PMI、M2 数据，分析当前宏观经济环境。

请识别以下维度：
1. 经济增长趋势（GDP 同比、社融）
2. 通胀趋势（CPI）
3. 流动性环境（LPR、M2）
4. 制造业景气度（PMI：>50 为扩张 expansion，<50 为收缩 contraction）
5. 对主要资产类别的影响（股、债、商品）

macro_regime 取值说明：
- growth: 经济扩张、增长稳健
- slowdown: 经济增速放缓
- recession: 衰退
- recovery: 复苏
- stagflation: 滞胀（高通胀+低增长）
- liquidity_expansion: 流动性宽松驱动

返回严格 JSON，只输出 JSON，不要其他文字：

{{
  "gdp_trend": "up | down | stable | unknown",
  "lpr_trend": "up | down | stable | unknown",
  "cpi_trend": "up | down | stable | unknown",
  "sf_trend": "up | down | stable | unknown",
  "m2_trend": "up | down | stable | unknown",
  "pmi_status": "expansion | contraction | stable | unknown",
  "growth_signal": "strong | weakening | stable | unknown",
  "inflation_signal": "rising | falling | stable | unknown",
  "liquidity_signal": "loose | neutral | tight | unknown",
  "macro_regime": "growth | slowdown | recession | recovery | stagflation | liquidity_expansion | unknown",
  "equity_market_bias": "bullish | neutral | bearish | unknown",
  "bond_market_bias": "bullish | neutral | bearish | unknown",
  "commodity_bias": "bullish | neutral | bearish | unknown",
  "liquidity_summary": "一句话解释流动性环境",
  "conclusion": "一句话宏观结论"
}}

无数据的指标填 unknown。"""

        human_msg = """【数据】以下为近期数据（按时间由远及近），请分析趋势。

GDP（季度）:
{gdp}

LPR（利率，日度）:
{lpr}

CPI（月度）:
{cpi}

社融（月度）:
{sf}

PMI（月度，制造业 PMI010000 等）:
{pmi}

M2（月度）:
{m2}

请分析并返回上述 JSON。"""

        prompt = ChatPromptTemplate.from_messages(
            [("system", system_msg), ("human", human_msg)]
        )

        try:
            logger.info("正在处理 宏观经济分析：宏观数据 → 分析 JSON")
            chain = prompt | llm
            raw = chain.invoke(
                {
                    "gdp": _format_data_tail(gdp),
                    "lpr": _format_data_tail(lpr),
                    "cpi": _format_data_tail(cpi),
                    "sf": _format_data_tail(sf),
                    "pmi": _format_data_tail(pmi),
                    "m2": _format_data_tail(m2),
                },
                config={**(config or {}), "run_name": "宏观经济分析"},
            )
            data = extract_json_text(raw)
            # 补全可能缺失的字段
            for k, v in _MACRO_ECONOMIST_DEFAULT.items():
                data.setdefault(k, v)
            return {"trade_date": trade_date, "macro_economist_analysis": data}
        except Exception as e:
            logger.warning("macro_economist_analysis LLM 失败: %s", e)
            return {
                "trade_date": trade_date,
                "macro_economist_analysis": {
                    **_MACRO_ECONOMIST_DEFAULT,
                    "liquidity_summary": "; ".join(data_summary),
                    "conclusion": f"LLM 分析失败: {e}",
                }
            }

    return macro_economist_analysis_node


def _write_json_atomic(path: Path, payload: Dict[str, Any]) -> None:
    """原子写入 JSON，避免中途中断留下半成品。"""
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_suffix(path.suffix + ".tmp")
    with open(tmp_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
    os.replace(tmp_path, path)


def create_macro_economist_result_persist_node():
    """将最终输出键 macro_economist_analysis 持久化到本地 artifacts。"""

    def macro_economist_result_persist_node(
        state: Dict[str, Any],
        config: Optional[RunnableConfig] = None,
    ) -> Dict[str, Any]:
        _ = config
        analysis = state.get("macro_economist_analysis")
        if not analysis:
            return state

        trade_date = str(state.get("trade_date") or datetime.now().strftime("%Y%m%d")).replace("-", "")[:8]
        artifact_dir = MACRO_ECONOMIST_ARTIFACT_ROOT / trade_date
        result_path = artifact_dir / "result.json"
        manifest_path = artifact_dir / "manifest.json"

        try:
            _write_json_atomic(result_path, analysis)
            _write_json_atomic(
                manifest_path,
                {
                    "artifact_type": "macro_economist_analysis",
                    "module": "agents.analyst.macro_analyst.macro_economist",
                    "trade_date": trade_date,
                    "created_at": datetime.now().astimezone().isoformat(),
                    "status": "success",
                    "result_path": result_path.as_posix(),
                },
            )
            logger.info("macro_economist_analysis 已写入本地 artifacts: %s", result_path)
            return {
                **state,
                "macro_economist_artifact_path": result_path.as_posix(),
                "macro_economist_manifest_path": manifest_path.as_posix(),
            }
        except Exception as e:
            logger.warning("写入 macro_economist artifacts 失败: %s", e)
            return state

    return macro_economist_result_persist_node
