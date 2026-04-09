"""
Sector Capital Flow Analyst（板块资金流分析师）- 节点函数

专注：同花顺概念/板块资金流（moneyflow_cnt_ths）与申万行业成交额（sw_daily），
给出多窗口（1/5/10/20 日）的板块/行业资金流入/流出前 10 名。
"""
import json
import logging
from datetime import datetime
from typing import Dict, Optional, List, Any, Set

from langchain_core.runnables import RunnableConfig

logger = logging.getLogger(__name__)

from ....utils import date_offset, to_serializable, extract_json_text


# 最大回溯天数：为了覆盖 20 日窗口，适当多留一些缓冲（自然日）
SECTOR_MONEYFLOW_LOOKBACK_DAYS =40
_WINDOWS = (1, 5, 10, 20)


# ---------------------------------------------------------------------------
# sector_capital_flow_fetch
# ---------------------------------------------------------------------------


def _get_ni_ths_codes_from_db() -> Set[str]:
    """从数据库查询 N-概念指数 和 I-行业指数 的板块代码"""
    try:
        from database import ThsIndex, get_session
        session = get_session()
        try:
            # 查询 index_type 为 N 或 I 的板块
            records = session.query(ThsIndex).filter(ThsIndex.index_type.in_(["N", "I"])).all()
            codes = {r.ts_code for r in records if r.ts_code}
            logger.info("从数据库加载 %d 个 N/I 类型同花顺板块代码", len(codes))
            return codes
        finally:
            session.close()
    except Exception as e:
        logger.warning("从数据库获取同花顺板块列表失败: %s", e)
        return set()


def create_sector_capital_flow_fetch_node():
    """
    构建 Sector Capital Flow 数据拉取节点。

    使用 dataflow.industry_data.fetch_moneyflow_cnt_ths_range 拉取
    同花顺概念/板块资金流数据（只保留 N-概念指数 和 I-行业指数）。
    """

    def sector_capital_flow_fetch_node(
        state: Dict,
        config: Optional[RunnableConfig] = None,
    ) -> Dict:
        trade_date = state.get("trade_date") or datetime.now().strftime("%Y%m%d")
        end_date = trade_date.replace("-", "")[:8]
        start_date = date_offset(end_date, days=SECTOR_MONEYFLOW_LOOKBACK_DAYS)

        # 同花顺概念/板块资金流
        try:
            from dataflow.industry_data import fetch_moneyflow_cnt_ths_range
        except Exception as e:  # pragma: no cover - 动态导入失败时的保护
            logger.warning("无法导入 fetch_moneyflow_cnt_ths_range: %s", e)
            fetch_moneyflow_cnt_ths_range = None  # type: ignore[assignment]

        # 从数据库获取 N/I 类型板块代码
        allowed_ths_codes = _get_ni_ths_codes_from_db()

        ths_df = None
        if fetch_moneyflow_cnt_ths_range is not None:
            try:
                ths_df = fetch_moneyflow_cnt_ths_range(start_date=start_date, end_date=end_date)
                # 过滤：只保留 N-概念指数 和 I-行业指数的数据
                if ths_df is not None and not ths_df.empty and allowed_ths_codes:
                    before_count = len(ths_df)
                    ths_df = ths_df[ths_df["ts_code"].isin(allowed_ths_codes)]
                    after_count = len(ths_df)
                    logger.info(
                        "同花顺资金流向数据过滤: %d -> %d (保留 N/I 类型板块)",
                        before_count,
                        after_count,
                    )
            except Exception as e:
                logger.warning(
                    "fetch_moneyflow_cnt_ths_range 失败: start=%s end=%s error=%s",
                    start_date,
                    end_date,
                    e,
                )

        return {
            "sector_moneyflow_data": to_serializable(ths_df),
            "sector_moneyflow_meta": {
                "start_date": start_date,
                "end_date": end_date,
                "lookback_days": SECTOR_MONEYFLOW_LOOKBACK_DAYS,
            },
        }

    return sector_capital_flow_fetch_node


# ---------------------------------------------------------------------------
# sector_capital_flow_analysis
# ---------------------------------------------------------------------------


def create_sector_capital_flow_analysis_node():
    """
    构建 Sector Capital Flow 分析节点。

    逻辑：
    - 同花顺概念/板块：对近 N 日 net_amount 聚合，1/5/10/20 日窗口给出
      资金净流入/净流出前 10 名（sector_capital_flow_top.ths_concept）。
    全程不依赖 LLM，仅做规则与排序。
    """

    def sector_capital_flow_analysis_node(
        state: Dict,
        config: Optional[RunnableConfig] = None,
    ) -> Dict:
        ths_records = state.get("sector_moneyflow_data") or []
        if not ths_records:
            return {"sector_capital_flow_top": {}}

        # 解析结束日期：优先使用 meta.end_date，否则用记录中最大 trade_date
        end_date_str = None
        meta = state.get("sector_moneyflow_meta") or {}
        if isinstance(meta, dict):
            end_date_str = meta.get("end_date")
        if not end_date_str:
            dates: List[str] = []
            for row in ths_records:
                td = row.get("trade_date")
                if not td:
                    continue
                dates.append(str(td).replace("-", "")[:8])
            if dates:
                end_date_str = max(dates)
        if not end_date_str:
            return {"sector_capital_flow_top": {}}

        try:
            end_dt = datetime.strptime(end_date_str, "%Y%m%d")
        except Exception:
            return {"sector_capital_flow_top": {}}

        # ------------------------------
        # 1）同花顺概念/板块资金流（moneyflow_cnt_ths）
        # ------------------------------
        per_ths_code: Dict[str, Dict[str, Any]] = {}
        for row in ths_records:
            try:
                ts_code = row.get("ts_code")
                name = row.get("name", ts_code)
                net_amount = float(row.get("net_amount", 0.0) or 0.0)
                net_buy = float(row.get("net_buy_amount", 0.0) or 0.0)
                net_sell = float(row.get("net_sell_amount", 0.0) or 0.0)
                trade_date = str(row.get("trade_date", "")).replace("-", "")[:8]
                trade_dt = datetime.strptime(trade_date, "%Y%m%d")
            except Exception:
                continue
            if not ts_code:
                continue
            info = per_ths_code.setdefault(ts_code, {"name": name, "rows": []})
            info["rows"].append(
                {
                    "trade_dt": trade_dt,
                    "net_amount": net_amount,
                    "net_buy": net_buy,
                    "net_sell": net_sell,
                }
            )

        # 先为每个 THS 板块、每个窗口计算汇总结果（含强度/持续性/异动/评分等指标）
        ths_windows: Dict[str, Dict[str, Dict[str, Any]]] = {}
        for ts_code, info in per_ths_code.items():
            rows = info.get("rows") or []
            if not rows:
                continue

            windows_data: Dict[str, Dict[str, Any]] = {}
            for w in _WINDOWS:
                # 按日期从近到远排序，窗口内不足 w 天时向前补足最近的 w 个交易日
                rows_sorted = sorted(rows, key=lambda r: r["trade_dt"], reverse=True)
                selected = rows_sorted[:w]
                if not selected:
                    continue

                days = len(selected)
                net_sum = sum(r["net_amount"] for r in selected)
                buy_sum = sum(r["net_buy"] for r in selected)
                sell_sum = sum(r["net_sell"] for r in selected)
                net_avg = net_sum / max(days, 1)

                # 资金强度：净流入占总流量比例
                total_flow = buy_sum + sell_sum
                flow_strength = net_sum / total_flow if total_flow > 0 else 0.0

                # 持续性：正净流入天数占比
                positive_days = sum(1 for r in selected if r["net_amount"] > 0)
                flow_days_ratio = positive_days / days if days > 0 else 0.0

                # 趋势：使用线性回归 slope 近似资金趋势强弱
                selected_sorted = sorted(selected, key=lambda r: r["trade_dt"])
                n = len(selected_sorted)
                if n > 1:
                    # x: 0, 1, ..., n-1；y: 对应日的 net_amount
                    xs = list(range(n))
                    ys = [r["net_amount"] for r in selected_sorted]
                    mean_x = sum(xs) / n
                    mean_y = sum(ys) / n
                    num = sum((x - mean_x) * (y - mean_y) for x, y in zip(xs, ys))
                    den = sum((x - mean_x) ** 2 for x in xs)
                    flow_trend_score = num / den if den != 0 else 0.0
                else:
                    flow_trend_score = 0.0

                # 趋势标签：结合净流入方向与趋势斜率，便于上层/LLM 解读
                # eps 用于过滤极小噪声
                eps = 1e-6
                if flow_trend_score > eps:
                    if net_sum > 0:
                        flow_trend_label = "inflow_strengthening"
                    elif net_sum < 0:
                        flow_trend_label = "outflow_weakening"
                    else:
                        flow_trend_label = "net_flow_up"
                elif flow_trend_score < -eps:
                    if net_sum > 0:
                        flow_trend_label = "inflow_weakening"
                    elif net_sum < 0:
                        flow_trend_label = "outflow_strengthening"
                    else:
                        flow_trend_label = "net_flow_down"
                else:
                    flow_trend_label = "net_flow_flat"

                # 异动：最近一天净流入远大于前 5 日平均
                last = max(selected_sorted, key=lambda r: r["trade_dt"])
                prev = [r for r in selected_sorted if r["trade_dt"] < last["trade_dt"]]
                prev_5 = prev[-5:]
                if prev_5:
                    prev_avg = sum(r["net_amount"] for r in prev_5) / len(prev_5)
                    capital_spike = last["net_amount"] > prev_avg * 2
                else:
                    capital_spike = False

                # 资金评分模型：兼顾强度、均值、持续性与异动（对 net_avg 做简单缩放）
                net_avg_scaled = net_avg / 10.0
                capital_flow_score = (
                    0.4 * flow_strength
                    + 0.3 * net_avg_scaled
                    + 0.2 * flow_days_ratio
                    + 0.1 * (1.0 if capital_spike else 0.0)
                )

                windows_data[f"{w}d"] = {
                    "ts_code": ts_code,
                    "name": info.get("name", ts_code),
                    "days": days,
                    "net_amount_sum": round(net_sum, 2),
                    "net_amount_avg": round(net_avg, 2),
                    "net_buy_sum": round(buy_sum, 2),
                    "net_sell_sum": round(sell_sum, 2),
                    "flow_strength": round(flow_strength, 4),
                    "flow_days_ratio": round(flow_days_ratio, 4),
                    "flow_trend_score": round(flow_trend_score, 6),
                    "flow_trend_label": flow_trend_label,
                    "capital_spike": capital_spike,
                    "capital_flow_score": round(capital_flow_score, 4),
                }

            if windows_data:
                ths_windows[ts_code] = windows_data

        # 按窗口汇总出：THS 概念/板块流入最多 / 流出最多的前 10 名，并附加 window 与 rank
        ths_top_by_window: Dict[str, Dict[str, List[Dict[str, Any]]]] = {}
        for w in _WINDOWS:
            key = f"{w}d"
            entries: List[Dict[str, Any]] = []
            for ts_code, win_map in ths_windows.items():
                data = win_map.get(key)
                if not data:
                    continue
                entries.append(data)
            if not entries:
                continue

            inflow_sorted = sorted(entries, key=lambda x: x["net_amount_sum"], reverse=True)
            outflow_sorted = sorted(entries, key=lambda x: x["net_amount_sum"])

            # 为每条结果标记 window 与在该窗口内的排名
            for idx, item in enumerate(inflow_sorted, start=1):
                item["window"] = key
                item["rank_inflow"] = idx
            for idx, item in enumerate(outflow_sorted, start=1):
                item["window"] = key
                item["rank_outflow"] = idx

            ths_top_by_window[key] = {
                "top_inflow": inflow_sorted[:10],
                "top_outflow": outflow_sorted[:10],
            }

        return {
            "sector_capital_flow_top": {
                "ths_concept": ths_top_by_window,
            },
        }

    return sector_capital_flow_analysis_node


# ---------------------------------------------------------------------------
# sector_capital_flow_llm_map (按数据源拆分，单次输入约减半)
# ---------------------------------------------------------------------------

_SECTOR_FLOW_INSIGHT_DEFAULT = {
    "summary": "",
    "conclusion": "",
    "highlights": [],
    "market_bias": "neutral",
    "hot_sectors": [],
    "risk_sectors": [],
}


_MAP_THS_FIELDS = """【同花顺 ths_concept 每条字段】
- ts_code、name：代码、名称；days：窗口内交易日天数
- net_amount_sum：净流入金额(万元)，正=流入负=流出；flow_strength：净流入占买卖比例
- flow_days_ratio：净流入为正的天数占比；flow_trend_score：趋势斜率，正=流入加大
- flow_trend_label：趋势标签(inflow_strengthening/weakening、outflow_strengthening/weakening、net_flow_flat)
- capital_spike：是否异动；capital_flow_score：综合资金评分；window、rank_inflow、rank_outflow
键 "1d"/"5d"/"10d"/"20d" 为近 1/5/10/20 个交易日窗口；每窗口有 top_inflow（净流入前10）、top_outflow（净流出前10）。"""


def create_sector_capital_flow_insight_node(llm):
    """
    构建板块资金流洞察节点（直接基于分析数据生成最终洞察）。
    """
    def _insight_node(state, config=None):
        top = state.get("sector_capital_flow_top") or {}
        data = top.get("ths_concept") or {}
        
        if not data:
            return {
                "sector_capital_flow_insight": {
                    **_SECTOR_FLOW_INSIGHT_DEFAULT,
                    "summary": "无有效资金流解读",
                    "conclusion": "无法生成结论",
                }
            }

        from langchain_core.prompts import ChatPromptTemplate
        import json

        data_str = json.dumps(data, ensure_ascii=False, indent=2)
        system_msg = f"""你是一位板块资金流分析师。下方是「同花顺概念/板块」多窗口资金流 Top 的 JSON，请根据字段含义做解读并生成最终结论。

{_MAP_THS_FIELDS}

解读要点：
1. 概括整体资金流向与结构（主线方向、资金认可与持续性）
2. 判断短中长期是否一致
3. 列出 hot_sectors（资金持续流入/强度居前的板块名称）
4. 列出 risk_sectors（持续流出或需警惕的板块名称）

返回严格 JSON，只输出 JSON：
{{{{
  "summary": "概括整体资金流向与结构",
  "conclusion": "结论（可含操作或观望建议）",
  "highlights": ["要点1", "要点2"],
  "market_bias": "bullish | neutral | bearish",
  "hot_sectors": ["热点板块/行业名称列表"],
  "risk_sectors": ["风险/弱势板块/行业名称列表"]
}}}}
market_bias 表示整体资金面偏多/中性/偏空。无明显方向时 market_bias 填 neutral。"""

        human_msg = """【同花顺概念/板块 数据】
{data}

请分析并返回上述 JSON。"""

        prompt = ChatPromptTemplate.from_messages(
            [("system", system_msg), ("human", human_msg)]
        )
        logger.info("正在处理 板块资金流洞察")
        chain = prompt | llm
        raw = chain.invoke(
            {"data": data_str},
            config={**(config or {}), "run_name": "板块资金流洞察"},
        )
        data = extract_json_text(raw)
        for k, v in _SECTOR_FLOW_INSIGHT_DEFAULT.items():
            data.setdefault(k, v)
        return {"sector_capital_flow_insight": data}

    return _insight_node


__all__ = [
    "create_sector_capital_flow_fetch_node",
    "create_sector_capital_flow_analysis_node",
    "create_sector_capital_flow_insight_node",
]
