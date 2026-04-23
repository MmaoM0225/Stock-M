"""
Stock Screener（股票筛选分析师）- 节点实现

使用 daily_basic（每日指标）拉取 PE/PB、总市值 total_mv 等；合并 stock_basic 补全名称、行业、上市日。
"""
import logging
import json
import os
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

import pandas as pd
from langchain_core.prompts import ChatPromptTemplate
from pydantic import BaseModel, ConfigDict, Field

from .criteria import ScreenerCriteria

logger = logging.getLogger(__name__)
_SECTOR_MANAGER_ARTIFACT_ROOT = Path("data") / "artifacts" / "manager" / "sector_manager"
_STOCK_SCREENER_ARTIFACT_ROOT = Path("data") / "artifacts" / "analyst" / "stock_analyst" / "stock_screener"

_SECTOR_TEMPLATE_LIBRARY: Dict[str, Dict[str, Any]] = {
    "value_defensive": {
        "min_market_cap": 150e8,
        "max_pe": 18.0,
        "max_pb": 2.5,
        "sort_by": "dv_ratio",
        "sort_order": "desc",
    },
    "quality_growth": {
        "min_market_cap": 80e8,
        "max_pe": 45.0,
        "max_pb": 8.0,
        "sort_by": "pe",
        "sort_order": "asc",
    },
    "cyclical_rebound": {
        "min_market_cap": 60e8,
        "max_pe": 30.0,
        "max_pb": 3.5,
        "sort_by": "pb",
        "sort_order": "asc",
    },
    "theme_momentum": {
        "min_market_cap": 40e8,
        "max_pe": 100.0,
        "max_pb": 15.0,
        "sort_by": "volume_ratio",
        "sort_order": "desc",
    },
    "fallback_balanced": {
        "min_market_cap": 70e8,
        "max_pe": 60.0,
        "max_pb": 10.0,
        "sort_by": "total_mv",
        "sort_order": "desc",
    },
}

_TEMPLATE_OVERRIDE_KEYS: Set[str] = {
    "min_market_cap",
    "max_market_cap",
    "min_pe",
    "max_pe",
    "min_pb",
    "max_pb",
    "sort_by",
    "sort_order",
}

_TEMPLATE_OVERRIDE_BOUNDS: Dict[str, Tuple[float, float]] = {
    "min_market_cap": (10e8, 3000e8),
    "max_market_cap": (10e8, 5000e8),
    "min_pe": (0.0, 120.0),
    "max_pe": (0.0, 200.0),
    "min_pb": (0.0, 20.0),
    "max_pb": (0.0, 30.0),
}


class SectorTemplateDecision(BaseModel):
    template_id: str = Field(description="模板ID")
    overrides: Dict[str, Any] = Field(default_factory=dict, description="模板微调字段")
    reason: str = Field(default="", description="选择理由")
    confidence: float = Field(default=0.6, ge=0.0, le=1.0, description="置信度")

    model_config = ConfigDict(extra="ignore")


class SectorTemplatePlan(BaseModel):
    decisions: Dict[str, SectorTemplateDecision] = Field(default_factory=dict)

    model_config = ConfigDict(extra="ignore")


def _ordered_unique_strings(items: List[Any], max_items: Optional[int] = None) -> List[str]:
    """去重并保序，过滤空值。"""
    out: List[str] = []
    seen = set()
    for item in items:
        if item is None:
            continue
        text = str(item).strip()
        if not text or text in seen:
            continue
        seen.add(text)
        out.append(text)
        if max_items is not None and len(out) >= max_items:
            break
    return out


def _load_sector_manager_sectors_from_artifact(trade_date: Any) -> List[str]:
    """
    从 sector_manager 的 result.json 动态提取板块：
    - 优先 favored_sectors
    - 其次 watchlist_sectors
    """
    trade_date_text = str(trade_date or "").replace("-", "")[:8]
    if not trade_date_text:
        return []

    result_path = _SECTOR_MANAGER_ARTIFACT_ROOT / trade_date_text / "result.json"
    if not result_path.exists():
        logger.info("未命中 sector_manager artifact: %s", result_path)
        return []

    try:
        with open(result_path, "r", encoding="utf-8") as f:
            payload = json.load(f) or {}
    except Exception as e:
        logger.warning("读取 sector_manager artifact 失败: %s, error=%s", result_path, e)
        return []

    favored = payload.get("favored_sectors") or []
    watchlist = payload.get("watchlist_sectors") or []
    sectors = _ordered_unique_strings([*favored, *watchlist], max_items=16)
    logger.info(
        "加载 sector_manager 板块完成: trade_date=%s, favored=%d, watchlist=%d, merged=%d",
        trade_date_text,
        len(favored),
        len(watchlist),
        len(sectors),
    )
    return sectors


def _resolve_ths_sector_members(sector_names: List[str]) -> Set[str]:
    """
    将同花顺板块名称解析为成分股代码集合（ts_code）。

    仅使用数据库中的 N/I 类型同花顺板块，确保与上游板块口径一致。
    """
    if not sector_names:
        return set()

    try:
        from database import ThsIndex, get_session
        from dataflow.market_data import fetch_ths_member
    except Exception as e:
        logger.warning("导入同花顺板块依赖失败: %s", e)
        return set()

    sector_names_clean = [str(x).strip() for x in sector_names if str(x).strip()]
    if not sector_names_clean:
        return set()

    session = get_session()
    try:
        records = (
            session.query(ThsIndex)
            .filter(ThsIndex.index_type.in_(["N", "I"]))
            .all()
        )
    finally:
        session.close()

    # 同名板块取第一个，避免重复映射导致不确定性
    name_to_code: Dict[str, str] = {}
    for r in records:
        name = (getattr(r, "name", None) or "").strip()
        code = (getattr(r, "ts_code", None) or "").strip()
        if name and code and name not in name_to_code:
            name_to_code[name] = code

    target_codes = [name_to_code[name] for name in sector_names_clean if name in name_to_code]
    missing_names = [name for name in sector_names_clean if name not in name_to_code]
    if missing_names:
        logger.warning("以下板块不在同花顺 N/I 列表中，已跳过: %s", missing_names)

    member_codes: Set[str] = set()
    for ths_code in target_codes:
        try:
            member_df = fetch_ths_member(ts_code=ths_code)
            if member_df is None or member_df.empty or "con_code" not in member_df.columns:
                continue
            one_codes = (
                member_df["con_code"]
                .dropna()
                .astype(str)
                .str.strip()
                .tolist()
            )
            member_codes.update(code for code in one_codes if code)
        except Exception as e:
            logger.warning("获取同花顺板块成分失败: %s, error=%s", ths_code, e)

    logger.info(
        "同花顺板块过滤准备完成: 输入板块=%d, 命中板块=%d, 成分股=%d",
        len(sector_names_clean),
        len(target_codes),
        len(member_codes),
    )
    return member_codes


def _resolve_ths_sector_members_by_name(sector_names: List[str]) -> Dict[str, Set[str]]:
    """
    将同花顺板块名称解析为「板块 -> 成分股代码集合」。

    仅使用数据库中的 N/I 类型同花顺板块，确保与上游板块口径一致。
    """
    if not sector_names:
        return {}

    try:
        from database import ThsIndex, get_session
        from dataflow.market_data import fetch_ths_member
    except Exception as e:
        logger.warning("导入同花顺板块依赖失败: %s", e)
        return {}

    sector_names_clean = [str(x).strip() for x in sector_names if str(x).strip()]
    if not sector_names_clean:
        return {}

    session = get_session()
    try:
        records = (
            session.query(ThsIndex)
            .filter(ThsIndex.index_type.in_(["N", "I"]))
            .all()
        )
    finally:
        session.close()

    name_to_code: Dict[str, str] = {}
    for r in records:
        name = (getattr(r, "name", None) or "").strip()
        code = (getattr(r, "ts_code", None) or "").strip()
        if name and code and name not in name_to_code:
            name_to_code[name] = code

    sector_to_codes: Dict[str, Set[str]] = {}
    missing_names: List[str] = []
    for name in sector_names_clean:
        ths_code = name_to_code.get(name)
        if not ths_code:
            missing_names.append(name)
            continue
        try:
            member_df = fetch_ths_member(ts_code=ths_code)
            if member_df is None or member_df.empty or "con_code" not in member_df.columns:
                sector_to_codes[name] = set()
                continue
            one_codes = (
                member_df["con_code"]
                .dropna()
                .astype(str)
                .str.strip()
                .tolist()
            )
            sector_to_codes[name] = {code for code in one_codes if code}
        except Exception as e:
            logger.warning("获取同花顺板块成分失败: %s(%s), error=%s", name, ths_code, e)
            sector_to_codes[name] = set()

    if missing_names:
        logger.warning("以下板块不在同花顺 N/I 列表中，已跳过: %s", missing_names)

    logger.info(
        "同花顺板块映射准备完成: 输入板块=%d, 命中板块=%d",
        len(sector_names_clean),
        len([k for k, v in sector_to_codes.items() if v]),
    )
    return sector_to_codes


def _allocate_balanced_codes_by_sector(
    ordered_codes: List[str],
    sector_order: List[str],
    sector_members: Dict[str, Set[str]],
    max_stocks: int,
) -> Tuple[List[str], Dict[str, int]]:
    """
    基于板块做均衡分配，避免全局排序后被单一板块吃满名额。

    分配策略：
    1) 先按 primary sector（按 sector_order 首个命中）分桶；
    2) 先做均分配额；
    3) 再轮询补位；
    4) 最后按全局顺序回填，确保数量尽量达到 max_stocks。
    """
    if max_stocks <= 0 or not ordered_codes:
        return [], {}

    selected: List[str] = []
    selected_set: Set[str] = set()
    code_primary_sector: Dict[str, str] = {}

    for code in ordered_codes:
        for sec in sector_order:
            if code in sector_members.get(sec, set()):
                code_primary_sector[code] = sec
                break

    buckets: Dict[str, List[str]] = {sec: [] for sec in sector_order}
    for code in ordered_codes:
        sec = code_primary_sector.get(code)
        if sec:
            buckets[sec].append(code)

    active_sectors = [sec for sec in sector_order if buckets.get(sec)]
    if not active_sectors:
        fallback = ordered_codes[:max_stocks]
        return fallback, {}

    base_quota = max_stocks // len(active_sectors)
    if base_quota <= 0:
        base_quota = 1

    pointers: Dict[str, int] = {sec: 0 for sec in active_sectors}
    sector_counts: Dict[str, int] = {sec: 0 for sec in active_sectors}

    # 第一轮：均分配额
    for sec in active_sectors:
        bucket = buckets[sec]
        while pointers[sec] < len(bucket) and sector_counts[sec] < base_quota and len(selected) < max_stocks:
            code = bucket[pointers[sec]]
            pointers[sec] += 1
            if code in selected_set:
                continue
            selected.append(code)
            selected_set.add(code)
            sector_counts[sec] += 1

    # 第二轮：轮询补位
    progressed = True
    while len(selected) < max_stocks and progressed:
        progressed = False
        for sec in active_sectors:
            bucket = buckets[sec]
            while pointers[sec] < len(bucket):
                code = bucket[pointers[sec]]
                pointers[sec] += 1
                if code in selected_set:
                    continue
                selected.append(code)
                selected_set.add(code)
                sector_counts[sec] += 1
                progressed = True
                break
            if len(selected) >= max_stocks:
                break

    # 第三轮：全局回填（兜底）
    if len(selected) < max_stocks:
        for code in ordered_codes:
            if code in selected_set:
                continue
            selected.append(code)
            selected_set.add(code)
            if len(selected) >= max_stocks:
                break

    return selected[:max_stocks], sector_counts


def _merge_template_into_criteria(base: ScreenerCriteria, template_id: str) -> ScreenerCriteria:
    """将模板参数合并到基础条件；用户显式传入的字段优先。"""
    template = _SECTOR_TEMPLATE_LIBRARY.get(template_id) or _SECTOR_TEMPLATE_LIBRARY["fallback_balanced"]
    merged = base.to_dict()
    for k, v in template.items():
        if merged.get(k) is None:
            merged[k] = v
    merged["sectors"] = base.sectors
    merged["exclude_st"] = base.exclude_st
    merged["exclude_delisting"] = base.exclude_delisting
    merged["min_listing_days"] = base.min_listing_days
    merged["max_stocks"] = base.max_stocks
    return ScreenerCriteria.from_dict(merged)


def _sanitize_template_overrides(overrides: Dict[str, Any]) -> Dict[str, Any]:
    """清洗模板微调字段，限制白名单和数值范围。"""
    out: Dict[str, Any] = {}
    for k, v in (overrides or {}).items():
        if k not in _TEMPLATE_OVERRIDE_KEYS:
            continue
        if k in ("sort_by", "sort_order"):
            out[k] = v
            continue
        try:
            numeric = float(v)
        except (TypeError, ValueError):
            continue
        lo, hi = _TEMPLATE_OVERRIDE_BOUNDS.get(k, (-1e18, 1e18))
        numeric = max(lo, min(hi, numeric))
        out[k] = numeric
    return out


def _apply_overrides_to_criteria(base: ScreenerCriteria, overrides: Dict[str, Any]) -> ScreenerCriteria:
    merged = base.to_dict()
    merged.update(_sanitize_template_overrides(overrides))
    merged["sectors"] = base.sectors
    merged["exclude_st"] = base.exclude_st
    merged["exclude_delisting"] = base.exclude_delisting
    merged["min_listing_days"] = base.min_listing_days
    merged["max_stocks"] = base.max_stocks
    candidate = ScreenerCriteria.from_dict(merged)
    errors = candidate.validate()
    if errors:
        logger.warning("模板微调参数无效，回退模板默认: %s", errors)
        return base
    return candidate


def _infer_template_id_for_sector(sector_name: str) -> str:
    """
    返回 fallback 模板。
    模板选择已改为由 LLM 直接决策，不再使用关键词匹配。
    """
    return "fallback_balanced"


def _apply_filters_without_sector(df: pd.DataFrame, criteria: ScreenerCriteria) -> pd.DataFrame:
    """应用非板块过滤条件（市值、估值、上市日）。"""
    out = df.copy()

    if criteria.min_market_cap is not None or criteria.max_market_cap is not None:
        if "total_mv" not in out.columns:
            logger.warning("缺少 total_mv 列，跳过市值过滤")
        else:
            mv_yuan = pd.to_numeric(out["total_mv"], errors="coerce") * 10000.0
            if criteria.min_market_cap is not None:
                out = out[mv_yuan >= float(criteria.min_market_cap)]
            if criteria.max_market_cap is not None:
                out = out[mv_yuan <= float(criteria.max_market_cap)]

    if criteria.min_pe is not None and "pe" in out.columns:
        out = out[out["pe"] >= criteria.min_pe]
    if criteria.max_pe is not None and "pe" in out.columns:
        out = out[out["pe"] <= criteria.max_pe]

    if criteria.min_pb is not None and "pb" in out.columns:
        out = out[out["pb"] >= criteria.min_pb]
    if criteria.max_pb is not None and "pb" in out.columns:
        out = out[out["pb"] <= criteria.max_pb]

    if criteria.max_price is not None:
        if "close" not in out.columns:
            logger.warning("缺少 close 列，跳过股价上限过滤")
        else:
            close_price = pd.to_numeric(out["close"], errors="coerce")
            out = out[close_price <= float(criteria.max_price)]

    if criteria.min_listing_days and "list_date" in out.columns:
        from datetime import datetime, timedelta
        cutoff_date = (datetime.now() - timedelta(days=criteria.min_listing_days)).strftime("%Y%m%d")
        out["list_date_str"] = out["list_date"].astype(str)
        out = out[out["list_date_str"] <= cutoff_date]

    return out


def _sort_by_criteria(df: pd.DataFrame, criteria: ScreenerCriteria) -> pd.DataFrame:
    """按 criteria 排序并返回新 DataFrame。"""
    out = df.copy()
    requested_sort = criteria.sort_by or "total_mv"
    fallback_candidates = [
        "total_mv", "circ_mv", "total_share", "float_share", "pe", "pe_ttm", "pb",
        "ps", "ps_ttm", "close", "dv_ratio", "dv_ttm", "turnover_rate",
        "volume_ratio", "eps", "ts_code",
    ]
    sort_candidates = [requested_sort] + [c for c in fallback_candidates if c != requested_sort]

    sort_column = None
    for candidate in sort_candidates:
        if candidate not in out.columns:
            continue
        if candidate != "ts_code" and out[candidate].notna().sum() == 0:
            continue
        sort_column = candidate
        break
    if sort_column is None:
        sort_column = "ts_code"

    ascending = (criteria.sort_order == "asc")
    if sort_column != "ts_code":
        out[sort_column] = pd.to_numeric(out[sort_column], errors="coerce")
        out = out.sort_values(by=sort_column, ascending=ascending, na_position="last")
    else:
        out = out.sort_values(by=sort_column, ascending=ascending)
    return out


def _allocate_from_sector_rankings(
    sector_ranked_codes: Dict[str, List[str]],
    sector_order: List[str],
    max_stocks: int,
) -> Tuple[List[str], Dict[str, int]]:
    """从每板块排序结果中做均衡分配（先均分，再轮询补位）。"""
    if max_stocks <= 0:
        return [], {}
    active = [sec for sec in sector_order if sector_ranked_codes.get(sec)]
    if not active:
        return [], {}

    base_quota = max(1, max_stocks // len(active))
    pointers = {sec: 0 for sec in active}
    picks = {sec: 0 for sec in active}
    selected: List[str] = []
    selected_set: Set[str] = set()

    for sec in active:
        codes = sector_ranked_codes[sec]
        while pointers[sec] < len(codes) and picks[sec] < base_quota and len(selected) < max_stocks:
            code = str(codes[pointers[sec]])
            pointers[sec] += 1
            if code in selected_set:
                continue
            selected.append(code)
            selected_set.add(code)
            picks[sec] += 1

    progressed = True
    while len(selected) < max_stocks and progressed:
        progressed = False
        for sec in active:
            codes = sector_ranked_codes[sec]
            while pointers[sec] < len(codes):
                code = str(codes[pointers[sec]])
                pointers[sec] += 1
                if code in selected_set:
                    continue
                selected.append(code)
                selected_set.add(code)
                picks[sec] += 1
                progressed = True
                break
            if len(selected) >= max_stocks:
                break
    return selected, picks


def _normalize_template_plan(
    sectors: List[str],
    raw_plan: Optional[Dict[str, Any]],
) -> Dict[str, Dict[str, Any]]:
    """
    标准化板块模板计划，确保每个板块都有可用模板，低置信度或非法值回退到关键词类比。
    """
    plan = raw_plan or {}
    decisions = plan.get("decisions") if isinstance(plan.get("decisions"), dict) else {}
    normalized: Dict[str, Dict[str, Any]] = {}
    for sec in sectors:
        raw = decisions.get(sec) if isinstance(decisions.get(sec), dict) else {}
        template_id = str(raw.get("template_id") or "").strip()
        confidence = raw.get("confidence", 0.0)
        try:
            confidence_val = float(confidence)
        except (TypeError, ValueError):
            confidence_val = 0.0
        if template_id not in _SECTOR_TEMPLATE_LIBRARY or confidence_val < 0.45:
            template_id = _infer_template_id_for_sector(sec)
        normalized[sec] = {
            "template_id": template_id,
            "overrides": _sanitize_template_overrides(raw.get("overrides") or {}),
            "reason": str(raw.get("reason") or ""),
            "confidence": confidence_val,
        }
    return normalized


def create_select_sector_templates_node(llm: Optional[Any] = None):
    """
    根据板块选择筛选模板的决策节点。

    优先级：
    1) state.sector_template_plan（外部已给定）
    2) LLM 决策（若传入 llm）
    3) 关键词类比兜底
    """
    def select_sector_templates_node(state: Dict[str, Any]) -> Dict[str, Any]:
        criteria: ScreenerCriteria = state.get("_criteria")
        sectors = [str(x).strip() for x in (criteria.sectors or []) if str(x).strip()] if criteria else []
        if not criteria or len(sectors) <= 1:
            return {
                **state,
                "_sector_template_plan": {},
            }

        # 外部注入计划（例如由上游 manager 的 agent 先决策）
        preset_plan = state.get("sector_template_plan")
        if isinstance(preset_plan, dict):
            return {
                **state,
                "_sector_template_plan": _normalize_template_plan(sectors, preset_plan),
            }

        if llm is None:
            fallback_plan = {
                "decisions": {
                    sec: {
                        "template_id": "fallback_balanced",
                        "overrides": {},
                        "reason": "LLM不可用，使用平衡模板兜底",
                        "confidence": 0.5,
                    }
                    for sec in sectors
                }
            }
            return {
                **state,
                "_sector_template_plan": _normalize_template_plan(sectors, fallback_plan),
            }

        try:
            macro_env = state.get("macro_environment") or {}
            market_sentiment = state.get("market_sentiment") or {}
            prompt = ChatPromptTemplate.from_messages(
                [
                    (
                        "system",
                        "你是A股选股筛选模板决策助手。"
                        "你只能从给定模板库中为每个板块选择 template_id，并给出少量 overrides。"
                        "严禁输出模板库外的 template_id。"
                        "overrides 仅允许字段：min_market_cap,max_market_cap,min_pe,max_pe,min_pb,max_pb,sort_by,sort_order。"
                        "如果把握不高，confidence 应低于0.45，系统会回退默认类比。"
                    ),
                    (
                        "human",
                        "板块列表：{sectors}\n"
                        "模板库：{template_library}\n"
                        "宏观环境：{macro_env}\n"
                        "市场情绪：{market_sentiment}\n"
                        "请输出结构化 decisions。",
                    ),
                ]
            )
            structured_llm = llm.with_structured_output(SectorTemplatePlan)
            chain = prompt | structured_llm
            result = chain.invoke(
                {
                    "sectors": ", ".join(sectors),
                    "template_library": json.dumps(_SECTOR_TEMPLATE_LIBRARY, ensure_ascii=False),
                    "macro_env": json.dumps(macro_env, ensure_ascii=False),
                    "market_sentiment": json.dumps(market_sentiment, ensure_ascii=False),
                }
            )
            raw_plan = result.model_dump() if hasattr(result, "model_dump") else {}
            return {
                **state,
                "_sector_template_plan": _normalize_template_plan(sectors, raw_plan),
            }
        except Exception as e:
            logger.warning("模板决策 LLM 失败，回退平衡模板: %s", e)
            fallback_plan = {
                "decisions": {
                    sec: {
                        "template_id": "fallback_balanced",
                        "overrides": {},
                        "reason": "LLM决策失败，使用平衡模板兜底",
                        "confidence": 0.5,
                    }
                    for sec in sectors
                }
            }
            return {
                **state,
                "_sector_template_plan": _normalize_template_plan(sectors, fallback_plan),
            }

    return select_sector_templates_node


def create_parse_criteria_node():
    """解析筛选条件节点"""

    def parse_criteria_node(state: Dict[str, Any]) -> Dict[str, Any]:
        """从 state 中解析筛选条件"""
        sectors = state.get("sectors")
        if not sectors:
            sectors = _load_sector_manager_sectors_from_artifact(state.get("trade_date"))

        criteria_dict = {
            "sectors": sectors,
            "exclude_st": state.get("exclude_st", True),
            "exclude_delisting": state.get("exclude_delisting", True),
            "min_listing_days": state.get("min_listing_days", 180),
            "min_market_cap": state.get("min_market_cap"),
            "max_market_cap": state.get("max_market_cap"),
            "min_pe": state.get("min_pe"),
            "max_pe": state.get("max_pe"),
            "min_pb": state.get("min_pb"),
            "max_pb": state.get("max_pb"),
            "max_price": state.get("max_price"),
            "max_stocks": state.get("max_stocks", 100),
            "sort_by": state.get("sort_by", "total_mv"),
            "sort_order": state.get("sort_order", "desc"),
        }

        criteria = ScreenerCriteria.from_dict(criteria_dict)

        # 验证条件
        errors = criteria.validate()
        if errors:
            logger.warning(f"筛选条件验证失败: {errors}")
            return {
                **state,
                "_criteria": criteria,
                "_criteria_errors": errors,
                "screener_result": {"error": "; ".join(errors)}
            }

        logger.info(f"筛选条件: {criteria.get_filter_summary()}")

        return {
            **state,
            "_criteria": criteria,
            "_criteria_errors": None,
            "sectors": sectors,
        }

    return parse_criteria_node


def create_fetch_stock_pool_node():
    """获取初始股票池节点"""

    def fetch_stock_pool_node(state: Dict[str, Any]) -> Dict[str, Any]:
        """从 tushare daily_basic 拉取当日指标，并合并 stock_basic 的名称/行业/上市日。"""
        criteria: ScreenerCriteria = state.get("_criteria")
        trade_date = state.get("trade_date")

        if not criteria:
            return {**state, "_raw_stock_list": [], "_fetch_error": "缺少筛选条件"}

        if not trade_date:
            return {**state, "_raw_stock_list": [], "_fetch_error": "缺少交易日期"}

        try:
            from dataflow.market_data import fetch_daily_basic, fetch_stock_basic

            daily_fields = (
                "ts_code,trade_date,close,turnover_rate,turnover_rate_f,volume_ratio,"
                "pe,pe_ttm,pb,ps,ps_ttm,dv_ratio,dv_ttm,"
                "total_share,float_share,free_share,total_mv,circ_mv"
            )
            df_daily = fetch_daily_basic(trade_date=trade_date, fields=daily_fields)

            if df_daily.empty:
                return {
                    **state,
                    "_raw_stock_list": [],
                    "_fetch_error": f"未能获取 {trade_date} 的 daily_basic 数据",
                }

            df_basic = fetch_stock_basic(
                exchange="",
                list_status="L",
                fields="ts_code,name,industry,list_date",
            )
            if df_basic.empty:
                logger.warning("stock_basic 为空，名称/行业将为空，ST 过滤可能不完整")
                df_basic = pd.DataFrame(columns=["ts_code", "name", "industry", "list_date"])

            df = df_daily.merge(df_basic, on="ts_code", how="left")
            df["name"] = df["name"].fillna("").astype(str)
            df["industry"] = df["industry"].fillna("").astype(str)

            # 基础过滤
            if criteria.exclude_st:
                df = df[~df["name"].str.contains("ST", na=False)]

            if criteria.exclude_delisting:
                df = df[~df["name"].str.contains("退", na=False)]

            logger.info(f"获取 {trade_date} 股票池: {len(df)} 只")

            return {
                **state,
                "_raw_stock_list": df.to_dict("records"),
                "_fetch_error": None,
            }

        except Exception as e:
            logger.exception("获取股票池失败")
            return {
                **state,
                "_raw_stock_list": [],
                "_fetch_error": str(e),
                "screener_result": {"error": f"获取股票池失败: {e}"}
            }

    return fetch_stock_pool_node


def create_apply_filters_node():
    """应用筛选条件节点"""

    def apply_filters_node(state: Dict[str, Any]) -> Dict[str, Any]:
        """对原始股票池应用筛选条件（支持PE、PB、市值等财务指标）"""
        criteria: ScreenerCriteria = state.get("_criteria")
        raw_stocks = state.get("_raw_stock_list", [])
        trade_date = state.get("trade_date")
        sector_template_plan = state.get("_sector_template_plan") or {}

        if not criteria:
            return {**state, "_filtered_stocks": [], "_filter_error": "缺少筛选条件"}

        if not raw_stocks:
            return {**state, "_filtered_stocks": [], "_filter_error": "股票池为空"}

        try:
            # 转换为 DataFrame
            df = pd.DataFrame(raw_stocks)

            sector_members_map: Dict[str, Set[str]] = {}

            # 板块过滤：仅按同花顺板块（N/I）成分股过滤，保持与上游口径一致
            if criteria.sectors:
                sector_members_map = _resolve_ths_sector_members_by_name(criteria.sectors)
                ths_member_codes: Set[str] = set()
                for codes in sector_members_map.values():
                    ths_member_codes.update(codes)
                if ths_member_codes:
                    df = df[df["ts_code"].isin(ths_member_codes)]
                else:
                    logger.warning("未解析到同花顺板块成分，板块过滤结果为空")
                    df = df.iloc[0:0]
                logger.info(f"板块过滤后: {len(df)} 只")

            df = _apply_filters_without_sector(df, criteria)
            logger.info(f"基础过滤后: {len(df)} 只")
            df = _sort_by_criteria(df, criteria)

            # 限制数量：多板块时优先做板块均衡分配，避免被单一板块吃满
            sector_template_applied: Dict[str, str] = {}
            sector_pick_counts: Dict[str, int] = {}
            if criteria.sectors and len(criteria.sectors) > 1 and sector_members_map:
                sector_order = [str(x).strip() for x in criteria.sectors if str(x).strip()]
                sector_ranked_codes: Dict[str, List[str]] = {}
                for sec in sector_order:
                    plan_item = sector_template_plan.get(sec) if isinstance(sector_template_plan, dict) else {}
                    template_id = str(plan_item.get("template_id") or _infer_template_id_for_sector(sec))
                    sector_template_applied[sec] = template_id
                    sec_criteria = _merge_template_into_criteria(criteria, template_id)
                    if isinstance(plan_item, dict) and plan_item.get("overrides"):
                        sec_criteria = _apply_overrides_to_criteria(sec_criteria, plan_item.get("overrides") or {})
                    sec_codes = sector_members_map.get(sec, set())
                    sec_df = df[df["ts_code"].astype(str).isin(sec_codes)].copy()
                    sec_df = _apply_filters_without_sector(sec_df, sec_criteria)
                    sec_df = _sort_by_criteria(sec_df, sec_criteria)
                    sector_ranked_codes[sec] = sec_df["ts_code"].astype(str).tolist()

                selected_codes, sector_pick_counts = _allocate_from_sector_rankings(
                    sector_ranked_codes=sector_ranked_codes,
                    sector_order=sector_order,
                    max_stocks=criteria.max_stocks,
                )
                if selected_codes:
                    selected_order = {code: idx for idx, code in enumerate(selected_codes)}
                    df = df[df["ts_code"].astype(str).isin(selected_codes)].copy()
                    df["_selected_rank"] = df["ts_code"].astype(str).map(selected_order)
                    df = df.sort_values(by="_selected_rank", ascending=True).drop(columns=["_selected_rank"])
                else:
                    df = df.head(criteria.max_stocks)
                logger.info("板块均衡分配结果: %s", sector_pick_counts)
            else:
                df = df.head(criteria.max_stocks)

            # 清理临时列
            if "list_date_str" in df.columns:
                df = df.drop(columns=["list_date_str"])

            logger.info(f"最终筛选结果: {len(df)} 只")

            return {
                **state,
                "_filtered_stocks": df.to_dict("records"),
                "_filter_error": None,
                "_sector_template_applied": sector_template_applied,
                "_sector_pick_counts": sector_pick_counts,
            }

        except Exception as e:
            logger.exception("应用筛选条件失败")
            return {
                **state,
                "_filtered_stocks": [],
                "_filter_error": str(e),
                "screener_result": {"error": f"筛选失败: {e}"}
            }

    return apply_filters_node


def create_format_output_node():
    """格式化输出节点"""

    def format_output_node(state: Dict[str, Any]) -> Dict[str, Any]:
        """将筛选结果格式化为标准输出"""
        criteria: ScreenerCriteria = state.get("_criteria")
        filtered_stocks = state.get("_filtered_stocks", [])
        sector_template_applied = state.get("_sector_template_applied") or {}
        sector_pick_counts = state.get("_sector_pick_counts") or {}

        if not filtered_stocks:
            # 检查是否有错误
            if state.get("screener_result"):
                return state  # 已有错误结果

            return {
                **state,
                "screener_result": {
                    "filtered_stocks": [],
                    "total_count": 0,
                    "filter_summary": "未找到符合条件的股票",
                    "applied_filters": criteria.get_filter_summary() if criteria else [],
                    "sector_distribution": {}
                }
            }

        # 格式化股票列表（保留所有字段）
        formatted_stocks = []
        for stock in filtered_stocks:
            formatted_stocks.append({
                "ts_code": stock.get("ts_code"),
                "name": stock.get("name"),
                "industry": stock.get("industry") or "未知",
                "close": stock.get("close"),
                "pe": stock.get("pe"),
                "pe_ttm": stock.get("pe_ttm"),
                "pb": stock.get("pb"),
                "total_share": stock.get("total_share"),
                "float_share": stock.get("float_share"),
                "total_mv": stock.get("total_mv"),
                "circ_mv": stock.get("circ_mv"),
                "turnover_rate": stock.get("turnover_rate"),
                "volume_ratio": stock.get("volume_ratio"),
                "dv_ratio": stock.get("dv_ratio"),
                "dv_ttm": stock.get("dv_ttm"),
                "ps": stock.get("ps"),
                "ps_ttm": stock.get("ps_ttm"),
            })

        # 统计行业分布
        sector_dist = {}
        for stock in filtered_stocks:
            sector = stock.get("industry") or "未知"
            sector_dist[sector] = sector_dist.get(sector, 0) + 1

        screener_result = {
            "filtered_stocks": formatted_stocks,
            "total_count": len(formatted_stocks),
            "filter_summary": f"从 {len(state.get('_raw_stock_list', []))} 只股票中筛选出 {len(formatted_stocks)} 只",
            "applied_filters": criteria.get_filter_summary() if criteria else [],
            "sector_distribution": sector_dist,
            "sector_template_applied": sector_template_applied,
            "sector_pick_counts": sector_pick_counts,
            "sector_template_plan": state.get("_sector_template_plan") or {},
        }

        logger.info(f"股票筛选完成: {screener_result['filter_summary']}")

        return {
            **state,
            "screener_result": screener_result,
        }

    return format_output_node


def _write_json_atomic(path: Path, payload: Dict[str, Any]) -> None:
    """原子写入 JSON，避免中断产生半成品文件。"""
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_suffix(path.suffix + ".tmp")
    with open(tmp_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
    os.replace(tmp_path, path)


def create_screener_result_persist_node():
    """将筛选结果 screener_result 持久化到本地 artifacts，并同步数据库主表。"""

    def screener_result_persist_node(state: Dict[str, Any]) -> Dict[str, Any]:
        screener_result = state.get("screener_result")
        if not screener_result:
            return state

        trade_date = str(state.get("trade_date") or datetime.now().strftime("%Y%m%d")).replace("-", "")[:8]
        artifact_dir = _STOCK_SCREENER_ARTIFACT_ROOT / trade_date
        result_path = artifact_dir / "result.json"
        manifest_path = artifact_dir / "manifest.json"

        try:
            _write_json_atomic(result_path, screener_result)
            _write_json_atomic(
                manifest_path,
                {
                    "artifact_type": "stock_screener_result",
                    "module": "agents.analyst.stock_analyst.stock_screener",
                    "trade_date": trade_date,
                    "created_at": datetime.now().astimezone().isoformat(),
                    "status": "success",
                    "result_path": result_path.as_posix(),
                },
            )
            # 每次运行成功落盘后，立即 upsert 到 stock_screener 主表。
            try:
                from database.data_sync.stock_screener import sync_single_result

                sync_single_result(result_path)
            except Exception as sync_err:
                logger.warning("stock_screener 数据库同步失败: %s", sync_err)
            logger.info("stock_screener 结果已写入本地 artifacts: %s", result_path)
            return {
                **state,
                "stock_screener_artifact_path": result_path.as_posix(),
                "stock_screener_manifest_path": manifest_path.as_posix(),
            }
        except Exception as e:
            logger.warning("写入 stock_screener artifacts 失败: %s", e)
            return state

    return screener_result_persist_node


__all__ = [
    "create_parse_criteria_node",
    "create_select_sector_templates_node",
    "create_fetch_stock_pool_node",
    "create_apply_filters_node",
    "create_format_output_node",
    "create_screener_result_persist_node",
    "ScreenerCriteria",
]
