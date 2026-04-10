"""
Stock Screener（股票筛选分析师）- 节点实现

使用 daily_basic（每日指标）拉取 PE/PB、总市值 total_mv 等；合并 stock_basic 补全名称、行业、上市日。
"""
import logging
import json
from pathlib import Path
from typing import Any, Dict, List, Optional, Set

import pandas as pd

from .criteria import ScreenerCriteria

logger = logging.getLogger(__name__)
_SECTOR_MANAGER_ARTIFACT_ROOT = Path("data") / "artifacts" / "manager" / "sector_manager"


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

        if not criteria:
            return {**state, "_filtered_stocks": [], "_filter_error": "缺少筛选条件"}

        if not raw_stocks:
            return {**state, "_filtered_stocks": [], "_filter_error": "股票池为空"}

        try:
            # 转换为 DataFrame
            df = pd.DataFrame(raw_stocks)

            # 板块过滤：仅按同花顺板块（N/I）成分股过滤，保持与上游口径一致
            if criteria.sectors:
                ths_member_codes = _resolve_ths_sector_members(criteria.sectors)
                if ths_member_codes:
                    df = df[df["ts_code"].isin(ths_member_codes)]
                else:
                    logger.warning("未解析到同花顺板块成分，板块过滤结果为空")
                    df = df.iloc[0:0]
                logger.info(f"板块过滤后: {len(df)} 只")

            # 市值过滤：daily_basic.total_mv 为万元，条件 min/max 为人民币元
            if criteria.min_market_cap is not None or criteria.max_market_cap is not None:
                if "total_mv" not in df.columns:
                    logger.warning("缺少 total_mv 列，跳过市值过滤")
                else:
                    mv_yuan = pd.to_numeric(df["total_mv"], errors="coerce") * 10000.0
                    if criteria.min_market_cap is not None:
                        df = df[mv_yuan >= float(criteria.min_market_cap)]
                    if criteria.max_market_cap is not None:
                        df = df[mv_yuan <= float(criteria.max_market_cap)]

                logger.info(f"市值过滤后: {len(df)} 只")

            # PE 过滤（市盈率）
            if criteria.min_pe is not None and "pe" in df.columns:
                df = df[df["pe"] >= criteria.min_pe]
            if criteria.max_pe is not None and "pe" in df.columns:
                df = df[df["pe"] <= criteria.max_pe]

            # PB 过滤（市净率）
            if criteria.min_pb is not None and "pb" in df.columns:
                df = df[df["pb"] >= criteria.min_pb]
            if criteria.max_pb is not None and "pb" in df.columns:
                df = df[df["pb"] <= criteria.max_pb]

            # 上市日期过滤
            if criteria.min_listing_days and "list_date" in df.columns:
                from datetime import datetime, timedelta
                cutoff_date = (datetime.now() - timedelta(days=criteria.min_listing_days)).strftime("%Y%m%d")
                df["list_date_str"] = df["list_date"].astype(str)
                df = df[df["list_date_str"] <= cutoff_date]

            # 排序（优先使用配置的 sort_by，缺失时回退）
            requested_sort = criteria.sort_by or "total_mv"
            fallback_candidates = [
                "total_mv",
                "circ_mv",
                "total_share",
                "float_share",
                "pe",
                "pe_ttm",
                "pb",
                "ps",
                "ps_ttm",
                "close",
                "dv_ratio",
                "dv_ttm",
                "turnover_rate",
                "volume_ratio",
                "eps",
                "ts_code",
            ]
            sort_candidates = [requested_sort] + [c for c in fallback_candidates if c != requested_sort]

            sort_column = None
            for candidate in sort_candidates:
                if candidate not in df.columns:
                    continue
                # 避免对全空列排序导致结果不可用
                if candidate != "ts_code" and df[candidate].notna().sum() == 0:
                    continue
                sort_column = candidate
                break

            if sort_column is None:
                # 理论上不应发生，作为兜底保护
                sort_column = "ts_code"

            ascending = (criteria.sort_order == "asc")
            if sort_column != "ts_code":
                # 数值字段转为数值，非法值置空，避免混合类型比较报错
                df[sort_column] = pd.to_numeric(df[sort_column], errors="coerce")
                # `na_position` 确保无效值被推到末尾，避免干扰前排结果
                df = df.sort_values(by=sort_column, ascending=ascending, na_position="last")
            else:
                df = df.sort_values(by=sort_column, ascending=ascending)

            logger.info(f"排序字段: {requested_sort} -> 实际使用: {sort_column} ({'asc' if ascending else 'desc'})")

            # 限制数量
            df = df.head(criteria.max_stocks)

            # 清理临时列
            if "list_date_str" in df.columns:
                df = df.drop(columns=["list_date_str"])

            logger.info(f"最终筛选结果: {len(df)} 只")

            return {
                **state,
                "_filtered_stocks": df.to_dict("records"),
                "_filter_error": None,
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
        }

        logger.info(f"股票筛选完成: {screener_result['filter_summary']}")

        return {
            **state,
            "screener_result": screener_result,
        }

    return format_output_node


__all__ = [
    "create_parse_criteria_node",
    "create_fetch_stock_pool_node",
    "create_apply_filters_node",
    "create_format_output_node",
    "ScreenerCriteria",
]
