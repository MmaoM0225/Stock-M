"""
Stock Screener（股票筛选分析师）- 节点实现

使用 bak_basic 接口获取每日股票基础数据（包含PE、PB、股本等）
"""
import logging
from typing import Any, Dict, List, Optional

import pandas as pd

from .criteria import ScreenerCriteria

logger = logging.getLogger(__name__)


def create_parse_criteria_node():
    """解析筛选条件节点"""

    def parse_criteria_node(state: Dict[str, Any]) -> Dict[str, Any]:
        """从 state 中解析筛选条件"""
        criteria_dict = {
            "sectors": state.get("sectors"),
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
            "sort_by": state.get("sort_by", "total_share"),
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
        }

    return parse_criteria_node


def create_fetch_stock_pool_node():
    """获取初始股票池节点"""

    def fetch_stock_pool_node(state: Dict[str, Any]) -> Dict[str, Any]:
        """从 tushare bak_basic 接口获取指定日期的股票基础数据"""
        criteria: ScreenerCriteria = state.get("_criteria")
        trade_date = state.get("trade_date")

        if not criteria:
            return {**state, "_raw_stock_list": [], "_fetch_error": "缺少筛选条件"}

        if not trade_date:
            return {**state, "_raw_stock_list": [], "_fetch_error": "缺少交易日期"}

        try:
            from dataflow.market_data import fetch_bak_basic

            # 使用 bak_basic 获取当日股票基础数据（含PE、PB、股本、资产等）
            df = fetch_bak_basic(
                trade_date=trade_date,
                fields="trade_date,ts_code,name,industry,area,pe,pb,float_share,total_share,eps,bvps,list_date,total_assets,liquid_assets"
            )

            if df.empty:
                return {
                    **state,
                    "_raw_stock_list": [],
                    "_fetch_error": f"未能获取 {trade_date} 的股票数据"
                }

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

            # 板块过滤
            if criteria.sectors:
                mask = df["industry"].isin(criteria.sectors)
                df = df[mask]
                logger.info(f"板块过滤后: {len(df)} 只")

            # 市值过滤（单位：亿，float_share * 股价 = 流通市值）
            if criteria.min_market_cap or criteria.max_market_cap:
                # 使用总股本估算市值（假设股价约10元，bak_basic不返回股价）
                # 更准确的做法是获取当日行情，但这里用总股本排序近似
                df["market_cap_approx"] = df["total_share"] * 10  # 粗略估算

                if criteria.min_market_cap:
                    min_cap = criteria.min_market_cap / 1e8  # 转换为亿
                    df = df[df["market_cap_approx"] >= min_cap]
                if criteria.max_market_cap:
                    max_cap = criteria.max_market_cap / 1e8
                    df = df[df["market_cap_approx"] <= max_cap]

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

            # 排序（按总股本近似）
            sort_column = "total_share" if "total_share" in df.columns else "ts_code"
            df = df.sort_values(by=sort_column, ascending=(criteria.sort_order == "asc"))

            # 限制数量
            df = df.head(criteria.max_stocks)

            # 清理临时列
            if "market_cap_approx" in df.columns:
                df = df.drop(columns=["market_cap_approx"])
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
                "pe": stock.get("pe"),
                "pb": stock.get("pb"),
                "total_share": stock.get("total_share"),
                "float_share": stock.get("float_share"),
                "eps": stock.get("eps"),
                "bvps": stock.get("bvps"),
                "total_assets": stock.get("total_assets"),
                "liquid_assets": stock.get("liquid_assets"),
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
