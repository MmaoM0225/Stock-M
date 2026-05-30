from datetime import datetime, timedelta
import json
import math
from pathlib import Path
from typing import Any, Dict, List, Optional

from sqlalchemy.orm import Session
from database.models import (
    MacroManagerKey,
    CommodityAnalystKey,
    MacroEconomistKey,
    MarketSentimentAnalystKey,
    NewsAnalystKey,
    PortfolioDecisionKey,
    SectorCapitalFlowAnalystKey,
    SectorManagerKey,
    SectorTrendAnalystKey,
    StockFundamentalAnalystKey,
    StockManagerKey,
    StockPoolManagerKey,
    StockScreenerKey,
    StockTechnicalAnalystKey,
)

DEFAULT_COMMODITY_META: List[Dict[str, str]] = [
    {"name": "黄金", "code": "Au99.99", "source": "sge"},
    {"name": "原油连续", "code": "SCL.INE", "source": "fut"},
    {"name": "生猪连续", "code": "LHL.DCE", "source": "fut"},
    {"name": "螺纹钢连续", "code": "RBL.SHF", "source": "fut"},
    {"name": "豆粕连续", "code": "ML.DCE", "source": "fut"},
    {"name": "焦炭连续", "code": "JL.DCE", "source": "fut"},
    {"name": "烧碱连续", "code": "SHL.ZCE", "source": "fut"},
    {"name": "沪铜连续", "code": "CUL.SHF", "source": "fut"},
    {"name": "沪铝连续", "code": "ALL.SHF", "source": "fut"},
    {"name": "棉花连续", "code": "CFL.ZCE", "source": "fut"},
]
DEFAULT_PORTFOLIO_VERSION = "daily_full_position_ver1"

class DataService:
    def __init__(self, db: Session, project_root: Optional[Path] = None):
        self.db = db
        self.project_root = project_root or Path(__file__).resolve().parents[2]
        self.artifacts_root = self.project_root / "data" / "artifacts"
        self.decision_root = self.artifacts_root / "decision"
        self._commodity_meta_by_name: Dict[str, Dict[str, Any]] = {
            str(item.get("name")): item
            for item in DEFAULT_COMMODITY_META
            if isinstance(item, dict) and item.get("name")
        }
        self._ths_index_map: Optional[Dict[str, str]] = None

    def _load_json(self, path: Path) -> Dict[str, Any]:
        if not path.exists():
            raise FileNotFoundError(f"result file not found: {path}")
        with path.open("r", encoding="utf-8") as f:
            return json.load(f)

    def _load_artifact(self, path: Path) -> Dict[str, Any]:
        return {"artifact_path": str(path.relative_to(self.project_root)), "result": self._load_json(path)}

    def _load_artifact_from_result_path(self, result_path: Optional[str]) -> Dict[str, Any]:
        if not result_path:
            raise FileNotFoundError("result_path is missing in database row")
        path = self.project_root / str(result_path)
        return self._load_artifact(path)

    def _load_json_from_result_path(self, result_path: Optional[str]) -> Dict[str, Any]:
        if not result_path:
            raise FileNotFoundError("result_path is missing in database row")
        path = self.project_root / str(result_path)
        return self._load_json(path)

    def _parse_portfolio_version_from_run_id(self, run_id: Any) -> str:
        text = str(run_id or "").strip()
        if not text.startswith("portfolio_decision:"):
            return "default"
        parts = text.split(":")
        if len(parts) >= 3:
            return parts[1] or "default"
        return "default"

    def get_macro_result(self, trade_date: str) -> Dict[str, Any]:
        row = (
            self.db.query(MacroManagerKey)
            .filter(MacroManagerKey.trade_date == trade_date)
            .first()
        )
        if not row:
            raise FileNotFoundError(f"macro_manager result not found in db: {trade_date}")
        return self._load_artifact_from_result_path(row.result_path)

    def get_macro_manager_result(self, trade_date: str) -> Dict[str, Any]:
        payload = self.get_macro_result(trade_date)
        result = payload.get("result", {})
        if not isinstance(result, dict):
            result = {}
        mapped = dict(result)
        mapped["market_direction"] = self._map_macro_manager_signal("market_direction", result.get("market_direction"))
        return {"trade_date": trade_date, **mapped}

    def get_sector_result(self, trade_date: str) -> Dict[str, Any]:
        row = (
            self.db.query(SectorManagerKey)
            .filter(SectorManagerKey.trade_date == trade_date)
            .first()
        )
        if not row:
            raise FileNotFoundError(f"sector_manager result not found in db: {trade_date}")
        return self._load_artifact_from_result_path(row.result_path)

    def get_sector_manager_result(self, trade_date: str) -> Dict[str, Any]:
        row = (
            self.db.query(SectorManagerKey)
            .filter(SectorManagerKey.trade_date == trade_date)
            .first()
        )
        if not row:
            raise FileNotFoundError(f"sector_manager result not found in db: {trade_date}")
        raw = self._load_json_from_result_path(row.result_path)
        if not isinstance(raw, dict):
            raw = {}
        favored = raw.get("favored_sectors")
        watch = raw.get("watchlist_sectors")
        risk = raw.get("risk_sectors")
        signals = raw.get("core_signals")
        return {
            "trade_date": trade_date,
            "market_regime": self._map_sector_manager_signal("market_regime", raw.get("market_regime")),
            "market_bias": self._map_sector_manager_signal("market_bias", raw.get("market_bias")),
            "action_bias": self._map_sector_manager_signal("action_bias", raw.get("action_bias")),
            "favored_sectors": favored if isinstance(favored, list) else [],
            "watchlist_sectors": watch if isinstance(watch, list) else [],
            "risk_sectors": risk if isinstance(risk, list) else [],
            "core_signals": signals if isinstance(signals, list) else [],
            "confidence": self._to_float_or_none(raw.get("confidence")),
            "sector_summary": raw.get("sector_summary"),
        }

    def get_screener_result(self, trade_date: str) -> Dict[str, Any]:
        row = (
            self.db.query(StockScreenerKey)
            .filter(StockScreenerKey.trade_date == trade_date)
            .first()
        )
        if not row:
            raise FileNotFoundError(f"stock_screener result not found in db: {trade_date}")
        return self._load_artifact_from_result_path(row.result_path)

    def get_stock_screener_api_result(self, trade_date: str) -> Dict[str, Any]:
        row = (
            self.db.query(StockScreenerKey)
            .filter(StockScreenerKey.trade_date == trade_date)
            .first()
        )
        if not row:
            raise FileNotFoundError(f"stock_screener result not found in db: {trade_date}")
        raw = self._load_json_from_result_path(row.result_path)
        if not isinstance(raw, dict):
            raw = {}
        stocks = raw.get("filtered_stocks")
        if not isinstance(stocks, list):
            stocks = []
        total = raw.get("total_count")
        if total is None:
            total = len(stocks)
        try:
            total_int = int(total)
        except (TypeError, ValueError):
            total_int = len(stocks)
        return {
            "trade_date": trade_date,
            "total_count": total_int,
            "filter_summary": raw.get("filter_summary") if isinstance(raw.get("filter_summary"), str) else "",
            "applied_filters": raw.get("applied_filters") if isinstance(raw.get("applied_filters"), list) else [],
            "sector_distribution": raw.get("sector_distribution")
            if isinstance(raw.get("sector_distribution"), dict)
            else {},
            "sector_template_applied": raw.get("sector_template_applied")
            if isinstance(raw.get("sector_template_applied"), dict)
            else {},
            "sector_pick_counts": raw.get("sector_pick_counts") if isinstance(raw.get("sector_pick_counts"), dict) else {},
            "sector_template_plan": raw.get("sector_template_plan")
            if isinstance(raw.get("sector_template_plan"), dict)
            else {},
            "filtered_stocks": stocks,
        }

    def get_stock_pool_result(self, trade_date: str) -> Dict[str, Any]:
        row = (
            self.db.query(StockPoolManagerKey)
            .filter(StockPoolManagerKey.trade_date == trade_date)
            .first()
        )
        if not row:
            raise FileNotFoundError(f"stock_pool_manager result not found in db: {trade_date}")
        return self._load_artifact_from_result_path(row.result_path)

    def get_stock_pool_api_result(self, trade_date: str) -> Dict[str, Any]:
        row = (
            self.db.query(StockPoolManagerKey)
            .filter(StockPoolManagerKey.trade_date == trade_date)
            .first()
        )
        if not row:
            raise FileNotFoundError(f"stock_pool_manager result not found in db: {trade_date}")
        raw = self._load_json_from_result_path(row.result_path)
        if not isinstance(raw, dict):
            raw = {}

        candidate = raw.get("candidate_stocks")
        if not isinstance(candidate, list):
            candidate = []
        top_stocks = raw.get("top_stocks")
        if not isinstance(top_stocks, list):
            top_stocks = []
        per_stock = raw.get("per_stock")
        if not isinstance(per_stock, list):
            per_stock = []

        def _map_pool_row(row: Dict[str, Any]) -> Dict[str, Any]:
            return {
                "ts_code": row.get("ts_code"),
                "name": row.get("name"),
                "industry": row.get("industry"),
                "overall_score": self._to_float_or_none(row.get("overall_score")),
                "action_signal": self._map_stock_manager_signal("action_signal", row.get("action_signal")),
                "risk_level": row.get("risk_level"),
                "selection_reason": row.get("selection_reason"),
                "analyze_error": row.get("analyze_error"),
            }

        mapped_candidate = [_map_pool_row(row) for row in candidate if isinstance(row, dict)]
        mapped_top = [_map_pool_row(row) for row in top_stocks if isinstance(row, dict)]

        mapped_per: List[Dict[str, Any]] = []
        for item in per_stock:
            if not isinstance(item, dict):
                continue
            sm = item.get("stock_manager_summary")
            if not isinstance(sm, dict):
                sm = {}
            mapped_per.append(
                {
                    "ts_code": item.get("ts_code"),
                    "name": item.get("name"),
                    "industry": item.get("industry"),
                    "stock_manager_summary": {
                        "overall_score": self._to_float_or_none(sm.get("overall_score")),
                        "confidence": sm.get("confidence"),
                        "action_signal": self._map_stock_manager_signal("action_signal", sm.get("action_signal")),
                        "risk_level": sm.get("risk_level"),
                        "key_points": sm.get("key_points") if isinstance(sm.get("key_points"), list) else [],
                        "risks": sm.get("risks") if isinstance(sm.get("risks"), list) else [],
                        "summary": sm.get("summary"),
                    },
                    "error": item.get("error"),
                }
            )

        return {
            "trade_date": trade_date,
            "pool_size": int(raw.get("pool_size") or 0),
            "analyzed_count": int(raw.get("analyzed_count") or 0),
            "analyze_success_count": int(raw.get("analyze_success_count") or 0),
            "analyze_error_count": int(raw.get("analyze_error_count") or 0),
            "summary_text": raw.get("summary_text"),
            "candidate_stocks": mapped_candidate,
            "top_stocks": mapped_top,
            "per_stock": mapped_per,
        }

    def list_manager_dates(self, manager_name: str) -> List[str]:
        model_map = {
            "macro_manager": MacroManagerKey,
            "sector_manager": SectorManagerKey,
            "stock_pool_manager": StockPoolManagerKey,
        }
        model = model_map.get(manager_name)
        if model is None:
            return []
        rows = self.db.query(model.trade_date).distinct().order_by(model.trade_date.desc()).all()
        return [str(r[0]) for r in rows if r and r[0]]

    def _latest_decision_version(self) -> str:
        rows = self.db.query(PortfolioDecisionKey.run_id).all()
        versions = sorted(
            {
                self._parse_portfolio_version_from_run_id(r[0])
                for r in rows
                if r and r[0]
            }
        )
        if not versions:
            raise FileNotFoundError("decision version directory not found")
        return versions[-1]

    def _default_portfolio_version(self) -> str:
        versions = self.list_portfolio_versions()
        if DEFAULT_PORTFOLIO_VERSION in versions:
            return DEFAULT_PORTFOLIO_VERSION
        if versions:
            return versions[0]
        raise FileNotFoundError("decision version directory not found")

    def get_portfolio_result(self, trade_date: str) -> Dict[str, Any]:
        version = self._default_portfolio_version()
        rows = (
            self.db.query(PortfolioDecisionKey)
            .filter(PortfolioDecisionKey.trade_date == trade_date)
            .all()
        )
        row = next(
            (
                x
                for x in rows
                if self._parse_portfolio_version_from_run_id(x.run_id) == version
            ),
            None,
        )
        if not row:
            raise FileNotFoundError(f"portfolio result not found in db: {version}/{trade_date}")
        return self._load_artifact_from_result_path(row.result_path)

    def list_portfolio_versions(self) -> List[str]:
        rows = self.db.query(PortfolioDecisionKey.run_id).all()
        versions = {
            self._parse_portfolio_version_from_run_id(r[0])
            for r in rows
            if r and r[0]
        }
        return sorted(versions, reverse=True)

    def _portfolio_version_dir(self, version: str) -> Path:
        # 保留接口兼容；数据库模式不再依赖目录探测。
        return self.decision_root / version

    def list_portfolio_dates_by_version(self, version: str) -> List[str]:
        rows = self.db.query(PortfolioDecisionKey.trade_date, PortfolioDecisionKey.run_id).all()
        dates = [
            str(trade_date)
            for trade_date, run_id in rows
            if trade_date and self._parse_portfolio_version_from_run_id(run_id) == version
        ]
        return sorted(set(dates), reverse=True)

    def get_portfolio_api_result(self, version: str, trade_date: str) -> Dict[str, Any]:
        normalized_date = self._normalize_trade_date(trade_date)
        if not normalized_date:
            raise ValueError("trade_date is required")
        rows = (
            self.db.query(PortfolioDecisionKey)
            .filter(PortfolioDecisionKey.trade_date == normalized_date)
            .all()
        )
        row = next(
            (
                x
                for x in rows
                if self._parse_portfolio_version_from_run_id(x.run_id) == version
            ),
            None,
        )
        if not row:
            raise FileNotFoundError(f"portfolio result not found in db: {version}/{normalized_date}")
        raw = self._load_json_from_result_path(row.result_path)
        if not isinstance(raw, dict):
            raw = {}

        portfolio_rows = raw.get("portfolio_table")
        if not isinstance(portfolio_rows, list):
            portfolio_rows = []
        operation_rows = raw.get("operation_reason_table")
        if not isinstance(operation_rows, list):
            operation_rows = []

        mapped_portfolio: List[Dict[str, Any]] = []
        for row in portfolio_rows:
            if not isinstance(row, dict):
                continue
            mapped_portfolio.append(
                {
                    "rank": str(row.get("排名")) if row.get("排名") is not None else None,
                    "asset_name": row.get("资产名称"),
                    "ts_code": row.get("ts_code"),
                    "market_value": self._to_float_or_none(row.get("市值 (元)")),
                    "position": row.get("仓位"),
                    "position_change": row.get("较上期仓位变化"),
                    "total_return": row.get("总收益 (%)"),
                    "total_pnl": self._to_float_or_none(row.get("总盈亏 (元)")),
                    "asset_type": row.get("资产类型"),
                    "shares": row.get("持仓股数"),
                    "cost_price": self._to_float_or_none(row.get("成本价")),
                    "action": row.get("操作"),
                    "open_price": self._to_float_or_none(row.get("开盘价")),
                }
            )

        mapped_operations: List[Dict[str, Any]] = []
        for row in operation_rows:
            if not isinstance(row, dict):
                continue
            mapped_operations.append(
                {
                    "asset_name": row.get("资产名称"),
                    "ts_code": row.get("ts_code"),
                    "action": row.get("操作"),
                    "old_position": row.get("原仓位"),
                    "new_position": row.get("新仓位"),
                    "position_change": row.get("仓位变化"),
                    "execution_price": self._to_float_or_none(row.get("执行价格(开盘价)")),
                    "target_amount": self._to_float_or_none(row.get("目标金额(元)")),
                    "actual_amount": self._to_float_or_none(row.get("实际成交金额(元)")),
                    "shares": row.get("成交股数"),
                    "cost_price": self._to_float_or_none(row.get("成本价")),
                    "reason": row.get("操作原因"),
                }
            )

        meta = raw.get("meta")
        if not isinstance(meta, dict):
            meta = {}
        mapped_meta = {
            "initial_capital": self._to_float_or_none(meta.get("initial_capital")),
            "total_capital": self._to_float_or_none(meta.get("total_capital")),
            "source_portfolio_path": meta.get("source_portfolio_path"),
            "generated_at": meta.get("generated_at"),
        }

        return {
            "strategy": raw.get("strategy") or version,
            "trade_date": self._format_trade_date(normalized_date),
            "portfolio_table": mapped_portfolio,
            "operation_reason_table": mapped_operations,
            "decision_summary": raw.get("decision_summary"),
            "meta": mapped_meta,
        }

    def get_stock_manager_result(self, ts_code: str, trade_date: str) -> Dict[str, Any]:
        row = (
            self.db.query(StockManagerKey)
            .filter(
                StockManagerKey.ts_code == ts_code,
                StockManagerKey.trade_date == trade_date,
            )
            .first()
        )
        if not row:
            raise FileNotFoundError(f"stock_manager result not found in db: {ts_code}/{trade_date}")
        return self._load_artifact_from_result_path(row.result_path)

    def list_stock_manager_ts_codes(self) -> List[str]:
        rows = self.db.query(StockManagerKey.ts_code).distinct().order_by(StockManagerKey.ts_code.asc()).all()
        return [str(r[0]) for r in rows if r and r[0]]

    def list_stock_manager_dates(self, ts_code: str) -> List[str]:
        rows = (
            self.db.query(StockManagerKey.trade_date)
            .filter(StockManagerKey.ts_code == ts_code)
            .distinct()
            .order_by(StockManagerKey.trade_date.desc())
            .all()
        )
        return [str(r[0]) for r in rows if r and r[0]]

    def get_stock_manager_api_result(self, ts_code: str, trade_date: str) -> Dict[str, Any]:
        row = (
            self.db.query(StockManagerKey)
            .filter(
                StockManagerKey.ts_code == ts_code,
                StockManagerKey.trade_date == trade_date,
            )
            .first()
        )
        if not row:
            raise FileNotFoundError(f"stock_manager result not found in db: {ts_code}/{trade_date}")
        raw = self._load_json_from_result_path(row.result_path)
        if not isinstance(raw, dict):
            raw = {}
        summary = raw.get("stock_manager_summary")
        if not isinstance(summary, dict):
            summary = raw
        component_scores = summary.get("component_scores")
        if not isinstance(component_scores, dict):
            component_scores = {}
        key_points = summary.get("key_points")
        risks = summary.get("risks")
        return {
            "ts_code": ts_code,
            "trade_date": trade_date,
            "success": bool(summary.get("success", False)),
            "overall_score": self._to_float_or_none(summary.get("overall_score")),
            "confidence": summary.get("confidence"),
            "selection_reason": summary.get("selection_reason"),
            "risk_level": summary.get("risk_level"),
            "component_scores": {
                "fundamental": self._to_float_or_none(component_scores.get("fundamental")),
                "technical": self._to_float_or_none(component_scores.get("technical")),
            },
            "action_signal": self._map_stock_manager_signal("action_signal", summary.get("action_signal")),
            "signal_reason": summary.get("signal_reason"),
            "key_points": key_points if isinstance(key_points, list) else [],
            "risks": risks if isinstance(risks, list) else [],
            "summary": summary.get("summary"),
        }

    def get_fundamental_result(self, ts_code: str, trade_date: str) -> Dict[str, Any]:
        row = (
            self.db.query(StockFundamentalAnalystKey)
            .filter(
                StockFundamentalAnalystKey.ts_code == ts_code,
                StockFundamentalAnalystKey.trade_date == trade_date,
            )
            .first()
        )
        if not row:
            raise FileNotFoundError(f"stock_fundamental result not found in db: {ts_code}/{trade_date}")
        return self._load_artifact_from_result_path(row.result_path)

    def list_fundamental_ts_codes(self) -> List[str]:
        rows = (
            self.db.query(StockFundamentalAnalystKey.ts_code)
            .distinct()
            .order_by(StockFundamentalAnalystKey.ts_code.asc())
            .all()
        )
        return [str(r[0]) for r in rows if r and r[0]]

    def list_fundamental_dates(self, ts_code: str) -> List[str]:
        rows = (
            self.db.query(StockFundamentalAnalystKey.trade_date)
            .filter(StockFundamentalAnalystKey.ts_code == ts_code)
            .distinct()
            .order_by(StockFundamentalAnalystKey.trade_date.desc())
            .all()
        )
        return [str(r[0]) for r in rows if r and r[0]]

    def get_fundamental_api_result(self, ts_code: str, trade_date: str) -> Dict[str, Any]:
        row = (
            self.db.query(StockFundamentalAnalystKey)
            .filter(
                StockFundamentalAnalystKey.ts_code == ts_code,
                StockFundamentalAnalystKey.trade_date == trade_date,
            )
            .first()
        )
        if not row:
            raise FileNotFoundError(f"stock_fundamental result not found in db: {ts_code}/{trade_date}")
        raw = self._load_json_from_result_path(row.result_path)
        if not isinstance(raw, dict):
            raw = {}
        company_raw = raw.get("company")
        if not isinstance(company_raw, dict):
            company_raw = raw.get("stock_company_info")
        if not isinstance(company_raw, dict):
            company_raw = {}

        company = {
            "name": company_raw.get("name") or company_raw.get("com_name"),
            "exchange": company_raw.get("exchange"),
            "chairman": company_raw.get("chairman"),
            "manager": company_raw.get("manager"),
            "secretary": company_raw.get("secretary"),
            "province": company_raw.get("province"),
            "city": company_raw.get("city"),
            "employees": company_raw.get("employees"),
            "website": company_raw.get("website"),
            "email": company_raw.get("email"),
            "main_business": company_raw.get("main_business"),
        }

        fetch_status_raw = raw.get("fetch_status")
        if not isinstance(fetch_status_raw, dict):
            fetch_status_raw = (raw.get("stock_fundamental_meta") or {}).get("fetch_status")
        if not isinstance(fetch_status_raw, dict):
            fetch_status_raw = {}
        meta = raw.get("stock_fundamental_meta")
        if not isinstance(meta, dict):
            meta = {}

        def _fetch_rows(name: str) -> int:
            item = fetch_status_raw.get(name)
            if isinstance(item, dict):
                try:
                    return int(item.get("rows", 0) or 0)
                except (TypeError, ValueError):
                    return 0
            try:
                return int(item or 0)
            except (TypeError, ValueError):
                return 0

        fetch_status = {
            "company_info": _fetch_rows("company_info"),
            "valuation": _fetch_rows("valuation"),
            "income": _fetch_rows("income"),
            "cashflow": _fetch_rows("cashflow"),
            "balancesheet": _fetch_rows("balancesheet"),
            "dividend": _fetch_rows("dividend"),
            "complete_success": bool(meta.get("complete_success", False)),
        }

        reduce_result = raw.get("reduce_result")
        if not isinstance(reduce_result, dict):
            reduce_result = raw.get("fundamental_reduce_result")
        if not isinstance(reduce_result, dict):
            reduce_result = {}

        valuation_src = raw.get("valuation_trend")
        if not isinstance(valuation_src, list):
            valuation_src = raw.get("stock_fundamental_daily")
        if not isinstance(valuation_src, list):
            valuation_src = []
        valuation_trend: List[Dict[str, Any]] = []
        for row in valuation_src:
            if not isinstance(row, dict):
                continue
            d = str(row.get("trade_date") or row.get("date") or "")
            d = d.replace("-", "")[:8]
            if len(d) != 8 or not d.isdigit():
                continue
            valuation_trend.append(
                {
                    "date": f"{d[4:6]}-{d[6:8]}",
                    "close": self._to_float_or_none(row.get("close")),
                    "pe_ttm": self._to_float_or_none(row.get("pe_ttm")),
                    "pb": self._to_float_or_none(row.get("pb")),
                }
            )
        dividend_yield_trend: List[Dict[str, Any]] = []
        for row in valuation_src:
            if not isinstance(row, dict):
                continue
            d = str(row.get("trade_date") or row.get("date") or "")
            d = d.replace("-", "")[:8]
            if len(d) != 8 or not d.isdigit():
                continue
            dividend_yield_trend.append(
                {
                    "date": f"{d[4:6]}-{d[6:8]}",
                    "dv_ratio": self._to_float_or_none(row.get("dv_ratio")),
                    "dv_ttm": self._to_float_or_none(row.get("dv_ttm")),
                }
            )

        income_src = raw.get("income_trend")
        if not isinstance(income_src, list):
            income_src = raw.get("stock_income_data")
        if not isinstance(income_src, list):
            income_src = []
        income_trend: List[Dict[str, Any]] = []
        for row in income_src:
            if not isinstance(row, dict):
                continue
            end_date = str(row.get("end_date") or "")
            if len(end_date) < 6:
                continue
            try:
                month = int(end_date[4:6])
                quarter = (month - 1) // 3 + 1
            except ValueError:
                continue
            revenue_raw = self._to_float_or_none(row.get("total_revenue") or row.get("revenue"))
            net_profit_raw = self._to_float_or_none(row.get("n_income") or row.get("net_profit"))
            op_profit_raw = self._to_float_or_none(row.get("operate_profit"))
            revenue = None if revenue_raw is None else round(revenue_raw / 10000.0, 2)
            net_profit = None if net_profit_raw is None else round(net_profit_raw / 10000.0, 2)
            op_margin = None
            net_margin = None
            if revenue_raw and op_profit_raw is not None:
                op_margin = round(op_profit_raw / revenue_raw * 100.0, 2)
            if revenue_raw and net_profit_raw is not None:
                net_margin = round(net_profit_raw / revenue_raw * 100.0, 2)
            income_trend.append(
                {
                    "period": f"{end_date[:4]}Q{quarter}",
                    "revenue": revenue,
                    "net_profit": net_profit,
                    "op_margin": op_margin,
                    "net_margin": net_margin,
                }
            )

        cashflow_src = raw.get("cashflow_trend")
        if not isinstance(cashflow_src, list):
            cashflow_src = raw.get("stock_cashflow_data")
        if not isinstance(cashflow_src, list):
            cashflow_src = []
        cashflow_trend: List[Dict[str, Any]] = []
        for row in cashflow_src:
            if not isinstance(row, dict):
                continue
            end_date = str(row.get("end_date") or "")
            if len(end_date) < 6:
                continue
            try:
                month = int(end_date[4:6])
                quarter = (month - 1) // 3 + 1
            except ValueError:
                continue
            cfo = self._to_float_or_none(row.get("c_inf_fr_operate_a"))
            cfi = self._to_float_or_none(row.get("n_cashflow_inv_act"))
            cff = self._to_float_or_none(row.get("n_cash_flows_fnc_act"))
            fcf = self._to_float_or_none(row.get("free_cashflow"))
            cashflow_trend.append(
                {
                    "period": f"{end_date[:4]}Q{quarter}",
                    "cfo": None if cfo is None else round(cfo / 10000.0, 2),
                    "cfi": None if cfi is None else round(cfi / 10000.0, 2),
                    "cff": None if cff is None else round(cff / 10000.0, 2),
                    "fcf": None if fcf is None else round(fcf / 10000.0, 2),
                }
            )

        liability_src = raw.get("liability_trend")
        if not isinstance(liability_src, list):
            liability_src = raw.get("stock_balancesheet_data")
        if not isinstance(liability_src, list):
            liability_src = []
        liability_trend: List[Dict[str, Any]] = []
        for row in liability_src:
            if not isinstance(row, dict):
                continue
            end_date = str(row.get("end_date") or "")
            if len(end_date) < 6:
                continue
            try:
                month = int(end_date[4:6])
                quarter = (month - 1) // 3 + 1
            except ValueError:
                continue
            assets_raw = self._to_float_or_none(row.get("total_assets"))
            liab_raw = self._to_float_or_none(row.get("total_liab"))
            debt_to_assets = None
            if assets_raw and liab_raw is not None:
                debt_to_assets = round(liab_raw / assets_raw * 100.0, 2)
            liability_trend.append(
                {
                    "period": f"{end_date[:4]}Q{quarter}",
                    "assets": None if assets_raw is None else round(assets_raw / 10000.0, 2),
                    "liab": None if liab_raw is None else round(liab_raw / 10000.0, 2),
                    "debt_to_assets": debt_to_assets,
                }
            )

        return {
            "ts_code": ts_code,
            "trade_date": trade_date,
            "company": company,
            "fetch_status": fetch_status,
            "reduce_result": reduce_result,
            "company_basic_analysis": raw.get("company_basic_analysis")
            if isinstance(raw.get("company_basic_analysis"), dict)
            else {},
            "valuation_map_analysis": raw.get("valuation_map_analysis")
            if isinstance(raw.get("valuation_map_analysis"), dict)
            else {},
            "income_map_analysis": raw.get("income_map_analysis")
            if isinstance(raw.get("income_map_analysis"), dict)
            else {},
            "cashflow_map_analysis": raw.get("cashflow_map_analysis")
            if isinstance(raw.get("cashflow_map_analysis"), dict)
            else {},
            "balancesheet_map_analysis": raw.get("balancesheet_map_analysis")
            if isinstance(raw.get("balancesheet_map_analysis"), dict)
            else {},
            "dividend_map_analysis": raw.get("dividend_map_analysis")
            if isinstance(raw.get("dividend_map_analysis"), dict)
            else {},
            "valuation_trend": valuation_trend,
            "income_trend": income_trend,
            "cashflow_trend": cashflow_trend,
            "liability_trend": liability_trend,
            "dividend_yield_trend": dividend_yield_trend,
        }

    def get_technical_result(self, ts_code: str, trade_date: str) -> Dict[str, Any]:
        row = (
            self.db.query(StockTechnicalAnalystKey)
            .filter(
                StockTechnicalAnalystKey.ts_code == ts_code,
                StockTechnicalAnalystKey.trade_date == trade_date,
            )
            .first()
        )
        if not row:
            raise FileNotFoundError(f"stock_technical result not found in db: {ts_code}/{trade_date}")
        return self._load_artifact_from_result_path(row.result_path)

    def list_technical_ts_codes(self) -> List[str]:
        rows = (
            self.db.query(StockTechnicalAnalystKey.ts_code)
            .distinct()
            .order_by(StockTechnicalAnalystKey.ts_code.asc())
            .all()
        )
        return [str(r[0]) for r in rows if r and r[0]]

    def list_technical_dates(self, ts_code: str) -> List[str]:
        rows = (
            self.db.query(StockTechnicalAnalystKey.trade_date)
            .filter(StockTechnicalAnalystKey.ts_code == ts_code)
            .distinct()
            .order_by(StockTechnicalAnalystKey.trade_date.desc())
            .all()
        )
        return [str(r[0]) for r in rows if r and r[0]]

    def get_technical_api_result(self, ts_code: str, trade_date: str) -> Dict[str, Any]:
        row = (
            self.db.query(StockTechnicalAnalystKey)
            .filter(
                StockTechnicalAnalystKey.ts_code == ts_code,
                StockTechnicalAnalystKey.trade_date == trade_date,
            )
            .first()
        )
        if not row:
            raise FileNotFoundError(f"stock_technical result not found in db: {ts_code}/{trade_date}")
        raw = self._load_json_from_result_path(row.result_path)
        if not isinstance(raw, dict):
            raw = {}

        meta = raw.get("stock_technical_meta")
        if not isinstance(meta, dict):
            meta = {}
        facts = raw.get("stock_technical_facts")
        if not isinstance(facts, dict):
            facts = {}
        analysis = raw.get("technical_analysis")
        if not isinstance(analysis, dict):
            analysis = {}

        indicators = analysis.get("technical_indicators")
        if not isinstance(indicators, dict):
            indicators = {}

        stock_kline_data = raw.get("stock_kline_data")
        if not isinstance(stock_kline_data, list):
            stock_kline_data = []

        recent_bars_raw = facts.get("recent_bars")
        if not isinstance(recent_bars_raw, list):
            recent_bars_raw = []
        recent_bars: List[Dict[str, Any]] = []
        for row in recent_bars_raw:
            if not isinstance(row, dict):
                continue
            recent_bars.append(
                {
                    "trade_date": str(row.get("trade_date", "")).replace("-", "")[:8],
                    "close": self._to_float_or_none(row.get("close")),
                    "pct_chg": self._to_float_or_none(row.get("pct_chg")),
                    "ma5": self._to_float_or_none(row.get("ma5")),
                    "ma10": self._to_float_or_none(row.get("ma10")),
                    "ma20": self._to_float_or_none(row.get("ma20")),
                    "ma60": self._to_float_or_none(row.get("ma60")),
                    "rsi14": self._to_float_or_none(row.get("rsi14")),
                    "macd_dif": self._to_float_or_none(row.get("macd_dif")),
                    "macd_dea": self._to_float_or_none(row.get("macd_dea")),
                    "macd_hist": self._to_float_or_none(row.get("macd_hist") or row.get("macd_macd")),
                    "k": self._to_float_or_none(row.get("k") or row.get("kdj_k")),
                    "d": self._to_float_or_none(row.get("d") or row.get("kdj_d")),
                    "j": self._to_float_or_none(row.get("j") or row.get("kdj_j")),
                    "boll_upper": self._to_float_or_none(row.get("boll_upper")),
                    "boll_mid": self._to_float_or_none(row.get("boll_mid")),
                    "boll_lower": self._to_float_or_none(row.get("boll_lower")),
                }
            )

        return {
            "ts_code": ts_code,
            "trade_date": trade_date,
            "start_date": meta.get("start_date"),
            "latest_price": self._to_float_or_none(
                analysis.get("latest_price") or facts.get("latest_price") or meta.get("latest_price")
            ),
            "latest_pct_chg": self._to_float_or_none(
                analysis.get("latest_pct_chg") or facts.get("latest_pct_chg") or meta.get("latest_pct_chg")
            ),
            "support_levels": analysis.get("support_levels") if isinstance(analysis.get("support_levels"), list) else [],
            "resistance_levels": analysis.get("resistance_levels")
            if isinstance(analysis.get("resistance_levels"), list)
            else [],
            "technical_score": self._to_float_or_none(analysis.get("technical_score")),
            "trend_signal": analysis.get("trend_signal"),
            "trend_strength": analysis.get("trend_strength"),
            "short_term_outlook": analysis.get("short_term_outlook"),
            "risk_reminder": analysis.get("risk_reminder"),
            "summary": analysis.get("summary"),
            "indicators": indicators,
            "stock_kline_data": stock_kline_data,
            "recent_bars": recent_bars,
        }

    def get_analyst_result(self, group: str, analyst: str, trade_date: str) -> Dict[str, Any]:
        model_map = {
            ("macro_analyst", "macro_economist"): MacroEconomistKey,
            ("macro_analyst", "market_sentiment_analyst"): MarketSentimentAnalystKey,
            ("macro_analyst", "news_analyst"): NewsAnalystKey,
            ("macro_analyst", "commodity_analyst"): CommodityAnalystKey,
            ("sector_analyst", "sector_trend_analyst"): SectorTrendAnalystKey,
            ("sector_analyst", "sector_capital_flow_analyst"): SectorCapitalFlowAnalystKey,
            ("stock_analyst", "stock_screener"): StockScreenerKey,
        }
        model = model_map.get((group, analyst))
        if model is None:
            raise FileNotFoundError(f"analyst mapping not found in db: {group}/{analyst}")
        row = self.db.query(model).filter(model.trade_date == trade_date).first()
        if not row:
            raise FileNotFoundError(f"analyst result not found in db: {group}/{analyst}/{trade_date}")
        return self._load_artifact_from_result_path(row.result_path)

    def get_macro_economist_result(self, trade_date: str) -> Dict[str, Any]:
        db_row = (
            self.db.query(MacroEconomistKey)
            .filter(MacroEconomistKey.trade_date == trade_date)
            .first()
        )
        if not db_row or not db_row.result_path:
            raise FileNotFoundError(f"macro_economist result not found in db: {trade_date}")
        llm_output = self._load_json_from_result_path(db_row.result_path)
        if not isinstance(llm_output, dict):
            llm_output = {}

        market = self._get_market_fetcher()
        lpr_raw = self._safe_fetch_lpr(market, trade_date)
        cpi_raw = self._safe_fetch_cpi(market, trade_date)
        sf_raw = self._safe_fetch_sf(market, trade_date)
        pmi_raw = self._safe_fetch_pmi(market, trade_date)
        m2_raw = self._safe_fetch_m2(market, trade_date)
        gdp_raw = self._safe_fetch_gdp(market, trade_date)

        return {
            "trade_date": trade_date,
            "lpr_data": self._build_monthly_series(lpr_raw, value_keys=["1y", "lpr1y", "lpr_1y", "value"]),
            "cpi_data": self._build_monthly_series(cpi_raw, value_keys=["nt_yoy", "nt_val", "cpi", "value"]),
            "sf_data": self._build_monthly_series(sf_raw, value_keys=["inc_month", "inc_yoy", "value"]),
            "pmi_data": self._build_monthly_series(pmi_raw, value_keys=["pmi010000", "pmi", "value"]),
            "m2_data": self._build_monthly_series(m2_raw, value_keys=["m2_yoy", "m2", "value"]),
            "gdp_data": self._build_quarterly_series(gdp_raw, value_keys=["gdp_yoy", "gdp", "value"]),
            "llm_output": self._map_macro_llm_output(llm_output),
        }

    def get_market_sentiment_result(self, trade_date: str) -> Dict[str, Any]:
        db_row = (
            self.db.query(MarketSentimentAnalystKey)
            .filter(MarketSentimentAnalystKey.trade_date == trade_date)
            .first()
        )
        if not db_row or not db_row.result_path:
            raise FileNotFoundError(f"market_sentiment_analyst result not found in db: {trade_date}")
        result = self._load_json_from_result_path(db_row.result_path)
        if not isinstance(result, dict):
            result = {}

        per_index = result.get("per_index", {})
        if not isinstance(per_index, dict):
            per_index = {}

        market = self._get_market_fetcher()
        index_items: List[Dict[str, Any]] = []
        for code, item in per_index.items():
            if not isinstance(item, dict):
                continue
            series = self._safe_fetch_index_series(market, str(code), trade_date, window=60)
            start_price = series[0]["close"] if series else None
            end_price = series[-1]["close"] if series else None
            start_volume = series[0]["volume"] if series else None
            end_volume = series[-1]["volume"] if series else None
            index_items.append(
                {
                    "code": str(code),
                    "name": item.get("name"),
                    "index_trend": self._map_market_sentiment_value("index_trend", item.get("index_trend")),
                    "turnover_summary": item.get("turnover_summary"),
                    "volatility_summary": item.get("volatility_summary"),
                    "market_conclusion": item.get("market_conclusion"),
                    "start_price": start_price,
                    "end_price": end_price,
                    "start_volume": start_volume,
                    "end_volume": end_volume,
                    "market_series": series,
                }
            )

        sentiment_output = {
            "index_trend": self._map_market_sentiment_value("index_trend", result.get("index_trend")),
            "market_sentiment": self._map_market_sentiment_value("market_sentiment", result.get("market_sentiment")),
            "volume_signal": self._map_market_sentiment_value("volume_signal", result.get("volume_signal")),
            "volatility_signal": self._map_market_sentiment_value("volatility_signal", result.get("volatility_signal")),
            "sentiment_summary": result.get("sentiment_summary"),
        }
        return {
            "trade_date": trade_date,
            "index_items": index_items,
            "sentiment_output": sentiment_output,
        }

    def get_sector_trend_result(self, trade_date: str) -> Dict[str, Any]:
        db_row = (
            self.db.query(SectorTrendAnalystKey)
            .filter(SectorTrendAnalystKey.trade_date == trade_date)
            .first()
        )
        if not db_row or not db_row.result_path:
            raise FileNotFoundError(f"sector_trend_analyst result not found in db: {trade_date}")
        result = self._load_json_from_result_path(db_row.result_path)
        if not isinstance(result, dict):
            result = {}

        leading = result.get("leading_themes", [])
        reversal = result.get("reversal_opportunities", [])
        risk = result.get("top_risk_sectors", [])

        sector_names: List[str] = []
        for group in [leading, reversal, risk]:
            if not isinstance(group, list):
                continue
            for name in group:
                if isinstance(name, str) and name and name not in sector_names:
                    sector_names.append(name)

        series_list: List[Dict[str, Any]] = []
        index_map = self._get_ths_index_map()
        for name in sector_names[:10]:
            ts_code = index_map.get(name)
            if not ts_code:
                continue
            series_list.append({"ts_code": ts_code, "name": name})

        return {
            "trade_date": trade_date,
            "summary": result.get("summary"),
            "conclusion": result.get("conclusion"),
            "leading_themes": leading if isinstance(leading, list) else [],
            "reversal_opportunities": reversal if isinstance(reversal, list) else [],
            "top_risk_sectors": risk if isinstance(risk, list) else [],
            "highlights": result.get("highlights") if isinstance(result.get("highlights"), list) else [],
            "market_regime": self._map_sector_trend_signal("market_regime", result.get("market_regime")),
            "series_list": series_list,
        }

    def get_sector_trend_series(self, code: str) -> Dict[str, Any]:
        ts_code = str(code).strip()
        if not ts_code:
            return {"ts_code": ts_code, "name": None, "rows": []}

        index_map = self._get_ths_index_map()
        name = None
        for k, v in index_map.items():
            if v == ts_code:
                name = k
                break

        dates = self.list_analyst_dates("sector_analyst", "sector_trend_analyst")
        end_date = dates[0] if dates else datetime.now().strftime("%Y%m%d")
        rows = self._safe_fetch_ths_series(ts_code, end_date, window=60)
        return {"ts_code": ts_code, "name": name, "rows": rows}

    def get_sector_capital_flow_result(self, trade_date: str) -> Dict[str, Any]:
        db_row = (
            self.db.query(SectorCapitalFlowAnalystKey)
            .filter(SectorCapitalFlowAnalystKey.trade_date == trade_date)
            .first()
        )
        if not db_row or not db_row.result_path:
            raise FileNotFoundError(f"sector_capital_flow_analyst result not found in db: {trade_date}")
        result = self._load_json_from_result_path(db_row.result_path)
        if not isinstance(result, dict):
            result = {}

        # 仅使用 analyst 落盘缓存，不现拉 Tushare。优先轻量 api_snapshot；否则兼容旧版全量 rows。
        snap = self._sector_capital_flow_api_snapshot_from_artifact(result, trade_date)
        if snap is not None:
            one_rows = snap.get("one_day_rows")
            one_flow = snap.get("one_day_sector_flow")
            return {
                "trade_date": trade_date,
                "summary": result.get("summary"),
                "conclusion": result.get("conclusion"),
                "highlights": result.get("highlights") if isinstance(result.get("highlights"), list) else [],
                "market_bias": self._map_sector_capital_flow_signal("market_bias", result.get("market_bias")),
                "one_day_net_amount": snap.get("one_day_net_amount"),
                "five_day_net_amount": snap.get("five_day_net_amount"),
                "twenty_day_net_amount": snap.get("twenty_day_net_amount"),
                "hot_sectors": result.get("hot_sectors") if isinstance(result.get("hot_sectors"), list) else [],
                "risk_sectors": result.get("risk_sectors") if isinstance(result.get("risk_sectors"), list) else [],
                "one_day_sector_flow": one_flow if isinstance(one_flow, list) else [],
                "one_day_rows": one_rows if isinstance(one_rows, list) else [],
            }

        rows = self._sector_moneyflow_rows_from_artifact(result, trade_date)
        one_day_rows_raw = [row for row in rows if str(row.get("trade_date", "")) == trade_date]
        one_day_rows = self._build_sector_moneyflow_rows(one_day_rows_raw, top_k=20)
        one_day_sector_flow = self._build_one_day_sector_flow(one_day_rows_raw, top_k=10)

        return {
            "trade_date": trade_date,
            "summary": result.get("summary"),
            "conclusion": result.get("conclusion"),
            "highlights": result.get("highlights") if isinstance(result.get("highlights"), list) else [],
            "market_bias": self._map_sector_capital_flow_signal("market_bias", result.get("market_bias")),
            "one_day_net_amount": self._sum_sector_moneyflow_by_dates(rows, {trade_date}),
            "five_day_net_amount": self._sum_sector_moneyflow_recent_days(rows, days=5),
            "twenty_day_net_amount": self._sum_sector_moneyflow_recent_days(rows, days=20),
            "hot_sectors": result.get("hot_sectors") if isinstance(result.get("hot_sectors"), list) else [],
            "risk_sectors": result.get("risk_sectors") if isinstance(result.get("risk_sectors"), list) else [],
            "one_day_sector_flow": one_day_sector_flow,
            "one_day_rows": one_day_rows,
        }

    def get_commodity_result(self, trade_date: str) -> Dict[str, Any]:
        db_row = (
            self.db.query(CommodityAnalystKey)
            .filter(CommodityAnalystKey.trade_date == trade_date)
            .first()
        )
        if not db_row or not db_row.result_path:
            raise FileNotFoundError(f"commodity_analyst result not found in db: {trade_date}")
        result = self._load_json_from_result_path(db_row.result_path)
        if not isinstance(result, dict):
            result = {}

        per_commodity = result.get("per_commodity", {})
        if not isinstance(per_commodity, dict):
            per_commodity = {}

        market = self._get_market_fetcher()
        commodity_items: List[Dict[str, Any]] = []
        for name, item in per_commodity.items():
            if not isinstance(item, dict):
                continue
            name_str = str(name)
            series = self._safe_fetch_commodity_series(market, name_str, trade_date, window=60)
            start = series[0]["close"] if series else None
            end = series[-1]["close"] if series else None
            commodity_items.append(
                {
                    "name": item.get("name", name_str),
                    "trend": self._map_simple_trend(item.get("trend")),
                    "start": start,
                    "end": end,
                    "price_summary": item.get("price_summary"),
                    "macro_implication": item.get("macro_implication"),
                    "market_series": series,
                }
            )

        output_summary = {
            "overall_trend": self._map_simple_trend(result.get("overall_trend")),
            "commodity_market_trend": self._map_commodity_market_trend(result.get("commodity_market_trend")),
            "macro_signals": self._map_commodity_macro_signals(result.get("macro_signals")),
            "macro_summary": result.get("macro_summary"),
        }
        return {
            "trade_date": trade_date,
            "commodity_items": commodity_items,
            "output_summary": output_summary,
        }

    def get_news_result(self, trade_date: str) -> Dict[str, Any]:
        db_row = (
            self.db.query(NewsAnalystKey)
            .filter(NewsAnalystKey.trade_date == trade_date)
            .first()
        )
        if not db_row or not db_row.result_path:
            raise FileNotFoundError(f"news_analyst result not found in db: {trade_date}")
        result = self._load_json_from_result_path(db_row.result_path)
        if not isinstance(result, dict):
            return {}
        return self._map_news_result(result)

    def list_analyst_dates(self, group: str, analyst: str) -> List[str]:
        if group == "sector_analyst" and analyst == "sector_trend_analyst":
            rows = (
                self.db.query(SectorTrendAnalystKey.trade_date)
                .distinct()
                .order_by(SectorTrendAnalystKey.trade_date.desc())
                .all()
            )
            dates = [str(r[0]) for r in rows if r and r[0]]
            if dates:
                return dates

        if group == "sector_analyst" and analyst == "sector_capital_flow_analyst":
            rows = (
                self.db.query(SectorCapitalFlowAnalystKey.trade_date)
                .distinct()
                .order_by(SectorCapitalFlowAnalystKey.trade_date.desc())
                .all()
            )
            dates = [str(r[0]) for r in rows if r and r[0]]
            if dates:
                return dates

        if group == "macro_analyst" and analyst == "news_analyst":
            rows = (
                self.db.query(NewsAnalystKey.trade_date)
                .distinct()
                .order_by(NewsAnalystKey.trade_date.desc())
                .all()
            )
            dates = [str(r[0]) for r in rows if r and r[0]]
            if dates:
                return dates

        if group == "macro_analyst" and analyst == "market_sentiment_analyst":
            rows = (
                self.db.query(MarketSentimentAnalystKey.trade_date)
                .distinct()
                .order_by(MarketSentimentAnalystKey.trade_date.desc())
                .all()
            )
            dates = [str(r[0]) for r in rows if r and r[0]]
            if dates:
                return dates

        if group == "macro_analyst" and analyst == "macro_economist":
            rows = (
                self.db.query(MacroEconomistKey.trade_date)
                .distinct()
                .order_by(MacroEconomistKey.trade_date.desc())
                .all()
            )
            dates = [str(r[0]) for r in rows if r and r[0]]
            if dates:
                return dates

        if group == "macro_analyst" and analyst == "commodity_analyst":
            rows = (
                self.db.query(CommodityAnalystKey.trade_date)
                .distinct()
                .order_by(CommodityAnalystKey.trade_date.desc())
                .all()
            )
            dates = [str(r[0]) for r in rows if r and r[0]]
            if dates:
                return dates

        if group == "stock_analyst" and analyst == "stock_screener":
            rows = (
                self.db.query(StockScreenerKey.trade_date)
                .distinct()
                .order_by(StockScreenerKey.trade_date.desc())
                .all()
            )
            dates = [str(r[0]) for r in rows if r and r[0]]
            if dates:
                return dates

        return []

    def _build_macro_economist_icon_data(self, result: Dict[str, Any]) -> List[Dict[str, str]]:
        field_specs = [
            ("gdp_trend", "GDP趋势", "chart-line"),
            ("cpi_trend", "CPI趋势", "thermometer"),
            ("m2_trend", "M2趋势", "banknote"),
            ("pmi_status", "PMI状态", "factory"),
            ("growth_signal", "增长信号", "activity"),
            ("inflation_signal", "通胀信号", "flame"),
            ("liquidity_signal", "流动性信号", "droplets"),
            ("equity_market_bias", "权益市场倾向", "candlestick-chart"),
            ("bond_market_bias", "债券市场倾向", "landmark"),
            ("commodity_bias", "商品市场倾向", "package"),
        ]
        icon_data: List[Dict[str, str]] = []
        for key, label, icon in field_specs:
            raw = result.get(key)
            if not isinstance(raw, str) or not raw.strip():
                continue
            value = raw.strip()
            icon_data.append(
                {
                    "key": key,
                    "label": label,
                    "value": value,
                    "icon": icon,
                    "tone": self._normalize_icon_tone(value),
                }
            )
        return icon_data

    def _normalize_icon_tone(self, value: str) -> str:
        mapping = {
            "up": "positive",
            "down": "negative",
            "rising": "negative",
            "weakening": "negative",
            "loose": "positive",
            "contraction": "negative",
            "bullish": "positive",
            "bearish": "negative",
            "neutral": "neutral",
            "stable": "neutral",
            "slowdown": "negative",
        }
        return mapping.get(value.lower(), "neutral")

    def _get_market_fetcher(self) -> Optional[Any]:
        try:
            from dataflow.market_data import MarketDataFetcher

            return MarketDataFetcher()
        except Exception:
            return None

    def _safe_fetch_lpr(self, market: Optional[Any], trade_date: str) -> List[Dict[str, Any]]:
        if market is None:
            return []
        end_date = trade_date[:8]
        start_date = f"{int(end_date[:4]) - 1}{end_date[4:6]}01"
        try:
            return market.fetch_shibor_lpr(start_date, end_date).to_dict(orient="records")
        except Exception:
            return []

    def _safe_fetch_cpi(self, market: Optional[Any], trade_date: str) -> List[Dict[str, Any]]:
        if market is None:
            return []
        end_m = trade_date[:6]
        start_m = f"{int(end_m[:4]) - 1}{end_m[4:6]}"
        try:
            return market.fetch_cpi(start_m=start_m, end_m=end_m).to_dict(orient="records")
        except Exception:
            return []

    def _safe_fetch_sf(self, market: Optional[Any], trade_date: str) -> List[Dict[str, Any]]:
        if market is None:
            return []
        end_m = trade_date[:6]
        start_m = f"{int(end_m[:4]) - 1}{end_m[4:6]}"
        try:
            return market.fetch_sf_month(start_m=start_m, end_m=end_m).to_dict(orient="records")
        except Exception:
            return []

    def _safe_fetch_pmi(self, market: Optional[Any], trade_date: str) -> List[Dict[str, Any]]:
        if market is None:
            return []
        end_m = trade_date[:6]
        start_m = f"{int(end_m[:4]) - 1}{end_m[4:6]}"
        try:
            return market.fetch_pmi(start_m=start_m, end_m=end_m).to_dict(orient="records")
        except Exception:
            return []

    def _safe_fetch_m2(self, market: Optional[Any], trade_date: str) -> List[Dict[str, Any]]:
        if market is None:
            return []
        end_m = trade_date[:6]
        start_m = f"{int(end_m[:4]) - 1}{end_m[4:6]}"
        try:
            return market.fetch_m2(start_m=start_m, end_m=end_m).to_dict(orient="records")
        except Exception:
            return []

    def _safe_fetch_gdp(self, market: Optional[Any], trade_date: str) -> List[Dict[str, Any]]:
        if market is None:
            return []
        end_m = trade_date[:6]
        start_m = f"{int(end_m[:4]) - 1}{end_m[4:6]}"
        start_q = self._to_year_quarter(start_m)
        end_q = self._to_year_quarter(end_m)
        try:
            return market.fetch_gdp(start_q=start_q, end_q=end_q).to_dict(orient="records")
        except Exception:
            return []

    def _safe_fetch_index_series(
        self, market: Optional[Any], ts_code: str, trade_date: str, window: int = 60
    ) -> List[Dict[str, Any]]:
        if market is None:
            return []
        end_date = trade_date[:8]
        start_date = (datetime.strptime(end_date, "%Y%m%d") - timedelta(days=180)).strftime("%Y%m%d")
        try:
            # MarketDataFetcher 暂无封装，直接走底层 ts_pro.index_daily。
            df = market.ts_pro.index_daily(ts_code=ts_code, start_date=start_date, end_date=end_date)
            if df is None or df.empty:
                return []
            rows = df.sort_values("trade_date").tail(window).to_dict(orient="records")
            series: List[Dict[str, Any]] = []
            for row in rows:
                date_str = str(row.get("trade_date", ""))
                if len(date_str) == 8 and date_str.isdigit():
                    date_fmt = f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]}"
                else:
                    date_fmt = date_str
                open_price = self._to_float_or_none(row.get("open"))
                high_price = self._to_float_or_none(row.get("high"))
                low_price = self._to_float_or_none(row.get("low"))
                close_price = self._to_float_or_none(row.get("close"))
                if (
                    open_price is None
                    or high_price is None
                    or low_price is None
                    or close_price is None
                    or open_price <= 0
                    or high_price <= 0
                    or low_price <= 0
                    or close_price <= 0
                ):
                    continue
                amount = self._to_float_or_none(row.get("amount"))
                vol = self._to_float_or_none(row.get("vol"))
                # 统一单位为“亿”，优先 amount（千元）换算，其次 vol 粗略换算。
                volume_in_yi = None
                if amount is not None:
                    volume_in_yi = round(amount / 100000.0, 4)
                elif vol is not None:
                    volume_in_yi = round(vol / 100000000.0, 4)

                series.append(
                    {
                        "date": date_fmt,
                        "open": open_price,
                        "high": high_price,
                        "low": low_price,
                        "close": close_price,
                        "volume": volume_in_yi,
                    }
                )
            return series
        except Exception:
            return []

    def _safe_fetch_commodity_series(
        self, market: Optional[Any], commodity_name: str, trade_date: str, window: int = 60
    ) -> List[Dict[str, Any]]:
        if market is None:
            return []
        meta = self._commodity_meta_by_name.get(commodity_name)
        if not meta:
            return []
        code = meta.get("code")
        source = str(meta.get("source", "")).lower()
        if not code:
            return []

        end_date = trade_date[:8]
        start_date = (datetime.strptime(end_date, "%Y%m%d") - timedelta(days=180)).strftime("%Y%m%d")
        try:
            if source == "sge":
                df = market.fetch_sge_daily(ts_code=code, start_date=start_date, end_date=end_date)
            elif source == "fut":
                df = market.fetch_fut_daily(ts_code=code, start_date=start_date, end_date=end_date)
            else:
                return []
            if df is None or df.empty:
                return []

            rows = df.sort_values("trade_date").tail(window).to_dict(orient="records")
            series: List[Dict[str, Any]] = []
            for row in rows:
                date_str = str(row.get("trade_date", ""))
                if len(date_str) == 8 and date_str.isdigit():
                    date_fmt = f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]}"
                else:
                    date_fmt = date_str
                open_price = self._to_float_or_none(row.get("open"))
                high_price = self._to_float_or_none(row.get("high"))
                low_price = self._to_float_or_none(row.get("low"))
                close_price = self._to_float_or_none(row.get("close"))
                if (
                    open_price is None
                    or high_price is None
                    or low_price is None
                    or close_price is None
                    or open_price <= 0
                    or high_price <= 0
                    or low_price <= 0
                    or close_price <= 0
                ):
                    continue

                amount = self._to_float_or_none(row.get("amount"))
                vol = self._to_float_or_none(row.get("vol"))
                volume_in_yi = None
                if amount is not None:
                    # sge amount 单位元；fut amount 单位万元
                    if source == "sge":
                        volume_in_yi = round(amount / 100000000.0, 4)
                    else:
                        volume_in_yi = round(amount / 10000.0, 4)
                elif vol is not None:
                    volume_in_yi = round(vol / 100000000.0, 4)

                series.append(
                    {
                        "date": date_fmt,
                        "open": open_price,
                        "high": high_price,
                        "low": low_price,
                        "close": close_price,
                        "volume": volume_in_yi,
                    }
                )
            return series
        except Exception:
            return []

    def _get_ths_index_map(self) -> Dict[str, str]:
        if self._ths_index_map is not None:
            return self._ths_index_map
        try:
            from dataflow.industry_data import fetch_ths_index

            df = fetch_ths_index()
            mapping: Dict[str, str] = {}
            if df is not None and not df.empty:
                rows = df.to_dict(orient="records")
                for row in rows:
                    name = row.get("name")
                    ts_code = row.get("ts_code")
                    if isinstance(name, str) and isinstance(ts_code, str) and name and ts_code:
                        mapping[name] = ts_code
            self._ths_index_map = mapping
            return mapping
        except Exception:
            self._ths_index_map = {}
            return {}

    def _safe_fetch_ths_series(self, ts_code: str, trade_date: str, window: int = 60) -> List[Dict[str, Any]]:
        try:
            from dataflow.industry_data import fetch_ths_daily
        except Exception:
            return []
        end_date = trade_date[:8]
        start_date = (datetime.strptime(end_date, "%Y%m%d") - timedelta(days=180)).strftime("%Y%m%d")
        try:
            df = fetch_ths_daily(ts_code=ts_code, start_date=start_date, end_date=end_date)
            if df is None or df.empty:
                return []
            rows = df.sort_values("trade_date").tail(window).to_dict(orient="records")
            out: List[Dict[str, Any]] = []
            for row in rows:
                date_str = str(row.get("trade_date", ""))
                date_fmt = date_str if len(date_str) == 8 and date_str.isdigit() else date_str.replace("-", "")[:8]
                open_price = self._to_float_or_none(row.get("open"))
                high_price = self._to_float_or_none(row.get("high"))
                low_price = self._to_float_or_none(row.get("low"))
                close_price = self._to_float_or_none(row.get("close"))
                if (
                    open_price is None
                    or high_price is None
                    or low_price is None
                    or close_price is None
                    or open_price <= 0
                    or high_price <= 0
                    or low_price <= 0
                    or close_price <= 0
                ):
                    continue
                out.append(
                    {
                        "trade_date": date_fmt,
                        "open": open_price,
                        "high": high_price,
                        "low": low_price,
                        "close": close_price,
                        "pct_change": self._to_float_or_none(row.get("pct_change")),
                        "vol": self._to_float_or_none(row.get("vol")),
                    }
                )
            return out
        except Exception:
            return []

    def _sector_capital_flow_api_snapshot_from_artifact(
        self, result: Dict[str, Any], trade_date: str
    ) -> Optional[Dict[str, Any]]:
        """读取新版轻量 api_snapshot（与 trade_date / meta.end_date 一致时有效）。"""
        cache = result.get("sector_moneyflow_cache")
        if not isinstance(cache, dict):
            return None
        snap = cache.get("api_snapshot")
        if not isinstance(snap, dict) or not snap:
            return None
        meta = cache.get("meta")
        if isinstance(meta, dict):
            end_date = str(meta.get("end_date") or "").replace("-", "")[:8]
            if end_date and end_date != str(trade_date).replace("-", "")[:8]:
                return None
        return snap

    def _sector_moneyflow_rows_from_artifact(
        self, result: Dict[str, Any], trade_date: str
    ) -> List[Dict[str, Any]]:
        """
        兼容旧版：仅当 sector_moneyflow_cache 含全量 rows 且与 trade_date 一致时使用。
        新版 artifact 已改用 api_snapshot，不再含 rows。
        """
        cache = result.get("sector_moneyflow_cache")
        if not isinstance(cache, dict):
            return []
        meta = cache.get("meta")
        if isinstance(meta, dict):
            end_date = str(meta.get("end_date") or "").replace("-", "")[:8]
            if end_date and end_date != str(trade_date).replace("-", "")[:8]:
                return []
        raw_rows = cache.get("rows")
        if not isinstance(raw_rows, list) or not raw_rows:
            return []
        out: List[Dict[str, Any]] = []
        for row in raw_rows:
            if not isinstance(row, dict):
                continue
            td = str(row.get("trade_date", "")).replace("-", "")[:8]
            if len(td) != 8 or not td.isdigit():
                continue
            out.append({**row, "trade_date": td})
        return out

    def _sum_sector_moneyflow_recent_days(self, rows: List[Dict[str, Any]], days: int) -> Optional[float]:
        if days <= 0:
            return None
        date_set = sorted(
            {
                str(row.get("trade_date"))
                for row in rows
                if isinstance(row, dict) and str(row.get("trade_date", "")).isdigit() and len(str(row.get("trade_date", ""))) == 8
            },
            reverse=True,
        )
        if not date_set:
            return None
        selected = set(date_set[:days])
        return self._sum_sector_moneyflow_by_dates(rows, selected)

    def _sum_sector_moneyflow_by_dates(self, rows: List[Dict[str, Any]], dates: set[str]) -> Optional[float]:
        if not dates:
            return None
        total = 0.0
        has_value = False
        for row in rows:
            if str(row.get("trade_date")) not in dates:
                continue
            v = self._to_float_or_none(row.get("net_amount"))
            if v is None:
                continue
            total += v
            has_value = True
        return round(total, 2) if has_value else None

    def _build_one_day_sector_flow(self, one_day_rows: List[Dict[str, Any]], top_k: int = 20) -> List[Dict[str, Any]]:
        by_name: Dict[str, float] = {}
        for row in one_day_rows:
            name = row.get("name")
            if not isinstance(name, str) or not name:
                continue
            net_amount = self._to_float_or_none(row.get("net_amount"))
            if net_amount is None:
                continue
            by_name[name] = by_name.get(name, 0.0) + net_amount
        items = [{"name": name, "net_amount": round(amount, 2)} for name, amount in by_name.items()]
        if top_k <= 0:
            return []

        half = top_k // 2
        inflow = sorted(
            [item for item in items if float(item.get("net_amount") or 0.0) > 0],
            key=lambda x: float(x.get("net_amount") or 0.0),
            reverse=True,
        )[:half]
        outflow_selected = sorted(
            [item for item in items if float(item.get("net_amount") or 0.0) < 0],
            key=lambda x: float(x.get("net_amount") or 0.0),
        )[:half]
        outflow = sorted(outflow_selected, key=lambda x: float(x.get("net_amount") or 0.0), reverse=True)

        # 若一侧不足，使用另一侧剩余补齐到 top_k。
        selected_names = {item["name"] for item in inflow + outflow if isinstance(item.get("name"), str)}
        if len(inflow) < half:
            need = half - len(inflow)
            extra_selected = [
                item
                for item in sorted(
                    [x for x in items if float(x.get("net_amount") or 0.0) < 0 and x.get("name") not in selected_names],
                    key=lambda x: float(x.get("net_amount") or 0.0),
                )
            ][:need]
            extra = sorted(extra_selected, key=lambda x: float(x.get("net_amount") or 0.0), reverse=True)
            outflow.extend(extra)
            selected_names.update(item["name"] for item in extra if isinstance(item.get("name"), str))
        if len(outflow) < half:
            need = half - len(outflow)
            extra = [
                item
                for item in sorted(
                    [x for x in items if float(x.get("net_amount") or 0.0) > 0 and x.get("name") not in selected_names],
                    key=lambda x: float(x.get("net_amount") or 0.0),
                    reverse=True,
                )
            ][:need]
            inflow.extend(extra)

        return inflow + outflow

    def _build_sector_moneyflow_rows(self, rows: List[Dict[str, Any]], top_k: int = 60) -> List[Dict[str, Any]]:
        out: List[Dict[str, Any]] = []
        for row in rows:
            td = str(row.get("trade_date", ""))
            if len(td) != 8 or not td.isdigit():
                continue
            out.append(
                {
                    "trade_date": td,
                    "ts_code": row.get("ts_code"),
                    "name": row.get("name"),
                    "lead_stock": row.get("lead_stock"),
                    "pct_change": self._to_float_or_none(row.get("pct_change")),
                    "net_amount": self._to_float_or_none(row.get("net_amount")),
                }
            )
        if top_k <= 0:
            return []
        half = top_k // 2

        inflow = sorted(
            [item for item in out if float(item.get("net_amount") or 0.0) > 0],
            key=lambda x: float(x.get("net_amount") or 0.0),
            reverse=True,
        )[:half]
        outflow_selected = sorted(
            [item for item in out if float(item.get("net_amount") or 0.0) < 0],
            key=lambda x: float(x.get("net_amount") or 0.0),
        )[:half]
        outflow = sorted(outflow_selected, key=lambda x: float(x.get("net_amount") or 0.0), reverse=True)

        # 一侧不足时，使用另一侧补齐。
        selected = {(item.get("ts_code"), item.get("trade_date")) for item in inflow + outflow}
        if len(inflow) < half:
            need = half - len(inflow)
            extra_selected = [
                item
                for item in sorted(
                    [
                        x
                        for x in out
                        if float(x.get("net_amount") or 0.0) < 0 and (x.get("ts_code"), x.get("trade_date")) not in selected
                    ],
                    key=lambda x: float(x.get("net_amount") or 0.0),
                )
            ][:need]
            extra = sorted(extra_selected, key=lambda x: float(x.get("net_amount") or 0.0), reverse=True)
            outflow.extend(extra)
            selected.update((item.get("ts_code"), item.get("trade_date")) for item in extra)
        if len(outflow) < half:
            need = half - len(outflow)
            extra = [
                item
                for item in sorted(
                    [
                        x
                        for x in out
                        if float(x.get("net_amount") or 0.0) > 0 and (x.get("ts_code"), x.get("trade_date")) not in selected
                    ],
                    key=lambda x: float(x.get("net_amount") or 0.0),
                    reverse=True,
                )
            ][:need]
            inflow.extend(extra)

        return inflow + outflow

    def _to_year_quarter(self, yyyymm: str) -> str:
        year = yyyymm[:4]
        month = int(yyyymm[4:6])
        quarter = (month - 1) // 3 + 1
        return f"{year}Q{quarter}"

    def _build_monthly_series(self, rows: List[Dict[str, Any]], value_keys: List[str], max_points: int = 12) -> List[Dict[str, Any]]:
        series: List[Dict[str, Any]] = []
        for row in rows[-max_points:]:
            if not isinstance(row, dict):
                continue
            raw_month = row.get("month") or row.get("date")
            month_label = self._format_month_label(raw_month)
            if month_label is None:
                continue
            value = self._extract_numeric_value(row, value_keys)
            if value is None:
                continue
            series.append({"month": month_label, "value": value})
        return series

    def _build_quarterly_series(
        self, rows: List[Dict[str, Any]], value_keys: List[str], max_points: int = 8
    ) -> List[Dict[str, Any]]:
        series: List[Dict[str, Any]] = []
        for row in rows[-max_points:]:
            if not isinstance(row, dict):
                continue
            quarter = row.get("quarter")
            quarter_label = self._format_quarter_label(quarter)
            if quarter_label is None:
                continue
            value = self._extract_numeric_value(row, value_keys)
            if value is None:
                continue
            series.append({"quarter": quarter_label, "value": value})
        return series

    def _format_month_label(self, raw: Any) -> Optional[str]:
        if raw is None:
            return None
        s = str(raw).strip()
        if len(s) >= 6 and s[:6].isdigit():
            return f"{int(s[4:6])}月"
        try:
            dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
            return f"{dt.month}月"
        except ValueError:
            return None

    def _format_quarter_label(self, raw: Any) -> Optional[str]:
        if raw is None:
            return None
        s = str(raw).strip().upper()
        if "Q" in s:
            q = s.split("Q")[-1]
            if q in {"1", "2", "3", "4"}:
                return f"Q{q}"
        return None

    def _extract_numeric_value(self, row: Dict[str, Any], keys: List[str]) -> Optional[float]:
        lower_key_map = {str(k).lower(): k for k in row.keys()}
        for key in keys:
            real_key = lower_key_map.get(key.lower())
            if real_key is None:
                continue
            raw = row.get(real_key)
            if raw is not None and raw != "":
                try:
                    value = float(raw)
                    if not math.isfinite(value):
                        continue
                    return round(value, 4)
                except (TypeError, ValueError):
                    continue
        for value in row.values():
            try:
                parsed = float(value)
                if not math.isfinite(parsed):
                    continue
                return round(parsed, 4)
            except (TypeError, ValueError):
                continue
        return None

    def _to_float_or_none(self, value: Any) -> Optional[float]:
        try:
            if value is None or value == "":
                return None
            parsed = float(value)
            if not math.isfinite(parsed):
                return None
            return round(parsed, 4)
        except (TypeError, ValueError):
            return None

    def _map_market_sentiment_value(self, field: str, raw: Any) -> Any:
        if not isinstance(raw, str):
            return raw
        key = raw.lower()
        mappings: Dict[str, Dict[str, str]] = {
            "index_trend": {
                "up": "上行",
                "down": "下行",
                "neutral": "中性",
                "stable": "稳定",
                "unknown": "未知",
            },
            "market_sentiment": {
                "bullish": "乐观",
                "bearish": "悲观",
                "neutral": "中性",
                "unknown": "未知",
            },
            "volume_signal": {
                "expanding": "放量",
                "contracting": "缩量",
                "stable": "平稳",
                "neutral": "中性",
                "unknown": "未知",
            },
            "volatility_signal": {
                "high": "高波动",
                "medium": "中波动",
                "low": "低波动",
                "stable": "平稳",
                "unknown": "未知",
            },
        }
        return mappings.get(field, {}).get(key, raw)

    def _map_simple_trend(self, raw: Any) -> Any:
        if not isinstance(raw, str):
            return raw
        mapping = {"up": "上行", "down": "下行", "neutral": "中性", "stable": "稳定", "unknown": "未知"}
        return mapping.get(raw.lower(), raw)

    def _map_commodity_market_trend(self, raw: Any) -> Any:
        if not isinstance(raw, str):
            return raw
        mapping = {"up": "上行", "down": "下行", "mixed": "分化", "neutral": "中性", "unknown": "未知"}
        return mapping.get(raw.lower(), raw)

    def _map_commodity_macro_signals(self, raw: Any) -> Dict[str, Any]:
        if not isinstance(raw, dict):
            raw = {}
        growth_mapping = {"strong": "强劲", "weakening": "走弱", "neutral": "中性", "unknown": "未知"}
        inflation_mapping = {"rising": "上行", "falling": "回落", "mixed": "分化", "neutral": "中性", "unknown": "未知"}
        risk_mapping = {"risk_on": "风险偏好", "risk_off": "风险规避", "neutral": "中性", "unknown": "未知"}
        growth = raw.get("growth_signal")
        inflation = raw.get("inflation_signal")
        risk = raw.get("risk_sentiment")
        return {
            "growth_signal": growth_mapping.get(str(growth).lower(), growth),
            "inflation_signal": inflation_mapping.get(str(inflation).lower(), inflation),
            "risk_sentiment": risk_mapping.get(str(risk).lower(), risk),
        }

    def _map_news_result(self, result: Dict[str, Any]) -> Dict[str, Any]:
        mapped = dict(result)

        events = mapped.get("events")
        if isinstance(events, list):
            mapped_events: List[Dict[str, Any]] = []
            for event in events:
                if not isinstance(event, dict):
                    continue
                event_mapped = dict(event)
                event_mapped["sentiment"] = self._map_news_signal("sentiment", event.get("sentiment"))
                mapped_events.append(event_mapped)
            mapped["events"] = mapped_events

        sector_impacts = mapped.get("sector_impacts")
        if isinstance(sector_impacts, dict):
            sector_mapped: Dict[str, Any] = {}
            for sector, info in sector_impacts.items():
                if not isinstance(info, dict):
                    sector_mapped[sector] = info
                    continue
                info_mapped = dict(info)
                info_mapped["sentiment"] = self._map_news_signal("sector_sentiment", info.get("sentiment"))
                sector_mapped[sector] = info_mapped
            mapped["sector_impacts"] = sector_mapped

        macro_env = mapped.get("macro_environment")
        if isinstance(macro_env, dict):
            macro_env_mapped = dict(macro_env)
            macro_env_mapped["liquidity"] = self._map_news_signal("liquidity", macro_env.get("liquidity"))
            macro_env_mapped["policy_bias"] = self._map_news_signal("policy_bias", macro_env.get("policy_bias"))
            macro_env_mapped["global_risk"] = self._map_news_signal("global_risk", macro_env.get("global_risk"))
            macro_env_mapped["market_sentiment"] = self._map_news_signal(
                "market_sentiment", macro_env.get("market_sentiment")
            )
            mapped["macro_environment"] = macro_env_mapped

        return mapped

    def _map_news_signal(self, field: str, raw: Any) -> Any:
        if not isinstance(raw, str):
            return raw
        key = raw.lower()
        mappings: Dict[str, Dict[str, str]] = {
            "sentiment": {"positive": "利好", "neutral": "中性", "negative": "利空"},
            "sector_sentiment": {"bullish": "看多", "neutral": "中性", "bearish": "看空"},
            "liquidity": {"loose": "宽松", "neutral": "中性", "tight": "偏紧"},
            "policy_bias": {"easing": "宽松", "neutral": "中性", "tightening": "收紧"},
            "global_risk": {"high": "高", "medium": "中", "low": "低"},
            "market_sentiment": {"bullish": "乐观", "neutral": "中性", "bearish": "悲观"},
        }
        return mappings.get(field, {}).get(key, raw)

    def _map_macro_manager_signal(self, field: str, raw: Any) -> Any:
        if not isinstance(raw, str):
            return raw
        key = raw.lower()
        mappings: Dict[str, Dict[str, str]] = {
            "market_direction": {
                "bullish": "看多",
                "bearish": "看空",
                "neutral": "中性",
                "neutral-bullish": "中性偏多",
                "neutral-bearish": "中性偏空",
                "unknown": "未知",
            }
        }
        return mappings.get(field, {}).get(key, raw)

    def _map_sector_trend_signal(self, field: str, raw: Any) -> Any:
        if not isinstance(raw, str):
            return raw
        key = raw.lower()
        mappings: Dict[str, Dict[str, str]] = {
            "market_regime": {
                "up": "上行",
                "down": "下行",
                "mixed": "分化",
                "trend_following": "趋势延续",
                "rotation": "轮动",
                "repair": "修复",
                "risk_off": "避险",
                "neutral": "中性",
                "stable": "稳定",
                "unknown": "未知",
            }
        }
        return mappings.get(field, {}).get(key, raw)

    def _map_sector_manager_signal(self, field: str, raw: Any) -> Any:
        if not isinstance(raw, str):
            return raw
        key = raw.lower()
        mappings: Dict[str, Dict[str, str]] = {
            "market_regime": {
                "up": "上行",
                "down": "下行",
                "mixed": "分化",
                "trend": "趋势",
                "trend_following": "趋势延续",
                "rotation": "轮动",
                "repair": "修复",
                "risk_off": "避险",
                "neutral": "中性",
                "stable": "稳定",
                "unknown": "未知",
            },
            "market_bias": {
                "bullish": "偏多",
                "bearish": "偏空",
                "neutral": "中性",
                "unknown": "未知",
            },
            "action_bias": {
                "follow_leaders": "跟随主线",
                "low_buy_repair": "逢低修复",
                "fast_rotation": "快速轮动",
                "defense": "防御",
                "neutral": "中性",
                "offense": "进攻",
                "wait_and_see": "观望",
                "unknown": "未知",
            },
        }
        return mappings.get(field, {}).get(key, raw)

    def _map_sector_capital_flow_signal(self, field: str, raw: Any) -> Any:
        if not isinstance(raw, str):
            return raw
        key = raw.lower()
        mappings: Dict[str, Dict[str, str]] = {
            "market_bias": {
                "bullish": "偏多",
                "neutral": "中性",
                "bearish": "偏空",
                "unknown": "未知",
            }
        }
        return mappings.get(field, {}).get(key, raw)

    def _map_stock_manager_signal(self, field: str, raw: Any) -> Any:
        if not isinstance(raw, str):
            return raw
        key = raw.lower()
        mappings: Dict[str, Dict[str, str]] = {
            "action_signal": {
                "buy": "买入",
                "watch": "观察",
                "sell": "卖出",
                "hold": "持有",
                "neutral": "中性",
                "unknown": "未知",
            }
        }
        return mappings.get(field, {}).get(key, raw)

    def _map_macro_llm_output(self, llm_output: Dict[str, Any]) -> Dict[str, Any]:
        mapped = dict(llm_output)
        enum_mapping = {
            "gdp_trend": {"up": "上行", "down": "下行", "stable": "稳定", "unknown": "未知"},
            "lpr_trend": {"up": "上行", "down": "下行", "stable": "稳定", "unknown": "未知"},
            "cpi_trend": {"up": "上行", "down": "下行", "stable": "稳定", "unknown": "未知"},
            "sf_trend": {"up": "上行", "down": "下行", "stable": "稳定", "unknown": "未知"},
            "m2_trend": {"up": "上行", "down": "下行", "stable": "稳定", "unknown": "未知"},
            "pmi_status": {"expansion": "扩张", "contraction": "收缩", "stable": "稳定", "unknown": "未知"},
            "growth_signal": {"strong": "强劲", "weakening": "走弱", "stable": "平稳", "unknown": "未知"},
            "inflation_signal": {"rising": "上行", "falling": "回落", "stable": "平稳", "unknown": "未知"},
            "liquidity_signal": {"loose": "宽松", "neutral": "中性", "tight": "偏紧", "unknown": "未知"},
            "macro_regime": {
                "growth": "增长",
                "slowdown": "放缓",
                "recession": "衰退",
                "recovery": "复苏",
                "stagflation": "滞胀",
                "liquidity_expansion": "流动性扩张",
                "unknown": "未知",
            },
            "equity_market_bias": {"bullish": "看多", "neutral": "中性", "bearish": "看空", "unknown": "未知"},
            "bond_market_bias": {"bullish": "看多", "neutral": "中性", "bearish": "看空", "unknown": "未知"},
            "commodity_bias": {"bullish": "看多", "neutral": "中性", "bearish": "看空", "unknown": "未知"},
        }
        for field, mapping in enum_mapping.items():
            raw = mapped.get(field)
            if isinstance(raw, str):
                mapped[field] = mapping.get(raw.lower(), raw)
        return mapped

    def get_stock_combined_result(self, ts_code: str, trade_date: str) -> Dict[str, Any]:
        result: Dict[str, Any] = {"trade_date": trade_date, "ts_code": ts_code, "available": {}}
        mappings = {
            "stock_manager": self.get_stock_manager_result,
            "fundamental": self.get_fundamental_result,
            "technical": self.get_technical_result,
        }
        for key, fn in mappings.items():
            try:
                result[key] = fn(ts_code, trade_date)
                result["available"][key] = True
            except FileNotFoundError:
                result[key] = None
                result["available"][key] = False
        return result

    def list_portfolio_dates(self) -> List[str]:
        default_version = self._default_portfolio_version()
        return sorted(self.list_portfolio_dates_by_version(default_version))

    def get_latest_portfolio(self) -> Dict[str, Any]:
        dates = self.list_portfolio_dates()
        if not dates:
            raise FileNotFoundError("no portfolio result found")
        return self.get_portfolio_result(dates[-1])

    def _normalize_trade_date(self, value: Optional[str]) -> Optional[str]:
        if not value:
            return None
        s = str(value).strip()
        if len(s) == 8 and s.isdigit():
            return s
        if len(s) == 10 and s[4] == "-" and s[7] == "-":
            compact = s.replace("-", "")
            if compact.isdigit():
                return compact
        raise ValueError("invalid date format, expected YYYYMMDD or YYYY-MM-DD")

    def _format_trade_date(self, yyyymmdd: str) -> str:
        return f"{yyyymmdd[:4]}-{yyyymmdd[4:6]}-{yyyymmdd[6:8]}"

    def _extract_total_capital_from_portfolio_payload(self, payload: Dict[str, Any]) -> float:
        meta = payload.get("meta")
        if isinstance(meta, dict):
            total = self._to_float_or_none(meta.get("total_capital"))
            if total is not None:
                return total
        rows = payload.get("portfolio_table")
        if not isinstance(rows, list):
            return 0.0
        total_capital = 0.0
        for row in rows:
            if not isinstance(row, dict):
                continue
            total_capital += float(self._to_float_or_none(row.get("市值 (元)")) or 0.0)
        return total_capital

    def _list_portfolio_dates_for_version(self, version: Optional[str]) -> List[str]:
        if version:
            return sorted(self.list_portfolio_dates_by_version(version))
        return sorted(self.list_portfolio_dates())

    def _get_portfolio_result_by_version(self, version: Optional[str], trade_date: str) -> Dict[str, Any]:
        if version:
            rows = (
                self.db.query(PortfolioDecisionKey)
                .filter(PortfolioDecisionKey.trade_date == trade_date)
                .all()
            )
            row = next(
                (
                    x
                    for x in rows
                    if self._parse_portfolio_version_from_run_id(x.run_id) == version
                ),
                None,
            )
            if not row:
                raise FileNotFoundError(f"portfolio result not found in db: {version}/{trade_date}")
            return self._load_artifact_from_result_path(row.result_path)
        return self.get_portfolio_result(trade_date)

    def get_portfolio_dates_api(self, version: Optional[str] = None) -> Dict[str, Any]:
        dates = sorted(self._list_portfolio_dates_for_version(version), reverse=True)
        return {"dates": dates}

    def get_portfolio_snapshot_api(self, trade_date: str, version: Optional[str] = None) -> Dict[str, Any]:
        normalized_date = self._normalize_trade_date(trade_date)
        if not normalized_date:
            raise ValueError("trade_date is required")

        payload = self._get_portfolio_result_by_version(version, normalized_date).get("result", {})
        if not isinstance(payload, dict):
            payload = {}

        all_dates = self._list_portfolio_dates_for_version(version)
        if normalized_date not in all_dates:
            raise FileNotFoundError(f"portfolio trade_date not found: {normalized_date}")

        capitals: List[float] = []
        for d in all_dates:
            if d > normalized_date:
                break
            result = self._get_portfolio_result_by_version(version, d).get("result", {})
            if not isinstance(result, dict):
                result = {}
            capitals.append(self._extract_total_capital_from_portfolio_payload(result))

        annualized_return_pct = None
        sharpe_ratio = None
        max_drawdown_pct = 0.0
        one_week_return_pct = None
        one_month_return_pct = None

        if capitals:
            first = capitals[0]
            latest = capitals[-1]
            if first > 0:
                start_dt = datetime.strptime(all_dates[0], "%Y%m%d")
                end_dt = datetime.strptime(normalized_date, "%Y%m%d")
                day_span = max((end_dt - start_dt).days, 1)
                year_span = day_span / 365.0
                annualized_return_pct = ((latest / first) ** (1.0 / year_span) - 1.0) * 100 if year_span > 0 else 0.0

            peak = capitals[0]
            for value in capitals:
                peak = max(peak, value)
                if peak > 0:
                    dd = (value - peak) / peak * 100
                    max_drawdown_pct = min(max_drawdown_pct, dd)

            if len(capitals) >= 2 and capitals[-2] > 0:
                one_week_return_pct = (capitals[-1] - capitals[-2]) / capitals[-2] * 100

            target_dt = datetime.strptime(normalized_date, "%Y%m%d")
            month_base = target_dt - timedelta(days=30)
            month_idx = 0
            for idx, d in enumerate(all_dates):
                if d > normalized_date:
                    break
                if datetime.strptime(d, "%Y%m%d") <= month_base:
                    month_idx = idx
            if capitals[month_idx] > 0:
                one_month_return_pct = (capitals[-1] - capitals[month_idx]) / capitals[month_idx] * 100

            rets: List[float] = []
            for i in range(1, len(capitals)):
                prev = capitals[i - 1]
                curr = capitals[i]
                if prev > 0:
                    rets.append((curr - prev) / prev)
            if rets:
                mean_ret = sum(rets) / len(rets)
                variance = sum((r - mean_ret) ** 2 for r in rets) / len(rets)
                std_ret = math.sqrt(variance)
                if std_ret > 0:
                    start_dt = datetime.strptime(all_dates[0], "%Y%m%d")
                    end_dt = datetime.strptime(normalized_date, "%Y%m%d")
                    day_span = max((end_dt - start_dt).days, 1)
                    periods_per_year = max(1.0, len(rets) * 365.0 / day_span)
                    sharpe_ratio = (mean_ret / std_ret) * math.sqrt(periods_per_year)

        rows = payload.get("portfolio_table")
        if not isinstance(rows, list):
            rows = []
        positions: List[Dict[str, Any]] = []
        for row in rows:
            if not isinstance(row, dict):
                continue
            weight = row.get("仓位")
            weight_num = None
            if isinstance(weight, str):
                w = weight.replace("%", "").strip()
                weight_num = self._to_float_or_none(w)
            elif isinstance(weight, (int, float)):
                weight_num = self._to_float_or_none(weight)
            positions.append(
                {
                    "ts_code": row.get("ts_code"),
                    "name": row.get("资产名称"),
                    "industry": row.get("行业/板块"),
                    "weight": weight_num,
                    "shares": row.get("持仓股数"),
                    "cost_price": self._to_float_or_none(row.get("成本价")),
                    "latest_price": self._to_float_or_none(row.get("收盘价"))
                    if row.get("收盘价") is not None
                    else self._to_float_or_none(row.get("开盘价")),
                    "total_return": row.get("总收益 (%)"),
                }
            )

        return {
            "trade_date": self._format_trade_date(normalized_date),
            "version": version,
            "metrics": {
                "annualized_return_pct": self._to_float_or_none(annualized_return_pct),
                "sharpe_ratio": self._to_float_or_none(sharpe_ratio),
                "max_drawdown_pct": self._to_float_or_none(max_drawdown_pct),
                "one_week_return_pct": self._to_float_or_none(one_week_return_pct),
                "one_month_return_pct": self._to_float_or_none(one_month_return_pct),
            },
            "positions": positions,
        }

    def get_portfolio_history_series_api(
        self,
        start_date: Optional[str],
        end_date: Optional[str],
        version: Optional[str] = None,
    ) -> Dict[str, Any]:
        normalized_start = self._normalize_trade_date(start_date) if start_date else None
        normalized_end = self._normalize_trade_date(end_date) if end_date else None

        dates = self._list_portfolio_dates_for_version(version)
        if normalized_start:
            dates = [d for d in dates if d >= normalized_start]
        if normalized_end:
            dates = [d for d in dates if d <= normalized_end]
        totals: List[float] = []
        for d in dates:
            payload = self._get_portfolio_result_by_version(version, d).get("result", {})
            if not isinstance(payload, dict):
                payload = {}
            totals.append(self._extract_total_capital_from_portfolio_payload(payload))

        if not totals:
            return {"series": []}

        base = totals[0] if totals[0] > 0 else 1.0
        peak_nv = totals[0] / base if base else 1.0
        series: List[Dict[str, Any]] = []
        prev_total = None
        for idx, d in enumerate(dates):
            total = totals[idx]
            net_value = total / base if base else 1.0
            peak_nv = max(peak_nv, net_value)
            daily_ret = None
            if prev_total and prev_total > 0:
                daily_ret = (total - prev_total) / prev_total * 100
            drawdown = (net_value - peak_nv) / peak_nv * 100 if peak_nv > 0 else 0.0
            series.append(
                {
                    "date": self._format_trade_date(d),
                    "net_value": self._to_float_or_none(net_value),
                    "daily_return_pct": self._to_float_or_none(daily_ret),
                    "drawdown_pct": self._to_float_or_none(drawdown),
                }
            )
            prev_total = total

        return {"series": series}

    def get_portfolio_history(
        self,
        start_date: Optional[str],
        end_date: Optional[str],
        page: int,
        page_size: int,
    ) -> Dict[str, Any]:
        dates = self.list_portfolio_dates()
        if start_date:
            dates = [d for d in dates if d >= start_date]
        if end_date:
            dates = [d for d in dates if d <= end_date]

        items = [self.get_portfolio_result(d) for d in dates]
        total = len(items)
        start = (page - 1) * page_size
        end = start + page_size
        return {"page": page, "page_size": page_size, "total": total, "items": items[start:end]}

    def compare_portfolio(self, dates: List[str]) -> Dict[str, Any]:
        return {"items": [self.get_portfolio_result(d) for d in dates]}

    def portfolio_performance(self, start_date: Optional[str], end_date: Optional[str]) -> Dict[str, Any]:
        dates = self.list_portfolio_dates()
        if start_date:
            dates = [d for d in dates if d >= start_date]
        if end_date:
            dates = [d for d in dates if d <= end_date]
        if len(dates) < 2:
            raise ValueError("at least two portfolio dates required")

        totals: List[float] = []
        for d in dates:
            payload = self.get_portfolio_result(d)["result"]
            meta = payload.get("meta", {})
            total_capital = meta.get("total_capital")
            if total_capital is None:
                portfolio_table = payload.get("portfolio_table", [])
                total_capital = sum(float(row.get("市值 (元)", 0) or 0) for row in portfolio_table)
            totals.append(float(total_capital))

        first, last = totals[0], totals[-1]
        total_return_pct = ((last - first) / first) * 100 if first else 0.0

        peak = totals[0]
        max_drawdown = 0.0
        for value in totals:
            peak = max(peak, value)
            if peak > 0:
                drawdown = (value - peak) / peak * 100
                max_drawdown = min(max_drawdown, drawdown)

        return {
            "start_date": dates[0],
            "end_date": dates[-1],
            "period_count": len(dates),
            "total_return_pct": round(total_return_pct, 4),
            "max_drawdown_pct": round(max_drawdown, 4),
        }

