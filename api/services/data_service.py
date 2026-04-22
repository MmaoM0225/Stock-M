import json
from pathlib import Path
from typing import Any, Dict, List, Optional

from sqlalchemy.orm import Session


class DataService:
    def __init__(self, db: Session, project_root: Optional[Path] = None):
        self.db = db
        self.project_root = project_root or Path(__file__).resolve().parents[2]
        self.artifacts_root = self.project_root / "data" / "artifacts"
        self.decision_root = self.artifacts_root / "decision"

    def _load_json(self, path: Path) -> Dict[str, Any]:
        if not path.exists():
            raise FileNotFoundError(f"result file not found: {path}")
        with path.open("r", encoding="utf-8") as f:
            return json.load(f)

    def _load_artifact(self, path: Path) -> Dict[str, Any]:
        return {"artifact_path": str(path.relative_to(self.project_root)), "result": self._load_json(path)}

    def get_macro_result(self, trade_date: str) -> Dict[str, Any]:
        path = self.artifacts_root / "manager" / "macro_manager" / trade_date / "result.json"
        return self._load_artifact(path)

    def get_sector_result(self, trade_date: str) -> Dict[str, Any]:
        path = self.artifacts_root / "manager" / "sector_manager" / trade_date / "result.json"
        return self._load_artifact(path)

    def get_screener_result(self, trade_date: str) -> Dict[str, Any]:
        path = self.artifacts_root / "analyst" / "stock_analyst" / "stock_screener" / trade_date / "result.json"
        return self._load_artifact(path)

    def get_stock_pool_result(self, trade_date: str) -> Dict[str, Any]:
        path = self.artifacts_root / "manager" / "stock_pool_manager" / trade_date / "result.json"
        return self._load_artifact(path)

    def _latest_decision_version(self) -> Path:
        versions = sorted([p for p in self.decision_root.iterdir() if p.is_dir()]) if self.decision_root.exists() else []
        if not versions:
            raise FileNotFoundError("decision version directory not found")
        return versions[-1]

    def get_portfolio_result(self, trade_date: str) -> Dict[str, Any]:
        version_dir = self._latest_decision_version()
        path = version_dir / "portfolio" / trade_date / "result.json"
        return self._load_artifact(path)

    def get_stock_manager_result(self, ts_code: str, trade_date: str) -> Dict[str, Any]:
        path = self.artifacts_root / "manager" / "stock_manager" / ts_code / trade_date / "result.json"
        return self._load_artifact(path)

    def get_fundamental_result(self, ts_code: str, trade_date: str) -> Dict[str, Any]:
        path = (
            self.artifacts_root
            / "analyst"
            / "stock_analyst"
            / "stock_fundamental_analyst"
            / ts_code
            / trade_date
            / "result.json"
        )
        return self._load_artifact(path)

    def get_technical_result(self, ts_code: str, trade_date: str) -> Dict[str, Any]:
        path = (
            self.artifacts_root
            / "analyst"
            / "stock_analyst"
            / "stock_technical_analyst"
            / ts_code
            / trade_date
            / "result.json"
        )
        return self._load_artifact(path)

    def get_analyst_result(self, group: str, analyst: str, trade_date: str) -> Dict[str, Any]:
        path = self.artifacts_root / "analyst" / group / analyst / trade_date / "result.json"
        return self._load_artifact(path)

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
        version_dir = self._latest_decision_version() / "portfolio"
        dates: List[str] = []
        if not version_dir.exists():
            return dates
        for date_dir in version_dir.iterdir():
            if date_dir.is_dir() and date_dir.name.isdigit() and len(date_dir.name) == 8:
                if (date_dir / "result.json").exists():
                    dates.append(date_dir.name)
        return sorted(dates)

    def get_latest_portfolio(self) -> Dict[str, Any]:
        dates = self.list_portfolio_dates()
        if not dates:
            raise FileNotFoundError("no portfolio result found")
        return self.get_portfolio_result(dates[-1])

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

