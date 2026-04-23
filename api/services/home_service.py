import json
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

from sqlalchemy.orm import Session


class HomeService:
    def __init__(self, db: Session, project_root: Optional[Path] = None):
        self.db = db
        self.project_root = project_root or Path(__file__).resolve().parents[2]
        self.artifacts_root = self.project_root / "data" / "artifacts"
        self._agent_sources: List[Dict[str, str]] = [
            {
                "agent": "宏观经理（Macro Manager）",
                "relative_dir": "manager/macro_manager",
                "to": "/data/macro",
            },
            {
                "agent": "板块经理（Sector Manager）",
                "relative_dir": "manager/sector_manager",
                "to": "/data/sector",
            },
            {
                "agent": "选股分析师（Stock Screener）",
                "relative_dir": "analyst/stock_analyst/stock_screener",
                "to": "/data/screener",
            },
            {
                "agent": "股票池经理（Stock Pool Manager）",
                "relative_dir": "manager/stock_pool_manager",
                "to": "/data/stock-pool",
            },
            {
                "agent": "组合决策（Portfolio Decision）",
                "relative_dir": "decision/202401-202604_7d_for_once_ver1.3/portfolio",
                "to": "/data/portfolio",
            },
            {
                "agent": "宏观经济分析师（Macro Economist）",
                "relative_dir": "analyst/macro_analyst/macro_economist",
                "to": "/data/analyst/macro/economist",
            },
            {
                "agent": "新闻分析师（News Analyst）",
                "relative_dir": "analyst/macro_analyst/news_analyst",
                "to": "/data/analyst/macro/news",
            },
            {
                "agent": "市场情绪分析师（Market Sentiment）",
                "relative_dir": "analyst/macro_analyst/market_sentiment_analyst",
                "to": "/data/analyst/macro/market-sentiment",
            },
            {
                "agent": "商品分析师（Commodity Analyst）",
                "relative_dir": "analyst/macro_analyst/commodity_analyst",
                "to": "/data/analyst/macro/commodity",
            },
        ]
        self._overall_portfolio_dir = self.artifacts_root / "decision" / "overall_ver1.4" / "portfolio"

    def get_agent_outputs_preview(self, page: int, page_size: int) -> Dict[str, Any]:
        items = [item for item in (self._build_agent_item(src) for src in self._agent_sources) if item]
        total = len(items)
        start = (page - 1) * page_size
        end = start + page_size
        return {"page": page, "page_size": page_size, "total": total, "items": items[start:end]}

    def get_portfolio_summary(self) -> Dict[str, Any]:
        dates = self._list_overall_portfolio_dates()
        if not dates:
            raise FileNotFoundError("no portfolio result found in overall_ver1.4")

        latest_date = dates[-1]
        latest_payload = self._load_json(self._overall_portfolio_dir / latest_date / "result.json")

        top_positions = self._extract_top_positions(latest_payload, top_n=5)
        monthly_returns = self._extract_recent_returns(dates, recent_n=5)
        metrics = self._extract_performance_metrics(dates)

        return {
            "top_positions": top_positions,
            "monthly_returns": monthly_returns,
            "metrics": metrics,
        }

    def _build_agent_item(self, source: Dict[str, str]) -> Optional[Dict[str, Any]]:
        base_dir = self.artifacts_root / source["relative_dir"]
        result_file = self._latest_result_file(base_dir)
        if result_file is None:
            return None

        payload = self._load_json(result_file)
        output = self._extract_output(payload)
        signal = self._extract_signal(payload)
        updated_at = self._extract_updated_at(payload, result_file)
        item: Dict[str, Any] = {
            "agent": source["agent"],
            "output": output,
            "updated_at": updated_at,
            "to": source["to"],
        }
        if signal:
            item["signal"] = signal
        return item

    def _list_overall_portfolio_dates(self) -> List[str]:
        if not self._overall_portfolio_dir.exists():
            return []
        dates: List[str] = []
        for child in self._overall_portfolio_dir.iterdir():
            if child.is_dir() and child.name.isdigit() and len(child.name) == 8:
                if (child / "result.json").exists():
                    dates.append(child.name)
        return sorted(dates)

    def _extract_top_positions(self, payload: Dict[str, Any], top_n: int) -> List[Dict[str, Any]]:
        rows = payload.get("portfolio_table", [])
        if not isinstance(rows, list):
            return []

        result: List[Dict[str, Any]] = []
        for row in rows:
            if not isinstance(row, dict):
                continue
            if row.get("ts_code") is None:
                continue
            name = row.get("资产名称")
            if not isinstance(name, str) or not name.strip():
                continue
            result.append(
                {
                    "name": name,
                    "cost_price": self._to_float(row.get("成本价")),
                    "weight": self._parse_percent(row.get("仓位")),
                }
            )
            if len(result) >= top_n:
                break
        return result

    def _extract_recent_returns(self, dates: List[str], recent_n: int) -> List[Dict[str, Any]]:
        picked_dates = dates[-recent_n:]
        if not picked_dates:
            return []

        capitals: List[float] = []
        for date in picked_dates:
            payload = self._load_json(self._overall_portfolio_dir / date / "result.json")
            total_capital = self._extract_total_capital(payload)
            capitals.append(total_capital)

        base = capitals[0] if capitals and capitals[0] else 0.0
        result: List[Dict[str, Any]] = []
        for idx, date in enumerate(picked_dates):
            value = 0.0
            if base:
                value = (capitals[idx] - base) / base * 100
            result.append({"month": f"{date[4:6]}-{date[6:8]}", "value": round(value, 2)})
        return result

    def _extract_performance_metrics(self, dates: List[str]) -> Dict[str, float]:
        capitals: List[float] = []
        for date in dates:
            payload = self._load_json(self._overall_portfolio_dir / date / "result.json")
            capitals.append(self._extract_total_capital(payload))

        if not capitals:
            return {"max_drawdown_pct": 0.0, "total_return_pct": 0.0}

        first = capitals[0]
        last = capitals[-1]
        total_return_pct = ((last - first) / first * 100) if first else 0.0

        peak = capitals[0]
        max_drawdown_pct = 0.0
        for capital in capitals:
            peak = max(peak, capital)
            if peak > 0:
                drawdown_pct = (capital - peak) / peak * 100
                max_drawdown_pct = min(max_drawdown_pct, drawdown_pct)

        return {
            "max_drawdown_pct": round(max_drawdown_pct, 2),
            "total_return_pct": round(total_return_pct, 2),
        }

    def _extract_total_capital(self, payload: Dict[str, Any]) -> float:
        meta = payload.get("meta")
        if isinstance(meta, dict) and meta.get("total_capital") is not None:
            return self._to_float(meta.get("total_capital"))
        table = payload.get("portfolio_table", [])
        if not isinstance(table, list):
            return 0.0
        total = 0.0
        for row in table:
            if isinstance(row, dict):
                total += self._to_float(row.get("市值 (元)"))
        return total

    def _latest_result_file(self, base_dir: Path) -> Optional[Path]:
        if not base_dir.exists():
            return None
        candidates: List[Path] = []
        for child in base_dir.iterdir():
            if child.is_dir() and child.name.isdigit():
                result_file = child / "result.json"
                if result_file.exists():
                    candidates.append(result_file)
        if not candidates:
            return None
        return max(candidates, key=lambda p: p.parent.name)

    def _load_json(self, path: Path) -> Dict[str, Any]:
        with path.open("r", encoding="utf-8") as f:
            return json.load(f)

    def _extract_output(self, payload: Dict[str, Any]) -> str:
        preferred_keys = [
            "decision_summary",
            "summary_text",
            "macro_summary",
            "sector_summary",
            "conclusion",
            "combined_summary",
            "sentiment_summary",
            "summary",
            "signal_reason",
            "selection_reason",
            "filter_summary",
        ]
        for key in preferred_keys:
            value = payload.get(key)
            if isinstance(value, str) and value.strip():
                return self._trim_text(value.strip())

        meta = payload.get("meta")
        if isinstance(meta, dict):
            llm_reasoning = meta.get("llm_reasoning")
            if isinstance(llm_reasoning, str) and llm_reasoning.strip():
                return self._trim_text(llm_reasoning.strip())

        events = payload.get("events")
        if isinstance(events, list):
            event_summaries: List[str] = []
            for event in events:
                if not isinstance(event, dict):
                    continue
                summary = event.get("summary")
                if isinstance(summary, str) and summary.strip():
                    event_summaries.append(summary.strip())
                if len(event_summaries) >= 2:
                    break
            if event_summaries:
                return self._trim_text("；".join(event_summaries))

        return "暂无有效输出预览。"

    def _extract_signal(self, payload: Dict[str, Any]) -> Optional[str]:
        for key in ["action_signal", "market_direction", "market_bias", "action_bias", "target_position"]:
            value = payload.get(key)
            if isinstance(value, str) and value.strip():
                return self._normalize_signal(value.strip())
        return None

    def _normalize_signal(self, raw: str) -> str:
        mapping = {
            "bullish": "中性偏多",
            "bearish": "中性偏空",
            "neutral": "中性",
            "hold": "持有",
            "watch": "观察",
            "follow_leaders": "跟随主线",
        }
        return mapping.get(raw.lower(), raw)

    def _extract_updated_at(self, payload: Dict[str, Any], result_file: Path) -> str:
        meta = payload.get("meta")
        if isinstance(meta, dict):
            generated_at = meta.get("generated_at")
            if isinstance(generated_at, str) and generated_at.strip():
                try:
                    dt = datetime.fromisoformat(generated_at)
                    return dt.strftime("%Y-%m-%d %H:%M:%S")
                except ValueError:
                    pass
        return datetime.fromtimestamp(result_file.stat().st_mtime).strftime("%Y-%m-%d %H:%M:%S")

    def _trim_text(self, text: str, max_len: int = 90) -> str:
        compact = " ".join(text.split())
        if len(compact) <= max_len:
            return compact
        return f"{compact[: max_len - 3]}..."

    def _parse_percent(self, value: Any) -> float:
        if isinstance(value, str):
            normalized = value.replace("%", "").strip()
            return self._to_float(normalized)
        return self._to_float(value)

    def _to_float(self, value: Any) -> float:
        try:
            if value is None or value == "":
                return 0.0
            return float(value)
        except (TypeError, ValueError):
            return 0.0
