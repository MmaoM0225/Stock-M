from pathlib import Path
from typing import Any, Dict, Optional

from dataflow.news_sentiment import NewsSentimentFetcher

from agents.analyst.macro_analyst.commodity_analyst.graph import create_commodity_analyst_graph
from agents.analyst.macro_analyst.liquidity_analyst.graph import create_liquidity_analyst_graph
from agents.analyst.macro_analyst.macro_economist.graph import create_macro_economist_graph
from agents.analyst.macro_analyst.market_sentiment_analyst.graph import create_market_sentiment_analyst_graph
from agents.analyst.macro_analyst.news_analyst.graph import create_news_graph
from agents.analyst.sector_analyst.sector_capital_flow_analyst.graph import create_sector_capital_flow_analyst_graph
from agents.analyst.sector_analyst.sector_trend_analyst.graph import create_sector_trend_analyst_graph
from agents.analyst.stock_analyst.stock_fundamental_analyst.graph import create_stock_fundamental_analyst_graph
from agents.analyst.stock_analyst.stock_screener.graph import create_stock_screener_graph
from agents.analyst.stock_analyst.stock_technical_analyst.graph import create_stock_technical_analyst_graph
from agents.decision.portfolio_decision.graph import create_portfolio_decision_graph
from agents.manager.macro_manager.graph import create_macro_manager_graph
from agents.manager.sector_manager.graph import create_sector_manager_graph
from agents.manager.stock_manager.graph import create_stock_manager_graph
from agents.manager.stock_pool_manager.graph import create_stock_pool_manager_graph


class AgentService:
    def __init__(self, llm: Any, project_root: Optional[Path] = None):
        self.llm = llm
        self.project_root = project_root or Path(__file__).resolve().parents[2]
        self.artifacts_root = self.project_root / "data" / "artifacts"
        try:
            self.news_fetcher = NewsSentimentFetcher()
        except Exception:
            self.news_fetcher = None

    def _artifact_relpath(self, path: Path) -> str:
        return str(path.relative_to(self.project_root))

    def _run_with_cache(self, graph: Any, invoke_input: Dict[str, Any], artifact_path: Path, force: bool) -> Dict[str, Any]:
        if artifact_path.exists() and not force:
            return {
                "trade_date": invoke_input.get("trade_date"),
                "artifact_path": self._artifact_relpath(artifact_path),
                "reused_existing": True,
                "result": {},
            }
        result = graph.invoke(invoke_input)
        return {
            "trade_date": invoke_input.get("trade_date"),
            "artifact_path": self._artifact_relpath(artifact_path),
            "reused_existing": False,
            "result": result,
        }

    def run_macro_manager(self, trade_date: str, force: bool = False) -> Dict[str, Any]:
        graph = create_macro_manager_graph(self.llm, news_fetcher=self.news_fetcher)
        artifact = self.artifacts_root / "manager" / "macro_manager" / trade_date / "result.json"
        return self._run_with_cache(graph, {"trade_date": trade_date}, artifact, force)

    def run_sector_manager(self, trade_date: str, force: bool = False) -> Dict[str, Any]:
        graph = create_sector_manager_graph(self.llm)
        artifact = self.artifacts_root / "manager" / "sector_manager" / trade_date / "result.json"
        return self._run_with_cache(graph, {"trade_date": trade_date}, artifact, force)

    def run_stock_screener(
        self,
        trade_date: str,
        min_market_cap: float = 80e8,
        max_stocks: int = 12,
        exclude_st: bool = True,
        force: bool = False,
    ) -> Dict[str, Any]:
        graph = create_stock_screener_graph(self.llm)
        artifact = self.artifacts_root / "analyst" / "stock_analyst" / "stock_screener" / trade_date / "result.json"
        criteria = {
            "trade_date": trade_date,
            "min_market_cap": min_market_cap,
            "max_stocks": max_stocks,
            "exclude_st": exclude_st,
        }
        return self._run_with_cache(graph, criteria, artifact, force)

    def run_stock_pool_manager(self, trade_date: str, force: bool = False) -> Dict[str, Any]:
        graph = create_stock_pool_manager_graph(self.llm)
        artifact = self.artifacts_root / "manager" / "stock_pool_manager" / trade_date / "result.json"
        return self._run_with_cache(graph, {"trade_date": trade_date}, artifact, force)

    def run_portfolio_decision(
        self,
        trade_date: str,
        initial_capital: float,
        portfolio_holdings: Optional[list] = None,
        force: bool = False,
    ) -> Dict[str, Any]:
        graph = create_portfolio_decision_graph(self.llm)
        version = "202401-202604_7d_for_once_ver1.3"
        artifact = self.artifacts_root / "decision" / version / "portfolio" / trade_date / "result.json"
        payload: Dict[str, Any] = {"trade_date": trade_date, "initial_capital": initial_capital}
        if portfolio_holdings:
            payload["portfolio_holdings"] = portfolio_holdings
        payload["portfolio_decision_root"] = str(self.artifacts_root / "decision" / version / "portfolio")
        return self._run_with_cache(graph, payload, artifact, force)

    def run_stock_manager(self, ts_code: str, trade_date: str, force: bool = False) -> Dict[str, Any]:
        graph = create_stock_manager_graph(self.llm)
        artifact = self.artifacts_root / "manager" / "stock_manager" / ts_code / trade_date / "result.json"
        return self._run_with_cache(graph, {"ts_code": ts_code, "trade_date": trade_date}, artifact, force)

    def run_stock_fundamental(self, ts_code: str, trade_date: str, force: bool = False) -> Dict[str, Any]:
        graph = create_stock_fundamental_analyst_graph(self.llm)
        artifact = (
            self.artifacts_root / "analyst" / "stock_analyst" / "stock_fundamental_analyst" / ts_code / trade_date / "result.json"
        )
        return self._run_with_cache(graph, {"ts_code": ts_code, "trade_date": trade_date}, artifact, force)

    def run_stock_technical(self, ts_code: str, trade_date: str, force: bool = False) -> Dict[str, Any]:
        graph = create_stock_technical_analyst_graph(self.llm)
        artifact = (
            self.artifacts_root / "analyst" / "stock_analyst" / "stock_technical_analyst" / ts_code / trade_date / "result.json"
        )
        return self._run_with_cache(graph, {"ts_code": ts_code, "trade_date": trade_date}, artifact, force)

    def run_sector_trend(self, trade_date: str, force: bool = False) -> Dict[str, Any]:
        graph = create_sector_trend_analyst_graph(self.llm)
        artifact = self.artifacts_root / "analyst" / "sector_analyst" / "sector_trend_analyst" / trade_date / "result.json"
        return self._run_with_cache(graph, {"trade_date": trade_date}, artifact, force)

    def run_sector_capital_flow(self, trade_date: str, force: bool = False) -> Dict[str, Any]:
        graph = create_sector_capital_flow_analyst_graph(self.llm)
        artifact = (
            self.artifacts_root / "analyst" / "sector_analyst" / "sector_capital_flow_analyst" / trade_date / "result.json"
        )
        return self._run_with_cache(graph, {"trade_date": trade_date}, artifact, force)

    def run_macro_economist(self, trade_date: str, force: bool = False) -> Dict[str, Any]:
        graph = create_macro_economist_graph(self.llm)
        artifact = self.artifacts_root / "analyst" / "macro_analyst" / "macro_economist" / trade_date / "result.json"
        return self._run_with_cache(graph, {"trade_date": trade_date}, artifact, force)

    def run_macro_news(self, trade_date: str, force: bool = False) -> Dict[str, Any]:
        finlight_fetcher = getattr(self.news_fetcher, "finlight_fetcher", None) if self.news_fetcher else None
        graph = create_news_graph(self.llm, finlight_fetcher=finlight_fetcher)
        artifact = self.artifacts_root / "analyst" / "macro_analyst" / "news_analyst" / trade_date / "result.json"
        return self._run_with_cache(graph, {"trade_date": trade_date}, artifact, force)

    def run_market_sentiment(self, trade_date: str, force: bool = False) -> Dict[str, Any]:
        graph = create_market_sentiment_analyst_graph(self.llm)
        artifact = (
            self.artifacts_root / "analyst" / "macro_analyst" / "market_sentiment_analyst" / trade_date / "result.json"
        )
        return self._run_with_cache(graph, {"trade_date": trade_date}, artifact, force)

    def run_liquidity(self, trade_date: str, force: bool = False) -> Dict[str, Any]:
        graph = create_liquidity_analyst_graph(self.llm)
        artifact = self.artifacts_root / "analyst" / "macro_analyst" / "liquidity_analyst" / trade_date / "result.json"
        return self._run_with_cache(graph, {"trade_date": trade_date}, artifact, force)

    def run_commodity(self, trade_date: str, force: bool = False) -> Dict[str, Any]:
        graph = create_commodity_analyst_graph(self.llm)
        artifact = self.artifacts_root / "analyst" / "macro_analyst" / "commodity_analyst" / trade_date / "result.json"
        return self._run_with_cache(graph, {"trade_date": trade_date}, artifact, force)

    def run_full_pipeline(self, trade_date: str, initial_capital: float, skip_existing: bool = True) -> Dict[str, Any]:
        macro = self.run_macro_manager(trade_date, force=not skip_existing)
        sector = self.run_sector_manager(trade_date, force=not skip_existing)
        screener = self.run_stock_screener(trade_date, force=not skip_existing)
        stock_pool = self.run_stock_pool_manager(trade_date, force=not skip_existing)
        portfolio = self.run_portfolio_decision(
            trade_date=trade_date,
            initial_capital=initial_capital,
            portfolio_holdings=None,
            force=not skip_existing,
        )
        return {
            "trade_date": trade_date,
            "steps": ["macro_manager", "sector_manager", "stock_screener", "stock_pool_manager", "portfolio_decision"],
            "results": {
                "macro_manager": macro,
                "sector_manager": sector,
                "stock_screener": screener,
                "stock_pool_manager": stock_pool,
                "portfolio_decision": portfolio,
            },
            "artifact_paths": [
                macro["artifact_path"],
                sector["artifact_path"],
                screener["artifact_path"],
                stock_pool["artifact_path"],
                portfolio["artifact_path"],
            ],
        }

