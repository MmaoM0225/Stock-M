from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field


class BaseRunRequest(BaseModel):
    trade_date: str = Field(..., pattern=r"^\d{8}$")
    force: bool = False


class ScreenerRunRequest(BaseRunRequest):
    min_market_cap: float = 80e8
    max_stocks: int = Field(default=12, ge=1, le=200)
    exclude_st: bool = True


class PortfolioDecisionRequest(BaseRunRequest):
    initial_capital: float = Field(default=500000.0, gt=0)
    portfolio_holdings: Optional[List[Dict[str, Any]]] = None


class FullPipelineRequest(BaseModel):
    trade_date: str = Field(..., pattern=r"^\d{8}$")
    initial_capital: float = Field(default=500000.0, gt=0)
    skip_existing: bool = True


class StockRunRequest(BaseRunRequest):
    ts_code: str


class AgentRunResult(BaseModel):
    trade_date: str
    artifact_path: Optional[str] = None
    reused_existing: bool = False
    result: Dict[str, Any]

