from typing import Any, Dict, List

from pydantic import BaseModel


class PortfolioDateList(BaseModel):
    dates: List[str]


class PortfolioHistoryResult(BaseModel):
    page: int
    page_size: int
    total: int
    items: List[Dict[str, Any]]


class PortfolioPerformanceResult(BaseModel):
    start_date: str
    end_date: str
    period_count: int
    total_return_pct: float
    max_drawdown_pct: float

