from typing import Any, Dict, Optional

from pydantic import BaseModel


class DataQueryResult(BaseModel):
    artifact_path: str
    result: Dict[str, Any]


class StockCombinedResult(BaseModel):
    stock_manager: Optional[Dict[str, Any]] = None
    fundamental: Optional[Dict[str, Any]] = None
    technical: Optional[Dict[str, Any]] = None
    trade_date: str
    ts_code: str
    available: Dict[str, bool]


class GenericDataEnvelope(BaseModel):
    data: Any

