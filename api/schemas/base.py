from typing import Any, Optional

from pydantic import BaseModel, Field


class APIResponse(BaseModel):
    success: bool = True
    message: str = "ok"
    data: Any = None
    error_code: Optional[str] = None
    trace_id: Optional[str] = None


class PaginationParams(BaseModel):
    page: int = Field(default=1, ge=1)
    page_size: int = Field(default=20, ge=1, le=200)

