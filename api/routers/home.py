from fastapi import APIRouter, Depends, Query
from fastapi import HTTPException

from api.dependencies import get_home_service
from api.schemas.base import APIResponse
from api.services.home_service import HomeService

router = APIRouter(prefix="/home", tags=["home"])


@router.get("/agent-outputs", response_model=APIResponse)
def get_agent_outputs(
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=4, ge=1, le=200),
    service: HomeService = Depends(get_home_service),
) -> APIResponse:
    return APIResponse(data=service.get_agent_outputs_preview(page=page, page_size=page_size))


@router.get("/portfolio/summary", response_model=APIResponse)
def get_portfolio_summary(service: HomeService = Depends(get_home_service)) -> APIResponse:
    try:
        return APIResponse(data=service.get_portfolio_summary())
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))
