from typing import Optional

from fastapi import APIRouter, Depends, HTTPException

from api.dependencies import get_data_service
from api.schemas.base import APIResponse
from api.services.data_service import DataService

router = APIRouter(prefix="/portfolio", tags=["portfolio"])


@router.get("/dates", response_model=APIResponse)
def portfolio_dates(service: DataService = Depends(get_data_service)) -> APIResponse:
    try:
        return APIResponse(data=service.get_portfolio_dates_api())
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))


@router.get("/{version}/history", response_model=APIResponse)
def portfolio_history_by_version(
    version: str,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    service: DataService = Depends(get_data_service),
) -> APIResponse:
    try:
        return APIResponse(
            data=service.get_portfolio_history_series_api(
                start_date=start_date,
                end_date=end_date,
                version=version,
            )
        )
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.get("/{version}/{trade_date}", response_model=APIResponse)
def portfolio_snapshot_by_version(version: str, trade_date: str, service: DataService = Depends(get_data_service)) -> APIResponse:
    try:
        return APIResponse(data=service.get_portfolio_snapshot_api(trade_date, version=version))
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.get("/latest", response_model=APIResponse)
def portfolio_latest(service: DataService = Depends(get_data_service)) -> APIResponse:
    try:
        return APIResponse(data=service.get_latest_portfolio())
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))


@router.get("/history", response_model=APIResponse)
def portfolio_history(
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    service: DataService = Depends(get_data_service),
) -> APIResponse:
    try:
        return APIResponse(data=service.get_portfolio_history_series_api(start_date=start_date, end_date=end_date, version=None))
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.get("/compare", response_model=APIResponse)
def portfolio_compare(dates: str, service: DataService = Depends(get_data_service)) -> APIResponse:
    date_list = [d.strip() for d in dates.split(",") if d.strip()]
    if len(date_list) < 2:
        raise HTTPException(status_code=400, detail="at least two dates required")
    try:
        return APIResponse(data=service.compare_portfolio(date_list))
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))


@router.get("/performance", response_model=APIResponse)
def portfolio_performance(
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    service: DataService = Depends(get_data_service),
) -> APIResponse:
    try:
        return APIResponse(data=service.portfolio_performance(start_date, end_date))
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.get("/{trade_date}", response_model=APIResponse)
def portfolio_snapshot(trade_date: str, service: DataService = Depends(get_data_service)) -> APIResponse:
    try:
        return APIResponse(data=service.get_portfolio_snapshot_api(trade_date))
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

