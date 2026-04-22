from fastapi import APIRouter, Depends, HTTPException

from api.dependencies import get_data_service
from api.schemas.base import APIResponse
from api.services.data_service import DataService

router = APIRouter(prefix="/data", tags=["data"])


def _handle_not_found(err: FileNotFoundError) -> None:
    raise HTTPException(status_code=404, detail=str(err))


@router.get("/macro/{trade_date}", response_model=APIResponse)
def get_macro(trade_date: str, service: DataService = Depends(get_data_service)) -> APIResponse:
    try:
        return APIResponse(data=service.get_macro_result(trade_date))
    except FileNotFoundError as e:
        _handle_not_found(e)


@router.get("/sector/{trade_date}", response_model=APIResponse)
def get_sector(trade_date: str, service: DataService = Depends(get_data_service)) -> APIResponse:
    try:
        return APIResponse(data=service.get_sector_result(trade_date))
    except FileNotFoundError as e:
        _handle_not_found(e)


@router.get("/screener/{trade_date}", response_model=APIResponse)
def get_screener(trade_date: str, service: DataService = Depends(get_data_service)) -> APIResponse:
    try:
        return APIResponse(data=service.get_screener_result(trade_date))
    except FileNotFoundError as e:
        _handle_not_found(e)


@router.get("/stock-pool/{trade_date}", response_model=APIResponse)
def get_stock_pool(trade_date: str, service: DataService = Depends(get_data_service)) -> APIResponse:
    try:
        return APIResponse(data=service.get_stock_pool_result(trade_date))
    except FileNotFoundError as e:
        _handle_not_found(e)


@router.get("/portfolio/{trade_date}", response_model=APIResponse)
def get_portfolio(trade_date: str, service: DataService = Depends(get_data_service)) -> APIResponse:
    try:
        return APIResponse(data=service.get_portfolio_result(trade_date))
    except FileNotFoundError as e:
        _handle_not_found(e)


@router.get("/stock/{ts_code}/{trade_date}", response_model=APIResponse)
def get_stock_combined(ts_code: str, trade_date: str, service: DataService = Depends(get_data_service)) -> APIResponse:
    return APIResponse(data=service.get_stock_combined_result(ts_code, trade_date))


@router.get("/fundamental/{ts_code}/{trade_date}", response_model=APIResponse)
def get_fundamental(ts_code: str, trade_date: str, service: DataService = Depends(get_data_service)) -> APIResponse:
    try:
        return APIResponse(data=service.get_fundamental_result(ts_code, trade_date))
    except FileNotFoundError as e:
        _handle_not_found(e)


@router.get("/technical/{ts_code}/{trade_date}", response_model=APIResponse)
def get_technical(ts_code: str, trade_date: str, service: DataService = Depends(get_data_service)) -> APIResponse:
    try:
        return APIResponse(data=service.get_technical_result(ts_code, trade_date))
    except FileNotFoundError as e:
        _handle_not_found(e)


@router.get("/analyst/sector/trend/{trade_date}", response_model=APIResponse)
def get_sector_trend(trade_date: str, service: DataService = Depends(get_data_service)) -> APIResponse:
    try:
        return APIResponse(data=service.get_analyst_result("sector_analyst", "sector_trend_analyst", trade_date))
    except FileNotFoundError as e:
        _handle_not_found(e)


@router.get("/analyst/sector/capital-flow/{trade_date}", response_model=APIResponse)
def get_sector_capital_flow(trade_date: str, service: DataService = Depends(get_data_service)) -> APIResponse:
    try:
        return APIResponse(data=service.get_analyst_result("sector_analyst", "sector_capital_flow_analyst", trade_date))
    except FileNotFoundError as e:
        _handle_not_found(e)


@router.get("/analyst/macro/economist/{trade_date}", response_model=APIResponse)
def get_macro_economist(trade_date: str, service: DataService = Depends(get_data_service)) -> APIResponse:
    try:
        return APIResponse(data=service.get_analyst_result("macro_analyst", "macro_economist", trade_date))
    except FileNotFoundError as e:
        _handle_not_found(e)


@router.get("/analyst/macro/news/{trade_date}", response_model=APIResponse)
def get_macro_news(trade_date: str, service: DataService = Depends(get_data_service)) -> APIResponse:
    try:
        return APIResponse(data=service.get_analyst_result("macro_analyst", "news_analyst", trade_date))
    except FileNotFoundError as e:
        _handle_not_found(e)


@router.get("/analyst/macro/market-sentiment/{trade_date}", response_model=APIResponse)
def get_market_sentiment(trade_date: str, service: DataService = Depends(get_data_service)) -> APIResponse:
    try:
        return APIResponse(data=service.get_analyst_result("macro_analyst", "market_sentiment_analyst", trade_date))
    except FileNotFoundError as e:
        _handle_not_found(e)


@router.get("/analyst/macro/commodity/{trade_date}", response_model=APIResponse)
def get_commodity(trade_date: str, service: DataService = Depends(get_data_service)) -> APIResponse:
    try:
        return APIResponse(data=service.get_analyst_result("macro_analyst", "commodity_analyst", trade_date))
    except FileNotFoundError as e:
        _handle_not_found(e)

