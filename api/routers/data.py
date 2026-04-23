from fastapi import APIRouter, Depends, HTTPException

from api.dependencies import get_data_service
from api.schemas.base import APIResponse
from api.services.data_service import DataService

router = APIRouter(prefix="/data", tags=["data"])


def _handle_not_found(err: FileNotFoundError) -> None:
    raise HTTPException(status_code=404, detail=str(err))


@router.get("/macro/dates", response_model=APIResponse)
def get_macro_dates(service: DataService = Depends(get_data_service)) -> APIResponse:
    return APIResponse(data={"dates": service.list_manager_dates("macro_manager")})


@router.get("/macro/{trade_date}", response_model=APIResponse)
def get_macro(trade_date: str, service: DataService = Depends(get_data_service)) -> APIResponse:
    try:
        return APIResponse(data=service.get_macro_manager_result(trade_date))
    except FileNotFoundError as e:
        _handle_not_found(e)


@router.get("/sector/dates", response_model=APIResponse)
def get_sector_dates(service: DataService = Depends(get_data_service)) -> APIResponse:
    return APIResponse(data={"dates": service.list_manager_dates("sector_manager")})


@router.get("/sector/{trade_date}", response_model=APIResponse)
def get_sector(trade_date: str, service: DataService = Depends(get_data_service)) -> APIResponse:
    if not (len(trade_date) == 8 and trade_date.isdigit()):
        raise HTTPException(
            status_code=400,
            detail="invalid trade_date, expected YYYYMMDD. For sector dates use /api/v1/data/sector/dates",
        )
    try:
        return APIResponse(data=service.get_sector_manager_result(trade_date))
    except FileNotFoundError as e:
        _handle_not_found(e)


@router.get("/screener/dates", response_model=APIResponse)
def get_screener_dates(service: DataService = Depends(get_data_service)) -> APIResponse:
    return APIResponse(data={"dates": service.list_analyst_dates("stock_analyst", "stock_screener")})


@router.get("/screener/{trade_date}", response_model=APIResponse)
def get_screener(trade_date: str, service: DataService = Depends(get_data_service)) -> APIResponse:
    if not (len(trade_date) == 8 and trade_date.isdigit()):
        raise HTTPException(
            status_code=400,
            detail="invalid trade_date, expected YYYYMMDD. For screener dates use /api/v1/data/screener/dates",
        )
    try:
        return APIResponse(data=service.get_stock_screener_api_result(trade_date))
    except FileNotFoundError as e:
        _handle_not_found(e)


@router.get("/stock-pool/dates", response_model=APIResponse)
def get_stock_pool_dates(service: DataService = Depends(get_data_service)) -> APIResponse:
    return APIResponse(data={"dates": service.list_manager_dates("stock_pool_manager")})


@router.get("/stock-pool/{trade_date}", response_model=APIResponse)
def get_stock_pool(trade_date: str, service: DataService = Depends(get_data_service)) -> APIResponse:
    if not (len(trade_date) == 8 and trade_date.isdigit()):
        raise HTTPException(
            status_code=400,
            detail="invalid trade_date, expected YYYYMMDD. For dates use /api/v1/data/stock-pool/dates",
        )
    try:
        return APIResponse(data=service.get_stock_pool_api_result(trade_date))
    except FileNotFoundError as e:
        _handle_not_found(e)


@router.get("/portfolio/versions", response_model=APIResponse)
def get_portfolio_versions(service: DataService = Depends(get_data_service)) -> APIResponse:
    return APIResponse(data={"versions": service.list_portfolio_versions()})


@router.get("/portfolio/{version}/dates", response_model=APIResponse)
def get_portfolio_dates(version: str, service: DataService = Depends(get_data_service)) -> APIResponse:
    try:
        return APIResponse(data=service.get_portfolio_dates_api(version=version))
    except FileNotFoundError as e:
        _handle_not_found(e)


@router.get("/portfolio/{version}/{trade_date}", response_model=APIResponse)
def get_portfolio_by_version(trade_date: str, version: str, service: DataService = Depends(get_data_service)) -> APIResponse:
    try:
        return APIResponse(data=service.get_portfolio_api_result(version, trade_date))
    except FileNotFoundError as e:
        _handle_not_found(e)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.get("/portfolio/{trade_date}", response_model=APIResponse)
def get_portfolio(trade_date: str, service: DataService = Depends(get_data_service)) -> APIResponse:
    try:
        return APIResponse(data=service.get_portfolio_result(trade_date))
    except FileNotFoundError as e:
        _handle_not_found(e)


@router.get("/stock/ts_codes", response_model=APIResponse)
def get_stock_manager_ts_codes(service: DataService = Depends(get_data_service)) -> APIResponse:
    return APIResponse(data={"ts_codes": service.list_stock_manager_ts_codes()})


@router.get("/stock/{ts_code}/dates", response_model=APIResponse)
def get_stock_manager_dates(ts_code: str, service: DataService = Depends(get_data_service)) -> APIResponse:
    return APIResponse(data={"ts_code": ts_code, "dates": service.list_stock_manager_dates(ts_code)})


@router.get("/stock/{ts_code}/{trade_date}", response_model=APIResponse)
def get_stock_manager(ts_code: str, trade_date: str, service: DataService = Depends(get_data_service)) -> APIResponse:
    if not (len(trade_date) == 8 and trade_date.isdigit()):
        raise HTTPException(
            status_code=400,
            detail="invalid trade_date, expected YYYYMMDD. For ts_codes use /api/v1/data/stock/ts_codes and dates use /api/v1/data/stock/{ts_code}/dates",
        )
    try:
        return APIResponse(data=service.get_stock_manager_api_result(ts_code, trade_date))
    except FileNotFoundError as e:
        _handle_not_found(e)


@router.get("/stock-combined/{ts_code}/{trade_date}", response_model=APIResponse)
def get_stock_combined(ts_code: str, trade_date: str, service: DataService = Depends(get_data_service)) -> APIResponse:
    return APIResponse(data=service.get_stock_combined_result(ts_code, trade_date))


@router.get("/fundamental/ts_codes", response_model=APIResponse)
def get_fundamental_ts_codes(service: DataService = Depends(get_data_service)) -> APIResponse:
    return APIResponse(data={"ts_codes": service.list_fundamental_ts_codes()})


@router.get("/fundamental/{ts_code}/dates", response_model=APIResponse)
def get_fundamental_dates(ts_code: str, service: DataService = Depends(get_data_service)) -> APIResponse:
    return APIResponse(data={"ts_code": ts_code, "dates": service.list_fundamental_dates(ts_code)})


@router.get("/fundamental/{ts_code}/{trade_date}", response_model=APIResponse)
def get_fundamental(ts_code: str, trade_date: str, service: DataService = Depends(get_data_service)) -> APIResponse:
    if not (len(trade_date) == 8 and trade_date.isdigit()):
        raise HTTPException(
            status_code=400,
            detail="invalid trade_date, expected YYYYMMDD. For ts_codes use /api/v1/data/fundamental/ts_codes and dates use /api/v1/data/fundamental/{ts_code}/dates",
        )
    try:
        return APIResponse(data=service.get_fundamental_api_result(ts_code, trade_date))
    except FileNotFoundError as e:
        _handle_not_found(e)


@router.get("/technical/ts_codes", response_model=APIResponse)
def get_technical_ts_codes(service: DataService = Depends(get_data_service)) -> APIResponse:
    return APIResponse(data={"ts_codes": service.list_technical_ts_codes()})


@router.get("/technical/{ts_code}/dates", response_model=APIResponse)
def get_technical_dates(ts_code: str, service: DataService = Depends(get_data_service)) -> APIResponse:
    return APIResponse(data={"ts_code": ts_code, "dates": service.list_technical_dates(ts_code)})


@router.get("/technical/{ts_code}/{trade_date}", response_model=APIResponse)
def get_technical(ts_code: str, trade_date: str, service: DataService = Depends(get_data_service)) -> APIResponse:
    if not (len(trade_date) == 8 and trade_date.isdigit()):
        raise HTTPException(
            status_code=400,
            detail="invalid trade_date, expected YYYYMMDD. For ts_codes use /api/v1/data/technical/ts_codes and dates use /api/v1/data/technical/{ts_code}/dates",
        )
    try:
        return APIResponse(data=service.get_technical_api_result(ts_code, trade_date))
    except FileNotFoundError as e:
        _handle_not_found(e)


@router.get("/analyst/sector/trend/dates", response_model=APIResponse)
def get_sector_trend_dates(service: DataService = Depends(get_data_service)) -> APIResponse:
    return APIResponse(data={"dates": service.list_analyst_dates("sector_analyst", "sector_trend_analyst")})


@router.get("/analyst/sector/trend/{trade_date}", response_model=APIResponse)
def get_sector_trend(trade_date: str, service: DataService = Depends(get_data_service)) -> APIResponse:
    if not (len(trade_date) == 8 and trade_date.isdigit()):
        raise HTTPException(
            status_code=400,
            detail="invalid trade_date, expected YYYYMMDD. For sector series use /api/v1/data/analyst/sector/trend/series/{code}",
        )
    try:
        return APIResponse(data=service.get_sector_trend_result(trade_date))
    except FileNotFoundError as e:
        _handle_not_found(e)


@router.get("/analyst/sector/trend/series/{code}", response_model=APIResponse)
def get_sector_trend_series(code: str, service: DataService = Depends(get_data_service)) -> APIResponse:
    return APIResponse(data=service.get_sector_trend_series(code))


@router.get("/analyst/sector/capital-flow/dates", response_model=APIResponse)
def get_sector_capital_flow_dates(service: DataService = Depends(get_data_service)) -> APIResponse:
    return APIResponse(data={"dates": service.list_analyst_dates("sector_analyst", "sector_capital_flow_analyst")})


@router.get("/analyst/sector/capital-flow/{trade_date}", response_model=APIResponse)
def get_sector_capital_flow(trade_date: str, service: DataService = Depends(get_data_service)) -> APIResponse:
    try:
        return APIResponse(data=service.get_sector_capital_flow_result(trade_date))
    except FileNotFoundError as e:
        _handle_not_found(e)


@router.get("/analyst/macro/economist/dates", response_model=APIResponse)
def get_macro_economist_dates(service: DataService = Depends(get_data_service)) -> APIResponse:
    return APIResponse(data={"dates": service.list_analyst_dates("macro_analyst", "macro_economist")})


@router.get("/analyst/macro/economist/{trade_date}", response_model=APIResponse)
def get_macro_economist(trade_date: str, service: DataService = Depends(get_data_service)) -> APIResponse:
    try:
        return APIResponse(data=service.get_macro_economist_result(trade_date))
    except FileNotFoundError as e:
        _handle_not_found(e)


@router.get("/analyst/macro/market-sentiment/dates", response_model=APIResponse)
def get_market_sentiment_dates(service: DataService = Depends(get_data_service)) -> APIResponse:
    return APIResponse(data={"dates": service.list_analyst_dates("macro_analyst", "market_sentiment_analyst")})


@router.get("/analyst/macro/commodity/dates", response_model=APIResponse)
def get_commodity_dates(service: DataService = Depends(get_data_service)) -> APIResponse:
    return APIResponse(data={"dates": service.list_analyst_dates("macro_analyst", "commodity_analyst")})


@router.get("/analyst/macro/news/dates", response_model=APIResponse)
def get_macro_news_dates(service: DataService = Depends(get_data_service)) -> APIResponse:
    return APIResponse(data={"dates": service.list_analyst_dates("macro_analyst", "news_analyst")})


@router.get("/analyst/macro/news/{trade_date}", response_model=APIResponse)
def get_macro_news(trade_date: str, service: DataService = Depends(get_data_service)) -> APIResponse:
    try:
        return APIResponse(data=service.get_news_result(trade_date))
    except FileNotFoundError as e:
        _handle_not_found(e)


@router.get("/analyst/macro/market-sentiment/{trade_date}", response_model=APIResponse)
def get_market_sentiment(trade_date: str, service: DataService = Depends(get_data_service)) -> APIResponse:
    try:
        return APIResponse(data=service.get_market_sentiment_result(trade_date))
    except FileNotFoundError as e:
        _handle_not_found(e)


@router.get("/analyst/macro/commodity/{trade_date}", response_model=APIResponse)
def get_commodity(trade_date: str, service: DataService = Depends(get_data_service)) -> APIResponse:
    try:
        return APIResponse(data=service.get_commodity_result(trade_date))
    except FileNotFoundError as e:
        _handle_not_found(e)

