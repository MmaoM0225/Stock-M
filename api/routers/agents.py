from fastapi import APIRouter, Depends, Path, Query

from api.dependencies import get_agent_service
from api.schemas.agent import BaseRunRequest, FullPipelineRequest, PortfolioDecisionRequest, ScreenerRunRequest
from api.schemas.base import APIResponse
from api.services.agent_service import AgentService

router = APIRouter(prefix="/agents", tags=["agents"])


@router.post("/macro/run", response_model=APIResponse)
def run_macro(req: BaseRunRequest, service: AgentService = Depends(get_agent_service)) -> APIResponse:
    return APIResponse(data=service.run_macro_manager(req.trade_date, req.force))


@router.post("/sector/run", response_model=APIResponse)
def run_sector(req: BaseRunRequest, service: AgentService = Depends(get_agent_service)) -> APIResponse:
    return APIResponse(data=service.run_sector_manager(req.trade_date, req.force))


@router.post("/screener/run", response_model=APIResponse)
def run_screener(req: ScreenerRunRequest, service: AgentService = Depends(get_agent_service)) -> APIResponse:
    return APIResponse(
        data=service.run_stock_screener(
            req.trade_date,
            min_market_cap=req.min_market_cap,
            max_stocks=req.max_stocks,
            exclude_st=req.exclude_st,
            force=req.force,
        )
    )


@router.post("/stock-pool/run", response_model=APIResponse)
def run_stock_pool(req: BaseRunRequest, service: AgentService = Depends(get_agent_service)) -> APIResponse:
    return APIResponse(data=service.run_stock_pool_manager(req.trade_date, req.force))


@router.post("/portfolio/decision", response_model=APIResponse)
def run_portfolio_decision(
    req: PortfolioDecisionRequest, service: AgentService = Depends(get_agent_service)
) -> APIResponse:
    return APIResponse(
        data=service.run_portfolio_decision(
            trade_date=req.trade_date,
            initial_capital=req.initial_capital,
            portfolio_holdings=req.portfolio_holdings,
            force=req.force,
        )
    )


@router.post("/stock/{ts_code}/analyze", response_model=APIResponse)
def run_stock(
    req: BaseRunRequest,
    ts_code: str = Path(..., description="股票代码，如 600519.SH"),
    service: AgentService = Depends(get_agent_service),
) -> APIResponse:
    return APIResponse(data=service.run_stock_manager(ts_code=ts_code, trade_date=req.trade_date, force=req.force))


@router.post("/full-pipeline", response_model=APIResponse)
def run_full_pipeline(req: FullPipelineRequest, service: AgentService = Depends(get_agent_service)) -> APIResponse:
    return APIResponse(data=service.run_full_pipeline(req.trade_date, req.initial_capital, req.skip_existing))


@router.post("/analyst/stock/fundamental/run", response_model=APIResponse)
def run_stock_fundamental(
    req: BaseRunRequest,
    ts_code: str = Query(..., description="股票代码，如 600519.SH"),
    service: AgentService = Depends(get_agent_service),
) -> APIResponse:
    return APIResponse(data=service.run_stock_fundamental(ts_code=ts_code, trade_date=req.trade_date, force=req.force))


@router.post("/analyst/stock/technical/run", response_model=APIResponse)
def run_stock_technical(
    req: BaseRunRequest,
    ts_code: str = Query(..., description="股票代码，如 600519.SH"),
    service: AgentService = Depends(get_agent_service),
) -> APIResponse:
    return APIResponse(data=service.run_stock_technical(ts_code=ts_code, trade_date=req.trade_date, force=req.force))


@router.post("/analyst/sector/trend/run", response_model=APIResponse)
def run_sector_trend(req: BaseRunRequest, service: AgentService = Depends(get_agent_service)) -> APIResponse:
    return APIResponse(data=service.run_sector_trend(req.trade_date, req.force))


@router.post("/analyst/sector/capital-flow/run", response_model=APIResponse)
def run_sector_capital_flow(req: BaseRunRequest, service: AgentService = Depends(get_agent_service)) -> APIResponse:
    return APIResponse(data=service.run_sector_capital_flow(req.trade_date, req.force))


@router.post("/analyst/macro/economist/run", response_model=APIResponse)
def run_macro_economist(req: BaseRunRequest, service: AgentService = Depends(get_agent_service)) -> APIResponse:
    return APIResponse(data=service.run_macro_economist(req.trade_date, req.force))


@router.post("/analyst/macro/news/run", response_model=APIResponse)
def run_macro_news(req: BaseRunRequest, service: AgentService = Depends(get_agent_service)) -> APIResponse:
    return APIResponse(data=service.run_macro_news(req.trade_date, req.force))


@router.post("/analyst/macro/market-sentiment/run", response_model=APIResponse)
def run_market_sentiment(req: BaseRunRequest, service: AgentService = Depends(get_agent_service)) -> APIResponse:
    return APIResponse(data=service.run_market_sentiment(req.trade_date, req.force))


@router.post("/analyst/macro/commodity/run", response_model=APIResponse)
def run_commodity(req: BaseRunRequest, service: AgentService = Depends(get_agent_service)) -> APIResponse:
    return APIResponse(data=service.run_commodity(req.trade_date, req.force))

