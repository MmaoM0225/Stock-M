from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session

from api.dependencies import get_db
from api.schemas.base import APIResponse
from database.models import Industry, StockList

router = APIRouter(tags=["stocks"])


@router.get("/stocks", response_model=APIResponse)
def list_stocks(
    industry: Optional[str] = None,
    market: Optional[str] = None,
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=20, ge=1, le=200),
    db: Session = Depends(get_db),
) -> APIResponse:
    query = db.query(StockList)
    if industry:
        query = query.filter(StockList.industry == industry)
    if market:
        query = query.filter(StockList.market == market)

    total = query.count()
    rows = query.offset((page - 1) * page_size).limit(page_size).all()
    items = [
        {
            "ts_code": r.ts_code,
            "symbol": r.symbol,
            "name": r.name,
            "area": r.area,
            "industry": r.industry,
            "market": r.market,
            "list_date": r.list_date,
        }
        for r in rows
    ]
    return APIResponse(data={"page": page, "page_size": page_size, "total": total, "items": items})


@router.get("/stocks/search", response_model=APIResponse)
def stock_search(
    keyword: str = Query(..., min_length=1),
    limit: int = Query(default=20, ge=1, le=100),
    db: Session = Depends(get_db),
) -> APIResponse:
    pattern = f"%{keyword}%"
    rows = (
        db.query(StockList)
        .filter((StockList.ts_code.like(pattern)) | (StockList.name.like(pattern)) | (StockList.symbol.like(pattern)))
        .limit(limit)
        .all()
    )
    items = [{"ts_code": r.ts_code, "name": r.name, "symbol": r.symbol, "industry": r.industry} for r in rows]
    return APIResponse(data={"items": items})


@router.get("/stocks/{ts_code}", response_model=APIResponse)
def stock_detail(ts_code: str, db: Session = Depends(get_db)) -> APIResponse:
    row = db.query(StockList).filter(StockList.ts_code == ts_code).first()
    if not row:
        raise HTTPException(status_code=404, detail=f"stock not found: {ts_code}")
    return APIResponse(
        data={
            "ts_code": row.ts_code,
            "symbol": row.symbol,
            "name": row.name,
            "area": row.area,
            "industry": row.industry,
            "market": row.market,
            "list_date": row.list_date,
        }
    )


@router.get("/industries", response_model=APIResponse)
def list_industries(
    level: Optional[str] = None,
    parent_code: Optional[int] = None,
    db: Session = Depends(get_db),
) -> APIResponse:
    query = db.query(Industry)
    if level:
        query = query.filter(Industry.level == level)
    if parent_code is not None:
        query = query.filter(Industry.parent_code == parent_code)

    rows = query.all()
    items = [
        {
            "index_code": r.index_code,
            "industry_name": r.industry_name,
            "level": r.level,
            "industry_code": r.industry_code,
            "parent_code": r.parent_code,
            "src": r.src,
        }
        for r in rows
    ]
    return APIResponse(data={"items": items})

