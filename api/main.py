from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from api.config import API_PREFIX, API_TITLE, API_VERSION
from api.routers import agents, data, portfolio, stocks
from api.schemas.base import APIResponse

app = FastAPI(title=API_TITLE, version=API_VERSION)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(agents.router, prefix=API_PREFIX)
app.include_router(data.router, prefix=API_PREFIX)
app.include_router(stocks.router, prefix=API_PREFIX)
app.include_router(portfolio.router, prefix=API_PREFIX)


@app.get("/health", response_model=APIResponse, tags=["system"])
def health_check() -> APIResponse:
    return APIResponse(data={"status": "ok"})

