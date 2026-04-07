"""
Stock Screener Analyst（股票筛选分析师）

从指定板块或全市场中筛选符合条件的股票池（行情与市值来自 daily_basic，名称/行业来自 stock_basic）。

Usage:
    from agents.analyst.stock_screener import create_stock_screener_graph

    graph = create_stock_screener_graph()
    result = graph.invoke({
        "trade_date": "20260330",
        "sectors": ["半导体", "人工智能"],
        "min_market_cap": 50e8,
        "exclude_st": True,
        "max_stocks": 100
    })
"""
from .graph import create_stock_screener_graph

__all__ = ["create_stock_screener_graph"]
