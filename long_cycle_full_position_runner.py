"""
长周期满仓实盘模拟脚本（独立于 live_trading.py）

用途：
- 以较长间隔（如20/60个交易日）运行完整链路
- 最终调用“满仓长期决策师”生成组合

示例：
    python long_cycle_full_position_runner.py --start-date 20240108 --end-date 20260424 --interval 21
    python long_cycle_full_position_runner.py --single-date 20240206
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
import time
import traceback
from pathlib import Path
from typing import Any, Dict, List, Optional

from langchain_openai import ChatOpenAI

project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

from agents.callbacks import get_llm_callbacks
from agents.config import get_llm_config, validate_config
from agents.manager.sector_manager.graph import create_sector_manager_graph
from agents.manager.stock_pool_manager.graph import create_stock_pool_manager_graph
from agents.manager.macro_manager.graph import create_macro_manager_graph
from agents.decision.full_position_decision.graph import create_full_position_decision_graph
from agents.analyst.stock_analyst.stock_screener.graph import create_stock_screener_graph
from dataflow.market_data import MarketDataFetcher
from dataflow.news_sentiment import NewsSentimentFetcher


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(project_root / "data" / "artifacts" / "long_cycle_full_position.log", encoding="utf-8"),
    ],
)
logger = logging.getLogger(__name__)


SECTOR_MANAGER_ROOT = Path("data/artifacts/manager/sector_manager")
STOCK_POOL_MANAGER_ROOT = Path("data/artifacts/manager/stock_pool_manager")
MACRO_MANAGER_ROOT = Path("data/artifacts/manager/macro_manager")
STOCK_SCREENER_ROOT = Path("data/artifacts/analyst/stock_analyst/stock_screener")
FULL_POSITION_DECISION_ROOT = Path("data/artifacts/decision/full_position_decision_63d/portfolio")
DEFAULT_INITIAL_CAPITAL = 1000000.0


def get_trading_days(start_date: str, end_date: str, interval: int = 20, exchange: str = "SSE") -> List[str]:
    fetcher = MarketDataFetcher()
    df = fetcher.fetch_trade_cal(exchange=exchange, start_date=start_date, end_date=end_date, is_open="1")
    if df.empty:
        raise RuntimeError(f"无法获取交易日历 {start_date}-{end_date}")
    all_days = sorted(df["cal_date"].tolist())
    return all_days[::interval]


def get_available_portfolio_dates() -> List[str]:
    dates: List[str] = []
    if FULL_POSITION_DECISION_ROOT.exists():
        for date_dir in FULL_POSITION_DECISION_ROOT.iterdir():
            if date_dir.is_dir() and date_dir.name.isdigit() and len(date_dir.name) == 8:
                if (date_dir / "result.json").exists():
                    dates.append(date_dir.name)
    return sorted(dates)


def load_portfolio_book(trade_date: str) -> Optional[List[Dict[str, Any]]]:
    book_path = FULL_POSITION_DECISION_ROOT / trade_date / "result.json"
    if not book_path.exists():
        return None
    try:
        with open(book_path, "r", encoding="utf-8") as f:
            data = json.load(f)
        return data.get("portfolio_table", [])
    except Exception as e:
        logger.warning("读取持仓失败 %s: %s", book_path, e)
        return None


def get_latest_portfolio_book_before(trade_date: str, available_dates: List[str]) -> Optional[List[Dict[str, Any]]]:
    prior_dates = [d for d in available_dates if d < trade_date]
    if not prior_dates:
        return None
    return load_portfolio_book(max(prior_dates))


def run_macro_manager(llm: ChatOpenAI, trade_date: str, news_fetcher: Optional[NewsSentimentFetcher]) -> None:
    if (MACRO_MANAGER_ROOT / trade_date / "result.json").exists():
        logger.info("[%s] Macro Manager 已存在，跳过", trade_date)
        return
    logger.info("[%s] 运行 Macro Manager...", trade_date)
    create_macro_manager_graph(llm, news_fetcher=news_fetcher).invoke({"trade_date": trade_date})


def run_sector_manager(llm: ChatOpenAI, trade_date: str) -> None:
    if (SECTOR_MANAGER_ROOT / trade_date / "result.json").exists():
        logger.info("[%s] Sector Manager 已存在，跳过", trade_date)
        return
    logger.info("[%s] 运行 Sector Manager...", trade_date)
    create_sector_manager_graph(llm=llm).invoke({"trade_date": trade_date})


def run_stock_screener(llm: ChatOpenAI, trade_date: str) -> None:
    if (STOCK_SCREENER_ROOT / trade_date / "result.json").exists():
        logger.info("[%s] Stock Screener 已存在，跳过", trade_date)
        return
    logger.info("[%s] 运行 Stock Screener...", trade_date)
    create_stock_screener_graph(llm=llm).invoke(
        {"trade_date": trade_date, "min_market_cap": 80e8, "exclude_st": True, "max_stocks": 18}
    )


def run_stock_pool_manager(llm: ChatOpenAI, trade_date: str) -> None:
    if (STOCK_POOL_MANAGER_ROOT / trade_date / "result.json").exists():
        logger.info("[%s] Stock Pool Manager 已存在，跳过", trade_date)
        return
    logger.info("[%s] 运行 Stock Pool Manager...", trade_date)
    create_stock_pool_manager_graph(llm=llm).invoke(
        {"trade_date": trade_date, "stock_pool_manager_root": str(STOCK_POOL_MANAGER_ROOT)}
    )


def run_full_position_decision(
    llm: ChatOpenAI, trade_date: str, initial_capital: float, portfolio_holdings: Optional[List[Dict[str, Any]]]
) -> Dict[str, Any]:
    logger.info("[%s] 运行 Full Position Decision...", trade_date)
    graph = create_full_position_decision_graph(llm=llm)
    invoke_input = {
        "trade_date": trade_date,
        "initial_capital": initial_capital,
        "full_position_decision_root": str(FULL_POSITION_DECISION_ROOT),
        "stock_pool_manager_root": str(STOCK_POOL_MANAGER_ROOT),
    }
    if portfolio_holdings:
        invoke_input["portfolio_holdings"] = portfolio_holdings
    return graph.invoke(invoke_input)


def main() -> None:
    parser = argparse.ArgumentParser(description="长周期满仓实盘模拟脚本")
    parser.add_argument("--start-date", default="20250101")
    parser.add_argument("--end-date", default="20251231")
    parser.add_argument("--interval", type=int, default=20, help="运行间隔（交易日）")
    parser.add_argument("--exchange", default="SSE")
    parser.add_argument("--initial-capital", type=float, default=DEFAULT_INITIAL_CAPITAL)
    parser.add_argument("--single-date", default=None)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--resume", action="store_true")
    parser.add_argument("--skip-managers", action="store_true")
    args = parser.parse_args()

    trading_days = [str(d) for d in get_trading_days(args.start_date, args.end_date, args.interval, args.exchange)]
    if args.single_date:
        trading_days = [args.single_date]
    if not trading_days:
        raise SystemExit("无可运行交易日")

    if args.dry_run:
        print("计划运行交易日：")
        for i, d in enumerate(trading_days, 1):
            print(f"{i:3d}. {d}")
        return

    llm_config = get_llm_config()
    if not validate_config(llm_config):
        raise SystemExit("LLM 配置无效")
    llm = ChatOpenAI(
        base_url=llm_config.base_url,
        api_key=llm_config.api_key,
        model=llm_config.model,
        temperature=llm_config.temperature,
        max_retries=llm_config.max_retries,
        timeout=llm_config.timeout,
        callbacks=get_llm_callbacks(),
    )

    try:
        news_fetcher = NewsSentimentFetcher()
    except Exception as e:
        logger.warning("新闻获取器初始化失败: %s", e)
        news_fetcher = None

    available_dates = get_available_portfolio_dates()
    if args.resume:
        trading_days = [d for d in trading_days if d not in available_dates]

    for trade_date in trading_days:
        logger.info("\n%s\n处理交易日 %s\n%s", "=" * 56, trade_date, "=" * 56)
        holdings = get_latest_portfolio_book_before(trade_date, available_dates)
        try:
            day_start = time.perf_counter()
            if not args.skip_managers:
                run_macro_manager(llm, trade_date, news_fetcher)
                run_sector_manager(llm, trade_date)
                run_stock_screener(llm, trade_date)
                run_stock_pool_manager(llm, trade_date)
            result = run_full_position_decision(llm, trade_date, args.initial_capital, holdings)
            available_dates = sorted(set([*available_dates, trade_date]))
            elapsed = time.perf_counter() - day_start
            summary = (result.get("decision_result") or {}).get("decision_summary", "")
            logger.info("[%s] 完成，耗时 %.2fs", trade_date, elapsed)
            logger.info("[%s] 摘要: %s", trade_date, summary[:220])
        except Exception as e:
            logger.error("[%s] 运行失败: %s", trade_date, e)
            logger.error(traceback.format_exc())


if __name__ == "__main__":
    main()

