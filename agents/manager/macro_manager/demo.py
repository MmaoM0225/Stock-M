"""
Macro Manager（宏观管理器）Demo。

编排 5 个微观分析师子图（新闻、市场情绪、流动性、大宗商品、宏观经济），
控制并发并汇总结果。

    python -m agents.manager.macro_manager.demo
    python -m agents.manager.macro_manager.demo 20260310
"""

import argparse
import logging
import re
import time
from datetime import datetime, timedelta
from typing import Any, Dict, Optional

from langchain_openai import ChatOpenAI

from agents.config import get_llm_config, validate_config
from agents.callbacks import get_llm_callbacks
from .graph import create_macro_manager_graph


def _parse_trade_date(s: str) -> datetime:
    s = s.strip()
    if re.fullmatch(r"\d{8}", s):
        return datetime.strptime(s, "%Y%m%d")
    raise ValueError(f"无效日期格式: {s}，请使用 YYYYMMDD")


def _resolve_trade_date(target_date: Optional[datetime]) -> str:
    today = datetime.now()
    if target_date is not None:
        return target_date.strftime("%Y%m%d")
    for i in range(7):
        check = today - timedelta(days=i)
        if check.weekday() < 5:
            return check.strftime("%Y%m%d")
    return today.strftime("%Y%m%d")


def main():
    from pprint import pprint

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    parser = argparse.ArgumentParser(description="Macro Manager Demo：编排 5 个分析师子图")
    parser.add_argument("date", nargs="?", help="交易日期 YYYYMMDD")
    args = parser.parse_args()

    target_date: Optional[datetime] = None
    if args.date:
        target_date = _parse_trade_date(args.date)

    llm_config = get_llm_config()
    if not validate_config(llm_config):
        raise SystemExit("LLM 配置无效，请检查环境变量")
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
        from dataflow.news_sentiment import NewsSentimentFetcher
        news_fetcher = NewsSentimentFetcher()
    except Exception:
        news_fetcher = None

    graph = create_macro_manager_graph(llm, news_fetcher=news_fetcher)
    trade_date = _resolve_trade_date(target_date)
    invoke_input: Dict[str, Any] = {"trade_date": trade_date}

    print(f"交易日 {trade_date}，运行 Macro Manager（5 个分析师子图）...")
    start = time.perf_counter()
    result = graph.invoke(invoke_input)
    elapsed = time.perf_counter() - start
    print(f"完成，耗时 {elapsed:.2f} 秒")

    keys = [
        "news_analysis",
        "market_sentiment_analyst_summary",
        "liquidity_analyst_summary",
        "commodity_analyst_summary",
        "macro_economist_analysis",
    ]
    for key in keys:
        print(f"\n=== {key} ===")
        pprint(result.get(key))

    print("\n=== macro_manager_summary ===")
    pprint(result.get("macro_manager_summary"))


if __name__ == "__main__":
    main()
