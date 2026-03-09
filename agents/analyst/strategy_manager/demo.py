"""
策略经理 Demo - 多子图编排，使用真实数据

流程：并行运行 macro_analyst + news_analyst → strategy_manager 综合

    python -m agents.analyst.strategy_manager.demo
    python -m agents.analyst.strategy_manager.demo 20260305
"""

import argparse
import re
from datetime import datetime, timedelta
from typing import Optional

import time

from langchain_openai import ChatOpenAI

from ...config import get_llm_config, validate_config
from .daily_pipeline import create_daily_pipeline


def _parse_trade_date(s: str) -> datetime:
    """解析交易日期，仅支持 YYYYMMDD。"""
    s = s.strip()
    if re.fullmatch(r"\d{8}", s):
        return datetime.strptime(s, "%Y%m%d")
    raise ValueError(f"无效日期格式: {s}，请使用 YYYYMMDD")


def _resolve_trade_date(target_date: Optional[datetime], fetcher) -> str:
    """
    解析要分析的交易日期。
    - 若指定 target_date：返回该日期（YYYYMMDD）。
    - 若未指定：从今天起往前查找最近有新闻数据的工作日。
    """
    today = datetime.now()
    if target_date is not None:
        if target_date.weekday() >= 5:
            print(f"警告: {target_date.date()} 为周末，仍将尝试获取该日数据。")
        return target_date.strftime("%Y%m%d")

    # 未指定日期：从今天往前查找有新闻数据的工作日
    for i in range(14):
        check_date = today - timedelta(days=i)
        if check_date.weekday() >= 5:
            continue
        trade_date = check_date.strftime("%Y%m%d")
        news_data = fetcher.get_news_by_date(trade_date)
        if news_data:
            print(f"使用日期 {trade_date}（有新闻数据）")
            return trade_date

    print("未找到近期新闻数据，使用最近工作日。")
    for i in range(7):
        check = today - timedelta(days=i)
        if check.weekday() < 5:
            return check.strftime("%Y%m%d")
    return today.strftime("%Y%m%d")


def main():
    parser = argparse.ArgumentParser(
        description="策略经理 Demo：macro + news 并行 → strategy，使用真实数据"
    )
    parser.add_argument(
        "date",
        nargs="?",
        help="策略基准日 YYYYMMDD，不指定则从今天起查找最近有新闻数据的工作日",
    )
    parser.add_argument("--date", "-d", dest="date_alt", help="同上")
    parser.add_argument(
        "--no-md",
        action="store_true",
        help="不生成 Markdown 报告",
    )
    args = parser.parse_args()

    target_date: Optional[datetime] = None
    raw = args.date or args.date_alt
    if raw:
        target_date = _parse_trade_date(raw)

    # 1. Fetcher（新闻分析必需）
    try:
        from dataflow.news_sentiment import NewsSentimentFetcher

        fetcher = NewsSentimentFetcher()
    except ImportError as e:
        print(f"无法导入 NewsSentimentFetcher: {e}")
        print("请确保 dataflow 包可用")
        return

    trade_date = _resolve_trade_date(target_date, fetcher)

    # 2. LLM（macro、news、strategy 三子图共用）
    llm_config = get_llm_config()
    if not validate_config(llm_config):
        print("LLM 配置无效，请设置环境变量后重试")
        return
    llm = ChatOpenAI(
        base_url=llm_config.base_url,
        api_key=llm_config.api_key,
        model=llm_config.model,
        temperature=llm_config.temperature,
        max_retries=llm_config.max_retries,
        timeout=llm_config.timeout,
    )

    # 3. 构建多子图流水线并运行
    pipeline = create_daily_pipeline(
        llm=llm,
        fetcher=fetcher,
        macro_config={"generate_markdown": not args.no_md},
        news_config={"generate_markdown": not args.no_md},
        strategy_config={"generate_markdown": not args.no_md},
    )

    print(f"交易日 {trade_date}，开始运行多子图流水线（macro ∥ news → strategy ∥ md_write）...")
    start_time = time.perf_counter()
    result = pipeline.invoke({"trade_date": trade_date})
    elapsed = time.perf_counter() - start_time
    print(f"流水线运行完成，耗时 {elapsed:.2f} 秒。")

    from pprint import pprint

    print("\n=== 宏观分析 (macro_analysis) ===")
    pprint(result.get("macro_analysis"))
    print("\n=== 新闻分析 (news_analysis) ===")
    pprint(result.get("news_analysis"))
    print("\n=== 策略分析 (strategy_analysis) ===")
    pprint(result.get("strategy_analysis"))


if __name__ == "__main__":
    main()
