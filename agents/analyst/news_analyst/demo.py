"""
新闻分析子图 Demo。

可以直接运行本文件，查看 map-reduce 子图是否能够正确运行：

    python -m agents.analyst.news_analyst.demo
    python -m agents.analyst.news_analyst.demo 20260305
"""

from typing import Any, Dict, List, Optional

import argparse
import re
from datetime import datetime, timedelta
import time

from langchain_openai import ChatOpenAI

from .graph import create_news_graph
from ...config import get_llm_config, validate_config
from dataflow.news_sentiment import NewsSentimentFetcher


def _parse_trade_date(s: str) -> datetime:
    """解析交易日期，仅支持 YYYYMMDD。"""
    s = s.strip()
    if re.fullmatch(r"\d{8}", s):
        return datetime.strptime(s, "%Y%m%d")
    raise ValueError(f"无效日期格式: {s}，请使用 YYYYMMDD")


def _resolve_trade_date(
    fetcher: NewsSentimentFetcher,
    target_date: Optional[datetime],
) -> str:
    """
    解析要分析的交易日期。
    - 若指定 target_date：返回该日期（YYYYMMDD）。
    - 若未指定：从今天起往前查找最近有数据的工作日。
    """
    today = datetime.now()
    if target_date is not None:
        if target_date.weekday() >= 5:
            print(f"警告: {target_date.date()} 为周末，仍将尝试获取该日新闻。")
        return target_date.strftime("%Y%m%d")

    # 未指定日期：从今天往前查找有数据的工作日
    for i in range(7):
        check_date = today - timedelta(days=i)
        if check_date.weekday() >= 5:
            continue
        trade_date = check_date.strftime("%Y%m%d")
        print(f"尝试查找日期 {trade_date} 是否有新闻数据...")
        news_data = fetcher.get_news_by_date(trade_date)
        if news_data:
            print(f"找到日期 {trade_date} 的新闻数据。")
            return trade_date

    print("未找到近期新闻数据，使用今日日期。")
    return today.strftime("%Y%m%d")


def main():
    parser = argparse.ArgumentParser(description="新闻分析子图 Demo，支持分析指定交易日的新闻")
    parser.add_argument(
        "date",
        nargs="?",
        help="要分析的交易日期，格式：YYYYMMDD。不指定则从今天起往前查找最近有数据的工作日",
    )
    parser.add_argument(
        "--date", "-d",
        dest="date_alt",
        help="同上，可用 --date 或 -d 显式指定",
    )
    args = parser.parse_args()
    target_date: Optional[datetime] = None
    raw = args.date or args.date_alt
    if raw:
        target_date = _parse_trade_date(raw)
        print(f"指定分析日期: {target_date.strftime('%Y%m%d')}")

    # 1. 构建 LLM（需配置环境变量）
    llm_config = get_llm_config()
    if not validate_config(llm_config):
        print("LLM 配置无效，请设置环境变量后重试")
        return
    print(f"使用 LLM: {llm_config.provider} / {llm_config.model}")
    llm = ChatOpenAI(
        base_url=llm_config.base_url,
        api_key=llm_config.api_key,
        model=llm_config.model,
        temperature=llm_config.temperature,
        max_retries=llm_config.max_retries,
        timeout=llm_config.timeout,
    )

    # 2. 构建 fetcher，fetch 节点负责获取新闻和完整行业列表（dataflow.industry_data）
    fetcher = NewsSentimentFetcher()
    graph = create_news_graph(llm, fetcher)

    # 3. 构建 invoke 输入（只传 trade_date，由 news_fetch 节点通过 fetcher 拉取新闻）
    trade_date = _resolve_trade_date(fetcher, target_date)
    invoke_input: Dict[str, Any] = {"trade_date": trade_date}
    print(f"交易日 {trade_date}，由 news_fetch 节点获取新闻（本地优先，无则抓取）。")

    print("开始运行新闻分析子图...")
    start_time = time.perf_counter()

    result = graph.invoke(invoke_input)

    elapsed = time.perf_counter() - start_time
    print(f"新闻分析子图运行完成，耗时 {elapsed:.2f} 秒。")

    from pprint import pprint

    print("=== 新闻分析子图运行结果 ===")
    pprint(result.get("news_analysis"))


if __name__ == "__main__":
    main()
