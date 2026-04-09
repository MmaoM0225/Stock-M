"""
Sector Manager（行业管理器）Demo。

编排 2 个行业分析师子图（行业趋势、板块资金流），控制并发并汇总结果。

    python -m agents.manager.sector_manager.demo
    python -m agents.manager.sector_manager.demo 20260316
"""

import argparse
import logging
import re
import time
from datetime import datetime, timedelta
from typing import Any, Dict, Optional

from langchain_openai import ChatOpenAI

from agents.callbacks import get_llm_callbacks
from agents.config import get_llm_config, validate_config
from .graph import create_sector_manager_graph


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


def main() -> None:
    from pprint import pprint

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    parser = argparse.ArgumentParser(description="Sector Manager Demo：编排 2 个行业分析师子图")
    parser.add_argument("date", nargs="?", help="交易日期 YYYYMMDD")
    args = parser.parse_args()

    target_date = _parse_trade_date(args.date) if args.date else None

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

    graph = create_sector_manager_graph(llm=llm)
    trade_date = _resolve_trade_date(target_date)
    invoke_input: Dict[str, Any] = {"trade_date": trade_date}

    print(f"交易日 {trade_date}，运行 Sector Manager（2 个行业分析师子图）...")
    start = time.perf_counter()
    result = graph.invoke(invoke_input)
    elapsed = time.perf_counter() - start
    print(f"完成，耗时 {elapsed:.2f} 秒")

    print("\n=== sector_manager_summary ===")
    pprint(result.get("sector_manager_summary"))

    print("\n=== sector_trend_insight ===")
    pprint(result.get("sector_trend_insight"))

    print("\n=== sector_capital_flow_insight ===")
    pprint(result.get("sector_capital_flow_insight"))


if __name__ == "__main__":
    main()
