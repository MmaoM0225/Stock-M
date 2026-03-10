"""
Macro Economist（宏观经济分析师）Demo。

    python -m agents.analyst.macro_economist.demo
    python -m agents.analyst.macro_economist.demo 20260305
"""

import re
import time
from datetime import datetime, timedelta
from typing import Any, Dict, Optional

from langchain_openai import ChatOpenAI

from .graph import create_macro_economist_graph
from ...config import get_llm_config, validate_config


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
    import argparse
    from pprint import pprint

    parser = argparse.ArgumentParser(description="Macro Economist Demo")
    parser.add_argument("date", nargs="?", help="交易日期 YYYYMMDD")
    args = parser.parse_args()

    target_date = None
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
    )

    graph = create_macro_economist_graph(llm=llm)
    trade_date = _resolve_trade_date(target_date)
    invoke_input: Dict[str, Any] = {"trade_date": trade_date}

    print(f"交易日 {trade_date}，运行 Macro Economist...")
    start = time.perf_counter()
    result = graph.invoke(invoke_input)
    print(f"完成，耗时 {time.perf_counter() - start:.2f} 秒")

    print("\n=== macro_economist_analysis ===")
    pprint(result.get("macro_economist_analysis"))


if __name__ == "__main__":
    main()
