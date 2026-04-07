"""
Stock Manager Demo。

    python -m agents.manager.stock_manager.demo 600519
    python -m agents.manager.stock_manager.demo 000001 20260407
"""

import re
import time
from datetime import datetime, timedelta
from typing import Any, Dict

from dataflow.utils import normalize_cn_ts_code

from .graph import create_stock_manager_graph


def _parse_trade_date(s: str) -> str:
    u = s.strip().replace("-", "")
    if re.fullmatch(r"\d{8}", u):
        return u
    raise ValueError(f"无效日期: {s}，请使用 YYYYMMDD")


def _default_trade_date() -> str:
    today = datetime.now()
    for i in range(10):
        d = today - timedelta(days=i)
        if d.weekday() < 5:
            return d.strftime("%Y%m%d")
    return today.strftime("%Y%m%d")


def main() -> None:
    import argparse
    from pprint import pprint

    from langchain_openai import ChatOpenAI

    from ...callbacks import get_llm_callbacks
    from ...config import get_llm_config, validate_config

    parser = argparse.ArgumentParser(description="Stock Manager Demo")
    parser.add_argument("ts_code", help="股票代码：6位数字或完整如 600519.SH")
    parser.add_argument("trade_date", nargs="?", help="交易日期 YYYYMMDD（可选）")
    args = parser.parse_args()

    try:
        ts_code = normalize_cn_ts_code(args.ts_code)
    except ValueError as e:
        raise SystemExit(str(e))
    trade_date = _parse_trade_date(args.trade_date) if args.trade_date else _default_trade_date()

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
    print(f"使用 LLM: {llm_config.provider} / {llm_config.model}")

    graph = create_stock_manager_graph(llm=llm)
    invoke_input: Dict[str, Any] = {"ts_code": ts_code, "trade_date": trade_date}

    print(f"{ts_code} 交易日 {trade_date}，运行 Stock Manager...")
    start = time.perf_counter()
    result = graph.invoke(invoke_input)
    print(f"完成，耗时 {time.perf_counter() - start:.2f} 秒\n")

    sm = result.get("stock_manager_summary") or {}
    print("================ 个股经理汇总 ================")
    pprint(sm, width=120)


if __name__ == "__main__":
    main()
