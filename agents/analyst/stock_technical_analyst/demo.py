"""
Stock Technical Analyst Demo。

    python -m agents.analyst.stock_technical_analyst.demo 000001
    python -m agents.analyst.stock_technical_analyst.demo 600519 20260401
"""

import re
import time
from datetime import datetime, timedelta
from typing import Any, Dict

from dataflow.utils import normalize_cn_ts_code

from .graph import create_stock_technical_analyst_graph


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

    parser = argparse.ArgumentParser(description="Stock Technical Analyst Demo")
    parser.add_argument("ts_code", help="股票代码：6 位数字（自动补 .SH/.SZ/.BJ）或完整如 000001.SZ")
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

    graph = create_stock_technical_analyst_graph(llm=llm)
    invoke_input: Dict[str, Any] = {"ts_code": ts_code, "trade_date": trade_date}

    print(f"{ts_code} 交易日 {trade_date}，运行 Stock Technical Analyst...")
    start = time.perf_counter()
    result = graph.invoke(invoke_input)
    print(f"完成，耗时 {time.perf_counter() - start:.2f} 秒\n")

    facts = result.get("stock_technical_facts")
    if facts:
        print("================ 技术面事实 ================")
        pprint(facts, width=120)

    ta = result.get("technical_analysis") or {}
    print("\n================ 技术面结论 ================")
    if ta.get("error"):
        print("错误:", ta["error"])
        return
    print("technical_score:", ta.get("technical_score"))
    print("trend_signal:", ta.get("trend_signal"))
    print("trend_strength:", ta.get("trend_strength"))
    print("short_term_outlook:", ta.get("short_term_outlook", ""))
    print("risk_reminder:", ta.get("risk_reminder", ""))
    print("summary:", ta.get("summary", ""))
    if ta.get("support_levels"):
        print("support_levels:", ta["support_levels"])
    if ta.get("resistance_levels"):
        print("resistance_levels:", ta["resistance_levels"])
    if ta.get("technical_indicators"):
        print("technical_indicators:", ta["technical_indicators"])


if __name__ == "__main__":
    main()
