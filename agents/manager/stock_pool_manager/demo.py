"""
Stock Pool Manager Demo：读取指定交易日的 stock_screener 结果，批量基本面+技术面分析。

    python -m agents.manager.stock_pool_manager.demo
    python -m agents.manager.stock_pool_manager.demo 20260409
    python -m agents.manager.stock_pool_manager.demo 20260409 --max-stocks 20
"""

from __future__ import annotations

import argparse
import logging
import re
import time
from datetime import datetime, timedelta
from pprint import pprint
from typing import Any, Dict, Optional

from langchain_openai import ChatOpenAI

from agents.callbacks import get_llm_callbacks
from agents.config import get_llm_config, validate_config

from .graph import create_stock_pool_manager_graph


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
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    parser = argparse.ArgumentParser(description="Stock Pool Manager：筛选池批量分析")
    parser.add_argument("date", nargs="?", help="交易日期 YYYYMMDD（默认最近工作日）")
    parser.add_argument("--max-stocks", type=int, default=None, help="最多分析只数（调试用）")
    parser.add_argument(
        "--screener-result",
        default=None,
        help="覆盖 screener result.json 路径",
    )
    args = parser.parse_args()

    trade_date = _parse_trade_date(args.date) if args.date else _default_trade_date()

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

    graph = create_stock_pool_manager_graph(llm=llm)
    invoke_input: Dict[str, Any] = {"trade_date": trade_date}
    if args.max_stocks is not None:
        invoke_input["max_stocks"] = args.max_stocks
    if args.screener_result:
        invoke_input["screener_result_path"] = args.screener_result

    print(f"交易日 {trade_date}，运行 Stock Pool Manager（读取 stock_screener 结果）...")
    start = time.perf_counter()
    result = graph.invoke(invoke_input)
    elapsed = time.perf_counter() - start
    print(f"完成，耗时 {elapsed:.2f} 秒\n")

    payload = result.get("stock_pool_manager_result") or {}
    if payload.get("pool_load_error"):
        print("加载筛选池失败:", payload.get("pool_load_error"))
        return

    print("================ 池级摘要 ================")
    print(payload.get("summary_text"))
    print("\n================ 前十 (top_stocks) ================")
    pprint(payload.get("top_stocks"), width=120)
    art = result.get("stock_pool_manager_artifact_path")
    if art:
        print(f"\n已写入: {art}")


if __name__ == "__main__":
    main()
