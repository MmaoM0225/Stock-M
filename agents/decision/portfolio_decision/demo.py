"""
Portfolio Decision Demo：基于指定交易日读取上游 manager 结果，生成组合决策。

    python -m agents.decision.portfolio_decision.demo
    python -m agents.decision.portfolio_decision.demo 20260414
"""
from __future__ import annotations

import argparse
import json
import logging
import re
import time
from datetime import datetime, timedelta
from pprint import pprint

from langchain_openai import ChatOpenAI

from agents.callbacks import get_llm_callbacks
from agents.config import get_llm_config, validate_config

from .graph import create_portfolio_decision_graph


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
    parser = argparse.ArgumentParser(description="Portfolio Decision：LLM优先组合决策")
    parser.add_argument("date", nargs="?", help="交易日期 YYYYMMDD（默认最近工作日）")
    parser.add_argument("--sector-result", default=None, help="覆盖 sector_manager result.json 路径")
    parser.add_argument("--stock-pool-result", default=None, help="覆盖 stock_pool_manager result.json 路径")
    parser.add_argument("--macro-result", default=None, help="覆盖 macro_manager result.json 路径")
    parser.add_argument("--initial-capital", type=float, default=500000.0, help="初始本金（默认 500000）")
    parser.add_argument(
        "--portfolio-holdings",
        default=None,
        help="当前资产组合 JSON 路径（数组；支持中文列名，如 排名/资产名称/仓位 等）",
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

    graph = create_portfolio_decision_graph(llm=llm)
    invoke_input = {"trade_date": trade_date, "initial_capital": args.initial_capital}
    if args.sector_result:
        invoke_input["sector_manager_result_path"] = args.sector_result
    if args.stock_pool_result:
        invoke_input["stock_pool_manager_result_path"] = args.stock_pool_result
    if args.macro_result:
        invoke_input["macro_manager_result_path"] = args.macro_result
    if args.portfolio_holdings:
        with open(args.portfolio_holdings, "r", encoding="utf-8") as f:
            holdings = json.load(f)
        if not isinstance(holdings, list):
            raise SystemExit("--portfolio-holdings 文件内容必须是 JSON 数组")
        invoke_input["portfolio_holdings"] = holdings

    print(f"交易日 {trade_date}，运行 Portfolio Decision（LLM优先）...")
    start = time.perf_counter()
    result = graph.invoke(invoke_input)
    elapsed = time.perf_counter() - start
    print(f"完成，耗时 {elapsed:.2f} 秒\n")

    payload = result.get("decision_result") or {}
    print("================ 决策摘要 ================")
    print(payload.get("decision_summary"))
    print("\n================ 资产组合表 (portfolio_table) ================")
    pprint(payload.get("portfolio_table"), width=140)
    print("\n================ 操作原因表 (operation_reason_table) ================")
    pprint(payload.get("operation_reason_table"), width=140)
    art = result.get("decision_artifact_path")
    if art:
        print(f"\n已写入: {art}")


if __name__ == "__main__":
    main()
