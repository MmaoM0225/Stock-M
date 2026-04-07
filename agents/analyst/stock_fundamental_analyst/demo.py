"""
Stock Fundamental Analyst Demo。

    python -m agents.analyst.stock_fundamental_analyst.demo 000001
    python -m agents.analyst.stock_fundamental_analyst.demo 600519 20260401
"""

import re
import time
from datetime import datetime, timedelta
from typing import Any, Dict

from dataflow.utils import normalize_cn_ts_code

from .graph import create_stock_fundamental_analyst_graph


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

    parser = argparse.ArgumentParser(description="Stock Fundamental Analyst Demo")
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

    graph = create_stock_fundamental_analyst_graph(llm=llm)
    invoke_input: Dict[str, Any] = {"ts_code": ts_code, "trade_date": trade_date}

    print(f"{ts_code} 交易日 {trade_date}，运行 Stock Fundamental Analyst...")
    start = time.perf_counter()
    result = graph.invoke(invoke_input)
    print(f"完成，耗时 {time.perf_counter() - start:.2f} 秒\n")

    facts = result.get("stock_fundamental_facts")
    if facts:
        print("================ 基础事实（供后续节点） ================")
        pprint(facts, width=120)

    base = result.get("fundamental_base_profile") or {}
    print("\n================ 基础数据摘要 ================")
    if base.get("error"):
        print("错误:", base["error"])
        return
    print("summary:", base.get("summary", ""))
    if base.get("company_profile"):
        print("company_profile:", base["company_profile"])
    if base.get("valuation_snapshot"):
        print("valuation_snapshot:", base["valuation_snapshot"])

    insight = result.get("company_basic_analysis") or {}
    print("\n================ LLM 公司基本信息解读 ================")
    if insight.get("error"):
        print("错误:", insight["error"])
        return
    print("company_profile_text:", insight.get("company_profile_text", ""))
    if insight.get("key_facts"):
        print("key_facts:", insight["key_facts"])

    valuation = result.get("valuation_map_analysis") or {}
    print("\n================ 估值Map分析 ================")
    if valuation.get("error"):
        print("错误:", valuation["error"])
        return
    print("valuation_level:", valuation.get("valuation_level", ""))
    if valuation.get("metric_interpretation"):
        print("metric_interpretation:", valuation.get("metric_interpretation"))
    if valuation.get("contradiction_checks"):
        print("contradiction_checks:", valuation.get("contradiction_checks"))
    print("market_sentiment_judgement:", valuation.get("market_sentiment_judgement", ""))
    if valuation.get("investor_views"):
        print("investor_views:", valuation.get("investor_views"))
    print("relative_valuation:", valuation.get("relative_valuation", ""))
    print("summary:", valuation.get("summary", ""))
    if valuation.get("key_points"):
        print("key_points:", valuation["key_points"])
    if valuation.get("risks"):
        print("risks:", valuation["risks"])
    if valuation.get("data_gaps"):
        print("data_gaps:", valuation["data_gaps"])

    income = result.get("income_map_analysis") or {}
    print("\n================ 利润表Map分析 ================")
    if income.get("error"):
        print("错误:", income["error"])
        return
    print("profitability_quality:", income.get("profitability_quality", ""))
    print("growth_signal:", income.get("growth_signal", ""))
    print("cost_control_signal:", income.get("cost_control_signal", ""))
    print("margin_comment:", income.get("margin_comment", ""))
    print("summary:", income.get("summary", ""))
    if income.get("key_points"):
        print("key_points:", income["key_points"])
    if income.get("risks"):
        print("risks:", income["risks"])

    cashflow = result.get("cashflow_map_analysis") or {}
    print("\n================ 现金流Map分析 ================")
    if cashflow.get("error"):
        print("错误:", cashflow["error"])
        return
    print("cashflow_quality:", cashflow.get("cashflow_quality", ""))
    print("fcf_signal:", cashflow.get("fcf_signal", ""))
    print("financing_dependency_signal:", cashflow.get("financing_dependency_signal", ""))
    print("cashflow_comment:", cashflow.get("cashflow_comment", ""))
    print("summary:", cashflow.get("summary", ""))
    if cashflow.get("key_points"):
        print("key_points:", cashflow["key_points"])
    if cashflow.get("risks"):
        print("risks:", cashflow["risks"])

    bs = result.get("balancesheet_map_analysis") or {}
    print("\n================ 资产负债表Map分析 ================")
    if bs.get("error"):
        print("错误:", bs["error"])
        return
    print("solvency_quality:", bs.get("solvency_quality", ""))
    print("leverage_signal:", bs.get("leverage_signal", ""))
    print("asset_quality_signal:", bs.get("asset_quality_signal", ""))
    print("liabilities_structure_comment:", bs.get("liabilities_structure_comment", ""))
    print("summary:", bs.get("summary", ""))
    if bs.get("key_points"):
        print("key_points:", bs["key_points"])
    if bs.get("risks"):
        print("risks:", bs["risks"])

    div = result.get("dividend_map_analysis") or {}
    print("\n================ 分红送股Map分析 ================")
    if div.get("error"):
        print("错误:", div["error"])
        return
    print("dividend_quality:", div.get("dividend_quality", ""))
    print("payout_style:", div.get("payout_style", ""))
    print("stability_signal:", div.get("stability_signal", ""))
    print("dividend_comment:", div.get("dividend_comment", ""))
    print("summary:", div.get("summary", ""))
    if div.get("key_points"):
        print("key_points:", div["key_points"])
    if div.get("risks"):
        print("risks:", div["risks"])

    reduced = result.get("fundamental_reduce_result") or {}
    print("\n================ 基本面Reduce汇总 ================")
    if reduced.get("error"):
        print("错误:", reduced["error"])
        return
    print("overall_score:", reduced.get("overall_score", ""))
    print("rating_label:", reduced.get("rating_label", ""))
    print("confidence:", reduced.get("confidence", ""))
    print("valuation_view:", reduced.get("valuation_view", ""))
    print("quality_view:", reduced.get("quality_view", ""))
    print("shareholder_return_view:", reduced.get("shareholder_return_view", ""))
    print("summary:", reduced.get("summary", ""))
    if reduced.get("key_conclusions"):
        print("key_conclusions:", reduced["key_conclusions"])
    if reduced.get("major_risks"):
        print("major_risks:", reduced["major_risks"])
    if reduced.get("next_data_needed"):
        print("next_data_needed:", reduced["next_data_needed"])


if __name__ == "__main__":
    main()

