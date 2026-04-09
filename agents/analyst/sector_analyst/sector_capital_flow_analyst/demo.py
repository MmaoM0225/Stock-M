"""
Sector Capital Flow Analyst（板块资金流分析师）Demo。

    python -m agents.analyst.sector_analyst.sector_capital_flow_analyst.demo
    python -m agents.analyst.sector_analyst.sector_capital_flow_analyst.demo 20260316
"""

import re
import time
from datetime import datetime, timedelta
from typing import Any, Dict, Optional

from .graph import create_sector_capital_flow_analyst_graph


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

    from langchain_openai import ChatOpenAI

    from ...config import get_llm_config, validate_config
    from ...callbacks import get_llm_callbacks

    parser = argparse.ArgumentParser(description="Sector Capital Flow Analyst Demo")
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
        callbacks=get_llm_callbacks(),
    )
    print(f"使用 LLM: {llm_config.provider} / {llm_config.model}")

    graph = create_sector_capital_flow_analyst_graph(llm=llm)
    trade_date = _resolve_trade_date(target_date)
    invoke_input: Dict[str, Any] = {"trade_date": trade_date}

    print(f"交易日 {trade_date}，运行 Sector Capital Flow Analyst...")
    start = time.perf_counter()
    result = graph.invoke(invoke_input)
    print(f"完成，耗时 {time.perf_counter() - start:.2f} 秒")

    # LLM 解读（若有）
    insight = result.get("sector_capital_flow_insight") or {}
    if insight:
        print("\n================ 板块资金流 LLM 解读 ================")
        print(insight.get("summary", ""))
        print("\n结论:", insight.get("conclusion", ""))
        if insight.get("highlights"):
            print("要点:", insight["highlights"])
        print("市场倾向:", insight.get("market_bias", "neutral"))
        if insight.get("hot_sectors"):
            print("热门/强势板块:", insight["hot_sectors"])
        if insight.get("risk_sectors"):
            print("风险/弱势板块:", insight["risk_sectors"])

    top = result.get("sector_capital_flow_top") or {}
    if not top:
        print("\n无资金流排行数据。")
        return

    ths_top = top.get("ths_concept") or {}
    sw_top = top.get("sw_industry") or {}

    # 同花顺概念/板块 - 每个区间展示前 2 个完整 JSON
    if ths_top:
        print("\n================ THS 概念/板块 资金流排行 ================")
        from pprint import pprint
        for window_key in ("1d", "5d", "10d", "20d"):
            data = ths_top.get(window_key)
            if not data:
                continue
            print(f"\n=== THS {window_key} 净流入最多的 2 个板块 ===")
            for item in data.get("top_inflow", [])[:2]:
                pprint(item)

            print(f"\n=== THS {window_key} 净流出最多的 2 个板块 ===")
            for item in data.get("top_outflow", [])[:2]:
                pprint(item)

    # 申万行业 - 每个区间展示前 2 个完整 JSON
    if sw_top:
        print("\n================ 申万行业 资金强弱排行 ================")
        from pprint import pprint
        for window_key in ("1d", "5d", "10d", "20d"):
            data = sw_top.get(window_key)
            if not data:
                continue
            print(f"\n=== SW {window_key} 资金流入最强的 2 个行业 ===")
            for item in data.get("top_inflow", [])[:2]:
                pprint(item)

            print(f"\n=== SW {window_key} 资金流出最弱的 2 个行业 ===")
            for item in data.get("top_outflow", [])[:2]:
                pprint(item)


if __name__ == "__main__":
    main()

