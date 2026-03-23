"""
Sector Trend Analyst（行业趋势分析师）Demo。

    python -m agents.analyst.sector_trend_analyst.demo
    python -m agents.analyst.sector_trend_analyst.demo 20260316
"""
import re
import time
from datetime import datetime, timedelta
from typing import Any, Dict, Optional

from .graph import create_sector_trend_analyst_graph


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
    import argparse
    from pprint import pprint

    from langchain_openai import ChatOpenAI

    from ...callbacks import get_llm_callbacks
    from ...config import get_llm_config, validate_config

    parser = argparse.ArgumentParser(description="Sector Trend Analyst Demo")
    parser.add_argument("date", nargs="?", help="交易日期 YYYYMMDD")
    args = parser.parse_args()

    target_date = _parse_trade_date(args.date) if args.date else None
    trade_date = _resolve_trade_date(target_date)
    invoke_input: Dict[str, Any] = {"trade_date": trade_date}

    llm = None
    llm_config = get_llm_config()
    if validate_config(llm_config):
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
    else:
        print("LLM 配置无效，回退为仅榜单模式。")

    print(f"交易日 {trade_date}，运行 Sector Trend Analyst...")
    graph = create_sector_trend_analyst_graph(llm=llm)
    start = time.perf_counter()
    result = graph.invoke(invoke_input)
    print(f"完成，耗时 {time.perf_counter() - start:.2f} 秒")

    insight = result.get("sector_trend_insight") or {}
    if insight:
        print("\n================ 行业趋势 LLM 解读 ================")
        print(insight.get("summary", ""))
        print("\n结论:", insight.get("conclusion", ""))
        if insight.get("highlights"):
            print("要点:", insight["highlights"])
        print("市场结构:", insight.get("market_regime", "mixed"))
        if insight.get("leading_themes"):
            print("主线方向:", insight["leading_themes"])
        if insight.get("reversal_opportunities"):
            print("修复候选:", insight["reversal_opportunities"])
        if insight.get("top_risk_sectors"):
            print("高位风险:", insight["top_risk_sectors"])

    rank = result.get("sector_trend_rank") or {}
    if not rank:
        print("无趋势结果。")
        return

    for source_key, source_name in [("ths_concept", "THS 概念/板块"), ("sw_industry", "申万行业")]:
        src = rank.get(source_key) or {}
        print(f"\n================ {source_name} ================")
        print("\n--- trend_strength_board (前3) ---")
        for item in (src.get("trend_strength_board") or [])[:3]:
            pprint(item)
        print("\n--- reversal_recovery_board (前3) ---")
        for item in (src.get("reversal_recovery_board") or [])[:3]:
            pprint(item)
        print("\n--- top_reversal_warning_board (前3) ---")
        for item in (src.get("top_reversal_warning_board") or [])[:3]:
            pprint(item)


if __name__ == "__main__":
    main()
