"""
宏观经济分析子图 Demo。

可以直接运行本文件，查看宏观分析图是否能够正确运行：

    python -m agents.analyst.macro_analyst.demo
    python -m agents.analyst.macro_analyst.demo 20260305
    python -m agents.analyst.macro_analyst.demo --us-stock  # 启用美股趋势分析
"""

from typing import Any, Dict, Optional

import argparse
import re
from datetime import datetime, timedelta
import time

from langchain_openai import ChatOpenAI

from .graph import create_macro_graph
from ...config import get_llm_config, validate_config, MACRO_USE_US_STOCK_TREND


def _parse_trade_date(s: str) -> datetime:
    """解析交易日期，仅支持 YYYYMMDD。"""
    s = s.strip()
    if re.fullmatch(r"\d{8}", s):
        return datetime.strptime(s, "%Y%m%d")
    raise ValueError(f"无效日期格式: {s}，请使用 YYYYMMDD")


def _resolve_trade_date(target_date: Optional[datetime]) -> str:
    """
    解析要分析的交易日期。
    - 若指定 target_date：返回该日期（YYYYMMDD）。
    - 若未指定：使用最近一个工作日。
    """
    today = datetime.now()
    if target_date is not None:
        if target_date.weekday() >= 5:
            print(f"警告: {target_date.date()} 为周末，仍将尝试获取该日数据。")
        return target_date.strftime("%Y%m%d")

    # 未指定：使用最近工作日
    for i in range(7):
        check = today - timedelta(days=i)
        if check.weekday() < 5:
            return check.strftime("%Y%m%d")
    return today.strftime("%Y%m%d")


def main():
    parser = argparse.ArgumentParser(
        description="宏观经济分析子图 Demo，支持分析指定交易日的宏观数据"
    )
    parser.add_argument(
        "date",
        nargs="?",
        help="要分析的交易日期，格式：YYYYMMDD。不指定则使用最近工作日",
    )
    parser.add_argument(
        "--date", "-d",
        dest="date_alt",
        help="同上，可用 --date 或 -d 显式指定",
    )
    parser.add_argument(
        "--us-stock",
        action="store_true",
        help="是否纳入美股趋势分析（默认从 config 读取，此处可覆盖为 True）",
    )
    parser.add_argument(
        "--no-llm",
        action="store_true",
        help="不使用 LLM，仅做数据拉取与简单汇总",
    )
    parser.add_argument(
        "--stream",
        "-s",
        action="store_true",
        help="流式输出，打印每个节点的输出",
    )
    args = parser.parse_args()

    target_date: Optional[datetime] = None
    raw = args.date or args.date_alt
    if raw:
        target_date = _parse_trade_date(raw)
        print(f"指定分析日期: {target_date.strftime('%Y%m%d')}")

    # 1. LLM（可选）
    llm = None
    if not args.no_llm:
        llm_config = get_llm_config()
        if not validate_config(llm_config):
            print("LLM 配置无效，将使用 --no-llm 模式（仅数据拉取与简单汇总）")
        else:
            print(f"使用 LLM: {llm_config.provider} / {llm_config.model}")
            llm = ChatOpenAI(
                base_url=llm_config.base_url,
                api_key=llm_config.api_key,
                model=llm_config.model,
                temperature=llm_config.temperature,
                max_retries=llm_config.max_retries,
                timeout=llm_config.timeout,
            )
    else:
        print("使用 --no-llm 模式，仅做数据拉取与简单汇总")

    # 2. 构建图
    graph = create_macro_graph(llm=llm)

    # 3. 构建 invoke 输入与 config
    trade_date = _resolve_trade_date(target_date)
    invoke_input: Dict[str, Any] = {"trade_date": trade_date}

    use_us = MACRO_USE_US_STOCK_TREND
    if args.us_stock:
        use_us = True
        print("已启用美股趋势分析 (--us-stock)")
    else:
        print(f"美股趋势分析: {'启用' if use_us else '关闭'}（默认 config）")

    run_config = {
        "configurable": {
            "macro_config": {
                "macro_use_us_stock_trend": use_us,
            }
        }
    }

    print(f"交易日 {trade_date}，开始运行宏观分析图...")
    start_time = time.perf_counter()

    from pprint import pprint

    if args.stream:
        # 流式输出，打印每个节点的输出，并合并得到最终状态
        # stream_mode="updates" 返回 {node_name: state_update} 字典
        result = dict(invoke_input)
        for event in graph.stream(
            invoke_input, config=run_config, stream_mode="updates"
        ):
            for node_name, node_output in event.items():
                print(f"\n=== 节点 [{node_name}] 输出 ===")
                pprint(node_output)
                if isinstance(node_output, dict):
                    result.update(node_output)
    else:
        result = graph.invoke(invoke_input, config=run_config)

    elapsed = time.perf_counter() - start_time
    print(f"\n宏观分析图运行完成，耗时 {elapsed:.2f} 秒。")

    print("\n=== 宏观分析结果 (macro_analysis) ===")
    pprint(result.get("macro_analysis"))


if __name__ == "__main__":
    main()
