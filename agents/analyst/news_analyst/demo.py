"""
新闻分析子图 Demo。

可以直接运行本文件，查看 map-reduce 子图是否能够正确运行：

    python -m agents.analyst.news_analyst.demo
    python -m agents.analyst.news_analyst.demo 20260305
"""

from typing import Any, Dict, List, Optional

import argparse
import re
from datetime import datetime, timedelta
import time

from langchain_openai import ChatOpenAI

from .graph import create_news_graph
from .node import (
    NewsEvent,
    SectorImpacts,
    SectorImpact,
    MacroEnvironment,
    SectorSentiment,
    Liquidity,
    PolicyBias,
    GlobalRisk,
)
from ...config import get_llm_config, validate_config
from dataflow.news_sentiment import NewsSentimentFetcher


class FakeToolkit:
    """简化版工具对象，只提供 get_industry_info 接口。

    这里仅用于为 LLM 提供一个“候选行业列表”提示，
    真正的行业识别逻辑可以后续接入更完整的数据源。
    """

    def get_industry_info(self, content: str):
        return {
            "matched_industries": [],
            "industry_count": 0,
            "all_industries": [],
        }


class FakeLLM:
    """
    Demo 用的假 LLM，绕过真实网络调用。

    通过 with_structured_output 记录当前期望的输出模型，
    invoke 时直接构造对应的 Pydantic 对象返回。
    """

    def __init__(self):
        self._output_model = None

    def with_structured_output(self, model_cls):
        self._output_model = model_cls
        return self

    def invoke(self, _input: Dict[str, Any]):
        if self._output_model is NewsEvent:
            # 返回一个固定的事件，确保 map 阶段能正常聚合
            return NewsEvent(
                source="测试来源",
                type="macro",
                summary="测试事件摘要",
                industry=["测试行业"],
                sentiment="neutral",
                impact_level=3,
            )

        if self._output_model is SectorImpacts:
            # 返回一个固定的板块与宏观环境分析结果
            return SectorImpacts(
                sector_impacts={
                    "测试板块": SectorImpact(
                        sentiment=SectorSentiment.neutral,
                        confidence=0.5,
                        reason=["测试原因"],
                    )
                },
                macro_environment=MacroEnvironment(
                    liquidity=Liquidity.neutral,
                    policy_bias=PolicyBias.neutral,
                    global_risk=GlobalRisk.medium,
                    market_sentiment=SectorSentiment.neutral,
                ),
            )

        raise RuntimeError("FakeLLM 未设置输出模型")


def _parse_trade_date(s: str) -> datetime:
    """解析交易日期，仅支持 YYYYMMDD。"""
    s = s.strip()
    if re.fullmatch(r"\d{8}", s):
        return datetime.strptime(s, "%Y%m%d")
    raise ValueError(f"无效日期格式: {s}，请使用 YYYYMMDD")


def _resolve_date_and_news(
    fetcher: NewsSentimentFetcher,
    target_date: Optional[datetime],
) -> tuple[str, Dict[str, Any]]:
    """
    根据目标日期解析交易日期和新闻数据。
    - 若指定 target_date：仅获取该日期的新闻，失败时用 demo 数据并仍使用该日期。
    - 若未指定：从今天起往前查找最近有数据的工作日。
    """
    today = datetime.now()
    if target_date is not None:
        if target_date.weekday() >= 5:
            print(f"警告: {target_date.date()} 为周末，仍将尝试获取该日新闻。")
        trade_date = target_date.strftime("%Y%m%d")
        print(f"尝试获取日期 {trade_date} 的新闻数据...")
        news_data = fetcher.get_news_by_date(trade_date)
        if not news_data:
            print("获取新闻失败，使用内置 Demo 新闻数据。")
            news_data = {
                "sections": [
                    {"title": "测试新闻 1", "content": "这是第一条测试新闻内容。"},
                    {"title": "测试新闻 2", "content": "这是第二条测试新闻内容。"},
                ]
            }
        return trade_date, news_data

    # 未指定日期：从今天往前查找
    for i in range(7):
        check_date = today - timedelta(days=i)
        if check_date.weekday() >= 5:
            continue
        trade_date = check_date.strftime("%Y%m%d")
        print(f"尝试获取日期 {trade_date} 的新闻数据...")
        news_data = fetcher.get_news_by_date(trade_date)
        if news_data:
            print(f"成功获取到日期 {trade_date} 的新闻数据。")
            return trade_date, news_data

    print("获取最新新闻失败，使用内置 Demo 新闻数据。")
    trade_date = today.strftime("%Y%m%d")
    news_data = {
        "sections": [
            {"title": "测试新闻 1", "content": "这是第一条测试新闻内容。"},
            {"title": "测试新闻 2", "content": "这是第二条测试新闻内容。"},
        ]
    }
    return trade_date, news_data


def main():
    parser = argparse.ArgumentParser(description="新闻分析子图 Demo，支持分析指定交易日的新闻")
    parser.add_argument(
        "date",
        nargs="?",
        help="要分析的交易日期，格式：YYYYMMDD。不指定则从今天起往前查找最近有数据的工作日",
    )
    parser.add_argument(
        "--date", "-d",
        dest="date_alt",
        help="同上，可用 --date 或 -d 显式指定",
    )
    args = parser.parse_args()
    target_date: Optional[datetime] = None
    raw = args.date or args.date_alt
    if raw:
        target_date = _parse_trade_date(raw)
        print(f"指定分析日期: {target_date.strftime('%Y%m%d')}")

    # 1. 根据配置构建真实 LLM，如配置或环境变量不完整则回退到 FakeLLM
    llm_config = get_llm_config()
    if validate_config(llm_config):
        print(f"使用真实 LLM 模型: {llm_config.provider} / {llm_config.model}")
        llm = ChatOpenAI(
            base_url=llm_config.base_url,
            api_key=llm_config.api_key,
            model=llm_config.model,
            temperature=llm_config.temperature,
            max_retries=llm_config.max_retries,
            timeout=llm_config.timeout,
        )
    else:
        print("LLM 配置无效，回退到 FakeLLM（仅用于本地结构验证）")
        llm = FakeLLM()

    # 2. 构建行业提示工具（简单版，用于给 LLM 提示行业标签，不依赖 @tool 装饰器）
    toolkit = FakeToolkit()
    fetcher = NewsSentimentFetcher()
    graph = create_news_graph(llm, toolkit, fetcher)

    # 3. 构建 invoke 输入
    # 指定日期：只传 trade_date，由 news_fetch_node 内部判断交易日、本地/fetch
    # 未指定：先查找有数据的工作日，传入 news_data，节点直接用
    if target_date is not None:
        trade_date = target_date.strftime("%Y%m%d")
        invoke_input: Dict[str, Any] = {"trade_date": trade_date}
        print(f"交易日 {trade_date}，由 news_fetch 节点内部获取新闻（本地优先，无则抓取）。")
    else:
        trade_date, news_data = _resolve_date_and_news(fetcher, None)
        invoke_input = {"trade_date": trade_date, "news_data": news_data}
        sections = news_data.get("sections", [])
        print(f"交易日 {trade_date}，预取 {len(sections)} 条新闻。")

    print("开始运行新闻分析子图...")
    start_time = time.perf_counter()

    result = graph.invoke(invoke_input)

    elapsed = time.perf_counter() - start_time
    print(f"新闻分析子图运行完成，耗时 {elapsed:.2f} 秒。")

    from pprint import pprint

    print("=== 新闻分析子图运行结果 ===")
    pprint(result.get("news_analysis"))


if __name__ == "__main__":
    main()

