"""
新闻分析子图 Demo。

可以直接运行本文件，查看 map-reduce 子图是否能够正确运行：

    python -m agents.analyst.news_analyst.demo
"""

from typing import Any, Dict, List

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


def main():
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

    graph = create_news_graph(llm, toolkit)

    # 3. 优先尝试使用 NewsSentimentFetcher 获取最新真实新闻；失败时退回到内置 demo 数据
    fetcher = NewsSentimentFetcher()
    news_data: Dict[str, Any] = None

    # 从今天开始往前查找最近一个有数据的工作日
    today = datetime.now()
    for i in range(7):
        check_date = today - timedelta(days=i)
        # 跳过周末（周六=5，周日=6）
        if check_date.weekday() >= 5:
            continue
        date_str = check_date.strftime("%Y%m%d")
        print(f"尝试获取日期 {date_str} 的新闻数据...")
        news_data = fetcher.get_news_by_date(date_str)
        if news_data:
            print(f"成功获取到日期 {date_str} 的新闻数据。")
            break

    if not news_data:
        print("获取最新新闻失败，使用内置 Demo 新闻数据。")
        news_data = {
            "sections": [
                {"title": "测试新闻 1", "content": "这是第一条测试新闻内容。"},
                {"title": "测试新闻 2", "content": "这是第二条测试新闻内容。"},
            ]
        }

    # 统计并打印本次要分析的新闻条数（全部纳入分析）
    sections = news_data.get("sections", [])
    print(f"本次共获取到 {len(sections)} 条新闻，将全部纳入分析。")

    print("开始运行新闻分析子图...")
    start_time = time.perf_counter()

    result = graph.invoke(
        {
            "trade_date": "2026-03-05",
            "news_data": news_data,
        }
    )

    elapsed = time.perf_counter() - start_time
    print(f"新闻分析子图运行完成，耗时 {elapsed:.2f} 秒。")

    from pprint import pprint

    print("=== 新闻分析子图运行结果 ===")
    pprint(result.get("news_analysis"))


if __name__ == "__main__":
    main()

