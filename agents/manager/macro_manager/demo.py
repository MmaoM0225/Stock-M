"""
Macro Manager 每日流水线 Demo

流程：并行运行 4 宏观分析师 + news_analyst → macro_manager 综合 + md_write

用法:
    python -m agents.manager.macro_manager.demo
    python -m agents.manager.macro_manager.demo 20260305
"""
import logging
import sys
from pprint import pprint

from langchain_openai import ChatOpenAI

from ...config import get_llm_config, validate_config
from .daily_pipeline import create_daily_pipeline

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger(__name__)
# 避免 httpx/openai 每条请求刷 INFO（POST 200 OK 无信息量）
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("openai").setLevel(logging.WARNING)


def main():
    trade_date = sys.argv[1] if len(sys.argv) > 1 else None
    if not trade_date:
        from datetime import datetime
        trade_date = datetime.now().strftime("%Y%m%d")

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
    )
    fetcher = None
    try:
        from dataflow.news_sentiment import NewsSentimentFetcher
        fetcher = NewsSentimentFetcher()
    except ImportError as e:
        logger.warning("NewsSentimentFetcher 不可用: %s，新闻分析将跳过", e)

    pipeline = create_daily_pipeline(
        llm=llm,
        fetcher=fetcher,
        macro_config={"generate_markdown": True},
        news_config={"generate_markdown": True},
        strategy_config={"generate_markdown": True},
    )

    state = {"trade_date": trade_date}
    result = pipeline.invoke(state)

    print("\n=== macro_analysis (合并 4 分析师) ===")
    pprint(result.get("macro_analysis"))

    print("\n=== strategy_analysis ===")
    pprint(result.get("strategy_analysis"))

    print("\n完成。报告已写入 data/analysis/")


if __name__ == "__main__":
    main()
