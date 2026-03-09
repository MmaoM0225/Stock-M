"""
每日分析流水线 - 父编排图

流程：并行运行 macro_analyst + news_analyst → 数据同时分发给 strategy_manager 与 md_write 节点（并行）
"""
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, Any, Optional, TypedDict

from langgraph.constants import Send
from langgraph.graph import StateGraph, START, END

from ..macro_analyst.graph import create_macro_graph
from ..macro_analyst.node import _write_macro_analysis_md
from ..news_analyst.graph import create_news_graph
from ..news_analyst.node import _write_news_analysis_md
from .graph import create_strategy_graph


class DailyPipelineState(TypedDict, total=False):
    """每日流水线状态。"""

    trade_date: str
    macro_analysis: Dict[str, Any]
    news_analysis: Dict[str, Any]
    strategy_analysis: Dict[str, Any]
    macro_full_state: Dict[str, Any]  # 宏观图完整状态，供 md_write 使用
    news_full_state: Dict[str, Any]   # 新闻图完整状态，供 md_write 使用


def _run_macro(trade_date: str, llm, macro_config: dict) -> Dict[str, Any]:
    """运行宏观分析图，返回完整状态（不写 md，由父图 md_write 负责）。"""
    cfg = dict(macro_config or {})
    cfg["generate_markdown"] = False  # 不在子图内写 md
    graph = create_macro_graph(llm=llm)
    result = graph.invoke(
        {"trade_date": trade_date},
        config={"configurable": {"macro_config": cfg}},
    )
    return {"macro_analysis": result.get("macro_analysis"), "macro_full_state": result}


def _run_news(trade_date: str, llm, fetcher, news_config: dict) -> Dict[str, Any]:
    """运行新闻分析图，返回完整状态（不写 md，由父图 md_write 负责）。"""
    cfg = dict(news_config or {})
    cfg["generate_markdown"] = False  # 不在子图内写 md
    graph = create_news_graph(llm=llm, fetcher=fetcher)
    result = graph.invoke(
        {"trade_date": trade_date},
        config={"configurable": {"news_config": cfg}},
    )
    return {"news_analysis": result.get("news_analysis"), "news_full_state": result}


def create_analyst_fetch_node(llm, fetcher, macro_config=None, news_config=None):
    """
    构建分析师并行拉取节点。
    使用 ThreadPoolExecutor 并行执行 macro 与 news 图。
    """

    def analyst_fetch_node(state: Dict) -> Dict:
        trade_date = state.get("trade_date", "")
        if not trade_date:
            from datetime import datetime

            trade_date = datetime.now().strftime("%Y%m%d")

        macro_cfg = macro_config or {}
        news_cfg = news_config or {}

        result = {
            "trade_date": trade_date,
            "macro_analysis": None,
            "news_analysis": None,
            "macro_full_state": None,
            "news_full_state": None,
        }

        with ThreadPoolExecutor(max_workers=2) as executor:
            futures = {
                executor.submit(_run_macro, trade_date, llm, macro_cfg): "macro",
                executor.submit(_run_news, trade_date, llm, fetcher, news_cfg): "news",
            }
            for future in as_completed(futures):
                try:
                    data = future.result()
                    if "macro_analysis" in data:
                        result["macro_analysis"] = data["macro_analysis"]
                        result["macro_full_state"] = data.get("macro_full_state")
                    if "news_analysis" in data:
                        result["news_analysis"] = data["news_analysis"]
                        result["news_full_state"] = data.get("news_full_state")
                except Exception as e:
                    key = futures[future]
                    if key == "macro":
                        result["macro_analysis"] = {
                            "date": trade_date,
                            "summary": f"宏观分析执行失败: {e}",
                        }
                    else:
                        result["news_analysis"] = {
                            "date": trade_date,
                            "sector_impacts": {},
                            "macro_environment": {},
                            "events": [],
                        }

        return result

    return analyst_fetch_node


def create_strategy_node(llm, strategy_config=None):
    """
    构建策略节点。
    将 state 传入 strategy_graph，返回合并后的 state。
    """

    def strategy_node(state: Dict) -> Dict:
        graph = create_strategy_graph(llm=llm)
        cfg = {"configurable": {"strategy_config": strategy_config or {}}}
        result = graph.invoke(state, config=cfg)
        return {"strategy_analysis": result.get("strategy_analysis")}

    return strategy_node


def create_md_write_node(llm, macro_config=None, news_config=None):
    """
    构建 md 文档写入节点。
    将 macro_analysis、news_analysis 写入 Markdown 文件。
    与 strategy 节点并行接收 analyst_fetch 的输出。
    """
    from ...config import MACRO_USE_LLM_FOR_MARKDOWN, NEWS_USE_LLM_FOR_MARKDOWN

    def md_write_node(state: Dict) -> Dict:
        trade_date = state.get("trade_date", "")
        macro_full = state.get("macro_full_state") or {}
        news_full = state.get("news_full_state") or {}
        macro_analysis = state.get("macro_analysis") or {}
        news_analysis = state.get("news_analysis") or {}

        macro_cfg = macro_config or {}
        news_cfg = news_config or {}
        gen_macro_md = macro_cfg.get("generate_markdown", True)
        gen_news_md = news_cfg.get("generate_markdown", True)

        # 写宏观 md
        if gen_macro_md and macro_analysis and macro_full:
            use_llm = macro_cfg.get("use_llm_for_markdown", MACRO_USE_LLM_FOR_MARKDOWN)
            _write_macro_analysis_md(
                macro_full,
                macro_analysis,
                trade_date,
                llm=llm,
                use_llm_for_md=use_llm,
            )

        # 写新闻 md
        if gen_news_md and news_analysis:
            use_llm = news_cfg.get("use_llm_for_markdown", NEWS_USE_LLM_FOR_MARKDOWN)
            _write_news_analysis_md(
                news_analysis,
                trade_date,
                llm=llm,
                use_llm_for_md=use_llm,
            )

        return {}  # 不修改 state，仅写文件

    return md_write_node


def _fan_out_after_analyst_fetch(state: Dict) -> list:
    """analyst_fetch 后并行分发：strategy 与 md_write 同时接收数据。"""
    return [Send("strategy", state), Send("md_write", state)]


def create_daily_pipeline(
    llm=None,
    fetcher=None,
    macro_config: Optional[Dict] = None,
    news_config: Optional[Dict] = None,
    strategy_config: Optional[Dict] = None,
):
    """
    构建每日分析流水线图。

    流程：analyst_fetch（macro + news 并行）→ 数据同时分发给 strategy 与 md_write（并行）

    Args:
        llm: LLM 实例，三个子图共用
        fetcher: NewsSentimentFetcher，新闻分析必需
        macro_config: 宏观分析师配置
        news_config: 新闻分析师配置
        strategy_config: 策略经理配置

    Returns:
        已编译的每日流水线图。
    """
    if fetcher is None:
        try:
            from dataflow.news_sentiment import NewsSentimentFetcher

            fetcher = NewsSentimentFetcher()
        except ImportError as e:
            raise ImportError(
                "每日流水线需要 NewsSentimentFetcher，请安装 dataflow 或传入 fetcher"
            ) from e

    builder = StateGraph(DailyPipelineState)

    builder.add_node(
        "analyst_fetch",
        create_analyst_fetch_node(
            llm, fetcher, macro_config, news_config
        ),
    )
    builder.add_node(
        "strategy",
        create_strategy_node(llm, strategy_config),
    )
    builder.add_node(
        "md_write",
        create_md_write_node(llm, macro_config, news_config),
    )

    builder.add_edge(START, "analyst_fetch")
    builder.add_conditional_edges("analyst_fetch", _fan_out_after_analyst_fetch)
    builder.add_edge("strategy", END)
    builder.add_edge("md_write", END)

    return builder.compile()
