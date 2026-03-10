"""
每日分析流水线 - 父编排图

流程：并行运行 4 宏观分析师 + news_analyst → 数据同时分发给 macro_manager 与 md_write 节点（并行）
"""
from concurrent.futures import ThreadPoolExecutor, as_completed
import json
import logging
import os
from datetime import datetime
from typing import Dict, Any, Optional, TypedDict

from langgraph.constants import Send
from langgraph.graph import StateGraph, START, END

from ...analyst.macro_economist.graph import create_macro_economist_graph
from ...analyst.commodity_analyst.graph import create_commodity_analyst_graph
from ...analyst.market_sentiment_analyst.graph import create_market_sentiment_analyst_graph
from ...analyst.liquidity_analyst.graph import create_liquidity_analyst_graph
from ...analyst.news_analyst.graph import create_news_graph
from ...analyst.news_analyst.node import _write_news_analysis_md
from .graph import create_macro_manager_graph

logger = logging.getLogger(__name__)


class DailyPipelineState(TypedDict, total=False):
    """每日流水线状态。"""

    trade_date: str
    macro_analysis: Dict[str, Any]
    news_analysis: Dict[str, Any]
    strategy_analysis: Dict[str, Any]
    macro_full_state: Dict[str, Any]  # 4 分析师完整状态，供 md_write 使用
    news_full_state: Dict[str, Any]   # 新闻图完整状态，供 md_write 使用


def _run_macro_economist(trade_date: str, llm, cfg: dict) -> Dict[str, Any]:
    """运行宏观经济分析师。"""
    try:
        graph = create_macro_economist_graph(llm=llm)
        result = graph.invoke(
            {"trade_date": trade_date},
            config={"configurable": cfg or {}},
        )
        return result.get("macro_economist_analysis") or {}
    except Exception as e:
        logger.warning("macro_economist 执行失败: %s", e)
        return {"error": str(e)}


def _run_commodity(trade_date: str, llm, cfg: dict) -> Dict[str, Any]:
    """运行大宗商品分析师。"""
    try:
        graph = create_commodity_analyst_graph(llm=llm)
        result = graph.invoke(
            {"trade_date": trade_date},
            config={"configurable": cfg or {}},
        )
        return result.get("commodity_analyst_summary") or {}
    except Exception as e:
        logger.warning("commodity_analyst 执行失败: %s", e)
        return {"error": str(e)}


def _run_market_sentiment(trade_date: str, llm, cfg: dict) -> Dict[str, Any]:
    """运行市场情绪分析师。"""
    try:
        graph = create_market_sentiment_analyst_graph(llm=llm)
        result = graph.invoke(
            {"trade_date": trade_date},
            config={"configurable": cfg or {}},
        )
        return result.get("market_sentiment_analyst_summary") or {}
    except Exception as e:
        logger.warning("market_sentiment_analyst 执行失败: %s", e)
        return {"error": str(e)}


def _run_liquidity(trade_date: str, llm, cfg: dict) -> Dict[str, Any]:
    """运行流动性分析师。"""
    try:
        graph = create_liquidity_analyst_graph(llm=llm)
        result = graph.invoke(
            {"trade_date": trade_date},
            config={"configurable": cfg or {}},
        )
        return result.get("liquidity_analyst_summary") or {}
    except Exception as e:
        logger.warning("liquidity_analyst 执行失败: %s", e)
        return {"error": str(e)}


def _run_macro_batch(
    trade_date: str,
    llm,
    macro_config: dict,
) -> Dict[str, Any]:
    """并行运行 4 宏观分析师，合并为 macro_analysis。"""
    cfg = dict(macro_config or {})
    result = {
        "date": trade_date,
        "macro_economist": {},
        "commodity": {},
        "market_sentiment": {},
        "liquidity": {},
        "summary": "",
    }
    full_states = {}

    with ThreadPoolExecutor(max_workers=4) as executor:
        futures = {
            executor.submit(_run_macro_economist, trade_date, llm, cfg): "macro_economist",
            executor.submit(_run_commodity, trade_date, llm, cfg): "commodity",
            executor.submit(_run_market_sentiment, trade_date, llm, cfg): "market_sentiment",
            executor.submit(_run_liquidity, trade_date, llm, cfg): "liquidity",
        }
        for future in as_completed(futures):
            key = futures[future]
            try:
                data = future.result()
                result[key] = data
                full_states[key] = data
            except Exception as e:
                result[key] = {"error": str(e)}
                full_states[key] = {"error": str(e)}

    # 简单汇总（可选，macro_synthesize 会做深度综合）
    summaries = []
    for k, v in result.items():
        if k in ("date", "summary") or not isinstance(v, dict):
            continue
        s = v.get("summary") or v.get("liquidity_summary") or v.get("sentiment_summary") or v.get("macro_summary") or v.get("conclusion", "")
        if s:
            summaries.append(f"{k}: {s[:80]}...")
    if summaries:
        result["summary"] = "；".join(summaries)[:500]

    return {
        "macro_analysis": result,
        "macro_full_state": {"analysts": full_states, "merged": result},
    }


def _run_news(trade_date: str, llm, fetcher, news_config: dict) -> Dict[str, Any]:
    """运行新闻分析图，返回完整状态。"""
    cfg = dict(news_config or {})
    cfg["generate_markdown"] = False
    graph = create_news_graph(llm=llm, fetcher=fetcher)
    result = graph.invoke(
        {"trade_date": trade_date},
        config={"configurable": {"news_config": cfg}},
    )
    return {"news_analysis": result.get("news_analysis"), "news_full_state": result}


def _write_macro_analysis_md(
    macro_full_state: Dict[str, Any],
    macro_analysis: Dict[str, Any],
    trade_date: str,
    llm=None,
    use_llm_for_md: bool = False,
) -> Optional[str]:
    """将 4 分析师合并的宏观分析写入 Markdown。"""
    try:
        lines = [
            f"# 宏观分析报告 {trade_date}",
            "",
            "## 综合摘要",
            "",
            macro_analysis.get("summary", "（无摘要）"),
            "",
            "---",
            "",
            "## 宏观经济",
            "",
            json.dumps(macro_analysis.get("macro_economist") or {}, ensure_ascii=False, indent=2, default=str),
            "",
            "---",
            "",
            "## 大宗商品",
            "",
            json.dumps(macro_analysis.get("commodity") or {}, ensure_ascii=False, indent=2, default=str),
            "",
            "---",
            "",
            "## 市场情绪",
            "",
            json.dumps(macro_analysis.get("market_sentiment") or {}, ensure_ascii=False, indent=2, default=str),
            "",
            "---",
            "",
            "## 流动性",
            "",
            json.dumps(macro_analysis.get("liquidity") or {}, ensure_ascii=False, indent=2, default=str),
            "",
            "---",
            "",
            "## 报告元数据",
            "",
            f"- 报告日期: {trade_date}",
            f"- 生成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
            "",
        ]
        md_content = "\n".join(lines)
        out_dir = os.path.join("data", "analysis")
        os.makedirs(out_dir, exist_ok=True)
        path = os.path.join(out_dir, f"{trade_date}_macro_analysis.md")
        with open(path, "w", encoding="utf-8") as f:
            f.write(md_content)
        logger.info("宏观报告已写入: %s", path)
        return path
    except Exception as e:
        logger.warning("写入宏观 Markdown 失败: %s", e)
        return None


def create_analyst_fetch_node(llm, fetcher, macro_config=None, news_config=None):
    """
    构建分析师并行拉取节点。
    并行执行：macro_batch（4 宏观分析师）与 news 图。
    """

    def analyst_fetch_node(state: Dict) -> Dict:
        trade_date = state.get("trade_date", "")
        if not trade_date:
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
                executor.submit(_run_macro_batch, trade_date, llm, macro_cfg): "macro",
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


def create_macro_manager_node(llm, strategy_config=None):
    """
    构建宏观管理节点。
    将 state 传入 macro_manager 图，返回合并后的 state。
    """

    def macro_manager_node(state: Dict) -> Dict:
        graph = create_macro_manager_graph(llm=llm)
        cfg = {"configurable": {"strategy_config": strategy_config or {}}}
        result = graph.invoke(state, config=cfg)
        return {"strategy_analysis": result.get("strategy_analysis")}

    return macro_manager_node


def create_md_write_node(llm, macro_config=None, news_config=None):
    """
    构建 md 文档写入节点。
    将 macro_analysis、news_analysis 写入 Markdown 文件。
    与 macro_manager 节点并行接收 analyst_fetch 的输出。
    """
    from ...config import NEWS_USE_LLM_FOR_MARKDOWN, MACRO_USE_LLM_FOR_MARKDOWN

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

        # 写宏观 md（4 分析师合并）
        if gen_macro_md and macro_analysis:
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

        return {}

    return md_write_node


def _fan_out_after_analyst_fetch(state: Dict) -> list:
    """analyst_fetch 后并行分发：macro_manager 与 md_write 同时接收数据。"""
    return [Send("macro_manager", state), Send("md_write", state)]


def create_daily_pipeline(
    llm=None,
    fetcher=None,
    macro_config: Optional[Dict] = None,
    news_config: Optional[Dict] = None,
    strategy_config: Optional[Dict] = None,
):
    """
    构建每日分析流水线图。

    流程：analyst_fetch（4 宏观分析师 + news 并行）→ 数据同时分发给 macro_manager 与 md_write（并行）

    Args:
        llm: LLM 实例，各子图共用
        fetcher: NewsSentimentFetcher，新闻分析必需
        macro_config: 宏观分析师配置（4 分析师共用）
        news_config: 新闻分析师配置
        strategy_config: 宏观管理/策略配置

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
        create_analyst_fetch_node(llm, fetcher, macro_config, news_config),
    )
    builder.add_node(
        "macro_manager",
        create_macro_manager_node(llm, strategy_config),
    )
    builder.add_node(
        "md_write",
        create_md_write_node(llm, macro_config, news_config),
    )

    builder.add_edge(START, "analyst_fetch")
    builder.add_conditional_edges("analyst_fetch", _fan_out_after_analyst_fetch)
    builder.add_edge("macro_manager", END)
    builder.add_edge("md_write", END)

    return builder.compile()
