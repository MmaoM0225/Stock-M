"""
Live Trading 顶层流程图可视化。

默认会导出：
- Mermaid 文本：data/visualization/live_trading_pipeline_graph.mmd
- PNG 图片（若当前环境支持）：data/visualization/live_trading_pipeline_graph.png

使用方法:
    python -m visualization.live_trading_pipeline_graph_visualization
    python -m visualization.live_trading_pipeline_graph_visualization --no-png
    python -m visualization.live_trading_pipeline_graph_visualization --mermaid-output data/visualization/custom_graph.mmd
    python -m visualization.live_trading_pipeline_graph_visualization --png-output data/visualization/custom_graph.png
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

# 允许直接以脚本形式运行
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from agents.orchestrator.live_trading_pipeline import create_live_trading_pipeline_graph
try:
    from langchain_core.runnables.graph_mermaid import MermaidDrawMethod
except Exception:
    MermaidDrawMethod = None


def _export_mermaid(graph_obj: object, output_path: Path) -> None:
    if not hasattr(graph_obj, "draw_mermaid"):
        raise RuntimeError("当前 langgraph 版本不支持 draw_mermaid()")
    mermaid_text = graph_obj.draw_mermaid()
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(mermaid_text, encoding="utf-8")
    print(f"Mermaid 已保存: {output_path}")


def _try_export_png(graph_obj: object, output_path: Path) -> bool:
    if not hasattr(graph_obj, "draw_mermaid_png"):
        print("当前环境不支持 draw_mermaid_png()，已跳过 PNG 导出")
        return False

    # 方案1：在线 mermaid.ink 渲染（提高重试次数）
    try:
        png_bytes = graph_obj.draw_mermaid_png(max_retries=5, retry_delay=2.0)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_bytes(png_bytes)
        print(f"PNG 已保存(mermaid.ink): {output_path}")
        return True
    except Exception as exc:
        print(f"在线 PNG 导出失败，准备尝试本地渲染: {exc}")

    # 方案2：本地 Pyppeteer 渲染（无需依赖 mermaid.ink）
    if MermaidDrawMethod is None:
        print("未检测到 MermaidDrawMethod，无法启用本地 PYPPETEER 渲染")
        return False

    try:
        png_bytes = graph_obj.draw_mermaid_png(draw_method=MermaidDrawMethod.PYPPETEER)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_bytes(png_bytes)
        print(f"PNG 已保存(PYPPETEER): {output_path}")
        return True
    except Exception as exc:
        print(f"PNG 导出失败（保留 Mermaid 文本即可）: {exc}")
        return False


def main() -> None:
    parser = argparse.ArgumentParser(description="导出 live trading 顶层流程图")
    parser.add_argument(
        "--mermaid-output",
        type=str,
        default=str(PROJECT_ROOT / "data" / "visualization" / "live_trading_pipeline_graph.mmd"),
        help="Mermaid 文本输出路径",
    )
    parser.add_argument(
        "--png-output",
        type=str,
        default=str(PROJECT_ROOT / "data" / "visualization" / "live_trading_pipeline_graph.png"),
        help="PNG 输出路径",
    )
    parser.add_argument(
        "--no-png",
        action="store_true",
        help="仅导出 Mermaid，不尝试导出 PNG",
    )
    args = parser.parse_args()

    # 仅绘图，不执行 invoke，因此 llm/news_fetcher 传 None 即可
    pipeline_graph = create_live_trading_pipeline_graph(llm=None, news_fetcher=None)
    graph_obj = pipeline_graph.get_graph()

    mermaid_output = Path(args.mermaid_output)
    png_output = Path(args.png_output)
    _export_mermaid(graph_obj, mermaid_output)

    if not args.no_png:
        _try_export_png(graph_obj, png_output)


if __name__ == "__main__":
    main()
