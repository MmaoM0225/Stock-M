"""
行业收益额柱状图（来自 strategy_holdings_periods --with-returns 的 JSON 中 by_industry）

用法:
    python -m visualization.industry_pnl_bar_chart -i summary.json -o industry_pnl.png
    python -m scripts.strategy_holdings_periods -s STRATEGY --with-returns --format json | \\
        python -m visualization.industry_pnl_bar_chart -o industry_pnl.png

输入 JSON 可为:
  - 含 \"by_industry\" 键的对象；或
  - 直接为 by_industry 数组（每项含 行业、总收益额_元）。

默认水平条形图（行业名易读）；可用 --vertical 改为竖向柱状图。
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any, Dict, List

import matplotlib.pyplot as plt
import numpy as np

plt.rcParams["font.sans-serif"] = ["SimHei", "Microsoft YaHei", "Arial Unicode MS", "DejaVu Sans"]
plt.rcParams["axes.unicode_minus"] = False


def _project_root() -> Path:
    return Path(__file__).resolve().parents[1]


def load_by_industry(source: Path | None) -> List[Dict[str, Any]]:
    if source is not None:
        raw = source.read_text(encoding="utf-8")
    else:
        raw = sys.stdin.read()
    if not raw.strip():
        raise ValueError("输入为空")
    data = json.loads(raw)
    if isinstance(data, dict) and "by_industry" in data:
        rows = data["by_industry"]
    elif isinstance(data, list):
        rows = data
    else:
        raise ValueError("JSON 需为 {by_industry: [...]} 或行业数组")
    if not isinstance(rows, list):
        raise ValueError("by_industry 须为数组")
    out: List[Dict[str, Any]] = []
    for r in rows:
        if not isinstance(r, dict):
            continue
        name = r.get("行业", r.get("industry", ""))
        amt = r.get("总收益额_元", r.get("total_pnl", 0))
        try:
            v = float(amt)
        except (TypeError, ValueError):
            continue
        out.append({"行业": str(name) if name is not None else "-", "总收益额_元": v})
    return out


def plot_industry_pnl(
    rows: List[Dict[str, Any]],
    *,
    title: str = "行业收益额汇总（元）",
    output_path: Path | None = None,
    show: bool = True,
    vertical: bool = False,
    fig_width: float = 10.0,
    fig_height: float | None = None,
) -> None:
    if not rows:
        raise ValueError("没有可绘制的行业数据")

    # 按收益额升序：水平条时自下而上从低到高，读起来自然；若希望大赚在上可 reverse
    sorted_rows = sorted(rows, key=lambda x: x["总收益额_元"])
    names = [r["行业"] for r in sorted_rows]
    vals = [r["总收益额_元"] for r in sorted_rows]
    colors = ["#c62828" if v < 0 else "#1b5e20" if v > 0 else "#616161" for v in vals]

    n = len(names)
    if fig_height is None:
        fig_height = max(6.0, 0.35 * n + 2.0)

    fig, ax = plt.subplots(figsize=(fig_width, fig_height))

    if vertical:
        x = np.arange(n)
        ax.bar(x, vals, color=colors, width=0.72, edgecolor="white", linewidth=0.5)
        ax.set_xticks(x)
        ax.set_xticklabels(names, rotation=45, ha="right", fontsize=9)
        ax.set_ylabel("收益额（元）")
        ax.axhline(0, color="#333333", linewidth=0.8)
    else:
        y = np.arange(n)
        ax.barh(y, vals, color=colors, height=0.72, edgecolor="white", linewidth=0.5)
        ax.set_yticks(y)
        ax.set_yticklabels(names, fontsize=9)
        ax.set_xlabel("收益额（元）")
        ax.axvline(0, color="#333333", linewidth=0.8)
        # 条尾标注数值
        pad = (max(vals) - min(vals)) * 0.01 if max(vals) != min(vals) else 1.0
        for i, v in enumerate(vals):
            tx = v + pad if v >= 0 else v - pad
            ha = "left" if v >= 0 else "right"
            ax.text(tx, i, f"{v:,.0f}", va="center", ha=ha, fontsize=8, color="#333333")

    ax.set_title(title)
    ax.grid(axis="x" if not vertical else "y", linestyle="--", alpha=0.35)
    fig.tight_layout()

    if output_path:
        fig.savefig(output_path, dpi=150, bbox_inches="tight")
    if show:
        plt.show()
    else:
        plt.close(fig)


def main() -> int:
    parser = argparse.ArgumentParser(description="行业收益额柱状图")
    parser.add_argument(
        "-i",
        "--input",
        type=Path,
        default=None,
        help="JSON 文件路径（默认可从 stdin 读）",
    )
    parser.add_argument("-o", "--output", type=Path, default=None, help="输出 PNG 路径")
    parser.add_argument("--title", default="行业收益额汇总（元）", help="图标题")
    parser.add_argument("--no-show", action="store_true", help="仅保存不弹窗")
    parser.add_argument("--vertical", action="store_true", help="竖向柱状图（默认水平条形图）")
    parser.add_argument("--width", type=float, default=10.0, help="图宽（英寸）")
    parser.add_argument("--height", type=float, default=None, help="图高（英寸），默认随行业数变化")
    args = parser.parse_args()

    if args.input is None and sys.stdin.isatty():
        parser.error("请指定 -i/--input JSON 文件，或将 JSON 通过管道传入 stdin")

    try:
        rows = load_by_industry(args.input)
    except FileNotFoundError as e:
        print(f"读取失败: {e}", file=sys.stderr)
        if args.input is not None:
            print(
                "提示: 请先在项目根目录生成 JSON，例如:\n"
                "  python -m scripts.strategy_holdings_periods -s <策略名> --with-returns --format json -o summary.json",
                file=sys.stderr,
            )
        return 1
    except Exception as e:
        print(f"读取失败: {e}", file=sys.stderr)
        return 1

    out = args.output
    if out is None:
        out = _project_root() / "visualization" / "industry_pnl_bar_chart.png"

    try:
        plot_industry_pnl(
            rows,
            title=args.title,
            output_path=out,
            show=not args.no_show,
            vertical=args.vertical,
            fig_width=args.width,
            fig_height=args.height,
        )
    except Exception as e:
        print(f"绘图失败: {e}", file=sys.stderr)
        return 1

    print(f"已保存: {out.resolve().as_posix()}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
