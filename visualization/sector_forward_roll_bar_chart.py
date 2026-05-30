"""
滚动板块前向收益 JSON 柱状图（macro / sector_manager 脚本输出的 result.json）

读取 `mode=roll` 且含 `periods[].equal_weight_return_pct` 的文件，按周期绘制等权组合收益率柱状图。

用法:
    python -m visualization.sector_forward_roll_bar_chart
    python -m visualization.sector_forward_roll_bar_chart -i data/artifacts/macro_sector_forward/result.json -o data/artifacts/macro_sector_forward/roll_bars.png
    python -m visualization.sector_forward_roll_bar_chart -i data/artifacts/sector_manager_sector_forward/result.json --no-show
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import matplotlib.pyplot as plt
import numpy as np

plt.rcParams["font.sans-serif"] = ["SimHei", "Microsoft YaHei", "Arial Unicode MS", "DejaVu Sans"]
plt.rcParams["axes.unicode_minus"] = False


def _project_root() -> Path:
    return Path(__file__).resolve().parents[1]


def load_roll_periods(path: Path) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    data = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(data, dict):
        raise ValueError("JSON 须为对象")
    if data.get("mode") != "roll":
        raise ValueError("仅支持 mode=roll 的结果（由 macro/sector_manager 滚动脚本生成）")
    periods = data.get("periods")
    if not isinstance(periods, list) or not periods:
        raise ValueError("缺少非空 periods 数组")
    return periods, data


def _extract_plot_rows(periods: List[Dict[str, Any]]) -> List[Tuple[str, float]]:
    """(x 轴标签, 组合收益率%%) 仅保留有组合收益且非缺 JSON 的周期。"""
    out: List[Tuple[str, float]] = []
    for p in periods:
        if not isinstance(p, dict):
            continue
        if p.get("missing_result_json"):
            continue
        v = p.get("equal_weight_return_pct")
        if v is None:
            continue
        try:
            fv = float(v)
        except (TypeError, ValueError):
            continue
        start = str(p.get("start", "")).strip()
        end = str(p.get("end", "")).strip()
        label = f"{start}→{end}" if start and end else str(len(out))
        out.append((label, fv))
    return out


def _bar_colors(vals: List[float]) -> List[str]:
    return ["#c62828" if v < 0 else "#2e7d32" if v > 0 else "#616161" for v in vals]


def plot_period_returns(
    rows: List[Tuple[str, float]],
    *,
    output_path: Optional[Path],
    show: bool,
    fig_width: Optional[float] = None,
    fig_height: float = 6.0,
) -> None:
    if not rows:
        raise ValueError("没有可绘制的周期（检查 equal_weight_return_pct 与 missing_result_json）")

    labels = [r[0] for r in rows]
    vals = [r[1] for r in rows]
    colors = _bar_colors(vals)

    n = len(labels)
    if fig_width is None:
        fig_width = min(48.0, max(10.0, 0.22 * n + 4.0))

    fig, ax = plt.subplots(figsize=(fig_width, fig_height))
    x = np.arange(n)
    ax.bar(x, vals, color=colors, width=0.82, edgecolor="white", linewidth=0.4)
    ax.set_xticks(x)
    ax.set_xticklabels(labels, rotation=55, ha="right", fontsize=7)
    ax.set_ylabel("等权组合收益率 (%)")
    ax.axhline(0, color="#333333", linewidth=0.9)
    ax.grid(axis="y", linestyle="--", alpha=0.35)

    ymax, ymin = max(vals), min(vals)
    pad = max(0.5, (ymax - ymin) * 0.08 + 0.3)
    ax.set_ylim(ymin - pad, ymax + pad)

    fig.tight_layout()

    if output_path is not None:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        fig.savefig(output_path, dpi=150, bbox_inches="tight")
        print(f"已保存: {output_path.resolve()}")

    if show:
        plt.show()
    else:
        plt.close(fig)


def main() -> int:
    parser = argparse.ArgumentParser(description="滚动板块前向收益 JSON → 柱状图")
    parser.add_argument(
        "-i",
        "--input",
        type=Path,
        default=Path("data/artifacts/macro_sector_forward/result.json"),
        help="滚动结果 JSON（默认 macro 输出路径）",
    )
    parser.add_argument(
        "-o",
        "--output",
        type=Path,
        default=None,
        help="输出 PNG；默认与输入同目录 roll_period_returns.png",
    )
    parser.add_argument(
        "--no-show",
        action="store_true",
        help="不弹窗，仅保存文件",
    )
    args = parser.parse_args()

    root = _project_root()
    inp = args.input
    if not inp.is_absolute():
        inp = root / inp
    if not inp.is_file():
        print(f"文件不存在: {inp}", file=sys.stderr)
        return 1

    out_path = args.output
    if out_path is None:
        out_path = inp.parent / "roll_period_returns.png"
    elif not out_path.is_absolute():
        out_path = root / out_path

    try:
        periods, _ = load_roll_periods(inp)
        plot_rows = _extract_plot_rows(periods)
        plot_period_returns(
            plot_rows,
            output_path=out_path,
            show=not args.no_show,
        )
    except Exception as e:
        print(f"失败: {e}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
