"""
Stock Pool Manager（批量个股池经理）- 节点

load_screener_pool：读取 stock_screener 的 result.json，得到待分析股票列表。
run_stock_pool：对每只股票调用已编译的 stock_manager 子图（基本面+技术面+单票汇总）。
pool_reduce：按综合分排序，生成 candidate_stocks / top_stocks。
persist：写入 data/artifacts/manager/stock_pool_manager/<trade_date>/。
"""
from __future__ import annotations

import json
import logging
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List

logger = logging.getLogger(__name__)

_STOCK_SCREENER_ARTIFACT_ROOT = Path("data") / "artifacts" / "analyst" / "stock_analyst" / "stock_screener"
_STOCK_POOL_MANAGER_ARTIFACT_ROOT = Path("data") / "artifacts" / "manager" / "stock_pool_manager"


def _write_json_atomic(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_suffix(path.suffix + ".tmp")
    with open(tmp_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
    os.replace(tmp_path, path)


def create_load_screener_pool_node():
    """从本地 stock_screener artifact 加载 filtered_stocks。"""

    def load_screener_pool_node(state: Dict[str, Any]) -> Dict[str, Any]:
        trade_date = str(state.get("trade_date") or "").replace("-", "")[:8]
        if not trade_date:
            return {
                **state,
                "pool_stocks": [],
                "pool_load_error": "缺少 trade_date",
                "screener_artifact_path": None,
            }

        path_str = state.get("screener_result_path")
        if path_str:
            path = Path(str(path_str))
        else:
            path = _STOCK_SCREENER_ARTIFACT_ROOT / trade_date / "result.json"

        if not path.exists():
            return {
                **state,
                "pool_stocks": [],
                "pool_load_error": f"未找到筛选结果: {path.as_posix()}",
                "screener_artifact_path": path.as_posix(),
            }

        try:
            with open(path, "r", encoding="utf-8") as f:
                data = json.load(f) or {}
        except Exception as e:
            logger.exception("读取 screener result 失败")
            return {
                **state,
                "pool_stocks": [],
                "pool_load_error": f"读取 screener 失败: {e}",
                "screener_artifact_path": path.as_posix(),
            }

        filtered = list(data.get("filtered_stocks") or [])
        max_n = state.get("max_stocks")
        if max_n is not None:
            try:
                n = int(max_n)
                if n > 0:
                    filtered = filtered[:n]
            except (TypeError, ValueError):
                pass

        pool_stocks: List[Dict[str, Any]] = []
        for row in filtered:
            if not isinstance(row, dict):
                continue
            tc = str(row.get("ts_code") or "").strip()
            if tc:
                pool_stocks.append(dict(row))

        return {
            **state,
            "pool_stocks": pool_stocks,
            "pool_load_error": None,
            "screener_artifact_path": path.as_posix(),
        }

    return load_screener_pool_node


def _analyze_one_stock(
    stock_manager_graph: Any,
    row: Dict[str, Any],
    trade_date: str,
) -> Dict[str, Any]:
    ts_code = str(row.get("ts_code") or "").strip()
    base = {
        "ts_code": ts_code,
        "name": row.get("name"),
        "industry": row.get("industry"),
        "screener_row": {k: v for k, v in row.items() if k in (
            "ts_code", "name", "industry", "close", "pe", "pe_ttm", "pb",
            "total_mv", "circ_mv", "turnover_rate", "volume_ratio",
        )},
    }
    try:
        invoke_input: Dict[str, Any] = {"ts_code": ts_code, "trade_date": trade_date}
        result = stock_manager_graph.invoke(invoke_input)
        return {
            **base,
            "fundamental_reduce_result": result.get("fundamental_reduce_result"),
            "technical_analysis": result.get("technical_analysis"),
            "stock_manager_summary": result.get("stock_manager_summary"),
            "error": None,
        }
    except Exception as e:
        logger.exception("stock_pool 单票分析失败 ts_code=%s", ts_code)
        return {
            **base,
            "fundamental_reduce_result": None,
            "technical_analysis": None,
            "stock_manager_summary": None,
            "error": str(e),
        }


def create_run_stock_pool_node(stock_manager_graph: Any, max_concurrent_stocks: int = 3):
    """
    并行对 pool_stocks 中每只股票调用 stock_manager（含基本面+技术面+单票 LLM 汇总）。
    结果顺序与 pool_stocks 一致。
    """

    def run_stock_pool_node(state: Dict[str, Any]) -> Dict[str, Any]:
        if state.get("pool_load_error"):
            return {**state, "stock_analyses": []}

        pool = list(state.get("pool_stocks") or [])
        trade_date = str(state.get("trade_date") or "").replace("-", "")[:8]
        if not pool:
            return {**state, "stock_analyses": []}

        workers = max(1, int(max_concurrent_stocks))
        results_by_code: Dict[str, Dict[str, Any]] = {}

        with ThreadPoolExecutor(max_workers=workers) as executor:
            future_map = {
                executor.submit(
                    _analyze_one_stock, stock_manager_graph, row, trade_date
                ): row.get("ts_code")
                for row in pool
            }
            for fut in as_completed(future_map):
                try:
                    item = fut.result()
                    tc = item.get("ts_code")
                    if tc:
                        results_by_code[str(tc)] = item
                except Exception as e:
                    logger.exception("stock_pool future 异常: %s", e)

        ordered: List[Dict[str, Any]] = []
        for row in pool:
            tc = str(row.get("ts_code") or "").strip()
            if tc in results_by_code:
                ordered.append(results_by_code[tc])

        return {**state, "stock_analyses": ordered}

    return run_stock_pool_node


def create_pool_reduce_node():
    """聚合排序，生成 candidate_stocks / top_stocks 与简短说明。"""

    def pool_reduce_node(state: Dict[str, Any]) -> Dict[str, Any]:
        trade_date = str(state.get("trade_date") or "").replace("-", "")[:8]
        analyses = list(state.get("stock_analyses") or [])
        pool_err = state.get("pool_load_error")

        candidate_stocks: List[Dict[str, Any]] = []
        for a in analyses:
            sm = a.get("stock_manager_summary") if isinstance(a.get("stock_manager_summary"), dict) else {}
            err = a.get("error")
            candidate_stocks.append(
                {
                    "ts_code": a.get("ts_code"),
                    "name": a.get("name"),
                    "industry": a.get("industry"),
                    "overall_score": sm.get("overall_score"),
                    "action_signal": sm.get("action_signal"),
                    "risk_level": sm.get("risk_level"),
                    "selection_reason": sm.get("selection_reason"),
                    "analyze_error": err,
                }
            )

        def _cand_sort_key(c: Dict[str, Any]) -> float:
            raw = c.get("overall_score")
            try:
                return float(raw)
            except (TypeError, ValueError):
                return float("-inf")

        candidate_stocks.sort(key=_cand_sort_key, reverse=True)
        top_stocks = candidate_stocks[:10]

        ok = sum(1 for a in analyses if not a.get("error"))
        err_n = len(analyses) - ok
        if pool_err:
            summary_text = f"未执行分析：{pool_err}"
        else:
            summary_text = (
                f"交易日 {trade_date}，自筛选池共 {len(state.get('pool_stocks') or [])} 只，"
                f"完成分析 {ok} 只，失败 {err_n} 只；已按综合分排序并给出前十关注列表。"
            )

        stock_pool_manager_result: Dict[str, Any] = {
            "trade_date": trade_date,
            "screener_artifact_path": state.get("screener_artifact_path"),
            "pool_load_error": pool_err,
            "pool_size": len(state.get("pool_stocks") or []),
            "analyzed_count": len(analyses),
            "analyze_success_count": ok,
            "analyze_error_count": err_n,
            "summary_text": summary_text,
            "candidate_stocks": candidate_stocks,
            "top_stocks": top_stocks,
            "per_stock": analyses,
        }

        return {**state, "stock_pool_manager_result": stock_pool_manager_result}

    return pool_reduce_node


def create_persist_stock_pool_manager_node():
    """将 stock_pool_manager_result 写入 artifacts。"""

    def persist_node(state: Dict[str, Any]) -> Dict[str, Any]:
        payload = state.get("stock_pool_manager_result")
        if not payload:
            return state

        trade_date = str(
            payload.get("trade_date") or state.get("trade_date") or datetime.now().strftime("%Y%m%d")
        ).replace("-", "")[:8]
        artifact_dir = _STOCK_POOL_MANAGER_ARTIFACT_ROOT / trade_date
        result_path = artifact_dir / "result.json"
        manifest_path = artifact_dir / "manifest.json"

        try:
            _write_json_atomic(result_path, payload)
            _write_json_atomic(
                manifest_path,
                {
                    "artifact_type": "stock_pool_manager_result",
                    "module": "agents.manager.stock_pool_manager",
                    "trade_date": trade_date,
                    "created_at": datetime.now().astimezone().isoformat(),
                    "status": "success",
                    "result_path": result_path.as_posix(),
                    "screener_artifact_path": payload.get("screener_artifact_path"),
                },
            )
            logger.info("stock_pool_manager 已写入: %s", result_path)
            return {
                **state,
                "stock_pool_manager_artifact_path": result_path.as_posix(),
                "stock_pool_manager_manifest_path": manifest_path.as_posix(),
            }
        except Exception as e:
            logger.warning("写入 stock_pool_manager artifacts 失败: %s", e)
            return state

    return persist_node


__all__ = [
    "create_load_screener_pool_node",
    "create_run_stock_pool_node",
    "create_pool_reduce_node",
    "create_persist_stock_pool_manager_node",
]
