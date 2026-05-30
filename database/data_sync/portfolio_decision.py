"""
portfolio_decision 主表同步
将高频查询字段 upsert 到 agent_portfolio_decision。

运行方式:
  python -m database.data_sync.portfolio_decision
  或
  python database/data_sync/portfolio_decision.py
"""
import hashlib
import json
import logging
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from database import PortfolioDecisionKey
from database.config import get_db_session

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parents[2]
ARTIFACT_ROOT = PROJECT_ROOT / "data" / "artifacts" / "decision"


def _safe_text(value: Any) -> Optional[str]:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _safe_int(value: Any) -> Optional[int]:
    try:
        if value is None or value == "":
            return None
        return int(value)
    except (TypeError, ValueError):
        return None


def _safe_float(value: Any) -> Optional[float]:
    try:
        if value is None or value == "":
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _result_hash(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def _pct_to_float(value: Any) -> float:
    if value is None:
        return 0.0
    text = str(value).strip().replace("%", "").replace("+", "")
    try:
        return float(text) / 100.0
    except (TypeError, ValueError):
        return 0.0


def _extract_portfolio_preview(portfolio_rows: List[Dict[str, Any]], max_items: int = 20) -> str:
    out: List[Dict[str, Any]] = []
    for row in portfolio_rows:
        if not isinstance(row, dict):
            continue
        if str(row.get("资产名称") or "") == "待投资现金":
            continue
        out.append(
            {
                "排名": row.get("排名"),
                "资产名称": row.get("资产名称"),
                "ts_code": row.get("ts_code"),
                "仓位": row.get("仓位"),
                "操作": row.get("操作"),
                "行业/板块": row.get("行业/板块"),
                "市值 (元)": row.get("市值 (元)"),
            }
        )
        if len(out) >= max_items:
            break
    return json.dumps(out, ensure_ascii=False, separators=(",", ":"))


def _extract_fields(payload: Dict[str, Any], result_path: Path) -> Dict[str, Any]:
    trade_date = _safe_text(payload.get("trade_date")) or _safe_text(result_path.parent.name)
    trade_date = (trade_date or "").replace("-", "")[:8]

    meta = payload.get("meta")
    if not isinstance(meta, dict):
        meta = {}
    warnings = meta.get("warnings")
    if not isinstance(warnings, list):
        warnings = []

    portfolio_table = payload.get("portfolio_table")
    if not isinstance(portfolio_table, list):
        portfolio_table = []
    reason_table = payload.get("operation_reason_table")
    if not isinstance(reason_table, list):
        reason_table = []

    stock_rows = [r for r in portfolio_table if isinstance(r, dict) and str(r.get("资产名称") or "") != "待投资现金"]
    cash_row = next((r for r in portfolio_table if isinstance(r, dict) and str(r.get("资产名称") or "") == "待投资现金"), None)

    buy_count = 0
    add_count = 0
    reduce_count = 0
    clear_count = 0
    hold_count = 0
    for row in reason_table:
        if not isinstance(row, dict):
            continue
        op = str(row.get("操作") or "").strip()
        if op == "建仓":
            buy_count += 1
        elif op == "加仓":
            add_count += 1
        elif op == "减仓":
            reduce_count += 1
        elif op == "清仓":
            clear_count += 1
        elif op == "持有":
            hold_count += 1

    return {
        "trade_date": trade_date,
        "total_capital": _safe_float(meta.get("total_capital")),
        "stock_count": len(stock_rows),
        "cash_amount": _safe_float(cash_row.get("市值 (元)") if isinstance(cash_row, dict) else None),
        "cash_weight": _pct_to_float(cash_row.get("仓位") if isinstance(cash_row, dict) else None),
        "operation_count": len(reason_table),
        "buy_count": buy_count,
        "add_count": add_count,
        "reduce_count": reduce_count,
        "clear_count": clear_count,
        "hold_count": hold_count,
        "warning_count": len(warnings),
        "decision_summary": _safe_text(payload.get("decision_summary")),
        "llm_reasoning": _safe_text(meta.get("llm_reasoning")),
        "portfolio_preview": _extract_portfolio_preview(portfolio_table),
    }


def _extract_version_from_path(result_path: Path) -> str:
    """
    从路径推断 decision 版本:
    - data/artifacts/decision/<version>/portfolio/<trade_date>/result.json -> <version>
    - data/artifacts/decision/portfolio/<trade_date>/result.json -> default
    """
    try:
        rel_parts = result_path.relative_to(PROJECT_ROOT).parts
    except Exception:
        rel_parts = result_path.parts

    if "decision" not in rel_parts:
        return "default"
    idx = rel_parts.index("decision")
    if idx + 1 >= len(rel_parts):
        return "default"
    first = rel_parts[idx + 1]
    if first == "portfolio":
        return "default"
    return str(first)


def _upsert_one(session, result_path: Path) -> bool:
    with result_path.open("r", encoding="utf-8") as f:
        payload = json.load(f)
    if not isinstance(payload, dict):
        raise ValueError(f"result.json 格式非法（非对象）: {result_path.as_posix()}")

    fields = _extract_fields(payload, result_path)
    trade_date = fields["trade_date"]
    if not trade_date:
        raise ValueError(f"缺少 trade_date: {result_path.as_posix()}")

    version = _extract_version_from_path(result_path)
    run_id = f"portfolio_decision:{version}:{trade_date}"
    rel_path = result_path.relative_to(PROJECT_ROOT).as_posix()

    row = session.query(PortfolioDecisionKey).filter(PortfolioDecisionKey.run_id == run_id).first()
    if row is None:
        row = PortfolioDecisionKey(
            run_id=run_id,
            result_path=rel_path,
            result_hash=_result_hash(result_path),
            **fields,
        )
        session.add(row)
        session.flush()
    else:
        row.trade_date = fields["trade_date"]
        row.total_capital = fields["total_capital"]
        row.stock_count = fields["stock_count"]
        row.cash_amount = fields["cash_amount"]
        row.cash_weight = fields["cash_weight"]
        row.operation_count = fields["operation_count"]
        row.buy_count = fields["buy_count"]
        row.add_count = fields["add_count"]
        row.reduce_count = fields["reduce_count"]
        row.clear_count = fields["clear_count"]
        row.hold_count = fields["hold_count"]
        row.warning_count = fields["warning_count"]
        row.decision_summary = fields["decision_summary"]
        row.llm_reasoning = fields["llm_reasoning"]
        row.portfolio_preview = fields["portfolio_preview"]
        row.result_path = rel_path
        row.result_hash = _result_hash(result_path)

    return True


def _cleanup_missing_records(session) -> int:
    """清理数据库中已无对应 result.json 文件的记录。"""
    removed = 0
    rows = session.query(PortfolioDecisionKey).all()
    for row in rows:
        run_id = _safe_text(row.run_id) or ""
        # 仅处理本同步脚本管理的数据，避免误删其他业务记录。
        if not run_id.startswith("portfolio_decision:"):
            continue

        rel_path = _safe_text(row.result_path)
        if not rel_path:
            session.delete(row)
            removed += 1
            continue

        abs_result_path = PROJECT_ROOT / rel_path
        if not abs_result_path.exists():
            session.delete(row)
            removed += 1
    return removed


def sync_single_result(result_path: Path) -> bool:
    """同步单个 result.json 到数据库（主表字段）。"""
    abs_path = result_path if result_path.is_absolute() else (PROJECT_ROOT / result_path)
    if not abs_path.exists():
        raise FileNotFoundError(f"result.json 不存在: {abs_path.as_posix()}")

    with get_db_session() as session:
        return _upsert_one(session, abs_path)


def sync_portfolio_decision() -> int:
    """扫描并同步历史结果。"""
    logger.info("=" * 50)
    logger.info("开始同步 portfolio_decision 主表")
    logger.info("=" * 50)

    if not ARTIFACT_ROOT.exists():
        logger.warning("artifact 根目录不存在: %s", ARTIFACT_ROOT.as_posix())
        return 0

    # 同时兼容：
    # 1) data/artifacts/decision/<version>/portfolio/<trade_date>/result.json
    # 2) data/artifacts/decision/portfolio/<trade_date>/result.json（旧结构）
    result_paths = set(ARTIFACT_ROOT.glob("*/portfolio/*/result.json"))
    result_paths.update(set(ARTIFACT_ROOT.glob("portfolio/*/result.json")))
    result_files = sorted(result_paths)
    if not result_files:
        logger.warning("未找到可同步的 result.json 文件")
        return 0

    success = 0
    removed = 0
    with get_db_session() as session:
        for result_path in result_files:
            try:
                _upsert_one(session, result_path)
                success += 1
            except Exception as e:
                logger.warning("同步失败 %s: %s", result_path.as_posix(), e)
        removed = _cleanup_missing_records(session)

    logger.info("同步完成，共写入/更新 %s 条", success)
    if removed:
        logger.info("已清理缺失文件对应的数据库记录 %s 条", removed)
    return success


if __name__ == "__main__":
    sync_portfolio_decision()
