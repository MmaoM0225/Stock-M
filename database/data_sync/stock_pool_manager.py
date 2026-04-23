"""
stock_pool_manager 主表同步
将高频查询字段 upsert 到 agent_stock_pool_manager。

运行方式:
  python -m database.data_sync.stock_pool_manager
  或
  python database/data_sync/stock_pool_manager.py
"""
import hashlib
import json
import logging
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from database import StockPoolManagerKey
from database.config import get_db_session

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parents[2]
ARTIFACT_ROOT = PROJECT_ROOT / "data" / "artifacts" / "manager" / "stock_pool_manager"


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


def _result_hash(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def _extract_top_preview(payload: Dict[str, Any], max_items: int = 20) -> str:
    rows = payload.get("top_stocks")
    if not isinstance(rows, list):
        return "[]"
    out: List[Dict[str, Any]] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        out.append(
            {
                "ts_code": row.get("ts_code"),
                "name": row.get("name"),
                "industry": row.get("industry"),
                "overall_score": row.get("overall_score"),
                "action_signal": row.get("action_signal"),
                "risk_level": row.get("risk_level"),
            }
        )
        if len(out) >= max_items:
            break
    return json.dumps(out, ensure_ascii=False, separators=(",", ":"))


def _extract_fields(payload: Dict[str, Any], result_path: Path) -> Dict[str, Any]:
    trade_date = _safe_text(payload.get("trade_date")) or _safe_text(result_path.parent.name)
    trade_date = (trade_date or "").replace("-", "")[:8]

    candidate = payload.get("candidate_stocks")
    top = payload.get("top_stocks")
    candidate_count = len(candidate) if isinstance(candidate, list) else 0
    top_count = len(top) if isinstance(top, list) else 0

    return {
        "trade_date": trade_date,
        "pool_load_error": _safe_text(payload.get("pool_load_error")),
        "pool_size": _safe_int(payload.get("pool_size")),
        "analyzed_count": _safe_int(payload.get("analyzed_count")),
        "analyze_success_count": _safe_int(payload.get("analyze_success_count")),
        "analyze_error_count": _safe_int(payload.get("analyze_error_count")),
        "summary_text": _safe_text(payload.get("summary_text")),
        "screener_artifact_path": _safe_text(payload.get("screener_artifact_path")),
        "top_stocks_preview": _extract_top_preview(payload),
        "candidate_count": candidate_count,
        "top_count": top_count,
    }


def _upsert_one(session, result_path: Path) -> bool:
    with result_path.open("r", encoding="utf-8") as f:
        payload = json.load(f)
    if not isinstance(payload, dict):
        raise ValueError(f"result.json 格式非法（非对象）: {result_path.as_posix()}")

    fields = _extract_fields(payload, result_path)
    trade_date = fields["trade_date"]
    if not trade_date:
        raise ValueError(f"缺少 trade_date: {result_path.as_posix()}")

    run_id = f"stock_pool_manager:{trade_date}"
    rel_path = result_path.relative_to(PROJECT_ROOT).as_posix()

    row = session.query(StockPoolManagerKey).filter(StockPoolManagerKey.run_id == run_id).first()
    if row is None:
        row = StockPoolManagerKey(
            run_id=run_id,
            result_path=rel_path,
            result_hash=_result_hash(result_path),
            **fields,
        )
        session.add(row)
        session.flush()
    else:
        row.trade_date = fields["trade_date"]
        row.pool_load_error = fields["pool_load_error"]
        row.pool_size = fields["pool_size"]
        row.analyzed_count = fields["analyzed_count"]
        row.analyze_success_count = fields["analyze_success_count"]
        row.analyze_error_count = fields["analyze_error_count"]
        row.summary_text = fields["summary_text"]
        row.screener_artifact_path = fields["screener_artifact_path"]
        row.top_stocks_preview = fields["top_stocks_preview"]
        row.candidate_count = fields["candidate_count"]
        row.top_count = fields["top_count"]
        row.result_path = rel_path
        row.result_hash = _result_hash(result_path)

    return True


def sync_single_result(result_path: Path) -> bool:
    """同步单个 result.json 到数据库（主表字段）。"""
    abs_path = result_path if result_path.is_absolute() else (PROJECT_ROOT / result_path)
    if not abs_path.exists():
        raise FileNotFoundError(f"result.json 不存在: {abs_path.as_posix()}")

    with get_db_session() as session:
        return _upsert_one(session, abs_path)


def sync_stock_pool_manager() -> int:
    """扫描并同步历史结果。"""
    logger.info("=" * 50)
    logger.info("开始同步 stock_pool_manager 主表")
    logger.info("=" * 50)

    if not ARTIFACT_ROOT.exists():
        logger.warning("artifact 根目录不存在: %s", ARTIFACT_ROOT.as_posix())
        return 0

    result_files = sorted(ARTIFACT_ROOT.glob("*/result.json"))
    if not result_files:
        logger.warning("未找到可同步的 result.json 文件")
        return 0

    success = 0
    with get_db_session() as session:
        for result_path in result_files:
            try:
                _upsert_one(session, result_path)
                success += 1
            except Exception as e:
                logger.warning("同步失败 %s: %s", result_path.as_posix(), e)

    logger.info("同步完成，共写入/更新 %s 条", success)
    return success


if __name__ == "__main__":
    sync_stock_pool_manager()
