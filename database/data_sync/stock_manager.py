"""
stock_manager 主表同步
将高频查询字段 upsert 到 agent_stock_manager。

运行方式:
  python -m database.data_sync.stock_manager
  或
  python database/data_sync/stock_manager.py
"""
import hashlib
import json
import logging
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from database import StockManagerKey
from database.config import get_db_session

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parents[2]
ARTIFACT_ROOT = PROJECT_ROOT / "data" / "artifacts" / "manager" / "stock_manager"


def _safe_text(value: Any) -> Optional[str]:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _safe_float(value: Any) -> Optional[float]:
    try:
        if value is None or value == "":
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _safe_list(value: Any) -> List[str]:
    if not isinstance(value, list):
        return []
    out: List[str] = []
    for item in value:
        if item is None:
            continue
        text = str(item).strip()
        if text:
            out.append(text)
    return out


def _result_hash(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def _extract_fields(payload: Dict[str, Any], result_path: Path) -> Dict[str, Any]:
    ts_code = _safe_text(payload.get("ts_code")) or _safe_text(result_path.parent.parent.name)
    trade_date = _safe_text(payload.get("trade_date")) or _safe_text(result_path.parent.name)
    trade_date = (trade_date or "").replace("-", "")[:8]

    summary = payload.get("stock_manager_summary")
    if not isinstance(summary, dict):
        summary = {}
    component = summary.get("component_scores")
    if not isinstance(component, dict):
        component = {}

    key_points = _safe_list(summary.get("key_points"))
    risks = _safe_list(summary.get("risks"))

    return {
        "ts_code": ts_code,
        "trade_date": trade_date,
        "success": 1 if bool(summary.get("success", False)) else 0,
        "overall_score": _safe_float(summary.get("overall_score")),
        "confidence": _safe_text(summary.get("confidence")),
        "risk_level": _safe_text(summary.get("risk_level")),
        "action_signal": _safe_text(summary.get("action_signal")),
        "selection_reason": _safe_text(summary.get("selection_reason")),
        "signal_reason": _safe_text(summary.get("signal_reason")),
        "summary": _safe_text(summary.get("summary")),
        "fundamental_score": _safe_float(component.get("fundamental")),
        "technical_score": _safe_float(component.get("technical")),
        "key_points": json.dumps(key_points, ensure_ascii=False, separators=(",", ":")),
        "risks": json.dumps(risks, ensure_ascii=False, separators=(",", ":")),
        "key_points_count": len(key_points),
        "risks_count": len(risks),
    }


def _upsert_one(session, result_path: Path) -> bool:
    with result_path.open("r", encoding="utf-8") as f:
        payload = json.load(f)
    if not isinstance(payload, dict):
        raise ValueError(f"result.json 格式非法（非对象）: {result_path.as_posix()}")

    fields = _extract_fields(payload, result_path)
    ts_code = fields["ts_code"]
    trade_date = fields["trade_date"]
    if not ts_code or not trade_date:
        raise ValueError(f"缺少 ts_code/trade_date: {result_path.as_posix()}")

    run_id = f"stock_manager:{ts_code}:{trade_date}"
    rel_path = result_path.relative_to(PROJECT_ROOT).as_posix()

    row = session.query(StockManagerKey).filter(StockManagerKey.run_id == run_id).first()
    if row is None:
        row = StockManagerKey(
            run_id=run_id,
            result_path=rel_path,
            result_hash=_result_hash(result_path),
            **fields,
        )
        session.add(row)
        session.flush()
    else:
        row.ts_code = fields["ts_code"]
        row.trade_date = fields["trade_date"]
        row.success = fields["success"]
        row.overall_score = fields["overall_score"]
        row.confidence = fields["confidence"]
        row.risk_level = fields["risk_level"]
        row.action_signal = fields["action_signal"]
        row.selection_reason = fields["selection_reason"]
        row.signal_reason = fields["signal_reason"]
        row.summary = fields["summary"]
        row.fundamental_score = fields["fundamental_score"]
        row.technical_score = fields["technical_score"]
        row.key_points = fields["key_points"]
        row.risks = fields["risks"]
        row.key_points_count = fields["key_points_count"]
        row.risks_count = fields["risks_count"]
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


def sync_stock_manager() -> int:
    """扫描并同步历史结果。"""
    logger.info("=" * 50)
    logger.info("开始同步 stock_manager 主表")
    logger.info("=" * 50)

    if not ARTIFACT_ROOT.exists():
        logger.warning("artifact 根目录不存在: %s", ARTIFACT_ROOT.as_posix())
        return 0

    result_files = sorted(ARTIFACT_ROOT.glob("*/*/result.json"))
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
    sync_stock_manager()
