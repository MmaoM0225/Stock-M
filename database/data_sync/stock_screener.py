"""
stock_screener 主表同步
将筛选结果摘要与统计字段 upsert 到 agent_stock_screener。

运行方式:
  python -m database.data_sync.stock_screener
  或
  python database/data_sync/stock_screener.py
"""
import hashlib
import json
import logging
import sys
from pathlib import Path
from typing import Any, Dict, Optional

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from database import StockScreenerKey
from database.config import get_db_session

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parents[2]
ARTIFACT_ROOT = (
    PROJECT_ROOT
    / "data"
    / "artifacts"
    / "analyst"
    / "stock_analyst"
    / "stock_screener"
)


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


def _json_text(value: Any) -> str:
    if isinstance(value, (dict, list)):
        return json.dumps(value, ensure_ascii=False, separators=(",", ":"))
    return "[]" if isinstance(value, list) else "{}"


def _result_hash(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def _extract_preview_codes(payload: Dict[str, Any], max_items: int = 20) -> str:
    rows = payload.get("filtered_stocks")
    if not isinstance(rows, list):
        return "[]"
    out = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        code = row.get("ts_code")
        if code is None:
            continue
        out.append(str(code))
        if len(out) >= max_items:
            break
    return json.dumps(out, ensure_ascii=False, separators=(",", ":"))


def _extract_fields(payload: Dict[str, Any], result_path: Path) -> Dict[str, Any]:
    trade_date = _safe_text(payload.get("trade_date")) or _safe_text(result_path.parent.name)
    trade_date = (trade_date or "").replace("-", "")[:8]
    sector_dist = payload.get("sector_distribution")
    if not isinstance(sector_dist, dict):
        sector_dist = {}
    return {
        "trade_date": trade_date,
        "total_count": _safe_int(payload.get("total_count")),
        "filter_summary": _safe_text(payload.get("filter_summary")),
        "applied_filters": _json_text(payload.get("applied_filters") if isinstance(payload.get("applied_filters"), list) else []),
        "sector_distribution": json.dumps(sector_dist, ensure_ascii=False, separators=(",", ":")),
        "sector_template_applied": _json_text(
            payload.get("sector_template_applied") if isinstance(payload.get("sector_template_applied"), dict) else {}
        ),
        "sector_pick_counts": _json_text(
            payload.get("sector_pick_counts") if isinstance(payload.get("sector_pick_counts"), dict) else {}
        ),
        "filtered_preview": _extract_preview_codes(payload),
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

    run_id = f"stock_screener:{trade_date}"
    rel_path = result_path.relative_to(PROJECT_ROOT).as_posix()

    row = session.query(StockScreenerKey).filter(StockScreenerKey.run_id == run_id).first()
    if row is None:
        row = StockScreenerKey(
            run_id=run_id,
            result_path=rel_path,
            result_hash=_result_hash(result_path),
            **fields,
        )
        session.add(row)
        session.flush()
    else:
        row.trade_date = fields["trade_date"]
        row.total_count = fields["total_count"]
        row.filter_summary = fields["filter_summary"]
        row.applied_filters = fields["applied_filters"]
        row.sector_distribution = fields["sector_distribution"]
        row.sector_template_applied = fields["sector_template_applied"]
        row.sector_pick_counts = fields["sector_pick_counts"]
        row.filtered_preview = fields["filtered_preview"]
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


def sync_stock_screener() -> int:
    """扫描并同步历史结果。"""
    logger.info("=" * 50)
    logger.info("开始同步 stock_screener 主表")
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
    sync_stock_screener()
