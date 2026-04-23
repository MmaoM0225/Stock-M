"""
sector_manager 主表同步
将高频查询字段 upsert 到 agent_sector_manager。

运行方式:
  python -m database.data_sync.sector_manager
  或
  python database/data_sync/sector_manager.py
"""
import hashlib
import json
import logging
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from database import SectorManagerKey
from database.config import get_db_session

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parents[2]
ARTIFACT_ROOT = PROJECT_ROOT / "data" / "artifacts" / "manager" / "sector_manager"


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
    trade_date = _safe_text(payload.get("trade_date")) or _safe_text(result_path.parent.name)
    trade_date = (trade_date or "").replace("-", "")[:8]

    favored = _safe_list(payload.get("favored_sectors"))
    watchlist = _safe_list(payload.get("watchlist_sectors"))
    risk = _safe_list(payload.get("risk_sectors"))
    core_signals = _safe_list(payload.get("core_signals"))

    return {
        "trade_date": trade_date,
        "market_regime": _safe_text(payload.get("market_regime")),
        "market_bias": _safe_text(payload.get("market_bias")),
        "action_bias": _safe_text(payload.get("action_bias")),
        "confidence": _safe_float(payload.get("confidence")),
        "sector_summary": _safe_text(payload.get("sector_summary")),
        "favored_sectors": json.dumps(favored, ensure_ascii=False, separators=(",", ":")),
        "watchlist_sectors": json.dumps(watchlist, ensure_ascii=False, separators=(",", ":")),
        "risk_sectors": json.dumps(risk, ensure_ascii=False, separators=(",", ":")),
        "core_signals": json.dumps(core_signals, ensure_ascii=False, separators=(",", ":")),
        "favored_count": len(favored),
        "watchlist_count": len(watchlist),
        "risk_count": len(risk),
        "core_signals_count": len(core_signals),
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

    run_id = f"sector_manager:{trade_date}"
    rel_path = result_path.relative_to(PROJECT_ROOT).as_posix()

    row = session.query(SectorManagerKey).filter(SectorManagerKey.run_id == run_id).first()
    if row is None:
        row = SectorManagerKey(
            run_id=run_id,
            result_path=rel_path,
            result_hash=_result_hash(result_path),
            **fields,
        )
        session.add(row)
        session.flush()
    else:
        row.trade_date = fields["trade_date"]
        row.market_regime = fields["market_regime"]
        row.market_bias = fields["market_bias"]
        row.action_bias = fields["action_bias"]
        row.confidence = fields["confidence"]
        row.sector_summary = fields["sector_summary"]
        row.favored_sectors = fields["favored_sectors"]
        row.watchlist_sectors = fields["watchlist_sectors"]
        row.risk_sectors = fields["risk_sectors"]
        row.core_signals = fields["core_signals"]
        row.favored_count = fields["favored_count"]
        row.watchlist_count = fields["watchlist_count"]
        row.risk_count = fields["risk_count"]
        row.core_signals_count = fields["core_signals_count"]
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


def sync_sector_manager() -> int:
    """扫描并同步历史结果。"""
    logger.info("=" * 50)
    logger.info("开始同步 sector_manager 主表")
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
    sync_sector_manager()
