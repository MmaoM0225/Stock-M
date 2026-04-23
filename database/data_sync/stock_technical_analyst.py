"""
stock_technical_analyst 主表同步
将高频查询字段 upsert 到 agent_stock_technical_analyst。

运行方式:
  python -m database.data_sync.stock_technical_analyst
  或
  python database/data_sync/stock_technical_analyst.py
"""
import hashlib
import json
import logging
import sys
from pathlib import Path
from typing import Any, Dict, Optional

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from database import StockTechnicalAnalystKey
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
    / "stock_technical_analyst"
)


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


def _result_hash(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def _derive_recommendation(analysis: Dict[str, Any]) -> Optional[str]:
    direct = _safe_text(analysis.get("recommendation"))
    if direct:
        return direct
    signal = _safe_text(analysis.get("trend_signal"))
    mapping = {"uptrend": "偏多", "downtrend": "偏空", "range": "震荡", "unknown": "未知"}
    return mapping.get(signal, signal)


def _extract_fields(payload: Dict[str, Any], result_path: Path) -> Dict[str, Any]:
    ts_code = _safe_text(payload.get("ts_code")) or _safe_text(result_path.parent.parent.name)
    trade_date = _safe_text(payload.get("trade_date")) or _safe_text(result_path.parent.name)
    trade_date = (trade_date or "").replace("-", "")[:8]

    meta = payload.get("stock_technical_meta")
    if not isinstance(meta, dict):
        meta = {}
    analysis = payload.get("technical_analysis")
    if not isinstance(analysis, dict):
        analysis = {}

    supports = analysis.get("support_levels")
    resistances = analysis.get("resistance_levels")
    return {
        "ts_code": ts_code,
        "trade_date": trade_date,
        "technical_score": _safe_float(analysis.get("technical_score")),
        "trend_signal": _safe_text(analysis.get("trend_signal")),
        "trend_strength": _safe_text(analysis.get("trend_strength")),
        "recommendation": _derive_recommendation(analysis),
        "summary": _safe_text(analysis.get("summary")),
        "fetch_complete_success": 1 if bool(meta.get("kline_ready", False)) else 0,
        "support_levels_count": len(supports) if isinstance(supports, list) else 0,
        "resistance_levels_count": len(resistances) if isinstance(resistances, list) else 0,
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

    run_id = f"stock_technical_analyst:{ts_code}:{trade_date}"
    rel_path = result_path.relative_to(PROJECT_ROOT).as_posix()

    row = session.query(StockTechnicalAnalystKey).filter(StockTechnicalAnalystKey.run_id == run_id).first()
    if row is None:
        row = StockTechnicalAnalystKey(
            run_id=run_id,
            result_path=rel_path,
            result_hash=_result_hash(result_path),
            **fields,
        )
        session.add(row)
        # 关键：在 autoflush=False 下，显式 flush 让后续同 run_id 查询可见，
        # 避免同一批次中重复 INSERT 触发 UNIQUE 约束。
        session.flush()
    else:
        row.ts_code = fields["ts_code"]
        row.trade_date = fields["trade_date"]
        row.technical_score = fields["technical_score"]
        row.trend_signal = fields["trend_signal"]
        row.trend_strength = fields["trend_strength"]
        row.recommendation = fields["recommendation"]
        row.summary = fields["summary"]
        row.fetch_complete_success = fields["fetch_complete_success"]
        row.support_levels_count = fields["support_levels_count"]
        row.resistance_levels_count = fields["resistance_levels_count"]
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


def sync_stock_technical_analyst() -> int:
    """扫描并同步历史结果。"""
    logger.info("=" * 50)
    logger.info("开始同步 stock_technical_analyst 主表")
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
    sync_stock_technical_analyst()
