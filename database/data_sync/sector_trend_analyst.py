"""
sector_trend_analyst 同步
将指定键值分列存入数据库（不保存全量 result.json）。

运行方式:
  python -m database.data_sync.sector_trend_analyst
  或
  python database/data_sync/sector_trend_analyst.py
"""
import hashlib
import json
import logging
import sys
from pathlib import Path
from typing import Any, Dict, Optional

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from database import SectorTrendAnalystKey
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
    / "sector_analyst"
    / "sector_trend_analyst"
)


def _safe_text(value: Any) -> Optional[str]:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _list_to_json_text(value: Any) -> str:
    if isinstance(value, list):
        return json.dumps(value, ensure_ascii=False, separators=(",", ":"))
    return "[]"


def _result_hash(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def _extract_fields(payload: Dict[str, Any]) -> Dict[str, Any]:
    leading = payload.get("leading_themes")
    reversal = payload.get("reversal_opportunities")
    risk = payload.get("top_risk_sectors")
    highlights = payload.get("highlights")
    return {
        "summary": _safe_text(payload.get("summary")),
        "conclusion": _safe_text(payload.get("conclusion")),
        "highlights": _list_to_json_text(highlights),
        "market_regime": _safe_text(payload.get("market_regime")),
        "leading_themes": _list_to_json_text(leading),
        "reversal_opportunities": _list_to_json_text(reversal),
        "top_risk_sectors": _list_to_json_text(risk),
        "leading_themes_count": len(leading) if isinstance(leading, list) else 0,
        "reversal_opportunities_count": len(reversal) if isinstance(reversal, list) else 0,
        "top_risk_sectors_count": len(risk) if isinstance(risk, list) else 0,
    }


def _upsert_one(session, result_path: Path) -> bool:
    trade_date = result_path.parent.name
    run_id = f"sector_trend_analyst:{trade_date}"
    rel_path = result_path.relative_to(PROJECT_ROOT).as_posix()

    with result_path.open("r", encoding="utf-8") as f:
        payload = json.load(f)
    if not isinstance(payload, dict):
        raise ValueError(f"result.json 格式非法（非对象）: {rel_path}")

    fields = _extract_fields(payload)
    row = session.query(SectorTrendAnalystKey).filter(SectorTrendAnalystKey.run_id == run_id).first()
    if row is None:
        row = SectorTrendAnalystKey(
            run_id=run_id,
            trade_date=trade_date,
            result_path=rel_path,
            result_hash=_result_hash(result_path),
            **fields,
        )
        session.add(row)
    else:
        row.trade_date = trade_date
        row.result_path = rel_path
        row.result_hash = _result_hash(result_path)
        row.summary = fields["summary"]
        row.conclusion = fields["conclusion"]
        row.highlights = fields["highlights"]
        row.market_regime = fields["market_regime"]
        row.leading_themes = fields["leading_themes"]
        row.reversal_opportunities = fields["reversal_opportunities"]
        row.top_risk_sectors = fields["top_risk_sectors"]
        row.leading_themes_count = fields["leading_themes_count"]
        row.reversal_opportunities_count = fields["reversal_opportunities_count"]
        row.top_risk_sectors_count = fields["top_risk_sectors_count"]

    return True


def sync_single_result(result_path: Path) -> bool:
    """同步单个 result.json 到数据库（指定键值）。"""
    abs_path = result_path if result_path.is_absolute() else (PROJECT_ROOT / result_path)
    if not abs_path.exists():
        raise FileNotFoundError(f"result.json 不存在: {abs_path.as_posix()}")

    with get_db_session() as session:
        return _upsert_one(session, abs_path)


def sync_sector_trend_analyst() -> int:
    """扫描并同步历史结果。"""
    logger.info("=" * 50)
    logger.info("开始同步 sector_trend_analyst（分列键值）")
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
    sync_sector_trend_analyst()
