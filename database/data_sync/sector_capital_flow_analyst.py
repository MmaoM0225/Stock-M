"""
sector_capital_flow_analyst 同步
仅将指定键值分列存入数据库（不保存全量 result.json）。

运行方式:
  python -m database.data_sync.sector_capital_flow_analyst
  或
  python database/data_sync/sector_capital_flow_analyst.py
"""
import hashlib
import json
import logging
import sys
from pathlib import Path
from typing import Any, Dict, Optional

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from sqlalchemy import text

from database import SectorCapitalFlowAnalystKey
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
    / "sector_capital_flow_analyst"
)


def _ensure_table_columns(session) -> None:
    """
    为已存在的表补齐新增列（兼容 SQLite 下 create_all 不会自动 alter）。
    """
    table = "agent_sector_capital_flow_analyst"
    rows = session.execute(text(f"PRAGMA table_info({table})")).fetchall()
    existing = {str(r[1]) for r in rows if len(r) > 1}
    alter_sql = {
        "summary": "ALTER TABLE agent_sector_capital_flow_analyst ADD COLUMN summary TEXT",
        "conclusion": "ALTER TABLE agent_sector_capital_flow_analyst ADD COLUMN conclusion TEXT",
        "highlights": "ALTER TABLE agent_sector_capital_flow_analyst ADD COLUMN highlights TEXT",
        "hot_sectors": "ALTER TABLE agent_sector_capital_flow_analyst ADD COLUMN hot_sectors TEXT",
        "risk_sectors": "ALTER TABLE agent_sector_capital_flow_analyst ADD COLUMN risk_sectors TEXT",
    }
    for col, sql in alter_sql.items():
        if col not in existing:
            session.execute(text(sql))
    session.flush()


def _safe_text(value: Any) -> Optional[str]:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _result_hash(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def _extract_fields(payload: Dict[str, Any]) -> Dict[str, Any]:
    hot = payload.get("hot_sectors")
    risk = payload.get("risk_sectors")
    return {
        "hot_sectors_count": len(hot) if isinstance(hot, list) else 0,
        "risk_sectors_count": len(risk) if isinstance(risk, list) else 0,
    }


def _list_to_json_text(value: Any) -> str:
    if isinstance(value, list):
        return json.dumps(value, ensure_ascii=False, separators=(",", ":"))
    return "[]"


def _extract_compact_fields(payload: Dict[str, Any]) -> Dict[str, Any]:
    """提取用户指定键值并转换为分列字段。"""
    highlights = payload.get("highlights")
    hot = payload.get("hot_sectors")
    risk = payload.get("risk_sectors")
    return {
        "summary": _safe_text(payload.get("summary")),
        "conclusion": _safe_text(payload.get("conclusion")),
        "highlights": _list_to_json_text(highlights),
        "market_bias": _safe_text(payload.get("market_bias")),
        "hot_sectors": _list_to_json_text(hot),
        "risk_sectors": _list_to_json_text(risk),
    }


def _upsert_one(session, result_path: Path) -> bool:
    trade_date = result_path.parent.name
    run_id = f"sector_capital_flow_analyst:{trade_date}"
    rel_path = result_path.relative_to(PROJECT_ROOT).as_posix()

    with result_path.open("r", encoding="utf-8") as f:
        payload = json.load(f)
    if not isinstance(payload, dict):
        raise ValueError(f"result.json 格式非法（非对象）: {rel_path}")

    fields = _extract_fields(payload)
    compact_fields = _extract_compact_fields(payload)
    row = (
        session.query(SectorCapitalFlowAnalystKey)
        .filter(SectorCapitalFlowAnalystKey.run_id == run_id)
        .first()
    )
    if row is None:
        row = SectorCapitalFlowAnalystKey(
            run_id=run_id,
            trade_date=trade_date,
            result_path=rel_path,
            result_hash=_result_hash(result_path),
            **fields,
            **compact_fields,
        )
        session.add(row)
    else:
        row.trade_date = trade_date
        row.result_path = rel_path
        row.result_hash = _result_hash(result_path)
        row.summary = compact_fields["summary"]
        row.conclusion = compact_fields["conclusion"]
        row.highlights = compact_fields["highlights"]
        row.market_bias = compact_fields["market_bias"]
        row.hot_sectors = compact_fields["hot_sectors"]
        row.risk_sectors = compact_fields["risk_sectors"]
        row.hot_sectors_count = fields["hot_sectors_count"]
        row.risk_sectors_count = fields["risk_sectors_count"]

    return True


def sync_single_result(result_path: Path) -> bool:
    """同步单个 result.json 到数据库（仅指定键值）。"""
    abs_path = result_path if result_path.is_absolute() else (PROJECT_ROOT / result_path)
    if not abs_path.exists():
        raise FileNotFoundError(f"result.json 不存在: {abs_path.as_posix()}")

    with get_db_session() as session:
        _ensure_table_columns(session)
        return _upsert_one(session, abs_path)


def sync_sector_capital_flow_analyst() -> int:
    """扫描并同步历史结果。"""
    logger.info("=" * 50)
    logger.info("开始同步 sector_capital_flow_analyst（精简键值）")
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
        _ensure_table_columns(session)
        for result_path in result_files:
            try:
                _upsert_one(session, result_path)
                success += 1
            except Exception as e:
                logger.warning("同步失败 %s: %s", result_path.as_posix(), e)

    logger.info("同步完成，共写入/更新 %s 条", success)
    return success


if __name__ == "__main__":
    sync_sector_capital_flow_analyst()
