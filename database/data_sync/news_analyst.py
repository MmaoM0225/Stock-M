"""
news_analyst 关键表同步
从 data/artifacts 中扫描已有 result.json，提取关键字段并 upsert 到数据库。

注意：仅存摘要字段，不存 events / sector_impacts 明细内容。

运行方式:
  python -m database.data_sync.news_analyst
  或
  python database/data_sync/news_analyst.py
"""
import hashlib
import json
import logging
import sys
from pathlib import Path
from typing import Any, Dict, Optional

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from database import NewsAnalystKey
from database.config import get_db_session

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parents[2]
NEWS_ARTIFACT_ROOT = (
    PROJECT_ROOT
    / "data"
    / "artifacts"
    / "analyst"
    / "macro_analyst"
    / "news_analyst"
)


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
    events = payload.get("events")
    sector_impacts = payload.get("sector_impacts")
    macro_environment = payload.get("macro_environment")
    if not isinstance(macro_environment, dict):
        macro_environment = {}

    return {
        "events_count": len(events) if isinstance(events, list) else 0,
        "sector_impacts_count": len(sector_impacts) if isinstance(sector_impacts, dict) else 0,
        "liquidity": _safe_text(macro_environment.get("liquidity")),
        "policy_bias": _safe_text(macro_environment.get("policy_bias")),
        "global_risk": _safe_text(macro_environment.get("global_risk")),
        "market_sentiment": _safe_text(macro_environment.get("market_sentiment")),
    }


def _upsert_one(session, result_path: Path) -> bool:
    trade_date = result_path.parent.name
    run_id = f"news_analyst:{trade_date}"
    rel_path = result_path.relative_to(PROJECT_ROOT).as_posix()

    with result_path.open("r", encoding="utf-8") as f:
        payload = json.load(f)
    if not isinstance(payload, dict):
        raise ValueError(f"result.json 格式非法（非对象）: {rel_path}")

    key_fields = _extract_fields(payload)
    row = session.query(NewsAnalystKey).filter(NewsAnalystKey.run_id == run_id).first()

    if row is None:
        row = NewsAnalystKey(
            run_id=run_id,
            trade_date=trade_date,
            result_path=rel_path,
            result_hash=_result_hash(result_path),
            **key_fields,
        )
        session.add(row)
    else:
        row.trade_date = trade_date
        row.result_path = rel_path
        row.result_hash = _result_hash(result_path)
        row.events_count = key_fields["events_count"]
        row.sector_impacts_count = key_fields["sector_impacts_count"]
        row.liquidity = key_fields["liquidity"]
        row.policy_bias = key_fields["policy_bias"]
        row.global_risk = key_fields["global_risk"]
        row.market_sentiment = key_fields["market_sentiment"]

    return True


def sync_single_result(result_path: Path) -> bool:
    """同步单个 news_analyst result.json 到数据库（upsert）。"""
    abs_path = result_path if result_path.is_absolute() else (PROJECT_ROOT / result_path)
    if not abs_path.exists():
        raise FileNotFoundError(f"result.json 不存在: {abs_path.as_posix()}")

    with get_db_session() as session:
        return _upsert_one(session, abs_path)


def sync_news_analyst() -> int:
    """扫描并同步 news_analyst 历史结果（upsert）。"""
    logger.info("=" * 50)
    logger.info("开始同步 news_analyst 关键数据")
    logger.info("=" * 50)

    if not NEWS_ARTIFACT_ROOT.exists():
        logger.warning("artifact 根目录不存在: %s", NEWS_ARTIFACT_ROOT.as_posix())
        return 0

    result_files = sorted(NEWS_ARTIFACT_ROOT.glob("*/result.json"))
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
    sync_news_analyst()
