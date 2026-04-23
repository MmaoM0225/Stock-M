"""
market_sentiment_analyst 关键表同步
从 data/artifacts 中扫描已有 result.json，提取关键字段并 upsert 到数据库。

运行方式:
  python -m database.data_sync.market_sentiment_analyst
  或
  python database/data_sync/market_sentiment_analyst.py
"""
import hashlib
import json
import logging
import sys
from pathlib import Path
from typing import Any, Dict, Optional

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from database import MarketSentimentAnalystKey
from database.config import get_db_session

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parents[2]
MARKET_SENTIMENT_ARTIFACT_ROOT = (
    PROJECT_ROOT
    / "data"
    / "artifacts"
    / "analyst"
    / "macro_analyst"
    / "market_sentiment_analyst"
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
    per_index = payload.get("per_index")
    index_count = len(per_index) if isinstance(per_index, dict) else 0
    return {
        "market_sentiment": _safe_text(payload.get("market_sentiment")),
        "index_trend": _safe_text(payload.get("index_trend")),
        "volume_signal": _safe_text(payload.get("volume_signal")),
        "volatility_signal": _safe_text(payload.get("volatility_signal")),
        "sentiment_summary": _safe_text(payload.get("sentiment_summary")),
        "combined_summary": _safe_text(payload.get("combined_summary")),
        "index_count": index_count,
    }


def _upsert_one(session, result_path: Path) -> bool:
    trade_date = result_path.parent.name
    run_id = f"market_sentiment_analyst:{trade_date}"
    rel_path = result_path.relative_to(PROJECT_ROOT).as_posix()

    with result_path.open("r", encoding="utf-8") as f:
        payload = json.load(f)
    if not isinstance(payload, dict):
        raise ValueError(f"result.json 格式非法（非对象）: {rel_path}")

    key_fields = _extract_fields(payload)
    row = session.query(MarketSentimentAnalystKey).filter(MarketSentimentAnalystKey.run_id == run_id).first()

    if row is None:
        row = MarketSentimentAnalystKey(
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
        row.market_sentiment = key_fields["market_sentiment"]
        row.index_trend = key_fields["index_trend"]
        row.volume_signal = key_fields["volume_signal"]
        row.volatility_signal = key_fields["volatility_signal"]
        row.sentiment_summary = key_fields["sentiment_summary"]
        row.combined_summary = key_fields["combined_summary"]
        row.index_count = key_fields["index_count"]

    return True


def sync_single_result(result_path: Path) -> bool:
    """同步单个 market_sentiment_analyst result.json 到数据库（upsert）。"""
    abs_path = result_path if result_path.is_absolute() else (PROJECT_ROOT / result_path)
    if not abs_path.exists():
        raise FileNotFoundError(f"result.json 不存在: {abs_path.as_posix()}")

    with get_db_session() as session:
        return _upsert_one(session, abs_path)


def sync_market_sentiment_analyst() -> int:
    """扫描并同步 market_sentiment_analyst 历史结果（upsert）。"""
    logger.info("=" * 50)
    logger.info("开始同步 market_sentiment_analyst 关键数据")
    logger.info("=" * 50)

    if not MARKET_SENTIMENT_ARTIFACT_ROOT.exists():
        logger.warning("artifact 根目录不存在: %s", MARKET_SENTIMENT_ARTIFACT_ROOT.as_posix())
        return 0

    result_files = sorted(MARKET_SENTIMENT_ARTIFACT_ROOT.glob("*/result.json"))
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
    sync_market_sentiment_analyst()
