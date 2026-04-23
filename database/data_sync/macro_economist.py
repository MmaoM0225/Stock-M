"""
macro_economist 关键表同步
从 data/artifacts 中扫描已有 result.json，提取关键字段并 upsert 到数据库。

运行方式:
  python -m database.data_sync.macro_economist
  或
  python database/data_sync/macro_economist.py
"""
import hashlib
import json
import logging
import sys
from pathlib import Path
from typing import Any, Dict, Optional

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from database import MacroEconomistKey
from database.config import get_db_session

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parents[2]
MACRO_ECONOMIST_ARTIFACT_ROOT = (
    PROJECT_ROOT
    / "data"
    / "artifacts"
    / "analyst"
    / "macro_analyst"
    / "macro_economist"
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
    return {
        "gdp_trend": _safe_text(payload.get("gdp_trend")),
        "lpr_trend": _safe_text(payload.get("lpr_trend")),
        "cpi_trend": _safe_text(payload.get("cpi_trend")),
        "sf_trend": _safe_text(payload.get("sf_trend")),
        "m2_trend": _safe_text(payload.get("m2_trend")),
        "pmi_status": _safe_text(payload.get("pmi_status")),
        "growth_signal": _safe_text(payload.get("growth_signal")),
        "inflation_signal": _safe_text(payload.get("inflation_signal")),
        "liquidity_signal": _safe_text(payload.get("liquidity_signal")),
        "macro_regime": _safe_text(payload.get("macro_regime")),
        "equity_market_bias": _safe_text(payload.get("equity_market_bias")),
        "bond_market_bias": _safe_text(payload.get("bond_market_bias")),
        "commodity_bias": _safe_text(payload.get("commodity_bias")),
        "liquidity_summary": _safe_text(payload.get("liquidity_summary")),
        "conclusion": _safe_text(payload.get("conclusion")),
    }


def _upsert_one(session, result_path: Path) -> bool:
    trade_date = result_path.parent.name
    run_id = f"macro_economist:{trade_date}"
    rel_path = result_path.relative_to(PROJECT_ROOT).as_posix()

    with result_path.open("r", encoding="utf-8") as f:
        payload = json.load(f)
    if not isinstance(payload, dict):
        raise ValueError(f"result.json 格式非法（非对象）: {rel_path}")

    key_fields = _extract_fields(payload)
    row = session.query(MacroEconomistKey).filter(MacroEconomistKey.run_id == run_id).first()

    if row is None:
        row = MacroEconomistKey(
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
        row.gdp_trend = key_fields["gdp_trend"]
        row.lpr_trend = key_fields["lpr_trend"]
        row.cpi_trend = key_fields["cpi_trend"]
        row.sf_trend = key_fields["sf_trend"]
        row.m2_trend = key_fields["m2_trend"]
        row.pmi_status = key_fields["pmi_status"]
        row.growth_signal = key_fields["growth_signal"]
        row.inflation_signal = key_fields["inflation_signal"]
        row.liquidity_signal = key_fields["liquidity_signal"]
        row.macro_regime = key_fields["macro_regime"]
        row.equity_market_bias = key_fields["equity_market_bias"]
        row.bond_market_bias = key_fields["bond_market_bias"]
        row.commodity_bias = key_fields["commodity_bias"]
        row.liquidity_summary = key_fields["liquidity_summary"]
        row.conclusion = key_fields["conclusion"]

    return True


def sync_single_result(result_path: Path) -> bool:
    """同步单个 macro_economist result.json 到数据库（upsert）。"""
    abs_path = result_path if result_path.is_absolute() else (PROJECT_ROOT / result_path)
    if not abs_path.exists():
        raise FileNotFoundError(f"result.json 不存在: {abs_path.as_posix()}")

    with get_db_session() as session:
        return _upsert_one(session, abs_path)


def sync_macro_economist() -> int:
    """扫描并同步 macro_economist 历史结果（upsert）。"""
    logger.info("=" * 50)
    logger.info("开始同步 macro_economist 关键数据")
    logger.info("=" * 50)

    if not MACRO_ECONOMIST_ARTIFACT_ROOT.exists():
        logger.warning("artifact 根目录不存在: %s", MACRO_ECONOMIST_ARTIFACT_ROOT.as_posix())
        return 0

    result_files = sorted(MACRO_ECONOMIST_ARTIFACT_ROOT.glob("*/result.json"))
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
    sync_macro_economist()
