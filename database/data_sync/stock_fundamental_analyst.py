"""
stock_fundamental_analyst 主表同步
将高频查询字段 upsert 到 agent_stock_fundamental_analyst。

运行方式:
  python -m database.data_sync.stock_fundamental_analyst
  或
  python database/data_sync/stock_fundamental_analyst.py
"""
import hashlib
import json
import logging
import sys
from pathlib import Path
from typing import Any, Dict, Optional

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from database import StockFundamentalAnalystKey
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
    / "stock_fundamental_analyst"
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


def _derive_recommendation(reduce_result: Dict[str, Any]) -> Optional[str]:
    direct = _safe_text(reduce_result.get("recommendation"))
    if direct:
        return direct
    rating = _safe_text(reduce_result.get("rating_label"))
    if rating:
        return rating
    return None


def _extract_fetch_rows(meta: Dict[str, Any], key: str) -> Optional[int]:
    fetch_status = meta.get("fetch_status")
    if isinstance(fetch_status, dict):
        item = fetch_status.get(key)
        if isinstance(item, dict):
            return _safe_int(item.get("rows"))
        return _safe_int(item)
    return None


def _extract_fields(payload: Dict[str, Any], result_path: Path) -> Dict[str, Any]:
    ts_code = _safe_text(payload.get("ts_code")) or _safe_text(result_path.parent.parent.name)
    trade_date = _safe_text(payload.get("trade_date")) or _safe_text(result_path.parent.name)
    trade_date = (trade_date or "").replace("-", "")[:8]

    company = payload.get("stock_company_info")
    if not isinstance(company, dict):
        company = payload.get("company")
    if not isinstance(company, dict):
        company = {}

    meta = payload.get("stock_fundamental_meta")
    if not isinstance(meta, dict):
        meta = {}

    reduce_result = payload.get("fundamental_reduce_result")
    if not isinstance(reduce_result, dict):
        reduce_result = payload.get("reduce_result")
    if not isinstance(reduce_result, dict):
        reduce_result = {}

    return {
        "ts_code": ts_code,
        "trade_date": trade_date,
        "company_name": _safe_text(company.get("com_name") or company.get("name")),
        "industry": _safe_text(payload.get("industry") or company.get("industry")),
        "overall_score": _safe_float(reduce_result.get("overall_score")),
        "valuation_score": _safe_float(reduce_result.get("valuation_score")),
        "quality_score": _safe_float(reduce_result.get("quality_score")),
        "growth_score": _safe_float(reduce_result.get("growth_score")),
        "risk_score": _safe_float(reduce_result.get("risk_score")),
        "recommendation": _derive_recommendation(reduce_result),
        "summary": _safe_text(reduce_result.get("summary")),
        "fetch_complete_success": 1 if bool(meta.get("complete_success", False)) else 0,
        "valuation_rows": _extract_fetch_rows(meta, "valuation"),
        "income_rows": _extract_fetch_rows(meta, "income"),
        "cashflow_rows": _extract_fetch_rows(meta, "cashflow"),
        "balancesheet_rows": _extract_fetch_rows(meta, "balancesheet"),
        "dividend_rows": _extract_fetch_rows(meta, "dividend"),
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

    run_id = f"stock_fundamental_analyst:{ts_code}:{trade_date}"
    rel_path = result_path.relative_to(PROJECT_ROOT).as_posix()

    row = session.query(StockFundamentalAnalystKey).filter(StockFundamentalAnalystKey.run_id == run_id).first()
    if row is None:
        row = StockFundamentalAnalystKey(
            run_id=run_id,
            result_path=rel_path,
            result_hash=_result_hash(result_path),
            **fields,
        )
        session.add(row)
    else:
        row.ts_code = fields["ts_code"]
        row.trade_date = fields["trade_date"]
        row.company_name = fields["company_name"]
        row.industry = fields["industry"]
        row.overall_score = fields["overall_score"]
        row.valuation_score = fields["valuation_score"]
        row.quality_score = fields["quality_score"]
        row.growth_score = fields["growth_score"]
        row.risk_score = fields["risk_score"]
        row.recommendation = fields["recommendation"]
        row.summary = fields["summary"]
        row.fetch_complete_success = fields["fetch_complete_success"]
        row.valuation_rows = fields["valuation_rows"]
        row.income_rows = fields["income_rows"]
        row.cashflow_rows = fields["cashflow_rows"]
        row.balancesheet_rows = fields["balancesheet_rows"]
        row.dividend_rows = fields["dividend_rows"]
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


def sync_stock_fundamental_analyst() -> int:
    """扫描并同步历史结果。"""
    logger.info("=" * 50)
    logger.info("开始同步 stock_fundamental_analyst 主表")
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
    sync_stock_fundamental_analyst()
