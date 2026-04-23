"""
Finlight 新闻表同步
从本地缓存 data/news_finlight 下读取新闻文件，upsert 写入数据库（按 pub_date）。
可单独运行: python -m database.data_sync.news_finlight
"""
import json
import logging
import re
from pathlib import Path
from typing import Any, Dict, Optional

from database import FinlightNews
from database.config import get_db_session

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parents[2]
NEWS_DIR = PROJECT_ROOT / "data" / "news_finlight"


def _safe_text(value: Any) -> Optional[str]:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _extract_pub_date_from_name(path: Path) -> Optional[str]:
    m = re.search(r"(20\d{6})", path.stem)
    if not m:
        return None
    return m.group(1)


def _pick_first_article(payload: Dict[str, Any]) -> Dict[str, Any]:
    articles = payload.get("articles")
    if isinstance(articles, list):
        for item in articles:
            if isinstance(item, dict):
                return item
    return {}


def _extract_row(payload: Dict[str, Any], file_path: Path) -> Dict[str, Optional[str]]:
    first = _pick_first_article(payload)
    title = _safe_text(first.get("title")) or _safe_text(payload.get("title"))
    summary = _safe_text(first.get("summary")) or _safe_text(payload.get("summary"))
    detail_url = _safe_text(first.get("link")) or _safe_text(payload.get("link"))

    pub_date = _safe_text(payload.get("date")) or _extract_pub_date_from_name(file_path)
    if pub_date:
        pub_date = pub_date.replace("-", "")[:8]

    rel_path = file_path.relative_to(PROJECT_ROOT).as_posix()
    return {
        "title": title,
        "summary": summary,
        "pub_date": pub_date,
        "detail_url": detail_url,
        "json_file_path": rel_path,
    }


def sync_finlight_news() -> int:
    """
    读取本地 Finlight 缓存并写入数据库（按 pub_date upsert）。

    Returns:
        int: 写入/更新记录数
    """
    logger.info("=" * 50)
    logger.info("开始同步 Finlight 新闻")
    logger.info("=" * 50)

    if not NEWS_DIR.exists():
        logger.warning("Finlight 新闻目录不存在: %s", NEWS_DIR.as_posix())
        return 0

    files = sorted(set(NEWS_DIR.glob("news_*.json")) | set(NEWS_DIR.glob("finlight_*.json")))
    if not files:
        logger.warning("未找到 Finlight 新闻文件")
        return 0

    count = 0
    with get_db_session() as session:
        for file_path in files:
            try:
                with file_path.open("r", encoding="utf-8") as f:
                    payload = json.load(f) or {}
                if not isinstance(payload, dict):
                    continue
                row = _extract_row(payload, file_path)
                pub_date = row["pub_date"]
                if not pub_date:
                    logger.warning("跳过无日期文件: %s", file_path.as_posix())
                    continue

                existing = session.query(FinlightNews).filter(FinlightNews.pub_date == pub_date).first()
                if existing:
                    existing.title = row["title"]
                    existing.summary = row["summary"]
                    existing.detail_url = row["detail_url"]
                    existing.json_file_path = row["json_file_path"]
                else:
                    session.add(
                        FinlightNews(
                            title=row["title"],
                            summary=row["summary"],
                            pub_date=pub_date,
                            detail_url=row["detail_url"],
                            json_file_path=row["json_file_path"],
                        )
                    )
                count += 1
            except Exception as e:
                logger.warning("处理失败 %s: %s", file_path.as_posix(), e)

    logger.info("Finlight 新闻同步完成，共 %s 条", count)
    return count


if __name__ == "__main__":
    sync_finlight_news()
