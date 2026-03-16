"""
数据库初始化脚本
创建所有表结构
python scripts/init_db.py 
"""
import logging
import sys
from pathlib import Path

# 将项目根目录加入 path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


def main():
    from database import init_db, get_engine

    engine = get_engine()
    logger.info(f"数据库连接: {engine.url}")
    init_db(engine)
    logger.info("数据库初始化完成")


if __name__ == "__main__":
    main()
