"""
数据库配置
本地开发默认 SQLite，通过 DATABASE_URL 可切换为 PostgreSQL
"""
import os
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

# 默认 SQLite 路径：项目根目录下的 data/stockm.db
DEFAULT_SQLITE_PATH = Path(__file__).resolve().parent.parent / "data" / "stockm.db"
# SQLite URL 需使用正斜杠
DEFAULT_SQLITE_URL = f"sqlite:///{DEFAULT_SQLITE_PATH.as_posix()}"

# 本地开发：设置 USE_SQLITE=true 强制使用 SQLite，忽略 DATABASE_URL 中的 PostgreSQL
_use_sqlite = os.getenv("USE_SQLITE", "").lower() in ("true", "1", "yes")
DATABASE_URL = (
    DEFAULT_SQLITE_URL
    if _use_sqlite or not os.getenv("DATABASE_URL")
    else os.getenv("DATABASE_URL", DEFAULT_SQLITE_URL)
)

# SQLite 需要 check_same_thread=False 以支持多线程（如 FastAPI）
_connect_args = {}
if DATABASE_URL.startswith("sqlite"):
    _connect_args["check_same_thread"] = False


def get_engine():
    """获取数据库引擎"""
    from sqlalchemy import create_engine

    return create_engine(
        DATABASE_URL,
        connect_args=_connect_args,
        echo=os.getenv("SQL_ECHO", "false").lower() == "true",  # 开发时可开启 SQL 日志
    )


def get_session():
    """获取数据库会话"""
    from sqlalchemy.orm import sessionmaker

    engine = get_engine()
    Session = sessionmaker(bind=engine, autoflush=False, autocommit=False)
    return Session()


def get_db_session():
    """获取数据库会话（上下文管理器，自动 commit/rollback/close）"""
    from contextlib import contextmanager

    @contextmanager
    def _cm():
        session = get_session()
        try:
            yield session
            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

    return _cm()


def init_db(engine=None):
    """
    初始化数据库，创建所有表
    若 engine 为 None，则使用默认引擎
    """
    from database.models import Base

    if engine is None:
        engine = get_engine()

    # 确保 SQLite 文件所在目录存在
    if DATABASE_URL.startswith("sqlite"):
        db_path = DATABASE_URL.replace("sqlite:///", "")
        Path(db_path).parent.mkdir(parents=True, exist_ok=True)

    Base.metadata.create_all(bind=engine)
