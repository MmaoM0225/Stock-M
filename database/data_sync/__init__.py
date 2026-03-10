"""
数据同步：远程拉取并写入本地数据库
每个表对应一个独立文件，可单独运行
"""
from .breakfast_news import sync_breakfast_news
from .industry import sync_industry

__all__ = [
    "sync_stock_list",
    "sync_stock_individual_info_em",
    "sync_industry",
    "sync_breakfast_news",
]


def __getattr__(name: str):
    """延迟导入 stock_list，避免 python -m database.data_sync.stock_list 时的模块冲突"""
    if name in ("sync_stock_list", "sync_stock_individual_info_em"):
        from .stock_list import sync_stock_list, sync_stock_individual_info_em
        return sync_stock_list if name == "sync_stock_list" else sync_stock_individual_info_em
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
