"""
数据同步：远程拉取并写入本地数据库
每个表对应一个独立文件，可单独运行
"""
from .breakfast_news import sync_breakfast_news
from .industry import sync_industry
from .stock_list import sync_stock_list
from .ths_index import (
    sync_ths_index,
    sync_ths_index_all_types,
    sync_ths_index_by_type,
)

__all__ = [
    "sync_stock_list",
    "sync_industry",
    "sync_breakfast_news",
    "sync_ths_index",
    "sync_ths_index_by_type",
    "sync_ths_index_all_types",
]
