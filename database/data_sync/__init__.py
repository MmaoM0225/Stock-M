"""
数据同步：远程拉取并写入本地数据库
每个表对应一个独立文件，可单独运行
"""
from .breakfast_news import sync_breakfast_news
from .industry import sync_industry
from .stock_list import sync_stock_list

__all__ = [
    "sync_stock_list",
    "sync_industry",
    "sync_breakfast_news",
]
