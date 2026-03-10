"""
数据库模型定义
"""
from datetime import datetime

from sqlalchemy import Column, DateTime, Float, Integer, Text
from sqlalchemy.orm import declarative_base

Base = declarative_base()


def _utc_now():
    return datetime.utcnow()


class StockList(Base):
    """股票列表表"""

    __tablename__ = "stock_list"

    ts_code = Column(Text, primary_key=True, comment="股票代码，如 000001.SZ")
    symbol = Column(Text, nullable=True, comment="6位代码")
    name = Column(Text, nullable=True, comment="股票名称")
    area = Column(Text, nullable=True, comment="所属地区")
    industry = Column(Text, nullable=True, comment="申万行业名称")
    market = Column(Text, nullable=True, comment="市场类型：主板/创业板/科创板/北交所")
    list_date = Column(Integer, nullable=True, comment="上市日期 YYYYMMDD")
    total_share = Column(Float, nullable=True, comment="总股本（股）")
    float_share = Column(Float, nullable=True, comment="流通股本（股）")
    total_mv = Column(Float, nullable=True, comment="总市值（元）")
    float_mv = Column(Float, nullable=True, comment="流通市值（元）")
    created_at = Column(DateTime, default=_utc_now, comment="记录创建时间")
    updated_at = Column(DateTime, default=_utc_now, onupdate=_utc_now, comment="记录更新时间")


class Industry(Base):
    """行业分类表（一级、二级统一存储）"""

    __tablename__ = "industry"

    index_code = Column(Text, primary_key=True, comment="行业指数代码，如 801010.SI")
    industry_name = Column(Text, nullable=True, comment="行业名称")
    level = Column(Text, nullable=True, comment="层级：L1 一级 / L2 二级")
    industry_code = Column(Integer, nullable=True, comment="行业编码")
    parent_code = Column(Integer, nullable=True, default=0, comment="父级行业编码，0 表示一级")
    is_pub = Column(Integer, nullable=True, default=1, comment="是否公开，0/1")
    src = Column(Text, nullable=True, comment="数据来源，如 SW2021")
    created_at = Column(DateTime, default=_utc_now, comment="记录创建时间")


class BreakfastNews(Base):
    """财经早餐表（摘要 + 详情页 URL + 本地 JSON 文件路径）"""

    __tablename__ = "breakfast_news"

    id = Column(Integer, primary_key=True, autoincrement=True)
    title = Column(Text, nullable=True, comment="标题")
    summary = Column(Text, nullable=True, comment="摘要")
    pub_date = Column(Text, nullable=True, unique=True, comment="日期 YYYYMMDD")
    detail_url = Column(Text, nullable=True, comment="详情页 URL")
    json_file_path = Column(Text, nullable=True, comment="本地 JSON 文件路径，如 data/news/news_20260309.json")
    created_at = Column(DateTime, default=_utc_now, comment="记录创建时间")
    updated_at = Column(DateTime, default=_utc_now, onupdate=_utc_now, comment="记录更新时间")
