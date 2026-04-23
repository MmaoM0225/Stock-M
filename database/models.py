"""
数据库模型定义
"""
from datetime import datetime

from sqlalchemy import Column, DateTime, Float, Index, Integer, Text
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


class ThsIndex(Base):
    """同花顺板块指数表（概念、行业、地域、特色、风格、主题、宽基）"""

    __tablename__ = "ths_index"

    ts_code = Column(Text, primary_key=True, comment="指数代码，如 885835.TI")
    name = Column(Text, nullable=True, comment="指数名称")
    count = Column(Integer, nullable=True, comment="成分个数")
    exchange = Column(Text, nullable=True, comment="交易所：A/HK/US")
    list_date = Column(Text, nullable=True, comment="上市日期 YYYYMMDD")
    index_type = Column(Text, nullable=True, comment="N概念 I行业 R地域 S特色 ST风格 TH主题 BB宽基")
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


class CommodityAnalystKey(Base):
    """大宗商品分析关键字段表（关键指标 + result 指针）"""

    __tablename__ = "agent_commodity_analyst_key"
    __table_args__ = (
        Index("idx_commodity_analyst_trade_date", "trade_date"),
        Index("idx_commodity_analyst_market_trend", "commodity_market_trend"),
        Index("idx_commodity_analyst_growth_signal", "growth_signal"),
    )

    id = Column(Integer, primary_key=True, autoincrement=True)
    run_id = Column(Text, nullable=False, unique=True, comment="业务唯一运行ID")
    trade_date = Column(Text, nullable=False, comment="交易日 YYYYMMDD")
    commodity_market_trend = Column(Text, nullable=True, comment="商品市场趋势 up/down/mixed/unknown")
    overall_trend = Column(Text, nullable=True, comment="整体趋势 up/down/neutral/unknown")
    growth_signal = Column(Text, nullable=True, comment="增长信号 strong/weakening/neutral/unknown")
    inflation_signal = Column(Text, nullable=True, comment="通胀信号 rising/falling/neutral/unknown")
    risk_sentiment = Column(Text, nullable=True, comment="风险情绪 risk_on/risk_off/neutral/unknown")
    macro_summary = Column(Text, nullable=True, comment="宏观一句话总结")
    combined_summary = Column(Text, nullable=True, comment="各品种汇总摘要")
    commodity_count = Column(Integer, nullable=True, comment="本次分析品种数量")
    result_path = Column(Text, nullable=False, comment="result.json 相对路径")
    result_hash = Column(Text, nullable=True, comment="result.json 哈希值")
    created_at = Column(DateTime, default=_utc_now, comment="记录创建时间")
    updated_at = Column(DateTime, default=_utc_now, onupdate=_utc_now, comment="记录更新时间")

