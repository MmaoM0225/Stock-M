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


class FinlightNews(Base):
    """Finlight 新闻表（摘要 + 详情页 URL + 本地 JSON 文件路径）"""

    __tablename__ = "finlight_news"

    id = Column(Integer, primary_key=True, autoincrement=True)
    title = Column(Text, nullable=True, comment="标题")
    summary = Column(Text, nullable=True, comment="摘要")
    pub_date = Column(Text, nullable=True, unique=True, comment="日期 YYYYMMDD")
    detail_url = Column(Text, nullable=True, comment="详情页 URL")
    json_file_path = Column(Text, nullable=True, comment="本地 JSON 文件路径，如 data/news_finlight/news_20260309.json")
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


class MacroEconomistKey(Base):
    """宏观经济分析关键字段表（关键指标 + result 指针）"""

    __tablename__ = "agent_macro_economist_key"
    __table_args__ = (
        Index("idx_macro_economist_trade_date", "trade_date"),
        Index("idx_macro_economist_macro_regime", "macro_regime"),
        Index("idx_macro_economist_growth_signal", "growth_signal"),
    )

    id = Column(Integer, primary_key=True, autoincrement=True)
    run_id = Column(Text, nullable=False, unique=True, comment="业务唯一运行ID")
    trade_date = Column(Text, nullable=False, comment="交易日 YYYYMMDD")
    gdp_trend = Column(Text, nullable=True, comment="GDP趋势 up/down/stable/unknown")
    lpr_trend = Column(Text, nullable=True, comment="LPR趋势 up/down/stable/unknown")
    cpi_trend = Column(Text, nullable=True, comment="CPI趋势 up/down/stable/unknown")
    sf_trend = Column(Text, nullable=True, comment="社融趋势 up/down/stable/unknown")
    m2_trend = Column(Text, nullable=True, comment="M2趋势 up/down/stable/unknown")
    pmi_status = Column(Text, nullable=True, comment="PMI状态 expansion/contraction/stable/unknown")
    growth_signal = Column(Text, nullable=True, comment="增长信号 strong/weakening/stable/unknown")
    inflation_signal = Column(Text, nullable=True, comment="通胀信号 rising/falling/stable/unknown")
    liquidity_signal = Column(Text, nullable=True, comment="流动性信号 loose/neutral/tight/unknown")
    macro_regime = Column(Text, nullable=True, comment="宏观阶段")
    equity_market_bias = Column(Text, nullable=True, comment="权益市场倾向")
    bond_market_bias = Column(Text, nullable=True, comment="债券市场倾向")
    commodity_bias = Column(Text, nullable=True, comment="商品市场倾向")
    liquidity_summary = Column(Text, nullable=True, comment="流动性环境一句话")
    conclusion = Column(Text, nullable=True, comment="宏观结论")
    result_path = Column(Text, nullable=False, comment="result.json 相对路径")
    result_hash = Column(Text, nullable=True, comment="result.json 哈希值")
    created_at = Column(DateTime, default=_utc_now, comment="记录创建时间")
    updated_at = Column(DateTime, default=_utc_now, onupdate=_utc_now, comment="记录更新时间")


class MarketSentimentAnalystKey(Base):
    """市场情绪分析关键字段表（关键指标 + result 指针）"""

    __tablename__ = "agent_market_sentiment_analyst_key"
    __table_args__ = (
        Index("idx_market_sentiment_trade_date", "trade_date"),
        Index("idx_market_sentiment_signal", "market_sentiment"),
        Index("idx_market_sentiment_index_trend", "index_trend"),
    )

    id = Column(Integer, primary_key=True, autoincrement=True)
    run_id = Column(Text, nullable=False, unique=True, comment="业务唯一运行ID")
    trade_date = Column(Text, nullable=False, comment="交易日 YYYYMMDD")
    market_sentiment = Column(Text, nullable=True, comment="市场情绪 bullish/neutral/bearish/unknown")
    index_trend = Column(Text, nullable=True, comment="指数趋势 up/down/neutral/unknown")
    volume_signal = Column(Text, nullable=True, comment="量能信号 expanding/contracting/neutral/unknown")
    volatility_signal = Column(Text, nullable=True, comment="波动信号 high/medium/low/unknown")
    sentiment_summary = Column(Text, nullable=True, comment="情绪一句话总结")
    combined_summary = Column(Text, nullable=True, comment="多指数综合摘要")
    index_count = Column(Integer, nullable=True, comment="参与汇总指数数量")
    result_path = Column(Text, nullable=False, comment="result.json 相对路径")
    result_hash = Column(Text, nullable=True, comment="result.json 哈希值")
    created_at = Column(DateTime, default=_utc_now, comment="记录创建时间")
    updated_at = Column(DateTime, default=_utc_now, onupdate=_utc_now, comment="记录更新时间")


class NewsAnalystKey(Base):
    """新闻分析关键字段表（仅摘要，不存事件与板块明细）"""

    __tablename__ = "agent_news_analyst_key"
    __table_args__ = (
        Index("idx_news_analyst_trade_date", "trade_date"),
        Index("idx_news_analyst_market_sentiment", "market_sentiment"),
        Index("idx_news_analyst_policy_bias", "policy_bias"),
    )

    id = Column(Integer, primary_key=True, autoincrement=True)
    run_id = Column(Text, nullable=False, unique=True, comment="业务唯一运行ID")
    trade_date = Column(Text, nullable=False, comment="交易日 YYYYMMDD")
    events_count = Column(Integer, nullable=True, comment="事件数量")
    sector_impacts_count = Column(Integer, nullable=True, comment="板块影响数量")
    liquidity = Column(Text, nullable=True, comment="流动性 tight/neutral/loose")
    policy_bias = Column(Text, nullable=True, comment="政策倾向 supportive/neutral/restrictive")
    global_risk = Column(Text, nullable=True, comment="全球风险 low/medium/high")
    market_sentiment = Column(Text, nullable=True, comment="市场情绪 bullish/bearish/neutral")
    result_path = Column(Text, nullable=False, comment="result.json 相对路径")
    result_hash = Column(Text, nullable=True, comment="result.json 哈希值")
    created_at = Column(DateTime, default=_utc_now, comment="记录创建时间")
    updated_at = Column(DateTime, default=_utc_now, onupdate=_utc_now, comment="记录更新时间")


class SectorCapitalFlowAnalystKey(Base):
    """板块资金流分析关键表（指定键值分列存储）"""

    __tablename__ = "agent_sector_capital_flow_analyst"
    __table_args__ = (
        Index("idx_sector_capital_flow_trade_date", "trade_date"),
        Index("idx_sector_capital_flow_market_bias", "market_bias"),
    )

    id = Column(Integer, primary_key=True, autoincrement=True)
    run_id = Column(Text, nullable=False, unique=True, comment="业务唯一运行ID")
    trade_date = Column(Text, nullable=False, comment="交易日 YYYYMMDD")
    summary = Column(Text, nullable=True, comment="整体资金流向摘要")
    conclusion = Column(Text, nullable=True, comment="结论与建议")
    highlights = Column(Text, nullable=True, comment="亮点列表（JSON 字符串）")
    market_bias = Column(Text, nullable=True, comment="资金面倾向 bullish/neutral/bearish")
    hot_sectors = Column(Text, nullable=True, comment="热点板块列表（JSON 字符串）")
    risk_sectors = Column(Text, nullable=True, comment="风险板块列表（JSON 字符串）")
    hot_sectors_count = Column(Integer, nullable=True, comment="热点板块数量")
    risk_sectors_count = Column(Integer, nullable=True, comment="风险板块数量")
    result_path = Column(Text, nullable=False, comment="result.json 相对路径")
    result_hash = Column(Text, nullable=True, comment="result.json 哈希值")
    created_at = Column(DateTime, default=_utc_now, comment="记录创建时间")
    updated_at = Column(DateTime, default=_utc_now, onupdate=_utc_now, comment="记录更新时间")


class SectorTrendAnalystKey(Base):
    """行业趋势分析关键表（指定键值分列存储）"""

    __tablename__ = "agent_sector_trend_analyst"
    __table_args__ = (
        Index("idx_sector_trend_trade_date", "trade_date"),
        Index("idx_sector_trend_market_regime", "market_regime"),
    )

    id = Column(Integer, primary_key=True, autoincrement=True)
    run_id = Column(Text, nullable=False, unique=True, comment="业务唯一运行ID")
    trade_date = Column(Text, nullable=False, comment="交易日 YYYYMMDD")
    summary = Column(Text, nullable=True, comment="整体趋势摘要")
    conclusion = Column(Text, nullable=True, comment="结论与建议")
    highlights = Column(Text, nullable=True, comment="亮点列表（JSON 字符串）")
    market_regime = Column(Text, nullable=True, comment="市场状态 trend_following/rotation/repair/risk_off/mixed")
    leading_themes = Column(Text, nullable=True, comment="主线主题列表（JSON 字符串）")
    reversal_opportunities = Column(Text, nullable=True, comment="修复机会列表（JSON 字符串）")
    top_risk_sectors = Column(Text, nullable=True, comment="风险板块列表（JSON 字符串）")
    leading_themes_count = Column(Integer, nullable=True, comment="主线主题数量")
    reversal_opportunities_count = Column(Integer, nullable=True, comment="修复机会数量")
    top_risk_sectors_count = Column(Integer, nullable=True, comment="风险板块数量")
    result_path = Column(Text, nullable=False, comment="result.json 相对路径")
    result_hash = Column(Text, nullable=True, comment="result.json 哈希值")
    created_at = Column(DateTime, default=_utc_now, comment="记录创建时间")
    updated_at = Column(DateTime, default=_utc_now, onupdate=_utc_now, comment="记录更新时间")


class StockFundamentalAnalystKey(Base):
    """个股基本面分析主表（高频查询字段）"""

    __tablename__ = "agent_stock_fundamental_analyst"
    __table_args__ = (
        Index("idx_stock_fundamental_ts_date", "ts_code", "trade_date"),
        Index("idx_stock_fundamental_trade_date", "trade_date"),
        Index("idx_stock_fundamental_overall_score", "overall_score"),
        Index("idx_stock_fundamental_recommendation", "recommendation"),
    )

    id = Column(Integer, primary_key=True, autoincrement=True)
    run_id = Column(Text, nullable=False, unique=True, comment="业务唯一运行ID")
    ts_code = Column(Text, nullable=False, comment="股票代码")
    trade_date = Column(Text, nullable=False, comment="交易日 YYYYMMDD")
    company_name = Column(Text, nullable=True, comment="公司名称")
    industry = Column(Text, nullable=True, comment="行业")
    overall_score = Column(Float, nullable=True, comment="综合评分")
    valuation_score = Column(Float, nullable=True, comment="估值评分")
    quality_score = Column(Float, nullable=True, comment="质量评分")
    growth_score = Column(Float, nullable=True, comment="成长评分")
    risk_score = Column(Float, nullable=True, comment="风险评分")
    recommendation = Column(Text, nullable=True, comment="建议（buy/hold/sell 或中文）")
    summary = Column(Text, nullable=True, comment="一句话总结")
    fetch_complete_success = Column(Integer, nullable=True, comment="数据抓取是否完整成功（0/1）")
    valuation_rows = Column(Integer, nullable=True, comment="估值数据行数")
    income_rows = Column(Integer, nullable=True, comment="利润表数据行数")
    cashflow_rows = Column(Integer, nullable=True, comment="现金流数据行数")
    balancesheet_rows = Column(Integer, nullable=True, comment="资产负债表数据行数")
    dividend_rows = Column(Integer, nullable=True, comment="分红数据行数")
    result_path = Column(Text, nullable=False, comment="result.json 相对路径")
    result_hash = Column(Text, nullable=True, comment="result.json 哈希值")
    created_at = Column(DateTime, default=_utc_now, comment="记录创建时间")
    updated_at = Column(DateTime, default=_utc_now, onupdate=_utc_now, comment="记录更新时间")


class StockTechnicalAnalystKey(Base):
    """个股技术面分析主表（高频查询字段）"""

    __tablename__ = "agent_stock_technical_analyst"
    __table_args__ = (
        Index("idx_stock_technical_ts_date", "ts_code", "trade_date"),
        Index("idx_stock_technical_trade_date", "trade_date"),
        Index("idx_stock_technical_score", "technical_score"),
        Index("idx_stock_technical_signal", "trend_signal"),
    )

    id = Column(Integer, primary_key=True, autoincrement=True)
    run_id = Column(Text, nullable=False, unique=True, comment="业务唯一运行ID")
    ts_code = Column(Text, nullable=False, comment="股票代码")
    trade_date = Column(Text, nullable=False, comment="交易日 YYYYMMDD")
    technical_score = Column(Float, nullable=True, comment="技术评分")
    trend_signal = Column(Text, nullable=True, comment="趋势信号 uptrend/downtrend/range/unknown")
    trend_strength = Column(Text, nullable=True, comment="趋势强弱 strong/medium/weak/unknown")
    recommendation = Column(Text, nullable=True, comment="技术倾向（可选）")
    summary = Column(Text, nullable=True, comment="一句话总结")
    fetch_complete_success = Column(Integer, nullable=True, comment="技术数据抓取是否完整成功（0/1）")
    support_levels_count = Column(Integer, nullable=True, comment="支撑位数量")
    resistance_levels_count = Column(Integer, nullable=True, comment="压力位数量")
    result_path = Column(Text, nullable=False, comment="result.json 相对路径")
    result_hash = Column(Text, nullable=True, comment="result.json 哈希值")
    created_at = Column(DateTime, default=_utc_now, comment="记录创建时间")
    updated_at = Column(DateTime, default=_utc_now, onupdate=_utc_now, comment="记录更新时间")


class StockScreenerKey(Base):
    """股票筛选主表（摘要与统计字段）"""

    __tablename__ = "agent_stock_screener"
    __table_args__ = (
        Index("idx_stock_screener_trade_date", "trade_date"),
        Index("idx_stock_screener_total_count", "total_count"),
    )

    id = Column(Integer, primary_key=True, autoincrement=True)
    run_id = Column(Text, nullable=False, unique=True, comment="业务唯一运行ID")
    trade_date = Column(Text, nullable=False, comment="交易日 YYYYMMDD")
    total_count = Column(Integer, nullable=True, comment="筛选结果数量")
    filter_summary = Column(Text, nullable=True, comment="筛选摘要")
    applied_filters = Column(Text, nullable=True, comment="应用筛选条件（JSON 字符串）")
    sector_distribution = Column(Text, nullable=True, comment="行业分布（JSON 字符串）")
    sector_template_applied = Column(Text, nullable=True, comment="板块模板应用（JSON 字符串）")
    sector_pick_counts = Column(Text, nullable=True, comment="板块入选数量（JSON 字符串）")
    filtered_preview = Column(Text, nullable=True, comment="筛选结果预览代码（JSON 字符串，最多20）")
    result_path = Column(Text, nullable=False, comment="result.json 相对路径")
    result_hash = Column(Text, nullable=True, comment="result.json 哈希值")
    created_at = Column(DateTime, default=_utc_now, comment="记录创建时间")
    updated_at = Column(DateTime, default=_utc_now, onupdate=_utc_now, comment="记录更新时间")


class MacroManagerKey(Base):
    """宏观经理主表（高频查询字段）"""

    __tablename__ = "agent_macro_manager"
    __table_args__ = (
        Index("idx_macro_manager_trade_date", "trade_date"),
        Index("idx_macro_manager_market_direction", "market_direction"),
        Index("idx_macro_manager_confidence", "confidence"),
    )

    id = Column(Integer, primary_key=True, autoincrement=True)
    run_id = Column(Text, nullable=False, unique=True, comment="业务唯一运行ID")
    trade_date = Column(Text, nullable=False, comment="交易日 YYYYMMDD")
    market_regime = Column(Text, nullable=True, comment="市场状态")
    market_direction = Column(Text, nullable=True, comment="市场方向 neutral/bullish/bearish/unknown")
    target_position = Column(Text, nullable=True, comment="建议仓位区间")
    confidence = Column(Float, nullable=True, comment="置信度 0-1")
    macro_summary = Column(Text, nullable=True, comment="宏观经理总结")
    focus_industry_sectors = Column(Text, nullable=True, comment="关注行业（JSON 字符串）")
    focus_concept_sectors = Column(Text, nullable=True, comment="关注概念（JSON 字符串）")
    avoid_sectors = Column(Text, nullable=True, comment="规避板块（JSON 字符串）")
    macro_themes = Column(Text, nullable=True, comment="宏观主题（JSON 字符串）")
    risk_factors = Column(Text, nullable=True, comment="风险因素（JSON 字符串）")
    focus_industry_count = Column(Integer, nullable=True, comment="关注行业数量")
    focus_concept_count = Column(Integer, nullable=True, comment="关注概念数量")
    avoid_sectors_count = Column(Integer, nullable=True, comment="规避板块数量")
    macro_themes_count = Column(Integer, nullable=True, comment="宏观主题数量")
    risk_factors_count = Column(Integer, nullable=True, comment="风险因素数量")
    result_path = Column(Text, nullable=False, comment="result.json 相对路径")
    result_hash = Column(Text, nullable=True, comment="result.json 哈希值")
    created_at = Column(DateTime, default=_utc_now, comment="记录创建时间")
    updated_at = Column(DateTime, default=_utc_now, onupdate=_utc_now, comment="记录更新时间")


class SectorManagerKey(Base):
    """行业经理主表（高频查询字段）"""

    __tablename__ = "agent_sector_manager"
    __table_args__ = (
        Index("idx_sector_manager_trade_date", "trade_date"),
        Index("idx_sector_manager_market_regime", "market_regime"),
        Index("idx_sector_manager_market_bias", "market_bias"),
        Index("idx_sector_manager_action_bias", "action_bias"),
    )

    id = Column(Integer, primary_key=True, autoincrement=True)
    run_id = Column(Text, nullable=False, unique=True, comment="业务唯一运行ID")
    trade_date = Column(Text, nullable=False, comment="交易日 YYYYMMDD")
    market_regime = Column(Text, nullable=True, comment="市场状态 trend_following/rotation/repair/risk_off/mixed/unknown")
    market_bias = Column(Text, nullable=True, comment="市场偏向 bullish/neutral/bearish")
    action_bias = Column(Text, nullable=True, comment="执行倾向 follow_leaders/low_buy_repair/fast_rotation/defense/wait_and_see/unknown")
    confidence = Column(Float, nullable=True, comment="置信度 0-1")
    sector_summary = Column(Text, nullable=True, comment="行业经理总结")
    favored_sectors = Column(Text, nullable=True, comment="优先板块（JSON 字符串）")
    watchlist_sectors = Column(Text, nullable=True, comment="观察板块（JSON 字符串）")
    risk_sectors = Column(Text, nullable=True, comment="风险板块（JSON 字符串）")
    core_signals = Column(Text, nullable=True, comment="核心信号（JSON 字符串）")
    favored_count = Column(Integer, nullable=True, comment="优先板块数量")
    watchlist_count = Column(Integer, nullable=True, comment="观察板块数量")
    risk_count = Column(Integer, nullable=True, comment="风险板块数量")
    core_signals_count = Column(Integer, nullable=True, comment="核心信号数量")
    result_path = Column(Text, nullable=False, comment="result.json 相对路径")
    result_hash = Column(Text, nullable=True, comment="result.json 哈希值")
    created_at = Column(DateTime, default=_utc_now, comment="记录创建时间")
    updated_at = Column(DateTime, default=_utc_now, onupdate=_utc_now, comment="记录更新时间")


class StockManagerKey(Base):
    """个股经理主表（高频查询字段）"""

    __tablename__ = "agent_stock_manager"
    __table_args__ = (
        Index("idx_stock_manager_ts_date", "ts_code", "trade_date"),
        Index("idx_stock_manager_trade_date", "trade_date"),
        Index("idx_stock_manager_action_signal", "action_signal"),
        Index("idx_stock_manager_overall_score", "overall_score"),
    )

    id = Column(Integer, primary_key=True, autoincrement=True)
    run_id = Column(Text, nullable=False, unique=True, comment="业务唯一运行ID")
    ts_code = Column(Text, nullable=False, comment="股票代码")
    trade_date = Column(Text, nullable=False, comment="交易日 YYYYMMDD")
    success = Column(Integer, nullable=True, comment="分析是否成功（0/1）")
    overall_score = Column(Float, nullable=True, comment="综合评分 0-100")
    confidence = Column(Text, nullable=True, comment="置信度 高/中/低")
    risk_level = Column(Text, nullable=True, comment="风险等级 低/中/高")
    action_signal = Column(Text, nullable=True, comment="动作信号 buy/hold/sell/watch")
    selection_reason = Column(Text, nullable=True, comment="入选理由")
    signal_reason = Column(Text, nullable=True, comment="信号理由")
    summary = Column(Text, nullable=True, comment="总结")
    fundamental_score = Column(Float, nullable=True, comment="基本面分")
    technical_score = Column(Float, nullable=True, comment="技术面分")
    key_points = Column(Text, nullable=True, comment="关键要点（JSON 字符串）")
    risks = Column(Text, nullable=True, comment="风险点（JSON 字符串）")
    key_points_count = Column(Integer, nullable=True, comment="关键要点数量")
    risks_count = Column(Integer, nullable=True, comment="风险点数量")
    result_path = Column(Text, nullable=False, comment="result.json 相对路径")
    result_hash = Column(Text, nullable=True, comment="result.json 哈希值")
    created_at = Column(DateTime, default=_utc_now, comment="记录创建时间")
    updated_at = Column(DateTime, default=_utc_now, onupdate=_utc_now, comment="记录更新时间")


class StockPoolManagerKey(Base):
    """批量个股池经理主表（高频查询字段）"""

    __tablename__ = "agent_stock_pool_manager"
    __table_args__ = (
        Index("idx_stock_pool_manager_trade_date", "trade_date"),
        Index("idx_stock_pool_manager_pool_size", "pool_size"),
        Index("idx_stock_pool_manager_success_count", "analyze_success_count"),
    )

    id = Column(Integer, primary_key=True, autoincrement=True)
    run_id = Column(Text, nullable=False, unique=True, comment="业务唯一运行ID")
    trade_date = Column(Text, nullable=False, comment="交易日 YYYYMMDD")
    pool_load_error = Column(Text, nullable=True, comment="加载筛选池错误")
    pool_size = Column(Integer, nullable=True, comment="筛选池规模")
    analyzed_count = Column(Integer, nullable=True, comment="分析总数")
    analyze_success_count = Column(Integer, nullable=True, comment="分析成功数")
    analyze_error_count = Column(Integer, nullable=True, comment="分析失败数")
    summary_text = Column(Text, nullable=True, comment="汇总说明")
    screener_artifact_path = Column(Text, nullable=True, comment="screener 结果路径")
    top_stocks_preview = Column(Text, nullable=True, comment="Top股票预览（JSON 字符串，最多20）")
    candidate_count = Column(Integer, nullable=True, comment="候选数量")
    top_count = Column(Integer, nullable=True, comment="Top数量")
    result_path = Column(Text, nullable=False, comment="result.json 相对路径")
    result_hash = Column(Text, nullable=True, comment="result.json 哈希值")
    created_at = Column(DateTime, default=_utc_now, comment="记录创建时间")
    updated_at = Column(DateTime, default=_utc_now, onupdate=_utc_now, comment="记录更新时间")


class PortfolioDecisionKey(Base):
    """组合决策主表（高频查询字段）"""

    __tablename__ = "agent_portfolio_decision"
    __table_args__ = (
        Index("idx_portfolio_decision_trade_date", "trade_date"),
        Index("idx_portfolio_decision_stock_count", "stock_count"),
        Index("idx_portfolio_decision_total_capital", "total_capital"),
    )

    id = Column(Integer, primary_key=True, autoincrement=True)
    run_id = Column(Text, nullable=False, unique=True, comment="业务唯一运行ID")
    trade_date = Column(Text, nullable=False, comment="交易日 YYYYMMDD")
    total_capital = Column(Float, nullable=True, comment="组合总资产")
    stock_count = Column(Integer, nullable=True, comment="持仓股票数量（不含现金）")
    cash_amount = Column(Float, nullable=True, comment="待投资现金金额")
    cash_weight = Column(Float, nullable=True, comment="现金仓位（0-1）")
    operation_count = Column(Integer, nullable=True, comment="操作记录总数")
    buy_count = Column(Integer, nullable=True, comment="建仓数量")
    add_count = Column(Integer, nullable=True, comment="加仓数量")
    reduce_count = Column(Integer, nullable=True, comment="减仓数量")
    clear_count = Column(Integer, nullable=True, comment="清仓数量")
    hold_count = Column(Integer, nullable=True, comment="持有数量")
    warning_count = Column(Integer, nullable=True, comment="warning 数量")
    decision_summary = Column(Text, nullable=True, comment="决策总结")
    llm_reasoning = Column(Text, nullable=True, comment="LLM 原因摘要")
    portfolio_preview = Column(Text, nullable=True, comment="持仓预览（JSON 字符串，最多20）")
    result_path = Column(Text, nullable=False, comment="result.json 相对路径")
    result_hash = Column(Text, nullable=True, comment="result.json 哈希值")
    created_at = Column(DateTime, default=_utc_now, comment="记录创建时间")
    updated_at = Column(DateTime, default=_utc_now, onupdate=_utc_now, comment="记录更新时间")