"""
市场数据获取模块
包括资金流向、融资融券、龙虎榜、机构数据等
"""
import pandas as pd
import tushare as ts
import akshare as ak
from typing import Dict, List, Optional, Any
from datetime import datetime, date
import logging

from .config import DATA_SOURCES
from .utils import (
    format_date, validate_stock_code,
    clean_dataframe, DataFlowException
)

try:
    import yfinance as yf
    YFINANCE_AVAILABLE = True
except ImportError:
    yf = None
    YFINANCE_AVAILABLE = False

logger = logging.getLogger(__name__)

# 雅虎财经常用美股指数代码
YAHOO_INDEX_SYMBOLS = {
    'SP500': '^GSPC',      # 标普500
    'NASDAQ': '^IXIC',     # 纳斯达克综合
    'DJI': '^DJI',         # 道琼斯工业
}


class MarketDataFetcher:
    """市场数据获取器"""
    
    def __init__(self):
        """初始化"""
        self.tushare_enabled = DATA_SOURCES['tushare']['enabled']
        if self.tushare_enabled:
            ts.set_token(DATA_SOURCES['tushare']['token'])
            self.ts_pro = ts.pro_api()
        
        self.akshare_enabled = DATA_SOURCES['akshare']['enabled']
        self.yahoo_enabled = DATA_SOURCES['yahoo_finance']['enabled'] and YFINANCE_AVAILABLE
    
    def fetch_stock_basic(
        self,
        ts_code: str = None,
        name: str = None,
        market: str = None,
        list_status: str = 'L',
        exchange: str = None,
        is_hs: str = None,
        fields: str = None
    ) -> pd.DataFrame:
        """
        获取基础信息数据，包括股票代码、名称、上市日期、退市日期等
        
        Args:
            ts_code: TS股票代码
            name: 名称
            market: 市场类别（主板/创业板/科创板/CDR/北交所）
            list_status: 上市状态 L上市 D退市 P暂停上市 G过会未交易，默认是L
            exchange: 交易所 SSE上交所 SZSE深交所 BSE北交所
            is_hs: 是否沪深港通标的，N否 H沪股通 S深股通
            fields: 需要获取的字段，如 'ts_code,symbol,name,area,industry,list_date'
        
        Returns:
            基础信息DataFrame
        """
        if not self.tushare_enabled:
            raise DataFlowException("Tushare未配置或未启用")
        
        try:
            logger.info(f"获取股票基础信息: ts_code={ts_code}, list_status={list_status}")
            
            params = {}
            if ts_code:
                params['ts_code'] = ts_code
            if name:
                params['name'] = name
            if market:
                params['market'] = market
            if list_status:
                params['list_status'] = list_status
            if exchange:
                params['exchange'] = exchange
            if is_hs:
                params['is_hs'] = is_hs
            if fields:
                params['fields'] = fields
            
            df = self.ts_pro.stock_basic(**params)
            
            if df.empty:
                logger.warning("未获取到股票基础信息")
                return pd.DataFrame()
            
            df = clean_dataframe(df)
            
            logger.info(f"成功获取 {len(df)} 条股票基础信息")
            return df
            
        except Exception as e:
            logger.error(f"获取股票基础信息失败: {e}")
            raise DataFlowException(f"获取股票基础信息失败: {e}")
    
    def fetch_stock_individual_info_em(
        self,
        symbol: str,
        timeout: Optional[float] = None
    ) -> pd.DataFrame:
        """
        东方财富-个股-股票信息
        
        用于补充 stock_list 表，主要包含流通股本、总市值、流通市值等，便于筛选。
        数据来源: http://quote.eastmoney.com/concept/sh603777.html
        
        Args:
            symbol: 股票代码，6位数字，如 "000001"、"603777"
                   若传入 ts_code 格式(如 000001.SZ)，会自动提取 6 位代码
            timeout: 请求超时秒数，默认不设置
        
        Returns:
            pd.DataFrame: 列 item, value，包含最新价、股票代码、股票简称、
                总股本、流通股、总市值、流通市值、行业、上市时间等
        """
        if not self.akshare_enabled:
            raise DataFlowException("AkShare 未配置或未启用")
        
        # 兼容 ts_code 格式，提取 6 位代码
        code = str(symbol).strip()
        if "." in code:
            code = code.split(".")[0]
        if len(code) != 6 or not code.isdigit():
            raise DataFlowException(f"股票代码格式错误，需 6 位数字: {symbol}")
        
        try:
            logger.debug(f"获取东财个股信息: {code}")
            kwargs = {"symbol": code}
            if timeout is not None:
                kwargs["timeout"] = timeout
            
            df = ak.stock_individual_info_em(**kwargs)
            
            if df is None or df.empty:
                logger.warning(f"未获取到个股信息: {code}")
                return pd.DataFrame()
            
            df = clean_dataframe(df)
            logger.debug(f"成功获取个股信息: {code}")
            return df
            
        except Exception as e:
            logger.error(f"获取东财个股信息失败: {e}")
            raise DataFlowException(f"获取东财个股信息失败: {e}")
    
    def fetch_money_flow(
        self,
        ts_code: str,
        start_date: str,
        end_date: str
    ) -> pd.DataFrame:
        """
        获取个股资金流向数据
        
        Args:
            ts_code: 股票代码
            start_date: 开始日期
            end_date: 结束日期
        
        Returns:
            资金流向DataFrame
        """
        if not self.tushare_enabled:
            raise DataFlowException("Tushare未配置或未启用")
        
        try:
            # 格式化日期
            start_date_fmt = format_date(start_date, 'tushare')
            end_date_fmt = format_date(end_date, 'tushare')
            
            logger.info(f"获取资金流向: {ts_code}, {start_date_fmt} - {end_date_fmt}")
            
            # 获取资金流向数据
            df = self.ts_pro.moneyflow(
                ts_code=ts_code,
                start_date=start_date_fmt,
                end_date=end_date_fmt
            )
            
            if df.empty:
                logger.warning(f"未获取到资金流向数据: {ts_code}")
                return pd.DataFrame()
            
            # 数据处理
            df = clean_dataframe(df)
            df = df.sort_values('trade_date').reset_index(drop=True)
            
            logger.info(f"成功获取 {len(df)} 条资金流向数据")
            return df
            
        except Exception as e:
            logger.error(f"获取资金流向失败: {e}")
            raise DataFlowException(f"获取资金流向失败: {e}")
    
    def fetch_margin_detail(
        self,
        trade_date: str,
        ts_code: str = None
    ) -> pd.DataFrame:
        """
        获取融资融券交易明细
        
        Args:
            trade_date: 交易日期
            ts_code: 股票代码（可选）
        
        Returns:
            融资融券明细DataFrame
        """
        if not self.tushare_enabled:
            raise DataFlowException("Tushare未配置或未启用")
        
        try:
            # 格式化日期
            trade_date_fmt = format_date(trade_date, 'tushare')
            
            logger.info(f"获取融资融券明细: {trade_date_fmt}, {ts_code or '全市场'}")
            
            # 获取融资融券明细
            df = self.ts_pro.margin_detail(
                trade_date=trade_date_fmt,
                ts_code=ts_code
            )
            
            if df.empty:
                logger.warning(f"未获取到融资融券明细: {trade_date_fmt}")
                return pd.DataFrame()
            
            # 数据处理
            df = clean_dataframe(df)
            
            logger.info(f"成功获取 {len(df)} 条融资融券明细")
            return df
            
        except Exception as e:
            logger.error(f"获取融资融券明细失败: {e}")
            raise DataFlowException(f"获取融资融券明细失败: {e}")
    
    def fetch_margin_target(self, ts_code: str = None) -> pd.DataFrame:
        """
        获取融资融券标的
        
        Args:
            ts_code: 股票代码（可选）
        
        Returns:
            融资融券标的DataFrame
        """
        if not self.tushare_enabled:
            raise DataFlowException("Tushare未配置或未启用")
        
        try:
            logger.info(f"获取融资融券标的: {ts_code or '全部'}")
            
            # 获取融资融券标的
            df = self.ts_pro.margin_target(ts_code=ts_code)
            
            if df.empty:
                logger.warning("未获取到融资融券标的")
                return pd.DataFrame()
            
            # 数据处理
            df = clean_dataframe(df)
            
            logger.info(f"成功获取 {len(df)} 条融资融券标的")
            return df
            
        except Exception as e:
            logger.error(f"获取融资融券标的失败: {e}")
            raise DataFlowException(f"获取融资融券标的失败: {e}")
    
    # Tushare index_dailybasic 接口支持的指数代码（文档 doc_id=128，需 400+ 积分）
    INDEX_DAILYBASIC_SUPPORTED_CODES = frozenset({
        "000001.SH",  # 上证综指
        "399001.SZ",  # 深证成指
        "000016.SH",  # 上证50
        "000905.SH",  # 中证500
        "399005.SZ",  # 中小板指
        "399006.SZ",  # 创业板指
    })

    def fetch_index_dailybasic(
        self,
        ts_code: Optional[str] = None,
        trade_date: Optional[str] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        fields: Optional[str] = None
    ) -> pd.DataFrame:
        """
        获取大盘指数每日指标
        
        目前只提供上证综指(000001.SH)、深证成指(399001.SZ)、上证50(000016.SH)、
        中证500(000905.SH)、中小板指(399005.SZ)、创业板指(399006.SZ)的每日指标数据。
        沪深300(000300.SH)、中证1000(000852.SH) 等不在支持列表，调用会返回空。
        数据从2004年1月开始提供。
        
        Args:
            ts_code: 指数代码，与 trade_date 至少输入一个 (如: 000001.SH)
            trade_date: 交易日期，格式为 YYYYMMDD (如: 20181018)
            start_date: 开始日期，格式为 YYYYMMDD
            end_date: 结束日期，格式为 YYYYMMDD
            fields: 指定获取的字段，用逗号分隔 (如: ts_code,trade_date,turnover_rate,pe)
                   可选: ts_code,trade_date,total_mv,float_mv,total_share,float_share,
                         free_share,turnover_rate,turnover_rate_f,pe,pe_ttm,pb
        
        Returns:
            pd.DataFrame: 包含以下字段的指数每日指标数据
                - ts_code (str): 指数代码
                - trade_date (str): 交易日期
                - total_mv (float): 当日总市值（元）
                - float_mv (float): 当日流通市值（元）
                - total_share (float): 当日总股本（股）
                - float_share (float): 当日流通股本（股）
                - free_share (float): 当日自由流通股本（股）
                - turnover_rate (float): 换手率
                - turnover_rate_f (float): 换手率(基于自由流通股本)
                - pe (float): 市盈率
                - pe_ttm (float): 市盈率TTM
                - pb (float): 市净率
        
        Raises:
            DataFlowException: 当 Tushare 未配置、参数不足或数据获取失败时
        
        Note:
            - trade_date 与 ts_code 至少要输入一个参数
            - 单次限量 3000 条（单一指数可提取超12年历史）
            - 用户需要至少 400 积分才可以调取
        """
        if not self.tushare_enabled:
            raise DataFlowException("Tushare未配置或未启用")
        
        if not ts_code and not trade_date:
            raise DataFlowException("ts_code 与 trade_date 至少要输入一个参数")
        
        try:
            kwargs: Dict[str, Any] = {}
            if ts_code:
                kwargs['ts_code'] = ts_code
            if trade_date:
                kwargs['trade_date'] = format_date(trade_date, 'tushare')
            if start_date:
                kwargs['start_date'] = format_date(start_date, 'tushare')
            if end_date:
                kwargs['end_date'] = format_date(end_date, 'tushare')
            if fields:
                kwargs['fields'] = fields
            
            logger.info(f"获取大盘指数每日指标: {kwargs}")
            
            df = self.ts_pro.index_dailybasic(**kwargs)
            
            if df.empty:
                logger.warning("未获取到大盘指数每日指标数据")
                return pd.DataFrame()
            
            df = clean_dataframe(df)
            df = df.sort_values('trade_date').reset_index(drop=True)
            
            logger.info(f"成功获取 {len(df)} 条大盘指数每日指标")
            return df
            
        except Exception as e:
            logger.error(f"获取大盘指数每日指标失败: {e}")
            raise DataFlowException(f"获取大盘指数每日指标失败: {e}")
    
    def fetch_sge_daily(
        self,
        ts_code: Optional[str] = None,
        trade_date: Optional[str] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        fields: Optional[str] = None
    ) -> pd.DataFrame:
        """
        获取上海黄金交易所现货合约日线行情
        
        支持 Au99.99、Au99.95、Au(T+D)、Ag(T+D)、Pt99.95 等品种。
        数据由当日9:00至15:30的交易和前一日夜盘的20:00至2:30数据构成，
        成交量和成交金额为双向计量。
        
        Args:
            ts_code: 合约代码，可通过基础信息接口获得 (如: Au99.99, Au(T+D))
            trade_date: 交易日期，格式为 YYYYMMDD (如: 20220311)
            start_date: 开始日期，格式为 YYYYMMDD
            end_date: 结束日期，格式为 YYYYMMDD
            fields: 指定获取的字段，用逗号分隔 (如: ts_code,close,open,vol)
                   可选: ts_code,trade_date,close,open,high,low,price_avg,change,
                         pct_change,vol,amount,oi,settle_vol,settle_dire
        
        Returns:
            pd.DataFrame: 包含以下字段的现货黄金日行情数据
                - ts_code (str): 现货合约代码
                - trade_date (str): 交易日
                - close (float): 收盘点(元/克)
                - open (float): 开盘点(元/克)
                - high (float): 最高点(元/克)
                - low (float): 最低点(元/克)
                - price_avg (float): 加权平均价(元/克)
                - change (float): 涨跌点位(元/克)
                - pct_change (float): 涨跌幅
                - vol (float): 成交量(千克)
                - amount (float): 成交金额(元)
                - oi (float): 市场持仓
                - settle_vol (float): 交收量
                - settle_dire (str): 持仓方向
        
        Raises:
            DataFlowException: 当 Tushare 未配置或数据获取失败时
        
        Note:
            - 单次最大 2000 条，可循环或分页提取
            - 用户需要至少 2000 积分才可以调取
        """
        if not self.tushare_enabled:
            raise DataFlowException("Tushare未配置或未启用")
        
        try:
            kwargs: Dict[str, Any] = {}
            if ts_code:
                kwargs['ts_code'] = ts_code
            if trade_date:
                kwargs['trade_date'] = format_date(trade_date, 'tushare')
            if start_date:
                kwargs['start_date'] = format_date(start_date, 'tushare')
            if end_date:
                kwargs['end_date'] = format_date(end_date, 'tushare')
            if fields:
                kwargs['fields'] = fields
            
            logger.info(f"获取现货黄金日行情: {kwargs}")
            
            df = self.ts_pro.sge_daily(**kwargs)
            
            if df.empty:
                logger.warning("未获取到现货黄金日行情数据")
                return pd.DataFrame()
            
            df = clean_dataframe(df)
            df = df.sort_values('trade_date').reset_index(drop=True)
            
            logger.info(f"成功获取 {len(df)} 条现货黄金日行情")
            return df
            
        except Exception as e:
            logger.error(f"获取现货黄金日行情失败: {e}")
            raise DataFlowException(f"获取现货黄金日行情失败: {e}")
    
    def fetch_fut_daily(
        self,
        ts_code: Optional[str] = None,
        trade_date: Optional[str] = None,
        exchange: Optional[str] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        fields: Optional[str] = None
    ) -> pd.DataFrame:
        """
        获取期货日线行情数据
        
        覆盖国内五大期货交易所：郑商所(ZCE)、上期所(SHF)、大商所(DCE)、
        中金所(CFX)、上海能源(INE)、广期所(GFE)。支持原油、黄金、铜、铝、
        螺纹钢、动力煤等品种。
        
        Args:
            ts_code: 合约代码 (如: CU1811.SHF 沪铜, SC2503.INE 原油)
            trade_date: 交易日期，格式为 YYYYMMDD (如: 20181113)
            exchange: 交易所代码 DCE/CZCE/SHF/CFFEX/INE/GFEX
            start_date: 开始日期，格式为 YYYYMMDD
            end_date: 结束日期，格式为 YYYYMMDD
            fields: 指定获取的字段，用逗号分隔
                   可选: ts_code,trade_date,pre_close,pre_settle,open,high,low,
                         close,settle,change1,change2,vol,amount,oi,oi_chg,delv_settle
        
        Returns:
            pd.DataFrame: 包含以下字段的期货日线行情数据
                - ts_code (str): TS合约代码
                - trade_date (str): 交易日期
                - pre_close (float): 昨收盘价
                - pre_settle (float): 昨结算价
                - open (float): 开盘价
                - high (float): 最高价
                - low (float): 最低价
                - close (float): 收盘价
                - settle (float): 结算价
                - change1 (float): 涨跌1(收盘价-昨结算价)
                - change2 (float): 涨跌2(结算价-昨结算价)
                - vol (float): 成交量(手)
                - amount (float): 成交金额(万元)
                - oi (float): 持仓量(手)
                - oi_chg (float): 持仓量变化
                - delv_settle (float): 交割结算价
        
        Raises:
            DataFlowException: 当 Tushare 未配置或数据获取失败时
        
        Note:
            - 单次最大 2000 条，总量不限制
            - 用户需要至少 2000 积分才可以调取
        """
        if not self.tushare_enabled:
            raise DataFlowException("Tushare未配置或未启用")
        
        try:
            kwargs: Dict[str, Any] = {}
            if ts_code:
                kwargs['ts_code'] = ts_code
            if trade_date:
                kwargs['trade_date'] = format_date(trade_date, 'tushare')
            if exchange:
                kwargs['exchange'] = exchange
            if start_date:
                kwargs['start_date'] = format_date(start_date, 'tushare')
            if end_date:
                kwargs['end_date'] = format_date(end_date, 'tushare')
            if fields:
                kwargs['fields'] = fields
            
            logger.info(f"获取期货日线行情: {kwargs}")
            
            df = self.ts_pro.fut_daily(**kwargs)
            
            if df.empty:
                logger.warning("未获取到期货日线行情数据")
                return pd.DataFrame()
            
            df = clean_dataframe(df)
            df = df.sort_values('trade_date').reset_index(drop=True)
            
            logger.info(f"成功获取 {len(df)} 条期货日线行情")
            return df
            
        except Exception as e:
            logger.error(f"获取期货日线行情失败: {e}")
            raise DataFlowException(f"获取期货日线行情失败: {e}")
    
    def fetch_yahoo_index_daily(
        self,
        symbol: str,
        start_date: str,
        end_date: str,
        period: str = 'daily',
        session: Optional[Any] = None
    ) -> pd.DataFrame:
        """
        通过雅虎财经(yfinance)获取美股指数日线行情。
        
        数据来源 Yahoo Finance，国内网络可能需要代理。常用指数代码：
        - ^GSPC: 标普500, ^IXIC: 纳斯达克综合, ^DJI: 道琼斯工业
        也可使用 YAHOO_INDEX_SYMBOLS 中的别名，如 'SP500', 'NASDAQ', 'DJI'。
        
        Args:
            symbol: 雅虎指数代码 (如 ^GSPC) 或别名 (如 SP500, NASDAQ, DJI)
            start_date: 开始日期，支持 YYYYMMDD 或 YYYY-MM-DD
            end_date: 结束日期，支持 YYYYMMDD 或 YYYY-MM-DD
            period: 周期 'daily' | 'weekly' | 'monthly'
            session: 可选，requests.Session 实例，用于设置代理等
        
        Returns:
            pd.DataFrame: 列包括 trade_date, open, high, low, close, vol
                (日期已转为 YYYYMMDD 字符串，列名小写与内部接口一致)
        
        Raises:
            DataFlowException: 未启用雅虎数据、未安装 yfinance 或拉取失败时
        """
        if not self.yahoo_enabled or not YFINANCE_AVAILABLE:
            raise DataFlowException(
                "雅虎财经未启用或未安装 yfinance，请安装: pip install yfinance，并在 config 中启用 yahoo_finance"
            )
        symbol = symbol.strip().upper()
        symbol = YAHOO_INDEX_SYMBOLS.get(symbol, symbol)
        if not symbol.startswith('^'):
            symbol = '^' + symbol
        start_s = format_date(start_date, 'tushare')
        end_s = format_date(end_date, 'tushare')
        start_d = f"{start_s[:4]}-{start_s[4:6]}-{start_s[6:8]}"
        end_d = f"{end_s[:4]}-{end_s[4:6]}-{end_s[6:8]}"
        interval_map = {'daily': '1d', 'weekly': '1wk', 'monthly': '1mo'}
        interval = interval_map.get(period, '1d')
        try:
            logger.info(f"雅虎指数行情: symbol={symbol}, {start_d} ~ {end_d}, period={period}")
            df = yf.download(
                symbol,
                start=start_d,
                end=end_d,
                interval=interval,
                progress=False,
                session=session,
                auto_adjust=True,
            )
            if df is None or df.empty:
                logger.warning(f"未获取到雅虎指数数据: {symbol}")
                return pd.DataFrame()
            if isinstance(df.columns, pd.MultiIndex):
                df.columns = df.columns.get_level_values(0)
            df = df.rename(columns={
                'Open': 'open', 'High': 'high', 'Low': 'low',
                'Close': 'close', 'Volume': 'vol'
            })
            df = df.reset_index()
            date_col = 'Date' if 'Date' in df.columns else df.columns[0]
            df = df.rename(columns={date_col: 'trade_date'})
            df['trade_date'] = pd.to_datetime(df['trade_date']).dt.strftime('%Y%m%d')
            for col in ['open', 'high', 'low', 'close', 'vol']:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce')
            out_cols = [c for c in ['trade_date', 'open', 'high', 'low', 'close', 'vol'] if c in df.columns]
            df = df[out_cols].sort_values('trade_date').reset_index(drop=True)
            logger.info(f"成功获取 {len(df)} 条雅虎指数行情")
            return df
        except Exception as e:
            logger.error(f"获取雅虎指数行情失败: {e}")
            raise DataFlowException(f"获取雅虎指数行情失败: {e}")
    
    def fetch_shibor_lpr(
        self,
        start_date: str,
        end_date: str,
        fields: str = None
    ) -> pd.DataFrame:
        """
        获取贷款市场报价利率(LPR)数据
        
        Args:
            start_date: 开始日期
            end_date: 结束日期
            fields: 需要获取的字段，如 'date,1y'，默认获取全部字段
        
        Returns:
            LPR利率DataFrame
        """
        if not self.tushare_enabled:
            raise DataFlowException("Tushare未配置或未启用")
        
        try:
            # 格式化日期
            start_date_fmt = format_date(start_date, 'tushare')
            end_date_fmt = format_date(end_date, 'tushare')
            
            logger.info(f"获取LPR利率数据: {start_date_fmt} - {end_date_fmt}")
            
            # 获取LPR利率数据
            if fields:
                df = self.ts_pro.shibor_lpr(
                    start_date=start_date_fmt,
                    end_date=end_date_fmt,
                    fields=fields
                )
            else:
                df = self.ts_pro.shibor_lpr(
                    start_date=start_date_fmt,
                    end_date=end_date_fmt
                )
            
            if df.empty:
                logger.warning(f"未获取到LPR利率数据: {start_date_fmt} - {end_date_fmt}")
                return pd.DataFrame()
            
            # 数据处理
            df = clean_dataframe(df)
            df = df.sort_values('date').reset_index(drop=True)
            
            logger.info(f"成功获取 {len(df)} 条LPR利率数据")
            return df
            
        except Exception as e:
            logger.error(f"获取LPR利率数据失败: {e}")
            raise DataFlowException(f"获取LPR利率数据失败: {e}")
    
    def fetch_cpi(
        self,
        start_m: str = None,
        end_m: str = None,
        m: str = None,
        fields: str = None
    ) -> pd.DataFrame:
        """
        获取居民消费价格指数(CPI)数据
        
        Args:
            start_m: 开始月份（YYYYMM）
            end_m: 结束月份（YYYYMM）
            m: 指定月份（YYYYMM），支持多个月份同时输入，逗号分隔
            fields: 需要获取的字段，如 'month,nt_val,nt_yoy'，默认获取全部字段
        
        Returns:
            CPI数据DataFrame，包含全国、城市和农村的CPI数据
        """
        if not self.tushare_enabled:
            raise DataFlowException("Tushare未配置或未启用")
        
        try:
            logger.info(f"获取CPI数据: start_m={start_m}, end_m={end_m}, m={m}")
            
            # 获取CPI数据
            if fields:
                df = self.ts_pro.cn_cpi(
                    start_m=start_m,
                    end_m=end_m,
                    m=m,
                    fields=fields
                )
            else:
                df = self.ts_pro.cn_cpi(
                    start_m=start_m,
                    end_m=end_m,
                    m=m
                )
            
            if df.empty:
                logger.warning(f"未获取到CPI数据")
                return pd.DataFrame()
            
            # 数据处理
            df = clean_dataframe(df)
            df = df.sort_values('month').reset_index(drop=True)
            
            logger.info(f"成功获取 {len(df)} 条CPI数据")
            return df
            
        except Exception as e:
            logger.error(f"获取CPI数据失败: {e}")
            raise DataFlowException(f"获取CPI数据失败: {e}")
    
    def fetch_sf_month(
        self,
        start_m: str = None,
        end_m: str = None,
        m: str = None
    ) -> pd.DataFrame:
        """
        获取月度社会融资数据
        
        Args:
            start_m: 开始月份（YYYYMM）
            end_m: 结束月份（YYYYMM）
            m: 指定月份（YYYYMM），支持多个月份同时输入，逗号分隔
        
        Returns:
            社融数据DataFrame，包含社融增量和存量数据
        """
        if not self.tushare_enabled:
            raise DataFlowException("Tushare未配置或未启用")
        
        try:
            logger.info(f"获取社融数据: start_m={start_m}, end_m={end_m}, m={m}")
            
            # 获取社融数据
            df = self.ts_pro.sf_month(
                start_m=start_m,
                end_m=end_m,
                m=m
            )
            
            if df.empty:
                logger.warning(f"未获取到社融数据")
                return pd.DataFrame()
            
            # 数据处理
            df = clean_dataframe(df)
            df = df.sort_values('month').reset_index(drop=True)
            
            logger.info(f"成功获取 {len(df)} 条社融数据")
            return df
            
        except Exception as e:
            logger.error(f"获取社融数据失败: {e}")
            raise DataFlowException(f"获取社融数据失败: {e}")
    

# 便捷函数（远程获取）
def fetch_stock_basic(
    ts_code: str = None,
    name: str = None,
    market: str = None,
    list_status: str = 'L',
    exchange: str = None,
    is_hs: str = None,
    fields: str = None
) -> pd.DataFrame:
    """
    获取股票基础信息的便捷函数（远程）
    
    Args:
        ts_code: TS股票代码
        name: 名称
        market: 市场类别（主板/创业板/科创板/CDR/北交所）
        list_status: 上市状态 L上市 D退市 P暂停上市 G过会未交易，默认是L
        exchange: 交易所 SSE上交所 SZSE深交所 BSE北交所
        is_hs: 是否沪深港通标的，N否 H沪股通 S深股通
        fields: 需要获取的字段，如 'ts_code,symbol,name,area,industry,list_date'
    
    Returns:
        基础信息DataFrame
    
    Example:
        >>> df = fetch_stock_basic(exchange='', list_status='L', fields='ts_code,symbol,name,area,industry,list_date')
        >>> df = fetch_stock_basic(ts_code='000001.SZ')
    """
    fetcher = MarketDataFetcher()
    return fetcher.fetch_stock_basic(ts_code, name, market, list_status, exchange, is_hs, fields)


def fetch_money_flow(
    ts_code: str,
    start_date: str,
    end_date: str
) -> pd.DataFrame:
    """
    获取资金流向的便捷函数（远程）
    """
    fetcher = MarketDataFetcher()
    return fetcher.fetch_money_flow(ts_code, start_date, end_date)


def fetch_margin_detail(
    trade_date: str,
    ts_code: str = None
) -> pd.DataFrame:
    """
    获取融资融券明细的便捷函数（远程）
    """
    fetcher = MarketDataFetcher()
    return fetcher.fetch_margin_detail(trade_date, ts_code)


def fetch_dragon_tiger_list(
    trade_date: str,
    ts_code: str = None
) -> pd.DataFrame:
    """
    获取龙虎榜的便捷函数（远程）
    """
    fetcher = MarketDataFetcher()
    return fetcher.fetch_dragon_tiger_list(trade_date, ts_code)


def fetch_top10_holders(
    ts_code: str,
    period: str,
    ann_date: str = None
) -> pd.DataFrame:
    """
    获取前十大股东的便捷函数（远程）
    """
    fetcher = MarketDataFetcher()
    return fetcher.fetch_top10_holders(ts_code, period, ann_date)


def fetch_block_trade(
    ts_code: str,
    start_date: str,
    end_date: str
) -> pd.DataFrame:
    """
    获取大宗交易的便捷函数（远程）
    """
    fetcher = MarketDataFetcher()
    return fetcher.fetch_block_trade(ts_code, start_date, end_date)


def fetch_shibor_lpr(
    start_date: str,
    end_date: str,
    fields: str = None
) -> pd.DataFrame:
    """
    获取贷款市场报价利率(LPR)数据的便捷函数（远程）
    
    Args:
        start_date: 开始日期
        end_date: 结束日期
        fields: 需要获取的字段，如 'date,1y'，默认获取全部字段
    
    Returns:
        LPR利率DataFrame
    
    Example:
        >>> df = fetch_shibor_lpr('20180101', '20181130', 'date,1y')
        >>> df = fetch_shibor_lpr('20180101', '20181130')
    """
    fetcher = MarketDataFetcher()
    return fetcher.fetch_shibor_lpr(start_date, end_date, fields)


def fetch_cpi(
    start_m: str = None,
    end_m: str = None,
    m: str = None,
    fields: str = None
) -> pd.DataFrame:
    """
    获取居民消费价格指数(CPI)数据的便捷函数（远程）
    
    Args:
        start_m: 开始月份（YYYYMM）
        end_m: 结束月份（YYYYMM）
        m: 指定月份（YYYYMM），支持多个月份同时输入，逗号分隔
        fields: 需要获取的字段，如 'month,nt_val,nt_yoy'，默认获取全部字段
    
    Returns:
        CPI数据DataFrame，包含全国、城市和农村的CPI数据
    
    Example:
        >>> df = fetch_cpi(start_m='201801', end_m='201903')
        >>> df = fetch_cpi(start_m='201801', end_m='201903', fields='month,nt_val,nt_yoy')
        >>> df = fetch_cpi(m='201801,201802,201803')
    """
    fetcher = MarketDataFetcher()
    return fetcher.fetch_cpi(start_m, end_m, m, fields)


def fetch_sf_month(
    start_m: str = None,
    end_m: str = None,
    m: str = None
) -> pd.DataFrame:
    """
    获取月度社会融资数据的便捷函数（远程）
    
    Args:
        start_m: 开始月份（YYYYMM）
        end_m: 结束月份（YYYYMM）
        m: 指定月份（YYYYMM），支持多个月份同时输入，逗号分隔
    
    Returns:
        社融数据DataFrame，包含社融增量和存量数据
    
    Example:
        >>> df = fetch_sf_month(start_m='201901', end_m='202307')
        >>> df = fetch_sf_month(m='201901,201902,201903')
    """
    fetcher = MarketDataFetcher()
    return fetcher.fetch_sf_month(start_m, end_m, m)

