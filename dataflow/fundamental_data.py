"""
基本面数据获取模块
包括公司基本信息、财务报表数据等
"""
import asyncio
import aiohttp
import pandas as pd
import tushare as ts
from typing import Dict, List, Optional, Any
from datetime import datetime, date
import logging

from .config import DATA_SOURCES
from .utils import (
    format_date, validate_stock_code, async_request,
    clean_dataframe, tushare_limiter, DataFlowException
)

logger = logging.getLogger(__name__)


class FundamentalDataFetcher:
    """基本面数据获取器"""
    
    def __init__(self):
        """初始化"""
        self.tushare_enabled = DATA_SOURCES['tushare']['enabled']
        if self.tushare_enabled:
            ts.set_token(DATA_SOURCES['tushare']['token'])
            self.ts_pro = ts.pro_api()
        
        self.session: Optional[aiohttp.ClientSession] = None
    
    async def __aenter__(self):
        """异步上下文管理器入口"""
        self.session = aiohttp.ClientSession()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """异步上下文管理器出口"""
        if self.session:
            await self.session.close()
    
    async def get_company_info(
        self, 
        ts_code: str, 
        fields: str = None
    ) -> pd.DataFrame:
        """
        获取上市公司基础信息
        
        Args:
            ts_code: 股票代码 (如: 000001.SZ)
            fields: 指定获取的字段，用逗号分隔。默认获取所有字段
                   可选字段: ts_code,com_name,com_id,exchange,chairman,manager,
                           secretary,reg_capital,setup_date,province,city,
                           website,email,employees,main_business,business_scope
        
        Returns:
            pd.DataFrame: 包含以下字段的公司基础信息
                - ts_code (str): 股票代码
                - com_name (str): 公司全称
                - com_id (str): 统一社会信用代码
                - exchange (str): 交易所代码
                - chairman (str): 法人代表
                - manager (str): 总经理
                - secretary (str): 董秘
                - reg_capital (float): 注册资本(万元)
                - setup_date (str): 注册日期
                - province (str): 所在省份
                - city (str): 所在城市
                - website (str): 公司主页
                - email (str): 电子邮件
                - employees (int): 员工人数
                - main_business (str): 主要业务及产品
                - business_scope (str): 经营范围
        
        Raises:
            DataFlowException: 当 Tushare 未配置、股票代码无效或数据获取失败时
        
        Note:
            - 单次提取4500条，可以根据交易所分批提取
            - 用户需要至少120积分才可以调取
        """
        if not self.tushare_enabled:
            raise DataFlowException("Tushare未配置或未启用")
        
        if not validate_stock_code(ts_code, 'cn'):
            raise DataFlowException(f"无效的股票代码: {ts_code}")
        
        try:
            # 限频
            await tushare_limiter.acquire()
            
            logger.info(f"获取公司基础信息: {ts_code}")
            
            # 构建请求参数
            params = {'ts_code': ts_code}
            if fields:
                params['fields'] = fields
            
            # 获取公司基础信息
            df = self.ts_pro.stock_company(**params)
            
            if df.empty:
                logger.warning(f"未获取到公司信息: {ts_code}")
                return pd.DataFrame()
            
            # 数据清理和处理
            df = clean_dataframe(df)
            
            logger.info(f"成功获取公司基础信息: {ts_code}")
            return df
            
        except Exception as e:
            logger.error(f"获取公司基础信息失败: {e}")
            raise DataFlowException(f"获取公司基础信息失败: {e}")
    
    async def get_daily_basic(
        self,
        ts_code: str = None,
        trade_date: str = None,
        start_date: str = None,
        end_date: str = None,
        fields: str = None
    ) -> pd.DataFrame:
        """
        获取全部股票每日重要的基本面指标
        
        Args:
            ts_code: 股票代码 (如: 000001.SZ)，与trade_date二选一
            trade_date: 交易日期 (如: 20180726)，与ts_code二选一
            start_date: 开始日期，格式为 YYYYMMDD (如: 20180701)
            end_date: 结束日期，格式为 YYYYMMDD (如: 20180718)
            fields: 指定获取的字段，用逗号分隔。默认获取所有字段
                   可选字段: ts_code,trade_date,close,turnover_rate,turnover_rate_f,
                           volume_ratio,pe,pe_ttm,pb,ps,ps_ttm,dv_ratio,dv_ttm,
                           total_share,float_share,free_share,total_mv,circ_mv
        
        Returns:
            pd.DataFrame: 包含以下字段的每日基本面指标
                - ts_code (str): TS股票代码
                - trade_date (str): 交易日期
                - close (float): 当日收盘价
                - turnover_rate (float): 换手率（%）
                - turnover_rate_f (float): 换手率（自由流通股）
                - volume_ratio (float): 量比
                - pe (float): 市盈率（总市值/净利润，亏损的PE为空）
                - pe_ttm (float): 市盈率（TTM，亏损的PE为空）
                - pb (float): 市净率（总市值/净资产）
                - ps (float): 市销率
                - ps_ttm (float): 市销率（TTM）
                - dv_ratio (float): 股息率（%）
                - dv_ttm (float): 股息率（TTM）（%）
                - total_share (float): 总股本（万股）
                - float_share (float): 流通股本（万股）
                - free_share (float): 自由流通股本（万）
                - total_mv (float): 总市值（万元）
                - circ_mv (float): 流通市值（万元）
        
        Raises:
            DataFlowException: 当 Tushare 未配置、参数无效或数据获取失败时
        
        Note:
            - 单次请求最大返回6000条数据，可按日线循环提取全部历史
            - 交易日每日15点～17点之间更新
            - 至少2000积分才可以调取，5000积分无总量限制
            - ts_code和trade_date必须二选一
        """
        if not self.tushare_enabled:
            raise DataFlowException("Tushare未配置或未启用")
        
        # 参数验证
        if not ts_code and not trade_date:
            raise DataFlowException("ts_code和trade_date必须提供其中一个")
        
        if ts_code and not validate_stock_code(ts_code, 'cn'):
            raise DataFlowException(f"无效的股票代码: {ts_code}")
        
        try:
            # 格式化日期参数
            params = {}
            if ts_code:
                params['ts_code'] = ts_code
            if trade_date:
                params['trade_date'] = format_date(trade_date, 'tushare')
            if start_date:
                params['start_date'] = format_date(start_date, 'tushare')
            if end_date:
                params['end_date'] = format_date(end_date, 'tushare')
            if fields:
                params['fields'] = fields
            
            # 限频
            await tushare_limiter.acquire()
            
            logger.info(f"获取每日基本面指标: {params}")
            
            # 获取每日基本面指标
            df = self.ts_pro.daily_basic(**params)
            
            if df.empty:
                logger.warning(f"未获取到每日基本面指标数据: {params}")
                return pd.DataFrame()
            
            # 数据清理和处理
            df = clean_dataframe(df)
            
            # 按日期排序
            if 'trade_date' in df.columns:
                df = df.sort_values('trade_date').reset_index(drop=True)
            
            logger.info(f"成功获取 {len(df)} 条每日基本面指标数据")
            return df
            
        except Exception as e:
            logger.error(f"获取每日基本面指标失败: {e}")
            raise DataFlowException(f"获取每日基本面指标失败: {e}")
    
    async def get_income_statement(
        self,
        ts_code: str,
        ann_date: str = None,
        f_ann_date: str = None,
        start_date: str = None,
        end_date: str = None,
        period: str = None,
        report_type: str = '1',
        comp_type: str = None,
        fields: str = None
    ) -> pd.DataFrame:
        """
        获取上市公司财务利润表数据
        
        Args:
            ts_code: 股票代码 (如: 600000.SH)
            ann_date: 公告日期，格式为 YYYYMMDD (如: 20180428)
            f_ann_date: 实际公告日期，格式为 YYYYMMDD
            start_date: 公告开始日期，格式为 YYYYMMDD (如: 20180101)
            end_date: 公告结束日期，格式为 YYYYMMDD (如: 20180730)
            period: 报告期，格式为 YYYYMMDD (如: 20171231表示年报)
            report_type: 报告类型，默认为'1'合并报表
            comp_type: 公司类型 (1一般工商业 2银行 3保险 4证券)
            fields: 指定获取的字段，用逗号分隔。默认获取关键字段
        
        Returns:
            pd.DataFrame: 包含以下关键字段的利润表数据
                - ts_code (str): TS代码
                - ann_date (str): 公告日期
                - f_ann_date (str): 实际公告日期
                - end_date (str): 报告期
                - report_type (str): 报告类型
                - comp_type (str): 公司类型
                - basic_eps (float): 基本每股收益
                - diluted_eps (float): 稀释每股收益
                - total_revenue (float): 营业总收入
                - revenue (float): 营业收入
                - total_cogs (float): 营业总成本
                - oper_cost (float): 营业成本
                - sell_exp (float): 销售费用
                - admin_exp (float): 管理费用
                - fin_exp (float): 财务费用
                - operate_profit (float): 营业利润
                - total_profit (float): 利润总额
                - income_tax (float): 所得税费用
                - n_income (float): 净利润(含少数股东损益)
                - n_income_attr_p (float): 净利润(不含少数股东损益)
                - ebit (float): 息税前利润
                - ebitda (float): 息税折旧摊销前利润
                - rd_exp (float): 研发费用
        
        Raises:
            DataFlowException: 当 Tushare 未配置、股票代码无效或数据获取失败时
        
        Note:
            - 当前接口只能按单只股票获取其历史数据
            - 用户需要至少2000积分才可以调取
            - 报告类型说明: 1合并报表 2单季合并 3调整单季合并表 4调整合并报表 5调整前合并报表
        """
        if not self.tushare_enabled:
            raise DataFlowException("Tushare未配置或未启用")
        
        if not validate_stock_code(ts_code, 'cn'):
            raise DataFlowException(f"无效的股票代码: {ts_code}")
        
        try:
            # 构建请求参数
            params = {'ts_code': ts_code}
            
            if ann_date:
                params['ann_date'] = format_date(ann_date, 'tushare')
            if f_ann_date:
                params['f_ann_date'] = format_date(f_ann_date, 'tushare')
            if start_date:
                params['start_date'] = format_date(start_date, 'tushare')
            if end_date:
                params['end_date'] = format_date(end_date, 'tushare')
            if period:
                params['period'] = format_date(period, 'tushare')
            if report_type:
                params['report_type'] = report_type
            if comp_type:
                params['comp_type'] = comp_type
            
            # 设置关键字段，保持数据干净整洁
            if not fields:
                fields = ('ts_code,ann_date,f_ann_date,end_date,report_type,comp_type,'
                         'basic_eps,diluted_eps,total_revenue,revenue,total_cogs,oper_cost,'
                         'sell_exp,admin_exp,fin_exp,operate_profit,total_profit,income_tax,'
                         'n_income,n_income_attr_p,ebit,ebitda,rd_exp')
            params['fields'] = fields
            
            # 限频
            await tushare_limiter.acquire()
            
            logger.info(f"获取利润表数据: {params}")
            
            # 获取利润表数据
            df = self.ts_pro.income(**params)
            
            if df.empty:
                logger.warning(f"未获取到利润表数据: {ts_code}")
                return pd.DataFrame()
            
            # 数据清理和处理
            df = clean_dataframe(df)
            
            # 按报告期排序
            if 'end_date' in df.columns:
                df = df.sort_values('end_date').reset_index(drop=True)
            
            logger.info(f"成功获取 {len(df)} 条利润表数据")
            return df
            
        except Exception as e:
            logger.error(f"获取利润表数据失败: {e}")
            raise DataFlowException(f"获取利润表数据失败: {e}")
    
    async def get_balance_sheet(
        self,
        ts_code: str,
        ann_date: str = None,
        start_date: str = None,
        end_date: str = None,
        period: str = None,
        report_type: str = '1',
        comp_type: str = None,
        fields: str = None
    ) -> pd.DataFrame:
        """
        获取上市公司资产负债表数据
        
        Args:
            ts_code: 股票代码 (如: 600000.SH)
            ann_date: 公告日期，格式为 YYYYMMDD (如: 20180428)
            start_date: 公告开始日期，格式为 YYYYMMDD (如: 20180101)
            end_date: 公告结束日期，格式为 YYYYMMDD (如: 20180730)
            period: 报告期，格式为 YYYYMMDD (如: 20171231表示年报)
            report_type: 报告类型，默认为'1'合并报表
            comp_type: 公司类型 (1一般工商业 2银行 3保险 4证券)
            fields: 指定获取的字段，用逗号分隔。默认获取关键字段
        
        Returns:
            pd.DataFrame: 包含以下关键字段的资产负债表数据
                - ts_code (str): TS股票代码
                - ann_date (str): 公告日期
                - f_ann_date (str): 实际公告日期
                - end_date (str): 报告期
                - report_type (str): 报表类型
                - comp_type (str): 公司类型
                - total_share (float): 期末总股本
                - cap_rese (float): 资本公积金
                - undistr_porfit (float): 未分配利润
                - surplus_rese (float): 盈余公积金
                - money_cap (float): 货币资金
                - accounts_receiv (float): 应收账款
                - inventories (float): 存货
                - total_cur_assets (float): 流动资产合计
                - fix_assets (float): 固定资产
                - intan_assets (float): 无形资产
                - goodwill (float): 商誉
                - total_nca (float): 非流动资产合计
                - total_assets (float): 资产总计
                - st_borr (float): 短期借款
                - acct_payable (float): 应付账款
                - total_cur_liab (float): 流动负债合计
                - lt_borr (float): 长期借款
                - total_ncl (float): 非流动负债合计
                - total_liab (float): 负债合计
                - minority_int (float): 少数股东权益
                - total_hldr_eqy_exc_min_int (float): 股东权益合计(不含少数股东权益)
                - total_hldr_eqy_inc_min_int (float): 股东权益合计(含少数股东权益)
                - total_liab_hldr_eqy (float): 负债及股东权益总计
        
        Raises:
            DataFlowException: 当 Tushare 未配置、股票代码无效或数据获取失败时
        
        Note:
            - 当前接口只能按单只股票获取其历史数据
            - 用户需要至少2000积分才可以调取
            - 报告类型说明: 1合并报表 2单季合并 3调整单季合并表 4调整合并报表 5调整前合并报表
        """
        if not self.tushare_enabled:
            raise DataFlowException("Tushare未配置或未启用")
        
        if not validate_stock_code(ts_code, 'cn'):
            raise DataFlowException(f"无效的股票代码: {ts_code}")
        
        try:
            # 构建请求参数
            params = {'ts_code': ts_code}
            
            if ann_date:
                params['ann_date'] = format_date(ann_date, 'tushare')
            if start_date:
                params['start_date'] = format_date(start_date, 'tushare')
            if end_date:
                params['end_date'] = format_date(end_date, 'tushare')
            if period:
                params['period'] = format_date(period, 'tushare')
            if report_type:
                params['report_type'] = report_type
            if comp_type:
                params['comp_type'] = comp_type
            
            # 设置关键字段，保持数据干净整洁
            if not fields:
                fields = ('ts_code,ann_date,f_ann_date,end_date,report_type,comp_type,'
                         'total_share,cap_rese,undistr_porfit,surplus_rese,money_cap,'
                         'accounts_receiv,inventories,total_cur_assets,fix_assets,'
                         'intan_assets,goodwill,total_nca,total_assets,st_borr,'
                         'acct_payable,total_cur_liab,lt_borr,total_ncl,total_liab,'
                         'minority_int,total_hldr_eqy_exc_min_int,total_hldr_eqy_inc_min_int,'
                         'total_liab_hldr_eqy')
            params['fields'] = fields
            
            # 限频
            await tushare_limiter.acquire()
            
            logger.info(f"获取资产负债表数据: {params}")
            
            # 获取资产负债表数据
            df = self.ts_pro.balancesheet(**params)
            
            if df.empty:
                logger.warning(f"未获取到资产负债表数据: {ts_code}")
                return pd.DataFrame()
            
            # 数据清理和处理
            df = clean_dataframe(df)
            
            # 按报告期排序
            if 'end_date' in df.columns:
                df = df.sort_values('end_date').reset_index(drop=True)
            
            logger.info(f"成功获取 {len(df)} 条资产负债表数据")
            return df
            
        except Exception as e:
            logger.error(f"获取资产负债表数据失败: {e}")
            raise DataFlowException(f"获取资产负债表数据失败: {e}")
    
    async def get_cashflow_statement(
        self,
        ts_code: str,
        ann_date: str = None,
        f_ann_date: str = None,
        start_date: str = None,
        end_date: str = None,
        period: str = None,
        report_type: str = '1',
        comp_type: str = None,
        is_calc: int = None,
        fields: str = None
    ) -> pd.DataFrame:
        """
        获取上市公司现金流量表数据
        
        Args:
            ts_code: 股票代码 (如: 600000.SH)
            ann_date: 公告日期，格式为 YYYYMMDD (如: 20180428)
            f_ann_date: 实际公告日期，格式为 YYYYMMDD
            start_date: 公告开始日期，格式为 YYYYMMDD (如: 20180101)
            end_date: 公告结束日期，格式为 YYYYMMDD (如: 20180730)
            period: 报告期，格式为 YYYYMMDD (如: 20171231表示年报)
            report_type: 报告类型，默认为'1'合并报表
            comp_type: 公司类型 (1一般工商业 2银行 3保险 4证券)
            is_calc: 是否计算报表
            fields: 指定获取的字段，用逗号分隔。默认获取关键字段
        
        Returns:
            pd.DataFrame: 包含以下关键字段的现金流量表数据
                - ts_code (str): TS股票代码
                - ann_date (str): 公告日期
                - f_ann_date (str): 实际公告日期
                - end_date (str): 报告期
                - comp_type (str): 公司类型
                - report_type (str): 报表类型
                - net_profit (float): 净利润
                - finan_exp (float): 财务费用
                - c_fr_sale_sg (float): 销售商品、提供劳务收到的现金
                - c_paid_goods_s (float): 购买商品、接受劳务支付的现金
                - c_paid_to_for_empl (float): 支付给职工以及为职工支付的现金
                - c_paid_for_taxes (float): 支付的各项税费
                - c_inf_fr_operate_a (float): 经营活动现金流入小计
                - st_cash_out_act (float): 经营活动现金流出小计
                - n_cashflow_act (float): 经营活动产生的现金流量净额
                - c_disp_withdrwl_invest (float): 收回投资收到的现金
                - c_recp_return_invest (float): 取得投资收益收到的现金
                - c_pay_acq_const_fiolta (float): 购建固定资产、无形资产和其他长期资产支付的现金
                - c_paid_invest (float): 投资支付的现金
                - stot_inflows_inv_act (float): 投资活动现金流入小计
                - stot_out_inv_act (float): 投资活动现金流出小计
                - n_cashflow_inv_act (float): 投资活动产生的现金流量净额
                - c_recp_borrow (float): 取得借款收到的现金
                - c_recp_cap_contrib (float): 吸收投资收到的现金
                - c_prepay_amt_borr (float): 偿还债务支付的现金
                - c_pay_dist_dpcp_int_exp (float): 分配股利、利润或偿付利息支付的现金
                - stot_cash_in_fnc_act (float): 筹资活动现金流入小计
                - stot_cashout_fnc_act (float): 筹资活动现金流出小计
                - n_cash_flows_fnc_act (float): 筹资活动产生的现金流量净额
                - n_incr_cash_cash_equ (float): 现金及现金等价物净增加额
                - c_cash_equ_beg_period (float): 期初现金及现金等价物余额
                - c_cash_equ_end_period (float): 期末现金及现金等价物余额
                - free_cashflow (float): 企业自由现金流量
        
        Raises:
            DataFlowException: 当 Tushare 未配置、股票代码无效或数据获取失败时
        
        Note:
            - 当前接口只能按单只股票获取其历史数据
            - 用户需要至少2000积分才可以调取
            - 报告类型说明: 1合并报表 2单季合并 3调整单季合并表 4调整合并报表 5调整前合并报表
        """
        if not self.tushare_enabled:
            raise DataFlowException("Tushare未配置或未启用")
        
        if not validate_stock_code(ts_code, 'cn'):
            raise DataFlowException(f"无效的股票代码: {ts_code}")
        
        try:
            # 构建请求参数
            params = {'ts_code': ts_code}
            
            if ann_date:
                params['ann_date'] = format_date(ann_date, 'tushare')
            if f_ann_date:
                params['f_ann_date'] = format_date(f_ann_date, 'tushare')
            if start_date:
                params['start_date'] = format_date(start_date, 'tushare')
            if end_date:
                params['end_date'] = format_date(end_date, 'tushare')
            if period:
                params['period'] = format_date(period, 'tushare')
            if report_type:
                params['report_type'] = report_type
            if comp_type:
                params['comp_type'] = comp_type
            if is_calc is not None:
                params['is_calc'] = is_calc
            
            # 设置关键字段，保持数据干净整洁
            if not fields:
                fields = ('ts_code,ann_date,f_ann_date,end_date,comp_type,report_type,'
                         'net_profit,finan_exp,c_fr_sale_sg,c_paid_goods_s,c_paid_to_for_empl,'
                         'c_paid_for_taxes,c_inf_fr_operate_a,st_cash_out_act,n_cashflow_act,'
                         'c_disp_withdrwl_invest,c_recp_return_invest,c_pay_acq_const_fiolta,'
                         'c_paid_invest,stot_inflows_inv_act,stot_out_inv_act,n_cashflow_inv_act,'
                         'c_recp_borrow,c_recp_cap_contrib,c_prepay_amt_borr,c_pay_dist_dpcp_int_exp,'
                         'stot_cash_in_fnc_act,stot_cashout_fnc_act,n_cash_flows_fnc_act,'
                         'n_incr_cash_cash_equ,c_cash_equ_beg_period,c_cash_equ_end_period,free_cashflow')
            params['fields'] = fields
            
            # 限频
            await tushare_limiter.acquire()
            
            logger.info(f"获取现金流量表数据: {params}")
            
            # 获取现金流量表数据
            df = self.ts_pro.cashflow(**params)
            
            if df.empty:
                logger.warning(f"未获取到现金流量表数据: {ts_code}")
                return pd.DataFrame()
            
            # 数据清理和处理
            df = clean_dataframe(df)
            
            # 按报告期排序
            if 'end_date' in df.columns:
                df = df.sort_values('end_date').reset_index(drop=True)
            
            logger.info(f"成功获取 {len(df)} 条现金流量表数据")
            return df
            
        except Exception as e:
            logger.error(f"获取现金流量表数据失败: {e}")
            raise DataFlowException(f"获取现金流量表数据失败: {e}")
    
    async def get_financial_indicators(
        self,
        ts_code: str,
        ann_date: str = None,
        start_date: str = None,
        end_date: str = None,
        period: str = None,
        fields: str = None
    ) -> pd.DataFrame:
        """
        获取上市公司财务指标数据
        
        Args:
            ts_code: 股票代码 (如: 600001.SH/000001.SZ)
            ann_date: 公告日期，格式为 YYYYMMDD
            start_date: 报告期开始日期，格式为 YYYYMMDD
            end_date: 报告期结束日期，格式为 YYYYMMDD
            period: 报告期，格式为 YYYYMMDD (如: 20171231表示年报)
            fields: 指定获取的字段，用逗号分隔。默认获取关键字段
        
        Returns:
            pd.DataFrame: 包含以下关键字段的财务指标数据
                - ts_code (str): TS代码
                - ann_date (str): 公告日期
                - end_date (str): 报告期
                - eps (float): 基本每股收益
                - dt_eps (float): 稀释每股收益
                - revenue_ps (float): 每股营业收入
                - bps (float): 每股净资产
                - ocfps (float): 每股经营活动产生的现金流量净额
                - gross_margin (float): 毛利
                - netprofit_margin (float): 销售净利率
                - grossprofit_margin (float): 销售毛利率
                - roe (float): 净资产收益率
                - roe_waa (float): 加权平均净资产收益率
                - roe_dt (float): 净资产收益率(扣除非经常损益)
                - roa (float): 总资产报酬率
                - roic (float): 投入资本回报率
                - current_ratio (float): 流动比率
                - quick_ratio (float): 速动比率
                - ar_turn (float): 应收账款周转率
                - ca_turn (float): 流动资产周转率
                - fa_turn (float): 固定资产周转率
                - assets_turn (float): 总资产周转率
                - debt_to_assets (float): 资产负债率
                - assets_to_eqt (float): 权益乘数
                - ebit (float): 息税前利润
                - ebitda (float): 息税折旧摊销前利润
                - fcff (float): 企业自由现金流量
                - fcfe (float): 股权自由现金流量
                - basic_eps_yoy (float): 基本每股收益同比增长率(%)
                - netprofit_yoy (float): 归属母公司股东的净利润同比增长率(%)
                - roe_yoy (float): 净资产收益率(摊薄)同比增长率(%)
                - tr_yoy (float): 营业总收入同比增长率(%)
                - or_yoy (float): 营业收入同比增长率(%)
        
        Raises:
            DataFlowException: 当 Tushare 未配置、股票代码无效或数据获取失败时
        
        Note:
            - 当前接口只能按单只股票获取其历史数据
            - 每次请求最多返回100条记录
            - 用户需要至少2000积分才可以调取
        """
        if not self.tushare_enabled:
            raise DataFlowException("Tushare未配置或未启用")
        
        if not validate_stock_code(ts_code, 'cn'):
            raise DataFlowException(f"无效的股票代码: {ts_code}")
        
        try:
            # 构建请求参数
            params = {'ts_code': ts_code}
            
            if ann_date:
                params['ann_date'] = format_date(ann_date, 'tushare')
            if start_date:
                params['start_date'] = format_date(start_date, 'tushare')
            if end_date:
                params['end_date'] = format_date(end_date, 'tushare')
            if period:
                params['period'] = format_date(period, 'tushare')
            
            # 设置关键字段，保持数据干净整洁
            if not fields:
                fields = ('ts_code,ann_date,end_date,eps,dt_eps,revenue_ps,bps,ocfps,'
                         'gross_margin,netprofit_margin,grossprofit_margin,roe,roe_waa,'
                         'roe_dt,roa,roic,current_ratio,quick_ratio,ar_turn,ca_turn,'
                         'fa_turn,assets_turn,debt_to_assets,assets_to_eqt,ebit,ebitda,'
                         'fcff,fcfe,basic_eps_yoy,netprofit_yoy,roe_yoy,tr_yoy,or_yoy')
            params['fields'] = fields
            
            # 限频
            await tushare_limiter.acquire()
            
            logger.info(f"获取财务指标数据: {params}")
            
            # 获取财务指标数据
            df = self.ts_pro.fina_indicator(**params)
            
            if df.empty:
                logger.warning(f"未获取到财务指标数据: {ts_code}")
                return pd.DataFrame()
            
            # 数据清理和处理
            df = clean_dataframe(df)
            
            # 按报告期排序
            if 'end_date' in df.columns:
                df = df.sort_values('end_date').reset_index(drop=True)
            
            logger.info(f"成功获取 {len(df)} 条财务指标数据")
            return df
            
        except Exception as e:
            logger.error(f"获取财务指标数据失败: {e}")
            raise DataFlowException(f"获取财务指标数据失败: {e}")
    