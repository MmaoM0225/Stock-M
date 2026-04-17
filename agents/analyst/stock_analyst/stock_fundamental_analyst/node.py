"""
Stock Fundamental Analyst（股票基本面分析师）- 节点实现

当前版本聚焦两步：
1. 基础数据获取：公司信息 + 估值快照
2. LLM 解读：对公司基本信息做结构化摘要，供后续节点使用
3. 持久化：将分析结果存储到本地 artifacts，支持缓存复用
"""
from __future__ import annotations

import json
import logging
import os
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd
from langchain_core.runnables import RunnableConfig

from ....utils import date_offset, extract_json_text, to_serializable

logger = logging.getLogger(__name__)

# 存储路径：data/artifacts/analyst/stock_analyst/stock_fundamental_analyst/{ts_code}/{trade_date}/result.json
_FUNDAMENTAL_ANALYST_ARTIFACT_ROOT = (
    Path("data") / "artifacts" / "analyst" / "stock_analyst" / "stock_fundamental_analyst"
)


def _write_json_atomic(path: Path, payload: Dict[str, Any]) -> None:
    """原子写入 JSON，避免中途中断留下半成品。"""
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_suffix(path.suffix + ".tmp")
    with open(tmp_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
    os.replace(tmp_path, path)


def _load_json_file(path: Path) -> Any:
    """读取 JSON 文件并返回解析结果。"""
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def _build_fundamental_result_path(ts_code: str, trade_date: str) -> Path:
    """构建基本面分析师结果文件路径。"""
    return _FUNDAMENTAL_ANALYST_ARTIFACT_ROOT / ts_code / trade_date / "result.json"


def _build_fundamental_manifest_path(ts_code: str, trade_date: str) -> Path:
    """构建基本面分析师 manifest 文件路径。"""
    return _FUNDAMENTAL_ANALYST_ARTIFACT_ROOT / ts_code / trade_date / "manifest.json"


def _norm_date(s: Optional[str]) -> str:
    if not s:
        return ""
    return str(s).replace("-", "")[:8]


def _scalar(x: Any) -> Any:
    if x is None or (isinstance(x, float) and pd.isna(x)):
        return None
    if hasattr(x, "item"):
        try:
            return x.item()
        except Exception:
            pass
    return x


def _latest_daily(daily_rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    if not daily_rows:
        return {}
    return dict(max(daily_rows, key=lambda r: _norm_date(r.get("trade_date"))))


def _safe_ratio(num: Any, den: Any) -> Optional[float]:
    try:
        n = float(num)
        d = float(den)
        if d == 0:
            return None
        return round(n / d, 4)
    except Exception:
        return None


def _pick_latest_income(rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    if not rows:
        return {}
    return dict(max(rows, key=lambda r: _norm_date(r.get("end_date"))))


def _pick_prev_income(rows: List[Dict[str, Any]], latest_end_date: str) -> Dict[str, Any]:
    if not rows:
        return {}
    candidates = [r for r in rows if _norm_date(r.get("end_date")) < latest_end_date]
    if not candidates:
        return {}
    return dict(max(candidates, key=lambda r: _norm_date(r.get("end_date"))))


def _pick_latest_cashflow(rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    if not rows:
        return {}
    return dict(max(rows, key=lambda r: _norm_date(r.get("end_date"))))


def _pick_prev_cashflow(rows: List[Dict[str, Any]], latest_end_date: str) -> Dict[str, Any]:
    if not rows:
        return {}
    candidates = [r for r in rows if _norm_date(r.get("end_date")) < latest_end_date]
    if not candidates:
        return {}
    return dict(max(candidates, key=lambda r: _norm_date(r.get("end_date"))))


def _pick_latest_balancesheet(rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    if not rows:
        return {}
    return dict(max(rows, key=lambda r: _norm_date(r.get("end_date"))))


def _pick_prev_balancesheet(rows: List[Dict[str, Any]], latest_end_date: str) -> Dict[str, Any]:
    if not rows:
        return {}
    candidates = [r for r in rows if _norm_date(r.get("end_date")) < latest_end_date]
    if not candidates:
        return {}
    return dict(max(candidates, key=lambda r: _norm_date(r.get("end_date"))))


def _pick_latest_dividend(rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    if not rows:
        return {}
    # 优先按分红年度+公告日取最新
    return dict(
        max(
            rows,
            key=lambda r: (_norm_date(r.get("end_date")), _norm_date(r.get("ann_date"))),
        )
    )


def _pick_prev_dividend(rows: List[Dict[str, Any]], latest_end_date: str, latest_ann_date: str) -> Dict[str, Any]:
    if not rows:
        return {}
    candidates = [
        r for r in rows
        if (_norm_date(r.get("end_date")), _norm_date(r.get("ann_date")))
        < (latest_end_date, latest_ann_date)
    ]
    if not candidates:
        return {}
    return dict(
        max(
            candidates,
            key=lambda r: (_norm_date(r.get("end_date")), _norm_date(r.get("ann_date"))),
        )
    )


def create_stock_fundamental_fetch_node():
    """拉取公司基础信息（stock_company）与估值快照（daily_basic）。"""

    def stock_fundamental_fetch_node(
        state: Dict[str, Any],
        config: Optional[RunnableConfig] = None,
    ) -> Dict[str, Any]:
        _ = config
        raw_code = (state.get("ts_code") or "").strip()
        if not raw_code:
            return {
                "stock_fundamental_meta": {"error": "missing ts_code"},
                "stock_company_info": {},
                "stock_fundamental_daily": [],
                "stock_income_data": [],
                "stock_cashflow_data": [],
                "stock_balancesheet_data": [],
                "stock_dividend_data": [],
            }

        try:
            from dataflow.utils import normalize_cn_ts_code

            ts_code = normalize_cn_ts_code(raw_code)
        except ValueError as e:
            return {
                "stock_fundamental_meta": {"error": str(e), "raw_ts_code": raw_code},
                "stock_company_info": {},
                "stock_fundamental_daily": [],
                "stock_income_data": [],
                "stock_cashflow_data": [],
                "stock_balancesheet_data": [],
                "stock_dividend_data": [],
            }

        trade_date = _norm_date(state.get("trade_date"))
        if not trade_date:
            from datetime import datetime

            trade_date = datetime.now().strftime("%Y%m%d")

        company_info: Dict[str, Any] = {}
        daily_records: List[Dict[str, Any]] = []
        income_records: List[Dict[str, Any]] = []
        cashflow_records: List[Dict[str, Any]] = []
        balancesheet_records: List[Dict[str, Any]] = []
        dividend_records: List[Dict[str, Any]] = []
        try:
            from dataflow.fundamental_data import FundamentalDataFetcher

            fetcher = FundamentalDataFetcher()
            company_df = fetcher.fetch_company_info(
                ts_code=ts_code,
                fields=(
                    "ts_code,com_name,com_id,exchange,chairman,manager,secretary,"
                    "reg_capital,setup_date,province,city,website,email,employees,"
                    "main_business,business_scope"
                ),
            )
            if company_df is not None and not company_df.empty:
                company_info = dict(company_df.iloc[0].to_dict())

            start_d = date_offset(trade_date, days=120)
            daily_df = fetcher.fetch_daily_basic(
                ts_code=ts_code,
                start_date=start_d,
                end_date=trade_date,
                fields=(
                    "ts_code,trade_date,close,turnover_rate,volume_ratio,pe,pe_ttm,pb,"
                    "ps,ps_ttm,dv_ratio,dv_ttm,total_mv,circ_mv"
                ),
            )
            if daily_df is not None and not daily_df.empty:
                daily_df = daily_df.sort_values("trade_date").tail(20)
                daily_records = to_serializable(daily_df) or []

            income_df = fetcher.fetch_income_statement(
                ts_code=ts_code,
                fields=(
                    "ts_code,ann_date,f_ann_date,end_date,report_type,comp_type,"
                    "basic_eps,diluted_eps,total_revenue,revenue,total_cogs,oper_cost,"
                    "sell_exp,admin_exp,fin_exp,rd_exp,operate_profit,total_profit,"
                    "income_tax,n_income,n_income_attr_p,ebit,ebitda"
                ),
            )
            if income_df is not None and not income_df.empty:
                income_df = income_df.sort_values("end_date").tail(12)
                income_records = to_serializable(income_df) or []

            cashflow_df = fetcher.fetch_cashflow_statement(
                ts_code=ts_code,
                fields=(
                    "ts_code,ann_date,f_ann_date,end_date,comp_type,report_type,"
                    "net_profit,c_inf_fr_operate_a,st_cash_out_act,n_cashflow_act,"
                    "stot_inflows_inv_act,stot_out_inv_act,n_cashflow_inv_act,"
                    "stot_cash_in_fnc_act,stot_cashout_fnc_act,n_cash_flows_fnc_act,"
                    "free_cashflow,c_cash_equ_beg_period,c_cash_equ_end_period,"
                    "n_incr_cash_cash_equ,c_pay_acq_const_fiolta,c_paid_invest,"
                    "c_recp_borrow,c_prepay_amt_borr,c_pay_dist_dpcp_int_exp"
                ),
            )
            if cashflow_df is not None and not cashflow_df.empty:
                cashflow_df = cashflow_df.sort_values("end_date").tail(12)
                cashflow_records = to_serializable(cashflow_df) or []

            bs_df = fetcher.fetch_balance_sheet(
                ts_code=ts_code,
                fields=(
                    "ts_code,ann_date,f_ann_date,end_date,report_type,comp_type,"
                    "money_cap,accounts_receiv,inventories,total_cur_assets,total_nca,total_assets,"
                    "st_borr,lt_borr,notes_payable,acct_payable,total_cur_liab,total_ncl,total_liab,"
                    "total_hldr_eqy_exc_min_int,total_hldr_eqy_inc_min_int,goodwill,intan_assets,"
                    "contract_liab,lease_liab"
                ),
            )
            if bs_df is not None and not bs_df.empty:
                bs_df = bs_df.sort_values("end_date").tail(12)
                balancesheet_records = to_serializable(bs_df) or []

            div_df = fetcher.fetch_dividend(
                ts_code=ts_code,
                fields=(
                    "ts_code,end_date,ann_date,div_proc,stk_div,stk_bo_rate,stk_co_rate,"
                    "cash_div,cash_div_tax,record_date,ex_date,pay_date,div_listdate,"
                    "imp_ann_date,base_date,base_share"
                ),
            )
            if div_df is not None and not div_df.empty:
                # 分红记录较稀疏，保留更多历史
                div_df = div_df.sort_values(["end_date", "ann_date"]).tail(20)
                dividend_records = to_serializable(div_df) or []
        except Exception as e:
            logger.warning("基础数据拉取失败 ts_code=%s: %s", ts_code, e)

        return {
            "ts_code": ts_code,
            "stock_fundamental_meta": {
                "ts_code": ts_code,
                "trade_date": trade_date,
                "company_info_ready": bool(company_info),
                "valuation_ready": bool(daily_records),
                "income_ready": bool(income_records),
                "cashflow_ready": bool(cashflow_records),
                "balancesheet_ready": bool(balancesheet_records),
                "dividend_ready": bool(dividend_records),
            },
            "stock_company_info": company_info,
            "stock_fundamental_daily": daily_records,
            "stock_income_data": income_records,
            "stock_cashflow_data": cashflow_records,
            "stock_balancesheet_data": balancesheet_records,
            "stock_dividend_data": dividend_records,
        }

    return stock_fundamental_fetch_node


def create_stock_fundamental_analysis_node():
    """封装基础事实，供后续专题节点复用。"""

    def stock_fundamental_analysis_node(
        state: Dict[str, Any],
        config: Optional[RunnableConfig] = None,
    ) -> Dict[str, Any]:
        _ = config
        meta = state.get("stock_fundamental_meta") or {}
        if isinstance(meta, dict) and meta.get("error"):
            return {
                "stock_fundamental_facts": None,
                "fundamental_base_profile": {
                    "ts_code": state.get("ts_code"),
                    "error": meta.get("error"),
                },
            }

        ts_code = (meta.get("ts_code") or state.get("ts_code") or "").strip()
        company_info: Dict[str, Any] = dict(state.get("stock_company_info") or {})
        daily_rows: List[Dict[str, Any]] = list(state.get("stock_fundamental_daily") or [])
        income_rows: List[Dict[str, Any]] = list(state.get("stock_income_data") or [])
        cashflow_rows: List[Dict[str, Any]] = list(state.get("stock_cashflow_data") or [])
        bs_rows: List[Dict[str, Any]] = list(state.get("stock_balancesheet_data") or [])
        div_rows: List[Dict[str, Any]] = list(state.get("stock_dividend_data") or [])

        latest_daily = _latest_daily(daily_rows)
        valuation_snapshot = {
            "trade_date": latest_daily.get("trade_date"),
            "close": _scalar(latest_daily.get("close")),
            "pe": _scalar(latest_daily.get("pe")),
            "pe_ttm": _scalar(latest_daily.get("pe_ttm")),
            "pb": _scalar(latest_daily.get("pb")),
            "ps": _scalar(latest_daily.get("ps")),
            "ps_ttm": _scalar(latest_daily.get("ps_ttm")),
            "dv_ratio": _scalar(latest_daily.get("dv_ratio")),
            "dv_ttm": _scalar(latest_daily.get("dv_ttm")),
            "turnover_rate": _scalar(latest_daily.get("turnover_rate")),
            "volume_ratio": _scalar(latest_daily.get("volume_ratio")),
            "total_mv": _scalar(latest_daily.get("total_mv")),
            "circ_mv": _scalar(latest_daily.get("circ_mv")),
        }

        company_profile = {
            "ts_code": company_info.get("ts_code") or ts_code,
            "com_name": company_info.get("com_name"),
            "com_id": company_info.get("com_id"),
            "exchange": company_info.get("exchange"),
            "chairman": company_info.get("chairman"),
            "manager": company_info.get("manager"),
            "secretary": company_info.get("secretary"),
            "reg_capital": _scalar(company_info.get("reg_capital")),
            "setup_date": company_info.get("setup_date"),
            "province": company_info.get("province"),
            "city": company_info.get("city"),
            "website": company_info.get("website"),
            "email": company_info.get("email"),
            "employees": _scalar(company_info.get("employees")),
            "main_business": company_info.get("main_business"),
            "business_scope": company_info.get("business_scope"),
        }

        latest_income = _pick_latest_income(income_rows)
        latest_end_date = _norm_date(latest_income.get("end_date"))
        prev_income = _pick_prev_income(income_rows, latest_end_date) if latest_end_date else {}

        rev_latest = _scalar(latest_income.get("revenue"))
        rev_prev = _scalar(prev_income.get("revenue"))
        np_latest = _scalar(latest_income.get("n_income_attr_p"))
        np_prev = _scalar(prev_income.get("n_income_attr_p"))
        op_latest = _scalar(latest_income.get("operate_profit"))
        ebitda_latest = _scalar(latest_income.get("ebitda"))
        rd_latest = _scalar(latest_income.get("rd_exp"))
        sell_latest = _scalar(latest_income.get("sell_exp"))
        admin_latest = _scalar(latest_income.get("admin_exp"))
        fin_latest = _scalar(latest_income.get("fin_exp"))
        oper_cost_latest = _scalar(latest_income.get("oper_cost"))

        income_snapshot = {
            "end_date": latest_income.get("end_date"),
            "ann_date": latest_income.get("ann_date"),
            "report_type": latest_income.get("report_type"),
            "comp_type": latest_income.get("comp_type"),
            "revenue": rev_latest,
            "total_revenue": _scalar(latest_income.get("total_revenue")),
            "operate_profit": op_latest,
            "total_profit": _scalar(latest_income.get("total_profit")),
            "net_profit_attr_parent": np_latest,
            "basic_eps": _scalar(latest_income.get("basic_eps")),
            "diluted_eps": _scalar(latest_income.get("diluted_eps")),
            "ebit": _scalar(latest_income.get("ebit")),
            "ebitda": ebitda_latest,
            "revenue_change_vs_prev": (
                round((float(rev_latest) - float(rev_prev)) / abs(float(rev_prev)), 4)
                if rev_latest is not None and rev_prev not in (None, 0)
                else None
            ),
            "net_profit_change_vs_prev": (
                round((float(np_latest) - float(np_prev)) / abs(float(np_prev)), 4)
                if np_latest is not None and np_prev not in (None, 0)
                else None
            ),
            "operating_margin": _safe_ratio(op_latest, rev_latest),
            "net_margin": _safe_ratio(np_latest, rev_latest),
            "ebitda_margin": _safe_ratio(ebitda_latest, rev_latest),
            "rd_ratio": _safe_ratio(rd_latest, rev_latest),
            "sell_exp_ratio": _safe_ratio(sell_latest, rev_latest),
            "admin_exp_ratio": _safe_ratio(admin_latest, rev_latest),
            "fin_exp_ratio": _safe_ratio(fin_latest, rev_latest),
            "oper_cost_ratio": _safe_ratio(oper_cost_latest, rev_latest),
        }

        latest_cf = _pick_latest_cashflow(cashflow_rows)
        latest_cf_end_date = _norm_date(latest_cf.get("end_date"))
        prev_cf = _pick_prev_cashflow(cashflow_rows, latest_cf_end_date) if latest_cf_end_date else {}

        cfo_latest = _scalar(latest_cf.get("n_cashflow_act"))
        cfo_prev = _scalar(prev_cf.get("n_cashflow_act"))
        fcf_latest = _scalar(latest_cf.get("free_cashflow"))
        fcf_prev = _scalar(prev_cf.get("free_cashflow"))
        cash_end = _scalar(latest_cf.get("c_cash_equ_end_period"))
        cash_beg = _scalar(latest_cf.get("c_cash_equ_beg_period"))
        inv_out = _scalar(latest_cf.get("stot_out_inv_act"))
        fin_in = _scalar(latest_cf.get("stot_cash_in_fnc_act"))
        fin_out = _scalar(latest_cf.get("stot_cashout_fnc_act"))

        cashflow_snapshot = {
            "end_date": latest_cf.get("end_date"),
            "ann_date": latest_cf.get("ann_date"),
            "report_type": latest_cf.get("report_type"),
            "comp_type": latest_cf.get("comp_type"),
            "net_profit": _scalar(latest_cf.get("net_profit")),
            "operating_cash_inflow": _scalar(latest_cf.get("c_inf_fr_operate_a")),
            "operating_cash_outflow": _scalar(latest_cf.get("st_cash_out_act")),
            "n_cashflow_act": cfo_latest,
            "n_cashflow_inv_act": _scalar(latest_cf.get("n_cashflow_inv_act")),
            "n_cash_flows_fnc_act": _scalar(latest_cf.get("n_cash_flows_fnc_act")),
            "free_cashflow": fcf_latest,
            "cash_equ_beg": cash_beg,
            "cash_equ_end": cash_end,
            "n_incr_cash_cash_equ": _scalar(latest_cf.get("n_incr_cash_cash_equ")),
            "capex_cash_out": _scalar(latest_cf.get("c_pay_acq_const_fiolta")),
            "invest_cash_out": _scalar(latest_cf.get("c_paid_invest")),
            "borrow_cash_in": _scalar(latest_cf.get("c_recp_borrow")),
            "debt_repay_cash_out": _scalar(latest_cf.get("c_prepay_amt_borr")),
            "dividend_interest_cash_out": _scalar(latest_cf.get("c_pay_dist_dpcp_int_exp")),
            "cfo_change_vs_prev": (
                round((float(cfo_latest) - float(cfo_prev)) / abs(float(cfo_prev)), 4)
                if cfo_latest is not None and cfo_prev not in (None, 0)
                else None
            ),
            "fcf_change_vs_prev": (
                round((float(fcf_latest) - float(fcf_prev)) / abs(float(fcf_prev)), 4)
                if fcf_latest is not None and fcf_prev not in (None, 0)
                else None
            ),
            "cash_conversion": _safe_ratio(cfo_latest, np_latest),
            "fcf_margin": _safe_ratio(fcf_latest, rev_latest),
            "cfo_margin": _safe_ratio(cfo_latest, rev_latest),
            "investment_intensity": _safe_ratio(inv_out, rev_latest),
            "financing_dependency": _safe_ratio(fin_in, fin_out),
        }

        latest_bs = _pick_latest_balancesheet(bs_rows)
        latest_bs_end = _norm_date(latest_bs.get("end_date"))
        prev_bs = _pick_prev_balancesheet(bs_rows, latest_bs_end) if latest_bs_end else {}

        total_assets = _scalar(latest_bs.get("total_assets"))
        total_liab = _scalar(latest_bs.get("total_liab"))
        current_assets = _scalar(latest_bs.get("total_cur_assets"))
        current_liab = _scalar(latest_bs.get("total_cur_liab"))
        cash = _scalar(latest_bs.get("money_cap"))
        ar = _scalar(latest_bs.get("accounts_receiv"))
        inv = _scalar(latest_bs.get("inventories"))
        st_borr = _scalar(latest_bs.get("st_borr"))
        lt_borr = _scalar(latest_bs.get("lt_borr"))
        eq_parent = _scalar(latest_bs.get("total_hldr_eqy_exc_min_int"))
        goodwill = _scalar(latest_bs.get("goodwill"))
        intan = _scalar(latest_bs.get("intan_assets"))
        lease_liab = _scalar(latest_bs.get("lease_liab"))
        contract_liab = _scalar(latest_bs.get("contract_liab"))

        prev_total_assets = _scalar(prev_bs.get("total_assets"))
        prev_total_liab = _scalar(prev_bs.get("total_liab"))

        balancesheet_snapshot = {
            "end_date": latest_bs.get("end_date"),
            "ann_date": latest_bs.get("ann_date"),
            "report_type": latest_bs.get("report_type"),
            "comp_type": latest_bs.get("comp_type"),
            "total_assets": total_assets,
            "total_liab": total_liab,
            "equity_parent": eq_parent,
            "current_assets": current_assets,
            "current_liab": current_liab,
            "cash": cash,
            "accounts_receiv": ar,
            "inventories": inv,
            "st_borr": st_borr,
            "lt_borr": lt_borr,
            "goodwill": goodwill,
            "intan_assets": intan,
            "lease_liab": lease_liab,
            "contract_liab": contract_liab,
            "asset_growth_vs_prev": (
                round((float(total_assets) - float(prev_total_assets)) / abs(float(prev_total_assets)), 4)
                if total_assets is not None and prev_total_assets not in (None, 0)
                else None
            ),
            "liab_growth_vs_prev": (
                round((float(total_liab) - float(prev_total_liab)) / abs(float(prev_total_liab)), 4)
                if total_liab is not None and prev_total_liab not in (None, 0)
                else None
            ),
            "debt_to_assets": _safe_ratio(total_liab, total_assets),
            "current_ratio": _safe_ratio(current_assets, current_liab),
            "cash_ratio": _safe_ratio(cash, current_liab),
            "working_capital": (
                round(float(current_assets) - float(current_liab), 2)
                if current_assets is not None and current_liab is not None
                else None
            ),
            "interest_bearing_debt": (
                round(float(st_borr or 0) + float(lt_borr or 0), 2)
                if st_borr is not None or lt_borr is not None
                else None
            ),
            "debt_to_equity": _safe_ratio(
                (float(st_borr or 0) + float(lt_borr or 0)) if (st_borr is not None or lt_borr is not None) else None,
                eq_parent,
            ),
            "receivable_asset_ratio": _safe_ratio(ar, total_assets),
            "inventory_asset_ratio": _safe_ratio(inv, total_assets),
            "goodwill_equity_ratio": _safe_ratio(goodwill, eq_parent),
        }

        latest_div = _pick_latest_dividend(div_rows)
        latest_div_end = _norm_date(latest_div.get("end_date"))
        latest_div_ann = _norm_date(latest_div.get("ann_date"))
        prev_div = (
            _pick_prev_dividend(div_rows, latest_div_end, latest_div_ann)
            if latest_div_end or latest_div_ann
            else {}
        )

        cash_div_tax = _scalar(latest_div.get("cash_div_tax"))
        stk_div = _scalar(latest_div.get("stk_div"))
        base_share = _scalar(latest_div.get("base_share"))
        prev_cash_div_tax = _scalar(prev_div.get("cash_div_tax"))
        prev_stk_div = _scalar(prev_div.get("stk_div"))

        dividend_snapshot = {
            "end_date": latest_div.get("end_date"),
            "ann_date": latest_div.get("ann_date"),
            "div_proc": latest_div.get("div_proc"),
            "stk_div": stk_div,
            "stk_bo_rate": _scalar(latest_div.get("stk_bo_rate")),
            "stk_co_rate": _scalar(latest_div.get("stk_co_rate")),
            "cash_div": _scalar(latest_div.get("cash_div")),
            "cash_div_tax": cash_div_tax,
            "record_date": latest_div.get("record_date"),
            "ex_date": latest_div.get("ex_date"),
            "pay_date": latest_div.get("pay_date"),
            "div_listdate": latest_div.get("div_listdate"),
            "imp_ann_date": latest_div.get("imp_ann_date"),
            "base_date": latest_div.get("base_date"),
            "base_share": base_share,
            "estimated_cash_div_total": (
                round(float(cash_div_tax) * float(base_share) * 10000, 2)
                if cash_div_tax is not None and base_share is not None
                else None
            ),
            "cash_div_change_vs_prev": (
                round((float(cash_div_tax) - float(prev_cash_div_tax)) / abs(float(prev_cash_div_tax)), 4)
                if cash_div_tax is not None and prev_cash_div_tax not in (None, 0)
                else None
            ),
            "stk_div_change_vs_prev": (
                round((float(stk_div) - float(prev_stk_div)) / abs(float(prev_stk_div)), 4)
                if stk_div is not None and prev_stk_div not in (None, 0)
                else None
            ),
        }

        facts: Dict[str, Any] = {
            "ts_code": ts_code,
            "trade_date": meta.get("trade_date"),
            "analysis_dimensions": {
                "company_profile": "上市公司基础信息（管理层、注册信息、地域、主营业务）",
                "valuation": "估值与交易快照（PE/PB/PS/股息率/市值/换手）",
                "income_statement": "利润表核心指标（收入、利润、费用结构、利润率）",
                "cashflow_statement": "现金流量表核心指标（经营/投资/筹资现金流、自由现金流、现金质量）",
                "balancesheet": "资产负债表核心指标（资产结构、偿债能力、负债结构、资产质量）",
                "dividend": "分红送股指标（现金分红、送转、实施进度、分红稳定性）",
            },
            "company_profile": {k: v for k, v in company_profile.items() if v is not None},
            "valuation_snapshot": {k: v for k, v in valuation_snapshot.items() if v is not None},
            "valuation_history": daily_rows[-20:] if daily_rows else [],
            "income_snapshot": {k: v for k, v in income_snapshot.items() if v is not None},
            "income_history": income_rows[-12:] if income_rows else [],
            "cashflow_snapshot": {k: v for k, v in cashflow_snapshot.items() if v is not None},
            "cashflow_history": cashflow_rows[-12:] if cashflow_rows else [],
            "balancesheet_snapshot": {k: v for k, v in balancesheet_snapshot.items() if v is not None},
            "balancesheet_history": bs_rows[-12:] if bs_rows else [],
            "dividend_snapshot": {k: v for k, v in dividend_snapshot.items() if v is not None},
            "dividend_history": div_rows[-20:] if div_rows else [],
        }

        base_profile = {
            "ts_code": ts_code,
            "company_profile": facts["company_profile"],
            "valuation_snapshot": facts["valuation_snapshot"],
            "income_snapshot": facts["income_snapshot"],
            "cashflow_snapshot": facts["cashflow_snapshot"],
            "balancesheet_snapshot": facts["balancesheet_snapshot"],
            "dividend_snapshot": facts["dividend_snapshot"],
            "summary": "已完成公司基础信息与估值快照汇总，可供后续专题节点分析。",
        }

        return {
            "stock_fundamental_facts": facts,
            "fundamental_base_profile": base_profile,
        }

    return stock_fundamental_analysis_node


def create_company_basic_insight_node(llm):
    """LLM 将 stock_company 字段转成可复用公司描述文本。"""

    def company_basic_insight_node(
        state: Dict[str, Any],
        config: Optional[RunnableConfig] = None,
    ) -> Dict[str, Any]:
        facts = state.get("stock_fundamental_facts")
        meta = state.get("stock_fundamental_meta") or {}
        if not facts or not isinstance(facts, dict):
            return {
                "company_profile_text": "",
                "company_basic_analysis": {
                    "ts_code": meta.get("ts_code") or state.get("ts_code"),
                    "error": "无有效基础事实数据",
                }
            }

        from langchain_core.prompts import ChatPromptTemplate

        profile_json = json.dumps(
            {
                "ts_code": facts.get("ts_code"),
                "company_profile": facts.get("company_profile") or {},
            },
            ensure_ascii=False,
            indent=2,
        )

        system_msg = """你是A股公司资料整理助手。请把输入的公司基础字段（stock_company）整理成一段可供下游分析复用的中文描述。
要求：
1) 只基于输入字段，不编造；
2) 字数 80~180；
3) 重点包含：公司身份、地域与注册信息、管理层、主营业务；
4) 若字段缺失，用“信息未披露/数据缺失”简短说明。
仅输出 JSON。"""
        human_msg = """输入：
{profile}

请输出 JSON，包含键：
- ts_code
- company_profile_text（单段中文）
- key_facts（字符串数组，3-6条）
"""
        prompt = ChatPromptTemplate.from_messages(
            [("system", system_msg), ("human", human_msg)]
        )

        chain = prompt | llm
        raw = chain.invoke(
            {"profile": profile_json},
            config={**(config or {}), "run_name": "公司基础描述生成"},
        )
        data = extract_json_text(raw) or {}

        out = {
            "ts_code": data.get("ts_code") or facts.get("ts_code"),
            "company_profile_text": data.get("company_profile_text") or "",
            "key_facts": data.get("key_facts") or [],
        }
        return {
            "company_profile_text": out["company_profile_text"],
            "company_basic_analysis": out,
        }

    return company_basic_insight_node


def create_valuation_map_node(llm):
    """估值 map 节点：消费公司描述文本 + 估值快照，输出估值分析。"""

    def valuation_map_node(
        state: Dict[str, Any],
        config: Optional[RunnableConfig] = None,
    ) -> Dict[str, Any]:
        facts = state.get("stock_fundamental_facts") or {}
        ts_code = facts.get("ts_code") or state.get("ts_code")
        valuation_snapshot = facts.get("valuation_snapshot") or {}
        profile_text = (state.get("company_profile_text") or "").strip()

        if not valuation_snapshot:
            return {
                "valuation_map_analysis": {
                    "ts_code": ts_code,
                    "error": "无估值快照数据",
                }
            }

        from langchain_core.prompts import ChatPromptTemplate

        system_msg = """你是数据驱动的A股估值快照分析师。你会收到公司描述文本与单日估值快照。
目标：完成“估值快照体检”，输出可供后续 reduce 节点汇总的结构化结论。

分析顺序（必须覆盖）：
1) 数据识别：优先使用滚动口径（pe_ttm、ps_ttm、dv_ttm），并记录缺失字段；
2) 单项解读：对 PE/PE_TTM、PB、PS/PS_TTM、股息率、换手率、量比、总市值逐项解释；
3) 交叉验证：检查估值与交易活跃度是否一致，识别可能矛盾（如高PE+低PB、高市值+高换手）；
4) 综合画像：给出公司在市场中的估值类型与情绪状态，并给出稳健/激进两类投资者的观察角度。

严格约束：
- 仅基于输入字段，不得编造行业分位、历史分位、DCF结果或隐含增长率数值；
- 允许给“可能性解释”，但必须用“可能/倾向/需验证”措辞；
- 缺失数据要明确写“数据不足”；
- 输出必须是严格 JSON，不要 Markdown。"""
        human_msg = """公司描述：
{profile_text}

估值快照：
{valuation}

请输出 JSON，包含键：
- ts_code
- valuation_level（低估|合理|偏高|过热|数据不足）
- metric_interpretation（对象；键含 pe_ttm/pb/ps_ttm/dv_ttm/turnover_rate/volume_ratio/total_mv，每项1-2句）
- contradiction_checks（字符串数组，2-5条；描述指标之间是否背离及可能含义）
- market_sentiment_judgement（偏冷|中性|偏热|过热|数据不足）
- investor_views（对象：conservative/aggressive 两个键，各1-2句）
- key_points（字符串数组，3-6条）
- risks（字符串数组，1-4条；无则空数组）
- summary（一句话总结）
"""
        prompt = ChatPromptTemplate.from_messages(
            [("system", system_msg), ("human", human_msg)]
        )
        chain = prompt | llm
        raw = chain.invoke(
            {
                "profile_text": profile_text or "公司描述缺失",
                "valuation": json.dumps(valuation_snapshot, ensure_ascii=False, indent=2),
            },
            config={**(config or {}), "run_name": "估值Map分析"},
        )
        data = extract_json_text(raw) or {}
        out = {
            "ts_code": data.get("ts_code") or ts_code,
            "valuation_level": data.get("valuation_level") or "数据不足",
            "metric_interpretation": data.get("metric_interpretation") or {},
            "contradiction_checks": data.get("contradiction_checks") or [],
            "market_sentiment_judgement": data.get("market_sentiment_judgement") or "数据不足",
            "investor_views": data.get("investor_views") or {},
            "relative_valuation": data.get("relative_valuation") or "",
            "key_points": data.get("key_points") or [],
            "risks": data.get("risks") or [],
            "data_gaps": data.get("data_gaps") or [],
            "summary": data.get("summary") or "",
        }
        return {"valuation_map_analysis": out}

    return valuation_map_node


def create_income_map_node(llm):
    """利润表 map 节点：对 income_snapshot 做盈利质量与费用结构分析。"""

    def income_map_node(
        state: Dict[str, Any],
        config: Optional[RunnableConfig] = None,
    ) -> Dict[str, Any]:
        facts = state.get("stock_fundamental_facts") or {}
        ts_code = facts.get("ts_code") or state.get("ts_code")
        income_snapshot = facts.get("income_snapshot") or {}
        profile_text = (state.get("company_profile_text") or "").strip()

        if not income_snapshot:
            return {
                "income_map_analysis": {
                    "ts_code": ts_code,
                    "error": "无利润表快照数据",
                }
            }

        from langchain_core.prompts import ChatPromptTemplate

        system_msg = """你是A股利润表分析师。请仅基于输入的利润表快照和公司描述做结构化分析。
重点：
1) 收入与利润质量（收入、营业利润、归母净利润及其变化）；
2) 费用结构（销售/管理/财务/研发费用率）；
3) 利润率（营业利润率、净利率、EBITDA利润率）。
不得编造行业对比、历史分位或未来预测。输出严格 JSON。"""
        human_msg = """公司描述：
{profile_text}

利润表快照：
{income_snapshot}

请输出 JSON，包含键：
- ts_code
- profitability_quality（优秀|良好|一般|偏弱|数据不足）
- growth_signal（改善|平稳|走弱|数据不足）
- cost_control_signal（优秀|良好|一般|偏弱|数据不足）
- margin_comment（2-4句）
- key_points（字符串数组，3-6条）
- risks（字符串数组，1-4条；无则空数组）
- summary（一句话总结）
"""
        prompt = ChatPromptTemplate.from_messages(
            [("system", system_msg), ("human", human_msg)]
        )
        chain = prompt | llm
        raw = chain.invoke(
            {
                "profile_text": profile_text or "公司描述缺失",
                "income_snapshot": json.dumps(income_snapshot, ensure_ascii=False, indent=2),
            },
            config={**(config or {}), "run_name": "利润表Map分析"},
        )
        data = extract_json_text(raw) or {}
        out = {
            "ts_code": data.get("ts_code") or ts_code,
            "profitability_quality": data.get("profitability_quality") or "数据不足",
            "growth_signal": data.get("growth_signal") or "数据不足",
            "cost_control_signal": data.get("cost_control_signal") or "数据不足",
            "margin_comment": data.get("margin_comment") or "",
            "key_points": data.get("key_points") or [],
            "risks": data.get("risks") or [],
            "summary": data.get("summary") or "",
        }
        return {"income_map_analysis": out}

    return income_map_node


def create_cashflow_map_node(llm):
    """现金流量表 map 节点：分析现金流质量、资本开支与融资依赖。"""

    def cashflow_map_node(
        state: Dict[str, Any],
        config: Optional[RunnableConfig] = None,
    ) -> Dict[str, Any]:
        facts = state.get("stock_fundamental_facts") or {}
        ts_code = facts.get("ts_code") or state.get("ts_code")
        cashflow_snapshot = facts.get("cashflow_snapshot") or {}
        profile_text = (state.get("company_profile_text") or "").strip()

        if not cashflow_snapshot:
            return {
                "cashflow_map_analysis": {
                    "ts_code": ts_code,
                    "error": "无现金流快照数据",
                }
            }

        from langchain_core.prompts import ChatPromptTemplate

        system_msg = """你是A股现金流分析师。请仅基于输入现金流快照与公司描述进行结构化分析。
重点：
1) 经营现金流质量（CFO、现金转换、与净利润匹配）；
2) 自由现金流与资本开支压力；
3) 融资依赖与偿债压力（筹资流入/流出、借款与还债、分红利息支付）。
不得编造历史分位、行业对比或未来预测。输出严格 JSON。"""
        human_msg = """公司描述：
{profile_text}

现金流快照：
{cashflow_snapshot}

请输出 JSON，包含键：
- ts_code
- cashflow_quality（优秀|良好|一般|偏弱|数据不足）
- fcf_signal（充裕|改善|承压|紧张|数据不足）
- financing_dependency_signal（低|中|高|数据不足）
- cashflow_comment（2-4句）
- key_points（字符串数组，3-6条）
- risks（字符串数组，1-4条；无则空数组）
- summary（一句话总结）
"""
        prompt = ChatPromptTemplate.from_messages(
            [("system", system_msg), ("human", human_msg)]
        )
        chain = prompt | llm
        raw = chain.invoke(
            {
                "profile_text": profile_text or "公司描述缺失",
                "cashflow_snapshot": json.dumps(cashflow_snapshot, ensure_ascii=False, indent=2),
            },
            config={**(config or {}), "run_name": "现金流Map分析"},
        )
        data = extract_json_text(raw) or {}
        out = {
            "ts_code": data.get("ts_code") or ts_code,
            "cashflow_quality": data.get("cashflow_quality") or "数据不足",
            "fcf_signal": data.get("fcf_signal") or "数据不足",
            "financing_dependency_signal": data.get("financing_dependency_signal") or "数据不足",
            "cashflow_comment": data.get("cashflow_comment") or "",
            "key_points": data.get("key_points") or [],
            "risks": data.get("risks") or [],
            "summary": data.get("summary") or "",
        }
        return {"cashflow_map_analysis": out}

    return cashflow_map_node


def create_balancesheet_map_node(llm):
    """资产负债表 map 节点：分析偿债能力、杠杆结构与资产质量。"""

    def balancesheet_map_node(
        state: Dict[str, Any],
        config: Optional[RunnableConfig] = None,
    ) -> Dict[str, Any]:
        facts = state.get("stock_fundamental_facts") or {}
        ts_code = facts.get("ts_code") or state.get("ts_code")
        bs_snapshot = facts.get("balancesheet_snapshot") or {}
        profile_text = (state.get("company_profile_text") or "").strip()

        if not bs_snapshot:
            return {
                "balancesheet_map_analysis": {
                    "ts_code": ts_code,
                    "error": "无资产负债表快照数据",
                }
            }

        from langchain_core.prompts import ChatPromptTemplate

        system_msg = """你是A股资产负债表分析师。请仅基于输入快照与公司描述做结构化分析。
重点：
1) 偿债能力（debt_to_assets/current_ratio/cash_ratio/debt_to_equity）；
2) 负债结构（短债、长债、租赁负债、合同负债）；
3) 资产质量（应收、存货、商誉/无形资产占比）。
不得编造行业对比或未来预测。输出严格 JSON。"""
        human_msg = """公司描述：
{profile_text}

资产负债表快照：
{bs_snapshot}

请输出 JSON，包含键：
- ts_code
- solvency_quality（优秀|良好|一般|偏弱|数据不足）
- leverage_signal（低杠杆|中性|高杠杆|数据不足）
- asset_quality_signal（优|中|弱|数据不足）
- liabilities_structure_comment（2-4句）
- key_points（字符串数组，3-6条）
- risks（字符串数组，1-4条；无则空数组）
- summary（一句话总结）
"""
        prompt = ChatPromptTemplate.from_messages(
            [("system", system_msg), ("human", human_msg)]
        )
        chain = prompt | llm
        raw = chain.invoke(
            {
                "profile_text": profile_text or "公司描述缺失",
                "bs_snapshot": json.dumps(bs_snapshot, ensure_ascii=False, indent=2),
            },
            config={**(config or {}), "run_name": "资产负债表Map分析"},
        )
        data = extract_json_text(raw) or {}
        out = {
            "ts_code": data.get("ts_code") or ts_code,
            "solvency_quality": data.get("solvency_quality") or "数据不足",
            "leverage_signal": data.get("leverage_signal") or "数据不足",
            "asset_quality_signal": data.get("asset_quality_signal") or "数据不足",
            "liabilities_structure_comment": data.get("liabilities_structure_comment") or "",
            "key_points": data.get("key_points") or [],
            "risks": data.get("risks") or [],
            "summary": data.get("summary") or "",
        }
        return {"balancesheet_map_analysis": out}

    return balancesheet_map_node


def create_dividend_map_node(llm):
    """分红送股 map 节点：分析分红稳定性、回报属性与送转特征。"""

    def dividend_map_node(
        state: Dict[str, Any],
        config: Optional[RunnableConfig] = None,
    ) -> Dict[str, Any]:
        facts = state.get("stock_fundamental_facts") or {}
        ts_code = facts.get("ts_code") or state.get("ts_code")
        div_snapshot = facts.get("dividend_snapshot") or {}
        profile_text = (state.get("company_profile_text") or "").strip()

        if not div_snapshot:
            return {
                "dividend_map_analysis": {
                    "ts_code": ts_code,
                    "error": "无分红送股快照数据",
                }
            }

        from langchain_core.prompts import ChatPromptTemplate

        system_msg = """你是A股分红送股分析师。请仅基于输入快照与公司描述输出结构化分析。
重点：
1) 分红属性（现金分红强度、送转强度、实施进度）；
2) 稳定性（与上一期变化）；
3) 投资者回报画像（偏分红回报/偏成长扩张）。
不得编造行业对比或未来预测。输出严格 JSON。"""
        human_msg = """公司描述：
{profile_text}

分红送股快照：
{div_snapshot}

请输出 JSON，包含键：
- ts_code
- dividend_quality（优秀|良好|一般|偏弱|数据不足）
- payout_style（现金回报型|送转扩张型|均衡型|不明显|数据不足）
- stability_signal（稳定|波动|下降|数据不足）
- dividend_comment（2-4句）
- key_points（字符串数组，3-6条）
- risks（字符串数组，1-4条；无则空数组）
- summary（一句话总结）
"""
        prompt = ChatPromptTemplate.from_messages(
            [("system", system_msg), ("human", human_msg)]
        )
        chain = prompt | llm
        raw = chain.invoke(
            {
                "profile_text": profile_text or "公司描述缺失",
                "div_snapshot": json.dumps(div_snapshot, ensure_ascii=False, indent=2),
            },
            config={**(config or {}), "run_name": "分红送股Map分析"},
        )
        data = extract_json_text(raw) or {}
        out = {
            "ts_code": data.get("ts_code") or ts_code,
            "dividend_quality": data.get("dividend_quality") or "数据不足",
            "payout_style": data.get("payout_style") or "数据不足",
            "stability_signal": data.get("stability_signal") or "数据不足",
            "dividend_comment": data.get("dividend_comment") or "",
            "key_points": data.get("key_points") or [],
            "risks": data.get("risks") or [],
            "summary": data.get("summary") or "",
        }
        return {"dividend_map_analysis": out}

    return dividend_map_node


def create_fundamental_reduce_node(llm):
    """Reduce 节点：汇总多维 map 结果，生成最终基本面结论。"""

    def fundamental_reduce_node(
        state: Dict[str, Any],
        config: Optional[RunnableConfig] = None,
    ) -> Dict[str, Any]:
        facts = state.get("stock_fundamental_facts") or {}
        ts_code = facts.get("ts_code") or state.get("ts_code")

        payload = {
            "ts_code": ts_code,
            "company_basic_analysis": state.get("company_basic_analysis") or {},
            "valuation_map_analysis": state.get("valuation_map_analysis") or {},
            "income_map_analysis": state.get("income_map_analysis") or {},
            "cashflow_map_analysis": state.get("cashflow_map_analysis") or {},
            "balancesheet_map_analysis": state.get("balancesheet_map_analysis") or {},
            "dividend_map_analysis": state.get("dividend_map_analysis") or {},
        }

        # 任何一个 map 出错时，reduce 仍尽量给结论，但标注数据缺失
        from langchain_core.prompts import ChatPromptTemplate

        system_msg = """你是A股基本面研究经理，需要把多个子节点分析汇总为最终结论。
请做以下事情：
1) 提炼 3-6 条关键结论（盈利、现金流、偿债、估值、分红）；
2) 明确主要风险（1-5条）；
3) 给出综合评级与置信度（高/中/低）；
4) 给出后续需补充的数据项（若有）。
要求：仅基于输入，不编造；输出严格 JSON。"""
        human_msg = """输入：
{payload}

请输出 JSON，包含键：
- ts_code
- overall_score（0-100 的数字，越高越好）
- rating_label（优秀|良好|一般|偏弱|数据不足）
- key_conclusions（字符串数组，3-6条）
- major_risks（字符串数组，1-5条）
- valuation_view（一句话）
- quality_view（一句话）
- shareholder_return_view（一句话）
- next_data_needed（字符串数组，0-5条）
- summary（2-4句中文）
"""
        prompt = ChatPromptTemplate.from_messages(
            [("system", system_msg), ("human", human_msg)]
        )
        chain = prompt | llm
        raw = chain.invoke(
            {"payload": json.dumps(payload, ensure_ascii=False, indent=2)},
            config={**(config or {}), "run_name": "基本面Reduce汇总"},
        )
        data = extract_json_text(raw) or {}
        score_raw = data.get("overall_score")
        try:
            overall_score = round(float(score_raw), 1)
            overall_score = max(0.0, min(100.0, overall_score))
        except (TypeError, ValueError):
            overall_score = None

        # 判断分析是否成功：至少有一些关键数据
        meta = state.get("stock_fundamental_meta") or {}
        has_data = (
            meta.get("company_info_ready")
            or meta.get("valuation_ready")
            or meta.get("income_ready")
            or meta.get("cashflow_ready")
            or meta.get("balancesheet_ready")
        )
        success = has_data and overall_score is not None and overall_score > 0

        out = {
            "ts_code": data.get("ts_code") or ts_code,
            "success": success,
            "overall_score": overall_score,
            "rating_label": data.get("rating_label") or "数据不足",
            "confidence": data.get("confidence") or "低",
            "key_conclusions": data.get("key_conclusions") or [],
            "major_risks": data.get("major_risks") or [],
            "valuation_view": data.get("valuation_view") or "",
            "quality_view": data.get("quality_view") or "",
            "shareholder_return_view": data.get("shareholder_return_view") or "",
            "next_data_needed": data.get("next_data_needed") or [],
            "summary": data.get("summary") or "",
        }
        return {"fundamental_reduce_result": out}

    return fundamental_reduce_node


def create_detect_fundamental_cache_node():
    """检测本地是否已有基本面分析的缓存结果。如果之前分析失败，则跳过缓存重新分析。"""

    def detect_cache_node(
        state: Dict[str, Any],
        config: Optional[RunnableConfig] = None,
    ) -> Dict[str, Any]:
        _ = config
        ts_code = (state.get("ts_code") or "").strip()
        trade_date = str(state.get("trade_date") or "").replace("-", "")[:8]

        if not ts_code or not trade_date:
            return {
                **state,
                "fundamental_cache_hit": False,
                "fundamental_cache_path": None,
            }

        result_path = _build_fundamental_result_path(ts_code, trade_date)
        manifest_path = _build_fundamental_manifest_path(ts_code, trade_date)

        if result_path.exists() and manifest_path.exists():
            try:
                cached_result = _load_json_file(result_path)
                reduce_result = cached_result.get("fundamental_reduce_result") if cached_result else None

                # 检查是否有有效结果
                if not reduce_result:
                    logger.info("fundamental_analyst 缓存无效（无reduce结果）: %s/%s", ts_code, trade_date)
                    return {
                        **state,
                        "fundamental_cache_hit": False,
                        "fundamental_cache_path": None,
                    }

                # 检查之前是否分析失败
                if not reduce_result.get("success", True):
                    logger.info("fundamental_analyst 缓存标记为失败，重新分析: %s/%s", ts_code, trade_date)
                    return {
                        **state,
                        "fundamental_cache_hit": False,
                        "fundamental_cache_path": None,
                    }

                logger.info("fundamental_analyst 缓存命中: %s/%s", ts_code, trade_date)
                return {
                    **state,
                    "fundamental_cache_hit": True,
                    "fundamental_cache_path": result_path.as_posix(),
                    # 恢复完整状态
                    "stock_fundamental_meta": cached_result.get("stock_fundamental_meta"),
                    "stock_company_info": cached_result.get("stock_company_info"),
                    "stock_fundamental_daily": cached_result.get("stock_fundamental_daily"),
                    "stock_income_data": cached_result.get("stock_income_data"),
                    "stock_cashflow_data": cached_result.get("stock_cashflow_data"),
                    "stock_balancesheet_data": cached_result.get("stock_balancesheet_data"),
                    "stock_dividend_data": cached_result.get("stock_dividend_data"),
                    "stock_fundamental_facts": cached_result.get("stock_fundamental_facts"),
                    "fundamental_base_profile": cached_result.get("fundamental_base_profile"),
                    "company_profile_text": cached_result.get("company_profile_text"),
                    "company_basic_analysis": cached_result.get("company_basic_analysis"),
                    "valuation_map_analysis": cached_result.get("valuation_map_analysis"),
                    "income_map_analysis": cached_result.get("income_map_analysis"),
                    "cashflow_map_analysis": cached_result.get("cashflow_map_analysis"),
                    "balancesheet_map_analysis": cached_result.get("balancesheet_map_analysis"),
                    "dividend_map_analysis": cached_result.get("dividend_map_analysis"),
                    "fundamental_reduce_result": reduce_result,
                }
            except Exception as e:
                logger.warning("读取 fundamental_analyst 缓存失败: %s", e)

        return {
            **state,
            "fundamental_cache_hit": False,
            "fundamental_cache_path": None,
        }

    return detect_cache_node


def create_fundamental_persist_node():
    """将基本面分析结果持久化到本地 artifacts。"""

    def persist_node(
        state: Dict[str, Any],
        config: Optional[RunnableConfig] = None,
    ) -> Dict[str, Any]:
        _ = config
        # 如果缓存已命中，不需要重复保存
        if state.get("fundamental_cache_hit"):
            return {
                **state,
                "fundamental_persisted": False,
                "fundamental_persist_reason": "cache_hit",
            }

        ts_code = (state.get("ts_code") or "").strip()
        trade_date = str(state.get("trade_date") or datetime.now().strftime("%Y%m%d")).replace("-", "")[:8]

        if not ts_code:
            return {**state, "fundamental_persisted": False, "fundamental_persist_reason": "missing_ts_code"}

        result = state.get("fundamental_reduce_result")
        if not result:
            return {**state, "fundamental_persisted": False, "fundamental_persist_reason": "no_result"}

        result_path = _build_fundamental_result_path(ts_code, trade_date)
        manifest_path = _build_fundamental_manifest_path(ts_code, trade_date)

        # 构建完整的结果对象
        result_payload = {
            "ts_code": ts_code,
            "trade_date": trade_date,
            "stock_fundamental_meta": state.get("stock_fundamental_meta"),
            "stock_company_info": state.get("stock_company_info"),
            "stock_fundamental_daily": state.get("stock_fundamental_daily"),
            "stock_income_data": state.get("stock_income_data"),
            "stock_cashflow_data": state.get("stock_cashflow_data"),
            "stock_balancesheet_data": state.get("stock_balancesheet_data"),
            "stock_dividend_data": state.get("stock_dividend_data"),
            "stock_fundamental_facts": state.get("stock_fundamental_facts"),
            "fundamental_base_profile": state.get("fundamental_base_profile"),
            "company_profile_text": state.get("company_profile_text"),
            "company_basic_analysis": state.get("company_basic_analysis"),
            "valuation_map_analysis": state.get("valuation_map_analysis"),
            "income_map_analysis": state.get("income_map_analysis"),
            "cashflow_map_analysis": state.get("cashflow_map_analysis"),
            "balancesheet_map_analysis": state.get("balancesheet_map_analysis"),
            "dividend_map_analysis": state.get("dividend_map_analysis"),
            "fundamental_reduce_result": result,
        }

        try:
            _write_json_atomic(result_path, result_payload)
            _write_json_atomic(
                manifest_path,
                {
                    "artifact_type": "stock_fundamental_analyst_result",
                    "module": "agents.analyst.stock_analyst.stock_fundamental_analyst",
                    "ts_code": ts_code,
                    "trade_date": trade_date,
                    "created_at": datetime.now().astimezone().isoformat(),
                    "status": "success",
                    "result_path": result_path.as_posix(),
                },
            )
            logger.info("fundamental_analyst 结果已持久化: %s", result_path)
            return {
                **state,
                "fundamental_persisted": True,
                "fundamental_result_path": result_path.as_posix(),
                "fundamental_manifest_path": manifest_path.as_posix(),
            }
        except Exception as e:
            logger.warning("fundamental_analyst 持久化失败: %s", e)
            return {**state, "fundamental_persisted": False, "fundamental_persist_error": str(e)}

    return persist_node


__all__ = [
    "create_stock_fundamental_fetch_node",
    "create_stock_fundamental_analysis_node",
    "create_company_basic_insight_node",
    "create_valuation_map_node",
    "create_income_map_node",
    "create_cashflow_map_node",
    "create_balancesheet_map_node",
    "create_dividend_map_node",
    "create_fundamental_reduce_node",
    "create_detect_fundamental_cache_node",
    "create_fundamental_persist_node",
]
