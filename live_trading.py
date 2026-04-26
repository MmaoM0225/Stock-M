"""
实盘模拟运行脚本

每7个交易日运行一次完整的投资决策流程（有严格的依赖顺序）：

依赖链：
                        Macro Manager
                              │
                              ▼
                       Sector Manager
                              │
                              ▼
                       Stock Screener ──▶ Stock Pool Manager
                              │                    │
                              └────────────────────┘
                                          │
                                          ▼
                                Portfolio Decision

执行顺序（不可并行）：
1. Macro Manager（宏观分析）- 独立运行，提供宏观判断
2. Sector Manager（行业分析）- 依赖 Macro Manager 结果（读取宏观摘要）
3. Stock Screener（股票筛选）- 依赖 Sector Manager 结果（读取 favored/watchlist 板块）
4. Stock Pool Manager（股票池分析）- 依赖 Stock Screener 结果（读取筛选出的股票池）
5. Portfolio Decision（组合决策）- 依赖所有上游结果

运行方式:
    python live_trading.py
    python live_trading.py --single-date 20250102
    python live_trading.py --start-date 20240108 --end-date 20260424 --interval 7
    python live_trading.py --dry-run  # 仅预览交易日历，不实际运行
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
import time
import traceback
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

from langchain_openai import ChatOpenAI

# 添加项目根目录到路径
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

from agents.callbacks import get_llm_callbacks
from agents.config import get_llm_config, validate_config
from agents.manager.sector_manager.graph import create_sector_manager_graph
from agents.manager.stock_pool_manager.graph import create_stock_pool_manager_graph
from agents.manager.macro_manager.graph import create_macro_manager_graph
from agents.decision.portfolio_decision.graph import create_portfolio_decision_graph
from agents.analyst.stock_analyst.stock_screener.graph import create_stock_screener_graph
from dataflow.market_data import MarketDataFetcher
from dataflow.news_sentiment import NewsSentimentFetcher

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(
            project_root / "data" / "artifacts" / "2025_7d_for_once.log",
            encoding="utf-8",
        ),
    ],
)
logger = logging.getLogger(__name__)

# 常量定义
SECTOR_MANAGER_ROOT = Path("data/artifacts/manager/sector_manager")
STOCK_POOL_MANAGER_ROOT = Path("data/artifacts/manager/stock_pool_manager")
MACRO_MANAGER_ROOT = Path("data/artifacts/manager/macro_manager")
STOCK_SCREENER_ROOT = Path("data/artifacts/analyst/stock_analyst/stock_screener")
PORTFOLIO_DECISION_ROOT = Path("data/artifacts/decision/overall_ver1.8/portfolio")

DEFAULT_INITIAL_CAPITAL = 1000000.0


def load_portfolio_book(trade_date: str) -> Optional[List[Dict[str, Any]]]:
    """加载指定日期的资产组合持仓数据"""
    book_path = PORTFOLIO_DECISION_ROOT / trade_date / "result.json"
    if not book_path.exists():
        logger.info(f"未找到 {trade_date} 的持仓记录，视为首次建仓")
        return None
    
    try:
        with open(book_path, "r", encoding="utf-8") as f:
            data = json.load(f)
        portfolio_table = data.get("portfolio_table", [])
        if portfolio_table:
            logger.info(f"成功加载 {trade_date} 的持仓记录，共 {len(portfolio_table)} 个资产")
        return portfolio_table
    except Exception as e:
        logger.error(f"加载持仓记录失败 {book_path}: {e}")
        return None


def get_latest_portfolio_book_before(
    trade_date: str, available_dates: List[str]
) -> Optional[List[Dict[str, Any]]]:
    """获取指定日期之前的最新持仓记录"""
    # 过滤出早于当前交易日的日期
    prior_dates = [d for d in available_dates if d < trade_date]
    if not prior_dates:
        return None
    
    # 取最近的日期
    latest_date = max(prior_dates)
    return load_portfolio_book(latest_date)


def get_available_portfolio_dates() -> List[str]:
    """获取所有已有的portfolio_decision日期列表"""
    dates = []
    if PORTFOLIO_DECISION_ROOT.exists():
        for date_dir in PORTFOLIO_DECISION_ROOT.iterdir():
            if date_dir.is_dir() and date_dir.name.isdigit() and len(date_dir.name) == 8:
                result_file = date_dir / "result.json"
                if result_file.exists():
                    dates.append(date_dir.name)
    return sorted(dates)


def run_sector_manager(
    llm: ChatOpenAI, trade_date: str
) -> Dict[str, Any]:
    """运行行业管理器"""
    logger.info(f"[{trade_date}] 开始运行 Sector Manager...")
    graph = create_sector_manager_graph(llm=llm)
    start = time.perf_counter()
    result = graph.invoke({"trade_date": trade_date})
    elapsed = time.perf_counter() - start
    logger.info(f"[{trade_date}] Sector Manager 完成，耗时 {elapsed:.2f} 秒")
    return result


def run_stock_pool_manager(
    llm: ChatOpenAI, trade_date: str
) -> Dict[str, Any]:
    """运行股票池管理器"""
    logger.info(f"[{trade_date}] 开始运行 Stock Pool Manager...")
    graph = create_stock_pool_manager_graph(llm=llm)
    start = time.perf_counter()
    result = graph.invoke({"trade_date": trade_date})
    elapsed = time.perf_counter() - start
    logger.info(f"[{trade_date}] Stock Pool Manager 完成，耗时 {elapsed:.2f} 秒")
    return result


def run_macro_manager(
    llm: ChatOpenAI, trade_date: str, news_fetcher: Optional[NewsSentimentFetcher] = None
) -> Dict[str, Any]:
    """运行宏观管理器"""
    logger.info(f"[{trade_date}] 开始运行 Macro Manager...")
    graph = create_macro_manager_graph(llm, news_fetcher=news_fetcher)
    start = time.perf_counter()
    result = graph.invoke({"trade_date": trade_date})
    elapsed = time.perf_counter() - start
    logger.info(f"[{trade_date}] Macro Manager 完成，耗时 {elapsed:.2f} 秒")
    return result


def run_stock_screener(
    llm: ChatOpenAI, trade_date: str
) -> Dict[str, Any]:
    """运行股票筛选器（使用LLM决策板块模板）"""
    logger.info(f"[{trade_date}] 开始运行 Stock Screener...")
    graph = create_stock_screener_graph(llm=llm)
    
    # 筛选条件配置
    criteria = {
        "trade_date": trade_date,
        "min_market_cap": 80e8,  # 最小市值80亿
        "exclude_st": True,       # 排除ST股票
        "max_stocks": 18,         # 最多返回18只
        # "max_price": 800,       # 股价上限预留字段，暂不使用
    }
    
    start = time.perf_counter()
    result = graph.invoke(criteria)
    elapsed = time.perf_counter() - start
    
    screener_result = result.get("screener_result", {})
    filtered_count = screener_result.get("total_count", 0)
    
    logger.info(f"[{trade_date}] Stock Screener 完成，耗时 {elapsed:.2f} 秒，筛选出 {filtered_count} 只股票")
    return result


def run_portfolio_decision(
    llm: ChatOpenAI,
    trade_date: str,
    initial_capital: float,
    portfolio_holdings: Optional[List[Dict[str, Any]]] = None,
) -> Dict[str, Any]:
    """运行组合决策"""
    logger.info(f"[{trade_date}] 开始运行 Portfolio Decision...")

    # 创建stock_manager子图（用于补跑缺失的股票分析）
    from agents.manager.stock_manager.graph import create_stock_manager_graph
    stock_manager_graph = create_stock_manager_graph(llm=llm)

    graph = create_portfolio_decision_graph(llm=llm)

    invoke_input = {
        "trade_date": trade_date,
        "initial_capital": initial_capital,
        # 传入自定义存储路径
        "portfolio_decision_root": str(PORTFOLIO_DECISION_ROOT),
    }

    if portfolio_holdings:
        invoke_input["portfolio_holdings"] = portfolio_holdings
        logger.info(f"[{trade_date}] 传入持仓: {len(portfolio_holdings)} 个资产")

    start = time.perf_counter()
    result = graph.invoke(invoke_input)
    elapsed = time.perf_counter() - start
    logger.info(f"[{trade_date}] Portfolio Decision 完成，耗时 {elapsed:.2f} 秒")
    
    # 输出决策摘要
    payload = result.get("decision_result") or {}
    summary = payload.get("decision_summary", "")
    portfolio_table = payload.get("portfolio_table", [])
    
    logger.info(f"[{trade_date}] 决策摘要: {summary[:200]}...")
    logger.info(f"[{trade_date}] 当前组合: {len(portfolio_table)} 个资产")
    
    # 记录 artifact 路径
    art = result.get("decision_artifact_path")
    if art:
        logger.info(f"[{trade_date}] 决策结果已保存: {art}")
    
    return result


def get_trading_days(
    start_date: str,
    end_date: str,
    interval: int = 7,
    exchange: str = "SSE"
) -> List[str]:
    """
    获取指定日期范围的交易日列表，按指定间隔筛选
    
    Args:
        start_date: 开始日期 (YYYYMMDD)
        end_date: 结束日期 (YYYYMMDD)
        interval: 运行间隔（交易日数），默认7天
        exchange: 交易所代码，默认SSE（上交所）
    
    Returns:
        筛选后的交易日列表（YYYYMMDD格式）
    """
    logger.info(f"获取交易日历 {start_date}-{end_date} (exchange={exchange})...")
    
    fetcher = MarketDataFetcher()
    
    # 获取指定日期范围的全部交易日（is_open=1表示交易日）
    df = fetcher.fetch_trade_cal(
        exchange=exchange,
        start_date=start_date,
        end_date=end_date,
        is_open="1",
    )
    
    if df.empty:
        raise RuntimeError(f"无法获取交易日历 {start_date}-{end_date}")
    
    # 提取交易日列表
    all_trading_days = df["cal_date"].tolist()
    all_trading_days.sort()
    
    logger.info(f"共 {len(all_trading_days)} 个交易日")
    
    # 按间隔筛选
    selected_days = all_trading_days[::interval]
    
    logger.info(f"按每 {interval} 天筛选，将运行 {len(selected_days)} 次")
    logger.info(f"首个交易日: {selected_days[0]}, 最后交易日: {selected_days[-1]}")
    
    return selected_days


def run_single_trading_day(
    llm: ChatOpenAI,
    trade_date: str,
    initial_capital: float,
    portfolio_holdings: Optional[List[Dict[str, Any]]],
    news_fetcher: Optional[NewsSentimentFetcher] = None,
    skip_managers: bool = False,
) -> Dict[str, Any]:
    """
    运行单个交易日的完整流程（按依赖顺序串行执行）
    
    依赖顺序:
        1. Macro Manager (独立)
        2. Sector Manager (依赖 Macro)
        3. Stock Screener (依赖 Sector)
        4. Stock Pool Manager (依赖 Screener)
        5. Portfolio Decision (依赖上述全部)
    
    Returns:
        portfolio_decision 的结果
    """
    logger.info(f"\n{'='*60}")
    logger.info(f"开始处理交易日: {trade_date}")
    logger.info(f"{'='*60}")
    
    day_start = time.perf_counter()
    
    try:
        # 1. 运行上游 Managers（按依赖顺序串行执行）
        if not skip_managers:
            # 注意：这些manager会自己读取artifact，如果已存在则会跳过
            
            macro_result_path = MACRO_MANAGER_ROOT / trade_date / "result.json"
            sector_result_path = SECTOR_MANAGER_ROOT / trade_date / "result.json"
            screener_result_path = STOCK_SCREENER_ROOT / trade_date / "result.json"
            stock_pool_result_path = STOCK_POOL_MANAGER_ROOT / trade_date / "result.json"
            
            # 1.1 Macro Manager - 必须先运行（Sector Manager 依赖它）
            if not macro_result_path.exists():
                run_macro_manager(llm, trade_date, news_fetcher)
            else:
                logger.info(f"[{trade_date}] Macro Manager 结果已存在，跳过")
            
            # 1.2 Sector Manager - 依赖 Macro Manager 结果
            if not sector_result_path.exists():
                run_sector_manager(llm, trade_date)
            else:
                logger.info(f"[{trade_date}] Sector Manager 结果已存在，跳过")
            
            # 1.3 Stock Screener - 依赖 Sector Manager 结果（读取 favored/watchlist 板块）
            if not screener_result_path.exists():
                run_stock_screener(llm, trade_date)
            else:
                logger.info(f"[{trade_date}] Stock Screener 结果已存在，跳过")
            
            # 1.4 Stock Pool Manager - 依赖 Stock Screener 结果（读取筛选出的股票池）
            if not stock_pool_result_path.exists():
                run_stock_pool_manager(llm, trade_date)
            else:
                logger.info(f"[{trade_date}] Stock Pool Manager 结果已存在，跳过")
        
        # 2. 运行组合决策
        decision_result = run_portfolio_decision(
            llm=llm,
            trade_date=trade_date,
            initial_capital=initial_capital,
            portfolio_holdings=portfolio_holdings,
        )
        
        day_elapsed = time.perf_counter() - day_start
        logger.info(f"[{trade_date}] 当日流程完成，总耗时 {day_elapsed:.2f} 秒")
        
        return decision_result
        
    except Exception as e:
        logger.error(f"[{trade_date}] 运行失败: {e}")
        logger.error(traceback.format_exc())
        raise


def main():
    parser = argparse.ArgumentParser(
        description="实盘模拟 - 每7个交易日运行一次投资决策"
    )
    parser.add_argument(
        "--start-date",
        default="20250101",
        help="开始日期 (YYYYMMDD)，默认20250101",
    )
    parser.add_argument(
        "--end-date",
        default="20251231",
        help="结束日期 (YYYYMMDD)，默认20251231",
    )
    parser.add_argument(
        "--interval",
        type=int,
        default=7,
        help="运行间隔（交易日数），默认7天",
    )
    parser.add_argument(
        "--exchange",
        default="SSE",
        help="交易所代码 (SSE/SZSE)，默认SSE",
    )
    parser.add_argument(
        "--initial-capital",
        type=float,
        default=DEFAULT_INITIAL_CAPITAL,
        help=f"初始本金，默认 {DEFAULT_INITIAL_CAPITAL}",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="仅预览交易日历，不实际运行",
    )
    parser.add_argument(
        "--resume",
        action="store_true",
        help="从上次中断处继续运行（根据已有portfolio_book记录判断）",
    )
    parser.add_argument(
        "--skip-managers",
        action="store_true",
        help="跳过manager阶段（假设已运行过），只运行decision",
    )
    parser.add_argument(
        "--single-date",
        default=None,
        help="仅运行单个指定日期 (YYYYMMDD)",
    )
    
    args = parser.parse_args()
    
    # 获取交易日列表
    try:
        all_trading_days = get_trading_days(
            start_date=str(args.start_date),
            end_date=str(args.end_date),
            interval=args.interval,
            exchange=args.exchange,
        )
    except Exception as e:
        logger.error(f"获取交易日历失败: {e}")
        sys.exit(1)
    
    # 交易日列表已经是筛选后的结果
    trading_days = [str(d) for d in all_trading_days]
    
    if not trading_days:
        logger.error(f"指定日期范围内无交易日: {args.start_date} - {args.end_date}")
        sys.exit(1)
    
    logger.info(f"\n{'#'*60}")
    logger.info(f"# 实盘模拟配置")
    logger.info(f"#{'#'*59}")
    logger.info(f"# 运行区间: {trading_days[0]} - {trading_days[-1]}")
    logger.info(f"# 运行次数: {len(trading_days)} 次")
    logger.info(f"# 运行间隔: 每 {args.interval} 个交易日")
    logger.info(f"# 初始本金: {args.initial_capital:,.2f} 元")
    logger.info(f"# 交易所: {args.exchange}")
    logger.info(f"{'#'*60}\n")
    
    # 仅预览模式
    if args.dry_run:
        print("\n预览模式 - 计划运行的交易日:")
        for i, date in enumerate(trading_days, 1):
            print(f"  {i:3d}. {date}")
        print(f"\n共 {len(trading_days)} 个交易日")
        return
    
    # 获取LLM配置
    llm_config = get_llm_config()
    if not validate_config(llm_config):
        logger.error("LLM 配置无效，请检查环境变量")
        sys.exit(1)
    
    llm = ChatOpenAI(
        base_url=llm_config.base_url,
        api_key=llm_config.api_key,
        model=llm_config.model,
        temperature=llm_config.temperature,
        max_retries=llm_config.max_retries,
        timeout=llm_config.timeout,
        callbacks=get_llm_callbacks(),
    )
    logger.info(f"LLM 配置: {llm_config.model} @ {llm_config.base_url}")
    
    # 初始化新闻获取器（可选）
    try:
        news_fetcher = NewsSentimentFetcher()
    except Exception as e:
        logger.warning(f"新闻获取器初始化失败: {e}")
        news_fetcher = None
    
    # 处理 resume 逻辑
    if args.resume:
        completed_dates = get_available_portfolio_dates()
        remaining_days = [d for d in trading_days if d not in completed_dates]
        if not remaining_days:
            logger.info("所有交易日已处理完毕")
            return
        logger.info(f"从上次中断处继续，剩余 {len(remaining_days)} 个交易日")
        trading_days = remaining_days
    
    # 处理 single-date 模式
    if args.single_date:
        if args.single_date not in trading_days:
            logger.error(f"指定日期 {args.single_date} 不在运行列表中")
            sys.exit(1)
        trading_days = [args.single_date]
    
    # 主循环
    completed_count = 0
    failed_count = 0
    available_portfolio_dates = get_available_portfolio_dates()
    
    for i, trade_date in enumerate(trading_days, 1):
        # 获取上一期的持仓
        portfolio_holdings = get_latest_portfolio_book_before(
            trade_date, available_portfolio_dates
        )
        
        try:
            decision_result = run_single_trading_day(
                llm=llm,
                trade_date=trade_date,
                initial_capital=args.initial_capital,
                portfolio_holdings=portfolio_holdings,
                news_fetcher=news_fetcher,
                skip_managers=args.skip_managers,
            )
            
            # 更新已完成的日期列表
            available_portfolio_dates.append(trade_date)
            available_portfolio_dates = sorted(set(available_portfolio_dates))
            completed_count += 1
            
            # 输出简要总结
            payload = decision_result.get("decision_result", {})
            portfolio_table = payload.get("portfolio_table", [])
            if portfolio_table:
                total_value = sum(
                    float(row.get("市值 (元)", 0) or 0) for row in portfolio_table
                )
                logger.info(
                    f"[{trade_date}] 组合总市值: {total_value:,.2f} 元, "
                    f"持仓数: {len(portfolio_table)}"
                )
            
        except Exception as e:
            failed_count += 1
            logger.error(f"[{trade_date}] 处理失败: {e}")
            # 可选择是否中断
            # raise
    
    # 最终总结
    logger.info(f"\n{'='*60}")
    logger.info(f"实盘模拟运行完成")
    logger.info(f"{'='*60}")
    logger.info(f"总交易日: {len(trading_days)}")
    logger.info(f"成功: {completed_count}")
    logger.info(f"失败: {failed_count}")
    logger.info(f"{'='*60}")


if __name__ == "__main__":
    main()
