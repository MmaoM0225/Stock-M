"""
数据获取使用示例
"""
import asyncio
import pandas as pd
from datetime import datetime, timedelta
import logging

# 配置日志
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# 导入数据获取模块
from .data_manager import DataManager, get_stock_data, get_market_data
from .kline_data import get_daily_kline, get_weekly_kline, get_stock_list
from .fundamental_data import get_company_basic_info, get_all_financial_data
from .market_data import get_money_flow, get_dragon_tiger_list
from .news_sentiment import get_announcements, analyze_news_sentiment


async def example_basic_usage():
    """基础使用示例"""
    print("=== 基础使用示例 ===")
    
    # 1. 获取股票列表
    print("1. 获取股票列表")
    stock_list = await get_stock_list('main')  # 主板股票
    print(f"获取到 {len(stock_list)} 只主板股票")
    if not stock_list.empty:
        print(stock_list.head())
    
    # 2. 获取日线数据
    print("\n2. 获取日线数据")
    ts_code = "000001.SZ"  # 平安银行
    start_date = "20240101"
    end_date = "20241201"
    
    daily_data = await get_daily_kline(ts_code, start_date, end_date)
    print(f"获取到 {len(daily_data)} 条日线数据")
    if not daily_data.empty:
        print(daily_data.head())
    
    # 3. 获取公司基本信息
    print("\n3. 获取公司基本信息")
    company_info = await get_company_basic_info(ts_code)
    if not company_info.empty:
        print(company_info.iloc[0])


async def example_comprehensive_data():
    """综合数据获取示例"""
    print("\n=== 综合数据获取示例 ===")
    
    ts_code = "000001.SZ"
    start_date = "20240101"
    end_date = "20241201"
    
    # 使用数据管理器获取综合数据
    comprehensive_data = await get_stock_data(
        ts_code=ts_code,
        start_date=start_date,
        end_date=end_date,
        data_types=['kline', 'financial', 'market', 'news']
    )
    
    print(f"股票代码: {comprehensive_data['ts_code']}")
    print(f"数据时间范围: {comprehensive_data['start_date']} - {comprehensive_data['end_date']}")
    
    # 检查各类数据
    if comprehensive_data.get('kline') is not None:
        kline_data = comprehensive_data['kline']
        print(f"K线数据: {len(kline_data)} 条记录")
        if not kline_data.empty:
            print("最新价格信息:")
            latest = kline_data.iloc[-1]
            print(f"  日期: {latest['trade_date']}")
            print(f"  收盘价: {latest['close']}")
            print(f"  涨跌幅: {latest.get('pct_chg', 'N/A')}%")
    
    if comprehensive_data.get('financial') is not None:
        financial_data = comprehensive_data['financial']
        print(f"财务数据包含: {list(financial_data.keys())}")
        
        # 显示最新财务指标
        if 'financial_indicators' in financial_data:
            indicators = financial_data['financial_indicators']
            if not indicators.empty:
                latest_indicators = indicators.iloc[-1]
                print("最新财务指标:")
                print(f"  ROE: {latest_indicators.get('roe', 'N/A')}")
                print(f"  ROA: {latest_indicators.get('roa', 'N/A')}")
                print(f"  毛利率: {latest_indicators.get('grossprofit_margin', 'N/A')}")
    
    if comprehensive_data.get('money_flow') is not None:
        money_flow = comprehensive_data['money_flow']
        print(f"资金流向数据: {len(money_flow)} 条记录")
    
    if comprehensive_data.get('news') is not None:
        news_data = comprehensive_data['news']
        print(f"新闻数据: {len(news_data)} 条记录")
        if not news_data.empty and 'sentiment' in news_data.columns:
            sentiment_counts = news_data['sentiment'].value_counts()
            print(f"情绪分析结果: {dict(sentiment_counts)}")


async def example_market_analysis():
    """市场分析示例"""
    print("\n=== 市场分析示例 ===")
    
    # 获取昨天的日期
    yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y%m%d')
    
    # 获取市场概览数据
    market_data = await get_market_data(yesterday, include_news=True)
    
    print(f"市场日期: {market_data['date']}")
    
    # 龙虎榜分析
    if market_data.get('dragon_tiger') is not None:
        dragon_tiger = market_data['dragon_tiger']
        if dragon_tiger.get('list') is not None:
            dt_list = dragon_tiger['list']
            print(f"龙虎榜股票数量: {len(dt_list)}")
            if not dt_list.empty:
                print("龙虎榜前5只股票:")
                print(dt_list[['ts_code', 'name', 'pct_chg', 'turnover_rate']].head())
    
    # 融资融券分析
    if market_data.get('margin') is not None:
        margin_data = market_data['margin']
        if not margin_data.empty:
            print(f"融资融券数据: {len(margin_data)} 条记录")
            # 计算融资融券总额
            total_margin = margin_data['rzye'].sum() if 'rzye' in margin_data.columns else 0
            print(f"融资余额总计: {total_margin/100000000:.2f} 亿元")
    
    # 新闻情绪分析
    if market_data.get('news') is not None:
        news_data = market_data['news']
        if not news_data.empty and 'sentiment' in news_data.columns:
            sentiment_stats = news_data['sentiment'].value_counts()
            print(f"市场新闻情绪分布: {dict(sentiment_stats)}")


async def example_custom_analysis():
    """自定义分析示例"""
    print("\n=== 自定义分析示例 ===")
    
    async with DataManager() as manager:
        # 1. 获取银行板块股票
        stock_list = await manager.get_stock_list('main')
        bank_stocks = stock_list[stock_list['industry'] == '银行'].head(5)
        
        if bank_stocks.empty:
            print("未找到银行股票")
            return
        
        print(f"分析银行股票: {len(bank_stocks)} 只")
        
        # 2. 批量获取银行股票数据
        start_date = "20241101"
        end_date = "20241201"
        
        bank_analysis = {}
        
        for _, stock in bank_stocks.iterrows():
            ts_code = stock['ts_code']
            name = stock['name']
            
            try:
                # 获取K线数据
                kline_data = await manager.get_kline_data(
                    ts_code, start_date, end_date, 'daily'
                )
                
                if not kline_data.empty:
                    # 计算基本统计信息
                    latest_price = kline_data.iloc[-1]['close']
                    price_change = kline_data['pct_chg'].mean()
                    volatility = kline_data['pct_chg'].std()
                    
                    bank_analysis[ts_code] = {
                        'name': name,
                        'latest_price': latest_price,
                        'avg_change': price_change,
                        'volatility': volatility,
                        'data_points': len(kline_data)
                    }
                
            except Exception as e:
                logger.error(f"获取 {ts_code} 数据失败: {e}")
        
        # 3. 显示分析结果
        print("\n银行股票分析结果:")
        print("-" * 80)
        print(f"{'股票代码':<12} {'股票名称':<10} {'最新价格':<10} {'平均涨幅':<10} {'波动率':<10}")
        print("-" * 80)
        
        for ts_code, data in bank_analysis.items():
            print(f"{ts_code:<12} {data['name']:<10} {data['latest_price']:<10.2f} "
                  f"{data['avg_change']:<10.2f} {data['volatility']:<10.2f}")


async def example_news_sentiment_analysis():
    """新闻情绪分析示例"""
    print("\n=== 新闻情绪分析示例 ===")
    
    ts_code = "000001.SZ"
    start_date = "20241101"
    end_date = "20241201"
    
    # 获取公告数据
    announcements = await get_announcements(ts_code, start_date, end_date)
    
    if announcements.empty:
        print("未获取到公告数据")
        return
    
    print(f"获取到 {len(announcements)} 条公告")
    
    # 提取公告标题进行情绪分析
    titles = announcements['title'].tolist()
    sentiments = await analyze_news_sentiment(titles)
    
    # 统计情绪分布
    sentiment_counts = {}
    sentiment_scores = []
    
    for sentiment in sentiments:
        sentiment_type = sentiment['sentiment']
        sentiment_counts[sentiment_type] = sentiment_counts.get(sentiment_type, 0) + 1
        sentiment_scores.append(sentiment['score'])
    
    print(f"情绪分布: {sentiment_counts}")
    print(f"平均情绪得分: {sum(sentiment_scores) / len(sentiment_scores):.3f}")
    
    # 显示情绪最极端的公告
    combined_data = list(zip(titles, sentiments))
    combined_data.sort(key=lambda x: abs(x[1]['score']), reverse=True)
    
    print("\n情绪最强烈的公告:")
    for i, (title, sentiment) in enumerate(combined_data[:3]):
        print(f"{i+1}. {title}")
        print(f"   情绪: {sentiment['sentiment']}, 得分: {sentiment['score']:.3f}")


async def main():
    """主函数 - 运行所有示例"""
    print("开始运行数据获取示例...")
    
    try:
        # 运行各个示例
        await example_basic_usage()
        await example_comprehensive_data()
        await example_market_analysis()
        await example_custom_analysis()
        await example_news_sentiment_analysis()
        
        print("\n所有示例运行完成！")
        
    except Exception as e:
        logger.error(f"示例运行失败: {e}")
        raise


if __name__ == "__main__":
    # 运行示例
    asyncio.run(main())
