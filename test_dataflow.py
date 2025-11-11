"""
数据流模块测试脚本
"""
import asyncio
import os
from datetime import datetime, timedelta

# 设置环境变量（测试用）
os.environ['TUSHARE_TOKEN'] = 'your_tushare_token_here'  # 请替换为实际的token

from dataflow.examples import main as run_examples
from dataflow.data_manager import get_stock_data


async def quick_test():
    """快速测试"""
    print("=== 快速测试 ===")
    
    try:
        # 测试获取股票数据
        ts_code = "000001.SZ"
        end_date = datetime.now().strftime('%Y%m%d')
        start_date = (datetime.now() - timedelta(days=30)).strftime('%Y%m%d')
        
        print(f"测试获取 {ts_code} 的数据...")
        print(f"时间范围: {start_date} - {end_date}")
        
        # 只获取K线数据进行快速测试
        data = await get_stock_data(
            ts_code=ts_code,
            start_date=start_date,
            end_date=end_date,
            data_types=['kline']  # 只测试K线数据
        )
        
        if data.get('kline') is not None:
            kline_data = data['kline']
            print(f"✓ 成功获取K线数据: {len(kline_data)} 条记录")
            
            if not kline_data.empty:
                latest = kline_data.iloc[-1]
                print(f"  最新交易日: {latest['trade_date']}")
                print(f"  收盘价: {latest['close']}")
                print(f"  成交量: {latest['vol']}")
        else:
            print("✗ 未获取到K线数据")
        
        print("快速测试完成！")
        
    except Exception as e:
        print(f"测试失败: {e}")
        print("请检查:")
        print("1. Tushare token是否正确配置")
        print("2. 网络连接是否正常")
        print("3. 股票代码是否有效")


async def main():
    """主测试函数"""
    print("Stock-M 数据流模块测试")
    print("=" * 50)
    
    # 检查环境变量
    tushare_token = os.getenv('TUSHARE_TOKEN')
    if not tushare_token or tushare_token == 'your_tushare_token_here':
        print("⚠️  警告: 未配置有效的Tushare Token")
        print("请在test_dataflow.py中设置正确的TUSHARE_TOKEN")
        print("或者设置环境变量: export TUSHARE_TOKEN=your_token")
        return
    
    print("✓ Tushare Token已配置")
    
    # 选择测试模式
    print("\n选择测试模式:")
    print("1. 快速测试 (只测试K线数据)")
    print("2. 完整示例 (所有功能)")
    
    try:
        choice = input("请输入选择 (1 或 2): ").strip()
        
        if choice == '1':
            await quick_test()
        elif choice == '2':
            print("\n开始运行完整示例...")
            await run_examples()
        else:
            print("无效选择，运行快速测试...")
            await quick_test()
            
    except KeyboardInterrupt:
        print("\n测试被用户中断")
    except Exception as e:
        print(f"\n测试过程中出现错误: {e}")


if __name__ == "__main__":
    asyncio.run(main())
