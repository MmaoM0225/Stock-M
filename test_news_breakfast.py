"""
测试财经早餐功能
"""
import sys
import logging
from pathlib import Path

# 添加项目根目录到路径
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

from dataflow.news_sentiment import NewsSentimentFetcher

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

def test_eastmoney_breakfast_news():
    """测试东方财富财经早餐数据获取"""
    print("=" * 60)
    print("测试东方财富财经早餐数据获取")
    print("=" * 60)
    
    try:
        # 创建获取器实例
        fetcher = NewsSentimentFetcher()
        
        # 获取财经早餐数据
        print("\n正在获取财经早餐数据...")
        news_df = fetcher.get_eastmoney_breakfast_news()
        
        # 显示结果
        print(f"\n数据获取成功！")
        print(f"数据行数: {len(news_df)}")
        print(f"数据列数: {len(news_df.columns)}")
        print(f"\n列名: {list(news_df.columns)}")
        
        # 显示前5条数据
        print("\n前5条数据预览:")
        print("-" * 60)
        print(news_df.head())
        
        # 显示最后5条数据
        print("\n最后5条数据预览:")
        print("-" * 60)
        print(news_df.tail())
        
        # 显示数据类型
        print("\n数据类型:")
        print("-" * 60)
        print(news_df.dtypes)
        
        # 显示一条完整数据示例
        if not news_df.empty:
            print("\n第一条完整数据示例:")
            print("-" * 60)
            first_row = news_df.iloc[0]
            for col in news_df.columns:
                print(f"{col}: {first_row[col]}")
        
        print("\n" + "=" * 60)
        print("测试完成！功能正常。")
        print("=" * 60)
        
        return True
        
    except Exception as e:
        print(f"\n测试失败！错误信息: {str(e)}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = test_eastmoney_breakfast_news()
    sys.exit(0 if success else 1)
