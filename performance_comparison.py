"""
同步 vs 异步性能对比示例
"""
import time
import asyncio
import aiohttp
import requests
from typing import List


def sync_fetch_data(urls: List[str]) -> List[dict]:
    """同步方式获取数据"""
    results = []
    start_time = time.time()
    
    for i, url in enumerate(urls):
        print(f"同步请求 {i+1}/{len(urls)}: {url}")
        try:
            # 模拟网络请求延迟
            time.sleep(0.5)  # 模拟500ms网络延迟
            results.append({"url": url, "data": f"data_{i}", "status": "success"})
        except Exception as e:
            results.append({"url": url, "error": str(e), "status": "error"})
    
    end_time = time.time()
    print(f"同步方式总耗时: {end_time - start_time:.2f}秒")
    return results


async def async_fetch_single(session: aiohttp.ClientSession, url: str, index: int) -> dict:
    """异步获取单个数据"""
    try:
        print(f"异步请求 {index}: {url}")
        # 模拟网络请求延迟
        await asyncio.sleep(0.5)  # 模拟500ms网络延迟
        return {"url": url, "data": f"data_{index}", "status": "success"}
    except Exception as e:
        return {"url": url, "error": str(e), "status": "error"}


async def async_fetch_data(urls: List[str]) -> List[dict]:
    """异步方式获取数据"""
    start_time = time.time()
    
    async with aiohttp.ClientSession() as session:
        # 创建所有任务
        tasks = [
            async_fetch_single(session, url, i) 
            for i, url in enumerate(urls)
        ]
        
        # 并发执行所有任务
        results = await asyncio.gather(*tasks)
    
    end_time = time.time()
    print(f"异步方式总耗时: {end_time - start_time:.2f}秒")
    return results


def simulate_stock_data_fetching():
    """模拟股票数据获取场景"""
    print("=== 股票数据获取性能对比 ===\n")
    
    # 模拟需要获取的数据源
    data_sources = [
        "tushare://daily_data/000001.SZ",
        "tushare://financial_data/000001.SZ", 
        "tushare://market_data/000001.SZ",
        "tushare://news_data/000001.SZ",
        "tushare://daily_data/000002.SZ",
        "tushare://financial_data/000002.SZ",
        "tushare://market_data/000002.SZ",
        "tushare://news_data/000002.SZ"
    ]
    
    print(f"需要获取 {len(data_sources)} 个数据源")
    print("每个请求模拟 500ms 网络延迟\n")
    
    # 同步方式
    print("1. 同步方式:")
    sync_results = sync_fetch_data(data_sources)
    print(f"成功获取: {sum(1 for r in sync_results if r['status'] == 'success')} 个\n")
    
    # 异步方式
    print("2. 异步方式:")
    async_results = asyncio.run(async_fetch_data(data_sources))
    print(f"成功获取: {sum(1 for r in async_results if r['status'] == 'success')} 个\n")
    
    # 性能提升计算
    sync_time = len(data_sources) * 0.5  # 同步总时间
    async_time = 0.5  # 异步总时间（最长请求时间）
    improvement = ((sync_time - async_time) / sync_time) * 100
    
    print("=== 性能分析 ===")
    print(f"同步方式预期耗时: {sync_time:.1f}秒")
    print(f"异步方式预期耗时: {async_time:.1f}秒") 
    print(f"性能提升: {improvement:.1f}%")


def real_world_scenario():
    """真实世界场景分析"""
    print("\n=== 真实场景分析 ===")
    
    scenarios = [
        {
            "name": "获取单只股票全量数据",
            "requests": 4,  # K线、财务、市场、新闻
            "avg_delay": 1.0,  # 平均1秒延迟
            "description": "获取一只股票的K线、财务、市场、新闻数据"
        },
        {
            "name": "获取投资组合数据", 
            "requests": 20,  # 10只股票 x 2类数据
            "avg_delay": 0.8,
            "description": "获取10只股票的K线和财务数据"
        },
        {
            "name": "市场扫描",
            "requests": 100,  # 100只股票基础数据
            "avg_delay": 0.5,
            "description": "扫描100只股票的基础行情数据"
        }
    ]
    
    for scenario in scenarios:
        sync_time = scenario["requests"] * scenario["avg_delay"]
        async_time = scenario["avg_delay"]  # 并发执行
        improvement = ((sync_time - async_time) / sync_time) * 100
        
        print(f"\n场景: {scenario['name']}")
        print(f"描述: {scenario['description']}")
        print(f"请求数量: {scenario['requests']}")
        print(f"同步耗时: {sync_time:.1f}秒")
        print(f"异步耗时: {async_time:.1f}秒")
        print(f"性能提升: {improvement:.1f}%")


def memory_and_resource_analysis():
    """内存和资源使用分析"""
    print("\n=== 资源使用分析 ===")
    
    print("同步方式:")
    print("- 每个请求创建新的连接")
    print("- 线程阻塞等待响应")
    print("- 内存使用: 中等")
    print("- CPU使用: 低（大部分时间在等待）")
    print("- 并发能力: 受限于线程数")
    
    print("\n异步方式:")
    print("- 复用连接池")
    print("- 事件循环管理")
    print("- 内存使用: 低")
    print("- CPU使用: 高效（事件驱动）")
    print("- 并发能力: 可处理大量并发请求")


if __name__ == "__main__":
    simulate_stock_data_fetching()
    real_world_scenario()
    memory_and_resource_analysis()
