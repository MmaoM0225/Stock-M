"""
不频繁更新数据获取脚本
用于获取不需要频繁更新的基础数据，如行业列表、股票列表等
"""
import logging
import pandas as pd
import json
from datetime import datetime

from dataflow.market_data import MarketDataFetcher
from dataflow.industry_data import fetch_industry_list as fetch_industry_list_api

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def fetch_stock_list(file_path: str = "data/stock_list.json") -> pd.DataFrame:
    """获取股票列表
    
    Args:
        file_path: 保存路径
    
    Returns:
        pd.DataFrame: 股票列表数据
    """
    try:
        logger.info("=" * 50)
        logger.info("开始获取股票列表")
        logger.info("=" * 50)
        
        fetcher = MarketDataFetcher()
        df = fetcher.fetch_stock_basic(
            exchange='',
            list_status='L',
            fields='ts_code,symbol,name,area,industry,market,list_date'
        )
        
        if df.empty:
            logger.warning("未获取到股票列表")
            return pd.DataFrame()
        
        import os
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        
        data = df.to_dict(orient='records')
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        
        logger.info(f"成功获取 {len(df)} 只股票，已保存到 {file_path}")
        return df
        
    except Exception as e:
        logger.error(f"获取股票列表失败: {str(e)}")
        raise


def fetch_industry_list(level: str = 'L1', file_path: str = "data/industry_list.json") -> pd.DataFrame:
    """获取行业分类列表
    
    Args:
        level: 行业级别，'L1'(一级行业), 'L2'(二级行业), 'L3'(三级行业)
        file_path: 保存路径
    
    Returns:
        pd.DataFrame: 行业分类列表数据
    """
    try:
        logger.info("=" * 50)
        logger.info(f"开始获取行业分类列表 (level={level})")
        logger.info("=" * 50)
        
        df = fetch_industry_list_api(level=level)
        
        if df.empty:
            logger.warning("未获取到行业分类列表")
            return pd.DataFrame()
        
        import os
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        
        data = df.to_dict(orient='records')
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        
        df_simple = df[['index_code', 'industry_name']].copy()
        simple_file_path = file_path.replace('.json', '_simple.json')
        simple_data = df_simple.to_dict(orient='records')
        with open(simple_file_path, 'w', encoding='utf-8') as f:
            json.dump(simple_data, f, ensure_ascii=False, indent=2)
        
        logger.info(f"成功获取 {len(df)} 个行业分类，已保存到 {file_path}")
        logger.info(f"简化版已保存到 {simple_file_path}")
        return df
        
    except Exception as e:
        logger.error(f"获取行业分类列表失败: {str(e)}")
        raise


def fetch_all_static_data():
    """获取所有不频繁更新的基础数据"""
    logger.info("=" * 50)
    logger.info(f"开始执行基础数据获取任务 - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info("=" * 50)
    
    results = {}
    
    try:
        results['stock_list'] = fetch_stock_list()
    except Exception as e:
        logger.error(f"获取股票列表时出错: {str(e)}")
        results['stock_list'] = None
    
    try:
        results['industry_list_l1'] = fetch_industry_list(level='L1')
    except Exception as e:
        logger.error(f"获取一级行业列表时出错: {str(e)}")
        results['industry_list_l1'] = None
    
    try:
        results['industry_list_l2'] = fetch_industry_list(level='L2', file_path="data/industry_list_l2.json")
    except Exception as e:
        logger.error(f"获取二级行业列表时出错: {str(e)}")
        results['industry_list_l2'] = None
    
    logger.info("=" * 50)
    logger.info("基础数据获取任务完成")
    logger.info("=" * 50)
    
    return results


if __name__ == "__main__":
    fetch_all_static_data()
