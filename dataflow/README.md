# DataFlow 数据获取模块

## 概述

DataFlow是Stock-M项目的数据获取模块，提供统一的接口来获取股票相关的各类数据，包括：

- **K线数据**: 日线、周线、月线、分钟级行情数据
- **基本面数据**: 公司信息、财务报表、财务指标
- **市场数据**: 资金流向、融资融券、龙虎榜、股东信息
- **新闻舆情**: 新闻数据、公告信息、情绪分析

## 主要特性

- 🚀 **异步支持**: 基于asyncio的高性能异步数据获取
- 🔄 **统一接口**: 提供统一的数据获取接口，简化使用
- 📊 **多数据源**: 支持Tushare Pro等多个数据源
- 🛡️ **错误处理**: 完善的异常处理和重试机制
- 📈 **技术指标**: 自动计算常用技术指标
- 🎯 **情绪分析**: 内置新闻情绪分析功能
- ⚡ **请求限频**: 智能请求频率控制

## 快速开始

### 1. 安装依赖

```bash
pip install -r requirements.txt
```

### 2. 配置环境变量

```bash
# Tushare Pro Token (必需)
export TUSHARE_TOKEN=your_tushare_token_here

# News API Key (可选，用于外部新闻)
export NEWS_API_KEY=your_news_api_key_here
```

### 3. 基础使用

```python
import asyncio
from dataflow.data_manager import get_stock_data

async def main():
    # 获取股票综合数据
    data = await get_stock_data(
        ts_code="000001.SZ",
        start_date="20240101", 
        end_date="20241201",
        data_types=['kline', 'financial', 'market', 'news']
    )
    
    print(f"K线数据: {len(data['kline'])} 条")
    print(f"财务数据: {list(data['financial'].keys())}")

asyncio.run(main())
```

## 模块结构

```
dataflow/
├── __init__.py              # 模块初始化
├── config.py                # 配置文件
├── utils.py                 # 工具函数
├── kline_data.py           # K线数据获取
├── fundamental_data.py     # 基本面数据获取
├── market_data.py          # 市场数据获取
├── news_sentiment.py       # 新闻舆情数据获取
├── data_manager.py         # 统一数据管理器
├── examples.py             # 使用示例
└── README.md               # 说明文档
```

## 详细使用指南

### K线数据获取

```python
from dataflow.kline_data import get_daily_kline, get_weekly_kline

# 获取日线数据
daily_data = await get_daily_kline(
    ts_code="000001.SZ",
    start_date="20240101",
    end_date="20241201",
    adj="qfq",  # 前复权
    with_indicators=True  # 包含技术指标
)

# 获取周线数据
weekly_data = await get_weekly_kline(
    ts_code="000001.SZ",
    start_date="20240101", 
    end_date="20241201"
)
```

### 基本面数据获取

```python
from dataflow.fundamental_data import (
    get_company_basic_info,
    get_all_financial_data
)

# 获取公司基本信息
company_info = await get_company_basic_info("000001.SZ")

# 获取所有财务数据
financial_data = await get_all_financial_data(
    ts_code="000001.SZ",
    start_date="20240101",
    end_date="20241201"
)

# financial_data包含:
# - income_statement: 利润表
# - balance_sheet: 资产负债表  
# - cashflow_statement: 现金流量表
# - financial_indicators: 财务指标
# - dividend_data: 分红数据
# - forecast_data: 业绩预告
# - express_data: 业绩快报
```

### 市场数据获取

```python
from dataflow.market_data import (
    get_money_flow,
    get_dragon_tiger_list,
    get_margin_detail
)

# 获取资金流向
money_flow = await get_money_flow(
    ts_code="000001.SZ",
    start_date="20240101",
    end_date="20241201"
)

# 获取龙虎榜
dragon_tiger = await get_dragon_tiger_list(
    trade_date="20241201"
)

# 获取融资融券明细
margin_detail = await get_margin_detail(
    trade_date="20241201"
)
```

### 新闻舆情数据获取

```python
from dataflow.news_sentiment import (
    get_announcements,
    analyze_news_sentiment,
    get_stock_news_with_sentiment
)

# 获取公告数据
announcements = await get_announcements(
    ts_code="000001.SZ",
    start_date="20240101",
    end_date="20241201"
)

# 情绪分析
texts = ["公司业绩大幅增长", "面临重大风险"]
sentiments = await analyze_news_sentiment(texts)

# 获取带情绪分析的股票新闻
news_with_sentiment = await get_stock_news_with_sentiment(
    ts_code="000001.SZ",
    start_date="20240101",
    end_date="20241201"
)
```

### 使用数据管理器

```python
from dataflow.data_manager import DataManager

async with DataManager() as manager:
    # 获取K线数据
    kline_data = await manager.get_kline_data(
        ts_code="000001.SZ",
        start_date="20240101",
        end_date="20241201",
        freq="daily"
    )
    
    # 获取财务报表
    financial_data = await manager.get_financial_statements(
        ts_code="000001.SZ",
        start_date="20240101",
        end_date="20241201",
        statement_type="all"
    )
    
    # 获取综合数据
    comprehensive_data = await manager.get_stock_comprehensive_data(
        ts_code="000001.SZ",
        start_date="20240101",
        end_date="20241201"
    )
```

## 配置说明

### 环境变量

| 变量名 | 说明 | 必需 |
|--------|------|------|
| TUSHARE_TOKEN | Tushare Pro API Token | 是 |
| NEWS_API_KEY | News API密钥 | 否 |
| ALPHA_VANTAGE_API_KEY | Alpha Vantage API密钥 | 否 |

### 配置文件

在`config.py`中可以调整以下配置：

```python
# 请求配置
REQUEST_TIMEOUT = 30        # 请求超时时间
MAX_RETRIES = 3            # 最大重试次数
RETRY_DELAY = 1            # 重试延迟

# 缓存配置
CACHE_CONFIG = {
    'enabled': True,
    'ttl': 3600,           # 缓存时间(秒)
    'max_size': 1000       # 最大缓存条目
}
```

## 错误处理

模块提供了完善的错误处理机制：

```python
from dataflow.utils import DataFlowException

try:
    data = await get_daily_kline("INVALID.CODE", "20240101", "20241201")
except DataFlowException as e:
    print(f"数据获取失败: {e}")
except Exception as e:
    print(f"其他错误: {e}")
```

## 性能优化

### 1. 请求限频

模块内置了智能请求限频机制，自动控制API调用频率：

```python
# Tushare Pro: 每分钟200次
# Alpha Vantage: 每分钟5次
```

### 2. 并发获取

支持并发获取多个数据：

```python
# 并发获取多只股票数据
tasks = [
    get_daily_kline("000001.SZ", start_date, end_date),
    get_daily_kline("000002.SZ", start_date, end_date),
    get_daily_kline("600000.SH", start_date, end_date)
]
results = await asyncio.gather(*tasks)
```

### 3. 数据缓存

支持数据缓存以提高性能（可在config.py中配置）。

## 测试

运行测试脚本：

```bash
python test_dataflow.py
```

或运行示例：

```bash
python -m dataflow.examples
```

## 注意事项

1. **API限制**: 请遵守各数据源的API调用限制
2. **数据质量**: 数据质量依赖于数据源，建议进行数据验证
3. **网络连接**: 确保网络连接稳定，模块会自动重试失败的请求
4. **Token安全**: 请妥善保管API Token，不要提交到代码仓库

## 扩展开发

### 添加新数据源

1. 在相应模块中添加新的获取方法
2. 实现统一的接口规范
3. 添加错误处理和限频机制
4. 更新数据管理器

### 添加新数据类型

1. 创建新的数据获取模块
2. 实现异步获取方法
3. 在数据管理器中集成
4. 添加使用示例

## 常见问题

### Q: 如何获取Tushare Token？
A: 访问 [Tushare Pro官网](https://tushare.pro) 注册账号并获取Token。

### Q: 数据获取失败怎么办？
A: 检查网络连接、API Token是否正确、股票代码是否有效。

### Q: 如何提高数据获取速度？
A: 使用并发获取、启用缓存、合理设置时间范围。

### Q: 支持哪些股票市场？
A: 目前主要支持A股和港股，美股支持正在开发中。

## 更新日志

### v1.0.0 (2024-12-11)
- 初始版本发布
- 支持K线数据、基本面数据、市场数据、新闻舆情数据获取
- 提供统一的数据管理接口
- 内置情绪分析功能

## 许可证

本项目采用MIT许可证。
