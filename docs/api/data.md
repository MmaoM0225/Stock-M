# 数据查询接口文档

## 1. 概述

数据查询接口只读取 `data/artifacts` 下已生成的 JSON 文件，不触发新的 Agent 运行。

API 前缀：`/api/v1/data`

## 2. 接口清单

### 2.1 获取宏观分析结果

- `GET /api/v1/data/macro/{trade_date}`

### 2.2 获取行业分析结果

- `GET /api/v1/data/sector/{trade_date}`

### 2.3 获取筛选结果

- `GET /api/v1/data/screener/{trade_date}`

### 2.4 获取股票池结果

- `GET /api/v1/data/stock-pool/{trade_date}`

### 2.5 获取组合决策结果

- `GET /api/v1/data/portfolio/{trade_date}`

### 2.6 获取个股综合分析结果

- `GET /api/v1/data/stock/{ts_code}/{trade_date}`

### 2.7 获取基本面分析结果

- `GET /api/v1/data/fundamental/{ts_code}/{trade_date}`

### 2.8 获取技术面分析结果

- `GET /api/v1/data/technical/{ts_code}/{trade_date}`

### 2.9 获取行业趋势 Analyst 结果

- `GET /api/v1/data/analyst/sector/trend/{trade_date}`

### 2.10 获取行业资金流 Analyst 结果

- `GET /api/v1/data/analyst/sector/capital-flow/{trade_date}`

### 2.11 获取宏观经济 Analyst 结果

- `GET /api/v1/data/analyst/macro/economist/{trade_date}`

### 2.12 获取新闻 Analyst 结果

- `GET /api/v1/data/analyst/macro/news/{trade_date}`

### 2.13 获取市场情绪 Analyst 结果

- `GET /api/v1/data/analyst/macro/market-sentiment/{trade_date}`

### 2.14 获取流动性 Analyst 结果

- `GET /api/v1/data/analyst/macro/liquidity/{trade_date}`

### 2.15 获取大宗商品 Analyst 结果

- `GET /api/v1/data/analyst/macro/commodity/{trade_date}`

## 3. 参数说明

- `trade_date`：交易日，格式 `YYYYMMDD`
- `ts_code`：股票代码（如 `600519.SH`）

## 4. 响应示例

成功：

```json
{
  "success": true,
  "message": "ok",
  "data": {
    "trade_date": "20260420",
    "result": {
      "artifact_type": "macro_manager_result",
      "status": "success"
    }
  }
}
```

数据不存在：

```json
{
  "success": false,
  "message": "result file not found",
  "error_code": "NOT_FOUND"
}
```

## 5. 数据路径映射

- 宏观：`data/artifacts/manager/macro_manager/{trade_date}/result.json`
- 行业：`data/artifacts/manager/sector_manager/{trade_date}/result.json`
- 筛选：`data/artifacts/analyst/stock_analyst/stock_screener/{trade_date}/result.json`
- 股票池：`data/artifacts/manager/stock_pool_manager/{trade_date}/result.json`
- 组合：`data/artifacts/decision/<version>/portfolio/{trade_date}/result.json`
- 个股管理：`data/artifacts/manager/stock_manager/{ts_code}/{trade_date}/result.json`
- 基本面：`data/artifacts/analyst/stock_analyst/stock_fundamental_analyst/{ts_code}/{trade_date}/result.json`
- 技术面：`data/artifacts/analyst/stock_analyst/stock_technical_analyst/{ts_code}/{trade_date}/result.json`
- 行业趋势 analyst：`data/artifacts/analyst/sector_analyst/sector_trend_analyst/{trade_date}/result.json`
- 行业资金流 analyst：`data/artifacts/analyst/sector_analyst/sector_capital_flow_analyst/{trade_date}/result.json`
- 宏观经济 analyst：`data/artifacts/analyst/macro_analyst/macro_economist/{trade_date}/result.json`
- 新闻 analyst：`data/artifacts/analyst/macro_analyst/news_analyst/{trade_date}/result.json`
- 市场情绪 analyst：`data/artifacts/analyst/macro_analyst/market_sentiment_analyst/{trade_date}/result.json`
- 流动性 analyst：`data/artifacts/analyst/macro_analyst/liquidity_analyst/{trade_date}/result.json`
- 大宗商品 analyst：`data/artifacts/analyst/macro_analyst/commodity_analyst/{trade_date}/result.json`

## 6. Analyst 数据接口汇总

为保证“每个 analyst 都有独立数据读取接口”，当前覆盖：

- Stock Analyst：`screener`、`fundamental`、`technical`
- Sector Analyst：`trend`、`capital-flow`
- Macro Analyst：`economist`、`news`、`market-sentiment`、`liquidity`、`commodity`

## 7. 建议

- 查询接口优先返回结构化 `data` 字段，不直接透传原始文本。
- 对历史查询可增加缓存，降低文件读取开销。
