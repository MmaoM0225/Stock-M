# Agent 执行接口文档

## 1. 概述

Agent 接口用于触发分析流程。执行顺序遵循依赖链：

`Macro -> Sector -> Screener -> StockPool -> Portfolio`

API 前缀：`/api/v1/agents`

## 2. 通用请求参数

- `trade_date`：交易日，格式 `YYYYMMDD`
- `force`：是否忽略已有结果并强制重跑，默认 `false`

---

## 3. 接口清单

### 3.1 运行宏观管理器

- `POST /api/v1/agents/macro/run`

请求体：

```json
{
  "trade_date": "20260420",
  "force": false
}
```

响应示例：

```json
{
  "success": true,
  "message": "macro manager finished",
  "data": {
    "trade_date": "20260420",
    "artifact_path": "data/artifacts/manager/macro_manager/20260420/result.json"
  }
}
```

### 3.2 运行行业管理器

- `POST /api/v1/agents/sector/run`

请求体：

```json
{
  "trade_date": "20260420",
  "force": false
}
```

### 3.3 运行股票筛选器

- `POST /api/v1/agents/screener/run`

请求体：

```json
{
  "trade_date": "20260420",
  "min_market_cap": 8000000000,
  "max_stocks": 12,
  "force": false
}
```

### 3.4 运行股票池管理器

- `POST /api/v1/agents/stock-pool/run`

请求体：

```json
{
  "trade_date": "20260420",
  "force": false
}
```

### 3.5 运行组合决策

- `POST /api/v1/agents/portfolio/decision`

请求体：

```json
{
  "trade_date": "20260420",
  "initial_capital": 500000.0,
  "portfolio_holdings": [],
  "force": false
}
```

### 3.6 运行单个股票分析

- `POST /api/v1/agents/stock/{ts_code}/analyze`

路径参数：

- `ts_code`：股票代码，例如 `600519.SH`

请求体：

```json
{
  "trade_date": "20260420",
  "force": false
}
```

### 3.7 运行完整流程

- `POST /api/v1/agents/full-pipeline`

请求体：

```json
{
  "trade_date": "20260420",
  "initial_capital": 500000.0,
  "skip_existing": true
}
```

响应示例：

```json
{
  "success": true,
  "message": "full pipeline finished",
  "data": {
    "trade_date": "20260420",
    "steps": [
      "macro_manager",
      "sector_manager",
      "stock_screener",
      "stock_pool_manager",
      "portfolio_decision"
    ],
    "artifact_paths": [
      "data/artifacts/manager/macro_manager/20260420/result.json",
      "data/artifacts/manager/sector_manager/20260420/result.json",
      "data/artifacts/analyst/stock_analyst/stock_screener/20260420/result.json",
      "data/artifacts/manager/stock_pool_manager/20260420/result.json",
      "data/artifacts/decision/<version>/portfolio/20260420/result.json"
    ]
  }
}
```

### 3.8 运行个股基本面 Analyst

- `POST /api/v1/agents/analyst/stock/fundamental/run`

请求体：

```json
{
  "ts_code": "600519.SH",
  "trade_date": "20260420",
  "force": false
}
```

### 3.9 运行个股技术面 Analyst

- `POST /api/v1/agents/analyst/stock/technical/run`

请求体：

```json
{
  "ts_code": "600519.SH",
  "trade_date": "20260420",
  "force": false
}
```

### 3.10 运行行业趋势 Analyst

- `POST /api/v1/agents/analyst/sector/trend/run`

请求体：

```json
{
  "trade_date": "20260420",
  "force": false
}
```

### 3.11 运行行业资金流 Analyst

- `POST /api/v1/agents/analyst/sector/capital-flow/run`

请求体：

```json
{
  "trade_date": "20260420",
  "force": false
}
```

### 3.12 运行宏观经济 Analyst

- `POST /api/v1/agents/analyst/macro/economist/run`

请求体：

```json
{
  "trade_date": "20260420",
  "force": false
}
```

### 3.13 运行新闻情绪 Analyst

- `POST /api/v1/agents/analyst/macro/news/run`

请求体：

```json
{
  "trade_date": "20260420",
  "force": false
}
```

### 3.14 运行市场情绪 Analyst

- `POST /api/v1/agents/analyst/macro/market-sentiment/run`

请求体：

```json
{
  "trade_date": "20260420",
  "force": false
}
```

### 3.15 运行大宗商品 Analyst

- `POST /api/v1/agents/analyst/macro/commodity/run`

请求体：

```json
{
  "trade_date": "20260420",
  "force": false
}
```

## 4. Analyst 接口汇总

为保证“每个 analyst 都有独立调用入口”，接口覆盖如下：

- Stock Analyst：`fundamental`、`technical`、`screener`
- Sector Analyst：`trend`、`capital-flow`
- Macro Analyst：`economist`、`news`、`market-sentiment`、`commodity`

## 5. 注意事项

- `full-pipeline` 接口内部必须串行执行，不能并行。
- 若 `force=false` 且已有结果，服务可直接返回已有 artifact 路径。
- 建议前端对长耗时接口设置较长超时或采用异步任务机制。
