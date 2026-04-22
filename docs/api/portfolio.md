# 组合管理接口文档

## 1. 概述

组合管理接口用于查询组合历史、最新组合和收益表现，数据来源于：

`data/artifacts/decision/<version>/portfolio/{trade_date}/result.json`

API 前缀：`/api/v1/portfolio`

## 2. 接口清单

### 2.1 获取全部组合日期

- `GET /api/v1/portfolio/dates`

响应示例：

```json
{
  "success": true,
  "data": {
    "dates": ["20260301", "20260308", "20260315"]
  }
}
```

### 2.2 获取最新组合

- `GET /api/v1/portfolio/latest`

### 2.3 获取历史组合

- `GET /api/v1/portfolio/history`

查询参数：

- `start_date`（可选）
- `end_date`（可选）
- `page`（默认 1）
- `page_size`（默认 20）

### 2.4 组合对比

- `GET /api/v1/portfolio/compare`

查询参数：

- `dates`：逗号分隔日期列表，例如 `20260301,20260315`

### 2.5 组合表现统计

- `GET /api/v1/portfolio/performance`

查询参数：

- `start_date`（可选）
- `end_date`（可选）

## 3. 关键字段说明

### 3.1 portfolio_table

常见字段：

- `资产名称`
- `股票代码`
- `市值 (元)`
- `仓位 (%)`
- `仓位变化 (%)`
- `总收益率 (%)`
- `总盈亏 (元)`

### 3.2 operation_reason_table

常见字段：

- `股票代码`
- `操作类型`
- `原仓位`
- `新仓位`
- `执行价格`
- `目标金额`
- `操作原因`

## 4. 示例：组合表现响应

```json
{
  "success": true,
  "message": "ok",
  "data": {
    "start_date": "20260101",
    "end_date": "20260420",
    "period_count": 12,
    "total_return_pct": 8.42,
    "max_drawdown_pct": -3.15
  }
}
```

## 5. 注意事项

- 组合路径中的 `<version>` 建议通过配置统一管理。
- 对比接口建议限制日期数量（例如最多 10 个），避免响应过大。
