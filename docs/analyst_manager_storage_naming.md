# Analyst / Manager 存储文件命名规则

## 1. 目的

本文档用于梳理当前 `agents/analyst` 与 `agents/manager` 中已经存在的本地落盘规则，并给出后续统一缓存/物化结果时建议采用的命名规范。

当前重点关注两类本地文件：

- 原始抓取数据
- 结构化分析结果

---

## 2. 当前代码中已存在的命名规则

### 2.1 宏观层 Analyst

#### `agents.analyst.macro_analyst.news_analyst`

1. 原始新闻 JSON

- 存储目录：`data/news/`
- 文件名模板：`news_{trade_date}.json`
- 示例：`data/news/news_20260309.json`
- 主键维度：`trade_date`
- 说明：
  - `trade_date` 使用 `YYYYMMDD`
  - 数据库 `breakfast_news.json_file_path` 会记录该文件路径
  - 读取时优先使用数据库中的 `json_file_path`，若为空则回退到默认路径 `data/news/news_{trade_date}.json`

2. 新闻分析结果

- 当前不再生成独立 Markdown 文件
- 当前结果主要停留在运行态 state 中，输出键为 `news_analysis`

#### `agents.analyst.macro_analyst.market_sentiment_analyst`

- 当前未发现独立本地结果文件命名规则
- 当前结果主要停留在运行态 state 中，输出键为 `market_sentiment_analyst_summary`

#### `agents.analyst.macro_analyst.liquidity_analyst`

- 当前未发现独立本地结果文件命名规则
- 当前结果主要停留在运行态 state 中，输出键为 `liquidity_analyst_summary`

#### `agents.analyst.macro_analyst.commodity_analyst`

- 当前未发现独立本地结果文件命名规则
- 当前结果主要停留在运行态 state 中，输出键为 `commodity_analyst_summary`

#### `agents.analyst.macro_analyst.macro_economist`

- 当前未发现独立本地结果文件命名规则
- 当前结果主要停留在运行态 state 中，输出键为 `macro_economist_analysis`

### 2.2 行业层 Analyst

#### `agents.analyst.sector_analyst.sector_trend_analyst`

- 当前未发现独立本地结果文件命名规则
- 当前结果主要停留在运行态 state 中，输出键为 `sector_trend_insight`

#### `agents.analyst.sector_analyst.sector_capital_flow_analyst`

- 当前未发现独立本地结果文件命名规则
- 当前结果主要停留在运行态 state 中，输出键为 `sector_capital_flow_insight`

### 2.3 个股层 Analyst

#### `agents.analyst.stock_analyst.stock_screener`

- 当前未发现独立本地结果文件命名规则
- 当前结果主要停留在运行态 state 中，输出键为 `screener_result`
- 后续如果做缓存，主键不能只用 `trade_date`，还应包含筛选条件摘要或哈希

#### `agents.analyst.stock_analyst.stock_fundamental_analyst`

- 当前未发现独立本地结果文件命名规则
- 当前结果主要停留在运行态 state 中，输出键为 `fundamental_reduce_result`

#### `agents.analyst.stock_analyst.stock_technical_analyst`

- 当前未发现独立本地结果文件命名规则
- 当前结果主要停留在运行态 state 中，输出键为 `technical_analysis`

---

## 3. 当前代码中已存在的 Manager 命名规则

### `agents.manager.macro_manager`

- 当前未发现独立本地结果文件命名规则
- 当前结果主要停留在运行态 state 中，输出键为 `macro_manager_summary`
- 后续如果做缓存，主键应至少包含 `trade_date`

### `agents.manager.sector_manager`

- 当前未发现独立本地结果文件命名规则
- 当前结果主要停留在运行态 state 中，输出键为 `sector_manager_summary`
- 后续如果做缓存，主键应至少包含 `trade_date`

### `agents.manager.stock_manager`

- 当前未发现独立本地结果文件命名规则
- 当前结果主要停留在运行态 state 中，输出键为 `stock_manager_summary`
- 后续如果做缓存，主键应至少包含 `trade_date + ts_code`

---

## 4. 当前命名风格总结

现有代码中的命名风格目前主要体现为一类：

1. 原始数据文件

- 目录通常按数据类型区分
- 例如：`data/news/news_{trade_date}.json`
- 命名规则倾向于：`{data_kind}_{trade_date}.{ext}`

---

## 5. 后续统一缓存时建议采用的命名规则

如果后续要实现“先查本地，没有再跑 agent”的机制，建议统一使用结构化缓存文件，不再依赖 Markdown 报告文件。

### 5.1 建议的目录分层

建议新增结构化缓存目录：

- `data/artifacts/`

建议目录结构：

```text
data/
  artifacts/
    analyst/
      macro_analyst/
        news_analyst/
        market_sentiment_analyst/
        liquidity_analyst/
        commodity_analyst/
        macro_economist/
      sector_analyst/
        sector_trend_analyst/
        sector_capital_flow_analyst/
      stock_analyst/
        stock_screener/
        stock_fundamental_analyst/
        stock_technical_analyst/
    manager/
      macro_manager/
      sector_manager/
      stock_manager/
```

### 5.2 建议的结构化缓存文件命名

#### 按交易日唯一的结果

适用于：

- `news_analyst`
- `market_sentiment_analyst`
- `liquidity_analyst`
- `commodity_analyst`
- `macro_economist`
- `sector_trend_analyst`
- `sector_capital_flow_analyst`
- `macro_manager`
- `sector_manager`

建议路径模板：

```text
data/artifacts/{kind}/{layer}/{module}/{trade_date}/result.json
data/artifacts/{kind}/{layer}/{module}/{trade_date}/manifest.json
```

示例：

```text
data/artifacts/analyst/macro_analyst/liquidity_analyst/20260309/result.json
data/artifacts/analyst/sector_analyst/sector_trend_analyst/20260309/result.json
data/artifacts/manager/macro_manager/20260309/result.json
```

#### 按交易日 + 股票唯一的结果

适用于：

- `stock_fundamental_analyst`
- `stock_technical_analyst`
- `stock_manager`

建议路径模板：

```text
data/artifacts/{kind}/stock_analyst/{module}/{trade_date}/{ts_code}/result.json
data/artifacts/{kind}/stock_analyst/{module}/{trade_date}/{ts_code}/manifest.json
```

示例：

```text
data/artifacts/analyst/stock_analyst/stock_fundamental_analyst/20260309/600519.SH/result.json
data/artifacts/analyst/stock_analyst/stock_technical_analyst/20260309/600519.SH/result.json
data/artifacts/manager/stock_manager/20260309/600519.SH/result.json
```

#### 按交易日 + 条件唯一的结果

适用于：

- `stock_screener`

建议路径模板：

```text
data/artifacts/analyst/stock_analyst/stock_screener/{trade_date}/{criteria_hash}/result.json
data/artifacts/analyst/stock_analyst/stock_screener/{trade_date}/{criteria_hash}/manifest.json
```

说明：

- `criteria_hash` 建议由标准化后的筛选条件字典计算得到
- 不建议直接把完整条件拼进文件名

### 5.3 建议的命名原则

1. 日期统一使用 `YYYYMMDD`
2. 股票代码统一使用标准 `ts_code`，如 `600519.SH`
3. 报告文件继续使用：

```text
{trade_date}_{result_type}.md
```

4. 结构化缓存文件统一使用固定文件名：

```text
result.json
manifest.json
```

5. 唯一性优先由目录层级表达，而不是把所有参数拼进单文件名

---

## 6. 建议的 manifest 字段

后续如果启用本地缓存，建议每个 `manifest.json` 至少包含：

```json
{
  "artifact_type": "stock_manager_summary",
  "module": "agents.manager.stock_manager",
  "trade_date": "20260309",
  "ts_code": "600519.SH",
  "criteria_hash": null,
  "created_at": "2026-03-09T15:30:00+08:00",
  "input_signature": {},
  "prompt_version": "v1",
  "code_version": "unknown",
  "status": "success"
}
```

说明：

- `artifact_type`：结构化结果类型，例如 `macro_manager_summary`
- `module`：产出该结果的模块路径
- `trade_date` / `ts_code` / `criteria_hash`：唯一性标识
- `prompt_version`：用于将来 prompt 升级后缓存失效
- `code_version`：用于代码逻辑变更后缓存失效
- `status`：建议至少区分 `success` / `failed`

---

## 7. 推荐执行顺序

后续实现缓存优先策略时，建议每个图统一遵循下面的流程：

1. 先根据输入参数生成缓存定位信息
2. 读取本地 `manifest.json + result.json`
3. 若命中且未过期，直接返回本地结果
4. 若缺失或失效，再执行当前 `analyst` / `manager`
5. 执行成功后回写 `result.json + manifest.json`
6. 若需要对外展示，再单独写入 `data/analysis/*.md`

这样可以统一依赖结构化缓存，避免后续层级依赖不可解析的展示型文本文件。
