# 首页与宏观经济页接口文档（当前阶段）

本文档当前包含：
- 首页两个接口
- 宏观经济分析师页面两个真实数据接口（日期列表 + 按日期结果）

- Base URL：`http://127.0.0.1:8000`
- API 前缀：`/api/v1`
- 请求方法：全部 `GET`
- 统一响应外层：

```json
{
  "success": true,
  "message": "ok",
  "data": {}
}
```

## GET /api/v1/portfolio/{version}/{trade_date}

### 用途

渲染关键指标卡、持仓占比图、行业分布图、全部持仓表。

### 路径参数

- `version`：策略版本标识
- `trade_date`：持仓日期，支持 `YYYY-MM-DD` 或 `YYYYMMDD`

### data 结构

```json
{
  "version": "202401-202604_7d_for_once_ver1.3",
  "trade_date": "2026-04-22",
  "metrics": {
    "annualized_return_pct": 15.6,
    "sharpe_ratio": 1.42,
    "max_drawdown_pct": -4.93,
    "one_week_return_pct": 2.87,
    "one_month_return_pct": 7.64
  },
  "positions": [
    {
      "ts_code": "600519.SH",
      "name": "贵州茅台",
      "industry": "白酒",
      "weight": 16.2,
      "shares": 100,
      "cost_price": 1420.0,
      "latest_price": 1498.5
    }
  ]
}
```

### 字段说明

- `metrics.annualized_return_pct`：历史收益（年化，%）
- `metrics.sharpe_ratio`：夏普比率
- `metrics.max_drawdown_pct`：最大回撤（%）
- `metrics.one_week_return_pct`：近一周收益（%）
- `metrics.one_month_return_pct`：近一月收益（%）
- `positions`：当期全部持仓（用于柱状图、饼图和表格）

## GET /api/v1/portfolio/{version}/history

### 用途

渲染总收益率折线与历史收益表。

### 路径参数

- `version`：策略版本标识

### 查询参数

- `start_date`：开始日期，支持 `YYYY-MM-DD` 或 `YYYYMMDD`（可选）
- `end_date`：结束日期，支持 `YYYY-MM-DD` 或 `YYYYMMDD`（可选）
- 未传参数时默认返回该版本下全量交易日

### 请求示例

```http
GET /api/v1/portfolio/202401-202604_7d_for_once_ver1.3/history?start_date=2026-04-01&end_date=2026-04-22
```

### data 结构

```json
{
  "series": [
    {
      "date": "2026-04-14",
      "net_value": 1.017,
      "daily_return_pct": 0.52,
      "drawdown_pct": -0.72
    }
  ]
}
```

### 字段说明

- `series`：历史收益时间序列（按日期升序）
- `date`：日期（`YYYY-MM-DD`）
- `net_value`：净值
- `daily_return_pct`：单日收益率（%）
- `drawdown_pct`：当日回撤（%）

### 交互说明

- `version` 下拉直接复用：`GET /api/v1/data/portfolio/versions`
- `trade_date` 下拉直接复用：`GET /api/v1/data/portfolio/{version}/dates`

## GET /api/v1/home/agent-outputs

### 用途

首页「Agents 输出预览」列表。

### 查询参数

- `page`：页码，默认 `1`，最小 `1`
- `page_size`：每页数量，默认 `4`，范围 `1~200`

### 请求示例

```http
GET /api/v1/home/agent-outputs?page=1&page_size=4
```

### data 结构

```json
{
  "page": 1,
  "page_size": 4,
  "total": 9,
  "items": [
    {
      "agent": "宏观经理（Macro Manager）",
      "output": "经济动能温和修复......",
      "signal": "中性偏多",
      "updated_at": "2026-04-21 09:05:00",
      "to": "/data/macro"
    }
  ]
}
```

### 字段说明

- `page`：当前页码
- `page_size`：当前页容量
- `total`：可用 agent 预览总数
- `items`：当前页列表
  - `agent`：Agent 名称
  - `output`：最近一天的有效输出摘要（已做截断）
  - `signal`：信号/倾向（可选字段；没有时不返回）
  - `updated_at`：更新时间，格式 `YYYY-MM-DD HH:mm:ss`
  - `to`：前端跳转路径

### 处理规则

- 每个 agent 仅取其目录下**最近一天** `result.json`
- `output` 优先从结构化摘要字段提取；若无则尝试从 `events[].summary` 兜底
- `signal` 仅在存在对应字段时返回，不再强制默认值

### 当前覆盖的 agent（9 个）

- 宏观经理（Macro Manager）
- 板块经理（Sector Manager）
- 选股分析师（Stock Screener）
- 股票池经理（Stock Pool Manager）
- 组合决策（Portfolio Decision）
- 宏观经济分析师（Macro Economist）
- 新闻分析师（News Analyst）
- 市场情绪分析师（Market Sentiment）
- 商品分析师（Commodity Analyst）

## GET /api/v1/home/portfolio/summary

### 用途

首页「当前组合前五大持仓 + 近 5 次收益率 + 指标卡」。

### 请求参数

无。

### 请求示例

```http
GET /api/v1/home/portfolio/summary
```

### data 结构

```json
{
  "top_positions": [
    {
      "name": "中际旭创",
      "cost_price": 129.4,
      "weight": 12.6
    }
  ],
  "monthly_returns": [
    { "month": "03-05", "value": 0.0 },
    { "month": "03-14", "value": -1.46 },
    { "month": "03-25", "value": -3.29 },
    { "month": "04-03", "value": -4.02 },
    { "month": "04-16", "value": -6.0 }
  ],
  "metrics": {
    "max_drawdown_pct": -6.0,
    "total_return_pct": 0.38
  }
}
```

### 字段说明

- `top_positions`：最新一期组合前五大持仓（不含现金）
  - `name`：资产名称（`资产名称`）
  - `cost_price`：成本价（`成本价`）
  - `weight`：仓位百分比（来自 `仓位`，去掉 `%`）
- `monthly_returns`：近 5 次收益率序列
  - `month`：日期标签，格式 `MM-DD`
  - `value`：收益率（百分比，保留 2 位小数）
- `metrics`：指标卡
  - `max_drawdown_pct`：最大回撤（百分比）
  - `total_return_pct`：总收益率（百分比）

### 数据源与计算口径

- 数据源固定：`data/artifacts/decision/overall_ver1.4/portfolio`
- `top_positions`：取最新日期 `portfolio_table` 前五条股票持仓（过滤现金）
- `monthly_returns`：取近 5 个快照，按“相对近 5 次中的第 1 次净值”计算累计收益率
- `metrics.total_return_pct`：全序列首末净值收益率
- `metrics.max_drawdown_pct`：全序列滚动峰值口径的最大回撤

## GET /api/v1/data/macro/dates

### 用途

`/data/macro` 页面日期选择。

### 请求参数

无。

### 请求示例

```http
GET /api/v1/data/macro/dates
```

### data 结构

```json
{
  "dates": ["20260422", "20260421", "20260420"]
}
```

### 字段说明

- `dates`：可选交易日列表（降序，最新日期在前）

### 数据来源与规则

- 数据目录：`data/artifacts/manager/macro_manager`
- 仅返回包含 `result.json` 的日期目录
- 日期格式：`YYYYMMDD`

## GET /api/v1/data/macro/{trade_date}

### 用途

渲染市场方向、仓位建议、重点方向、风险因子与宏观摘要。

### 路径参数

- `trade_date`：交易日，格式 `YYYYMMDD`

### data 结构

```json
{
  "trade_date": "20260422",
  "market_regime": "流动性偏宽松，增长弱修复，市场情绪偏悲观。",
  "market_direction": "neutral-bearish",
  "target_position": "30%-45%",
  "focus_industry_sectors": ["贵金属", "有色金属"],
  "focus_concept_sectors": ["避险资产", "资源品"],
  "avoid_sectors": ["地产链", "可选消费"],
  "macro_themes": ["风险偏好回落", "防御配置优先"],
  "risk_factors": ["制造业景气偏弱", "需求修复不及预期"],
  "confidence": 0.55,
  "macro_summary": "市场延续弱势震荡，策略以防御与结构性机会为主。"
}
```

### 字段说明

- `trade_date`：当前数据交易日
- `market_regime`：市场状态描述
- `market_direction`：市场方向判断
- `target_position`：建议仓位区间
- `focus_industry_sectors`：重点行业列表
- `focus_concept_sectors`：重点概念列表
- `avoid_sectors`：规避方向列表
- `macro_themes`：宏观主题列表
- `risk_factors`：风险因子列表
- `confidence`：置信度（0~1）
- `macro_summary`：宏观摘要

## GET /api/v1/data/sector/dates

### 用途

`/data/sector` 页面日期选择。

### 请求参数

无。

### 请求示例

```http
GET /api/v1/data/sector/dates
```

### data 结构

```json
{
  "dates": ["20250320", "20250319", "20250318"]
}
```

### 字段说明

- `dates`：可选交易日列表（降序，最新日期在前）

### 数据来源与规则

- 数据目录：`data/artifacts/manager/sector_manager`
- 仅返回包含 `result.json` 的日期目录
- 日期格式：`YYYYMMDD`

## GET /api/v1/data/sector/{trade_date}

### 用途

渲染市场状态、市场偏向、执行偏向、优选/观察/风险板块、核心信号与行业经理总结。

### 路径参数

- `trade_date`：交易日，格式 `YYYYMMDD`

### data 结构

```json
{
  "trade_date": "20250320",
  "market_regime": "分化",
  "market_bias": "偏空",
  "action_bias": "防御",
  "favored_sectors": ["黄金", "贵金属", "贵金属Ⅲ"],
  "watchlist_sectors": ["冰雪产业", "火电", "公路铁路运输"],
  "risk_sectors": ["房地产开发", "汽车整车", "证券"],
  "core_signals": [
    "宏观避险情绪升温，明确聚焦黄金等贵金属板块。",
    "行业趋势显示电信、医疗、生物科技等主线持续走强。"
  ],
  "confidence": 0.5,
  "sector_summary": "当日行业结构呈现分化，趋势主线与宏观避险主线并存..."
}
```

### 字段说明

- `trade_date`：当前数据交易日
- `market_regime`：行业层市场状态（英文枚举已映射为中文，如 `mixed`→`分化`、`trend_following`→`趋势延续` 等）
- `market_bias`：市场偏向（`bullish/neutral/bearish` → `偏多/中性/偏空`）
- `action_bias`：执行偏向（如 `defense`→`防御`、`wait_and_see`→`观望`）
- `favored_sectors`：优选板块名称列表
- `watchlist_sectors`：观察板块名称列表
- `risk_sectors`：风险规避板块名称列表
- `core_signals`：核心信号要点列表
- `confidence`：置信度（0~1）
- `sector_summary`：行业经理综合总结

### 数据来源与规则

- 数据文件：`data/artifacts/manager/sector_manager/{trade_date}/result.json`
- `market_regime`、`market_bias`、`action_bias` 为便于前端展示已做中文映射；列表与长文本字段保持原文

## GET /api/v1/data/screener/dates

### 用途

`/data/stock/stock-screener` 页面日期选择。

### 请求参数

无。

### 请求示例

```http
GET /api/v1/data/screener/dates
```

### data 结构

```json
{
  "dates": ["20250320", "20250319", "20250318"]
}
```

### 字段说明

- `dates`：可选交易日列表（降序，最新日期在前）

### 数据来源与规则

- 数据目录：`data/artifacts/analyst/stock_analyst/stock_screener`
- 仅返回包含 `result.json` 的日期目录
- 日期格式：`YYYYMMDD`

## GET /api/v1/data/screener/{trade_date}

### 用途

渲染筛选摘要、已应用条件、板块分布、板块模板映射、各板块入选数量与入选股票清单。

### 路径参数

- `trade_date`：交易日，格式 `YYYYMMDD`

### data 结构

```json
{
  "trade_date": "20250320",
  "total_count": 12,
  "filter_summary": "从 5152 只股票中筛选出 12 只",
  "applied_filters": [
    "板块:黄金,贵金属,...",
    "剔除ST",
    "市值:80亿-",
    "最多12只",
    "排序:total_mv(倒序)"
  ],
  "sector_distribution": {
    "小金属": 1,
    "火力发电": 4,
    "路桥": 2
  },
  "sector_template_applied": {
    "黄金": "value_defensive",
    "火电": "value_defensive"
  },
  "sector_pick_counts": {
    "冰雪产业": 4,
    "火电": 4
  },
  "sector_template_plan": {
    "黄金": {
      "template_id": "value_defensive",
      "overrides": {},
      "confidence": 0.7
    }
  },
  "filtered_stocks": [
    {
      "ts_code": "600459.SH",
      "name": "贵研铂业",
      "industry": "小金属",
      "close": 13.98,
      "pe": 26.1396,
      "pe_ttm": 22.0192,
      "pb": 1.6869,
      "total_mv": 1063852.246,
      "circ_mv": 1045295.25,
      "turnover_rate": 0.4974,
      "volume_ratio": 1.01,
      "dv_ratio": 1.1804,
      "ps_ttm": 0.2261
    }
  ]
}
```

### 字段说明

- `trade_date`：当前结果对应交易日
- `total_count`：入选股票只数
- `filter_summary`：一句话筛选摘要
- `applied_filters`：已应用筛选条件说明列表
- `sector_distribution`：入选结果按行业名称聚合后的数量（行业名 → 只数）
- `sector_template_applied`：各关注板块选用的筛选模板 ID（板块名 → 模板 ID）
- `sector_pick_counts`：各关注板块在最终结果中的入选只数
- `sector_template_plan`：可选，模板 + 覆盖参数 + 置信度（`overrides` 为字段级覆盖，键名与模板参数一致、建议 snake_case）
- `filtered_stocks`：入选股票明细（字段与筛选节点输出一致，含 `ts_code`、`name`、`industry`、`close`、估值与市值、换手、量比、`dv_ratio`、`ps_ttm` 等）

### 数据来源与规则

- 数据文件：`data/artifacts/analyst/stock_analyst/stock_screener/{trade_date}/result.json`
- 无该日 `result.json` 时不会出现在 `.../screener/dates`；详情请求该日返回 404
- 筛选模板库默认规则可由前端静态配置；本接口不强制返回模板定义全文

## GET /api/v1/data/fundamental/ts_codes

### 用途

`/data/stock/stock-fundamental-analyst` 页面「选择股票」。

### 请求参数

无。

### 请求示例

```http
GET /api/v1/data/fundamental/ts_codes
```

### data 结构

```json
{
  "ts_codes": ["000027.SZ", "600900.SH"]
}
```

### 字段说明

- `ts_codes`：可选股票代码列表（按代码字典序）

### 数据来源与规则

- 数据目录：`data/artifacts/analyst/stock_analyst/stock_fundamental_analyst`
- 目录结构：`{ts_code}/{trade_date}/result.json`
- 凡某 `ts_code` 下任意交易日存在该文件，则该 `ts_code` 进入列表

## GET /api/v1/data/fundamental/{ts_code}/dates

### 用途

页面「选择日期」；在已选股票下列出可查看的交易日。

### 路径参数

- `ts_code`：证券代码（如 `000027.SZ`）

### 请求示例

```http
GET /api/v1/data/fundamental/000027.SZ/dates
```

### data 结构

```json
{
  "ts_code": "000027.SZ",
  "dates": ["20240228", "20240117"]
}
```

### 字段说明

- `dates`：该股票已生成基本面报告的交易日列表（降序，最新日期在前）
- 日期格式：`YYYYMMDD`

### 数据来源与规则

- 扫描 `data/artifacts/analyst/stock_analyst/stock_fundamental_analyst/{ts_code}/*/result.json`

## GET /api/v1/data/fundamental/{ts_code}/{trade_date}

### 用途

渲染公司画像、抓取状态、综合评分与结论、估值/利润/现金流/负债/分红等序列图表数据。

### 路径参数

- `ts_code`：证券代码（如 `000027.SZ`）
- `trade_date`：交易日，格式 `YYYYMMDD`

### data 结构

```json
{
  "ts_code": "000027.SZ",
  "trade_date": "20240117",
  "company": {},
  "fetch_status": {},
  "reduce_result": {},
  "valuation_trend": [],
  "income_trend": [],
  "cashflow_trend": [],
  "liability_trend": [],
  "dividend_yield_trend": []
}
```

### 字段说明

- `company`：公司静态画像
- `fetch_status`：各模块抓取或合并情况
- `reduce_result`：Agent 归约后的结论与评分
- `valuation_trend`：估值与价格序列
- `income_trend`：利润表相关序列
- `cashflow_trend`：现金流量表核心项
- `liability_trend`：资产、负债与资产负债率
- `dividend_yield_trend`：股息率时间序列（来自日频基础面数据，含 `dv_ratio`、`dv_ttm`）

### 数据来源与规则

- 数据文件：`data/artifacts/analyst/stock_analyst/stock_fundamental_analyst/{ts_code}/{trade_date}/result.json`
- `trade_date` 非 `YYYYMMDD` 时返回 400
- 无对应 `result.json` 时返回 404

## GET /api/v1/data/technical/ts_codes

### 用途

`/data/stock/stock-technical-analyst` 页面「选择股票」下拉框。

### 请求参数

无。

### 请求示例

```http
GET /api/v1/data/technical/ts_codes
```

### data 结构

```json
{
  "ts_codes": ["000060.SZ", "000027.SZ"]
}
```

## GET /api/v1/data/technical/{ts_code}/dates

### 用途

页面「选择日期」下拉框。

### 路径参数

- `ts_code`：证券代码（如 `000060.SZ`）

### 请求示例

```http
GET /api/v1/data/technical/000060.SZ/dates
```

### data 结构

```json
{
  "ts_code": "000060.SZ",
  "dates": ["20241104", "20241031"]
}
```

### 字段说明

- `dates`：该股票可选交易日列表（降序，最新日期在前）
- 日期格式：`YYYYMMDD`

## GET /api/v1/data/technical/{ts_code}/{trade_date}

### 用途

渲染技术评分、趋势信号、支撑/压力位、K 线与成交量、指标文字解读、技术结论与风险提示。

### 路径参数

- `ts_code`：证券代码（如 `000060.SZ`）
- `trade_date`：交易日，格式 `YYYYMMDD`

### data 结构

```json
{
  "ts_code": "000060.SZ",
  "trade_date": "20241104",
  "start_date": "20240329",
  "latest_price": 5.05,
  "latest_pct_chg": 0.7984,
  "support_levels": [4.85, 4.6],
  "resistance_levels": [5.1, 5.11],
  "technical_score": 65,
  "trend_signal": "uptrend",
  "trend_strength": "medium",
  "short_term_outlook": "短期价格可能面临回调压力...",
  "risk_reminder": "关注超买后回撤、放量冲高回落...",
  "summary": "股票整体仍处上升结构，短中期均线维持多头排列...",
  "indicators": {
    "ma": "多头排列，短期MA（5/10/20）高于MA60。",
    "macd": "DIF与DEA仍在零轴上方，但柱体较短，动能趋缓。",
    "rsi": "RSI14=74.29，位于超买区。",
    "kdj": "KDJ高位，J值偏高，短线震荡概率提升。",
    "boll": "价格接近布林上轨，存在上方压力。"
  },
  "stock_kline_data": [
    {
      "trade_date": "20241104",
      "open": 5.08,
      "high": 5.1,
      "low": 4.96,
      "close": 5.05,
      "pct_chg": 0.7984,
      "vol": 900503.43
    }
  ],
  "recent_bars": [
    {
      "trade_date": "20241104",
      "close": 5.05,
      "pct_chg": 0.7984,
      "ma5": 4.934,
      "ma10": 4.884,
      "ma20": 4.8545,
      "ma60": 4.383,
      "rsi14": 74.29,
      "macd_dif": 0.1459,
      "macd_dea": 0.1445,
      "macd_hist": 0.0028,
      "k": 72.61,
      "d": 68.39,
      "j": 81.05,
      "boll_upper": 5.1117,
      "boll_mid": 4.8545,
      "boll_lower": 4.5973
    }
  ]
}
```

### 数据来源与规则

- 数据文件：`data/artifacts/analyst/stock_analyst/stock_technical_analyst/{ts_code}/{trade_date}/result.json`
- `trade_date` 非 `YYYYMMDD` 时返回 400
- 无对应 `result.json` 时返回 404

## GET /api/v1/data/portfolio/versions

### 用途

页面「选择版本」下拉框。

### 请求参数

无。

### 请求示例

```http
GET /api/v1/data/portfolio/versions
```

### data 结构

```json
{
  "versions": ["202401-202604_7d_for_once_ver1.3", "202401-202604_14d_for_once_ver1.2"]
}
```

### 字段说明

- `versions`：可选策略版本列表

## GET /api/v1/data/portfolio/{version}/dates

### 用途

页面「选择日期」下拉框。

### 路径参数

- `version`：策略版本标识

### 请求示例

```http
GET /api/v1/data/portfolio/202401-202604_7d_for_once_ver1.3/dates
```

### data 结构

```json
{
  "dates": ["2026-03-16", "2026-03-09", "2026-03-02"]
}
```

### 字段说明

- `dates`：该版本下可选交易日列表（降序，最新日期在前）
- 日期格式：`YYYY-MM-DD`

## GET /api/v1/data/portfolio/{version}/{trade_date}

### 用途

渲染组合资产表、调仓原因表、资金统计与决策摘要。

### 路径参数

- `version`：策略版本标识
- `trade_date`：交易日，支持 `YYYYMMDD` 或 `YYYY-MM-DD`

### data 结构

```json
{
  "strategy": "202401-202604_7d_for_once_ver1.3",
  "trade_date": "2026-03-16",
  "portfolio_table": [
    {
      "rank": "1",
      "asset_name": "赤峰黄金",
      "ts_code": "600988.SH",
      "market_value": 64480,
      "position": "7.44%",
      "position_change": "3.14%",
      "total_return": "12.66%",
      "total_pnl": 7247.97,
      "asset_type": "个股",
      "shares": 1600,
      "cost_price": 35.77,
      "action": "加仓",
      "open_price": 40.3
    }
  ],
  "operation_reason_table": [
    {
      "asset_name": "紫金矿业",
      "ts_code": "601899.SH",
      "action": "清仓",
      "old_position": "2.19%",
      "new_position": "0.00%",
      "position_change": "-2.19%",
      "execution_price": 35.04,
      "target_amount": 0,
      "actual_amount": 0,
      "shares": 0,
      "cost_price": null,
      "reason": "基本面增长遇阻且技术面处于强下跌趋势，同时属于规避板块，执行清仓。"
    }
  ],
  "decision_summary": "本次调仓后总仓位由 42.00% 变为 53.02%...",
  "meta": {
    "initial_capital": 876632,
    "total_capital": 866730,
    "source_portfolio_path": "data/artifacts/decision/202401-202604_7d_for_once_ver1.3/portfolio/20260305/result.json",
    "generated_at": "2026-04-22T13:45:19.627317+08:00"
  }
}
```

### 字段说明

- `portfolio_table`：组合资产快照（含仓位、市值、收益、操作）
- `operation_reason_table`：调仓动作及其原因
- `decision_summary`：组合层文字总结
- `meta.initial_capital` / `meta.total_capital`：资金统计
- `meta.source_portfolio_path`：来源快照路径
- `meta.generated_at`：结果生成时间
- `strategy`：策略标识（若缺失使用 `version` 回填）

### 数据来源与规则

- 数据文件：`data/artifacts/decision/{version}/portfolio/{trade_date}/result.json`
- `trade_date` 非 `YYYYMMDD` 时返回 400
- `version` 不存在或无对应 `result.json` 时返回 404

## GET /api/v1/data/stock/ts_codes

### 用途

`/data/stock/stock-manager` 页面「选择股票」下拉框。

### 请求参数

无。

### 请求示例

```http
GET /api/v1/data/stock/ts_codes
```

### data 结构

```json
{
  "ts_codes": ["000519.SZ", "000027.SZ"]
}
```

### 字段说明

- `ts_codes`：存在至少一份 stock_manager 报告的证券代码列表

## GET /api/v1/data/stock/{ts_code}/dates

### 用途

页面「选择日期」下拉框。

### 路径参数

- `ts_code`：证券代码（如 `000519.SZ`）

### 请求示例

```http
GET /api/v1/data/stock/000519.SZ/dates
```

### data 结构

```json
{
  "ts_code": "000519.SZ",
  "dates": ["20250410", "20250409"]
}
```

### 字段说明

- `dates`：该股票可选交易日列表（降序，最新日期在前）
- 日期格式：`YYYYMMDD`

## GET /api/v1/data/stock/{ts_code}/{trade_date}

### 用途

渲染综合评分、组件评分、动作信号、关键结论、主要风险与总结。

### 路径参数

- `ts_code`：证券代码（如 `000519.SZ`）
- `trade_date`：交易日，格式 `YYYYMMDD`

### data 结构

```json
{
  "ts_code": "000519.SZ",
  "trade_date": "20250410",
  "success": true,
  "overall_score": 48,
  "confidence": "中",
  "selection_reason": "基本面偏弱与技术面震荡的综合评估结果。",
  "risk_level": "高",
  "component_scores": {
    "fundamental": 45,
    "technical": 55
  },
  "action_signal": "watch",
  "signal_reason": "基本面存在显著盈利与现金流风险，技术面方向不明...",
  "key_points": ["基本面核心矛盾突出...", "技术面呈区间震荡格局..."],
  "risks": ["盈利与增长质量风险...", "现金流断裂风险..."],
  "summary": "中兵红箭呈现显著的财务结构性矛盾..."
}
```

### 字段说明

- `overall_score`：综合评分
- `component_scores`：子模块评分对象（`fundamental`、`technical`）
- `action_signal`：动作信号（已映射：`buy/watch/sell/hold/neutral/unknown` -> `买入/观察/卖出/持有/中性/未知`）
- `selection_reason`：动作信号的简要原因
- `signal_reason`：更详细的信号解释
- `key_points`：关键结论列表
- `risks`：主要风险列表
- `summary`：综合摘要

### 数据来源与规则

- 数据文件：`data/artifacts/manager/stock_manager/{ts_code}/{trade_date}/result.json`
- `trade_date` 非 `YYYYMMDD` 时返回 400
- 无对应 `result.json` 时返回 404

## GET /api/v1/data/stock-pool/dates

### 用途

`/data/stock/stock-pool-manager` 页面「选择日期」下拉框。

### 请求参数

无。

### 请求示例

```http
GET /api/v1/data/stock-pool/dates
```

### data 结构

```json
{
  "dates": ["20240729", "20240726", "20240725"]
}
```

### 字段说明

- `dates`：可选交易日列表（降序，最新日期在前）
- 日期格式：`YYYYMMDD`

## GET /api/v1/data/stock-pool/{trade_date}

### 用途

渲染池子规模、成功/失败统计、候选股列表、Top 列表与逐票分析摘要。

### 路径参数

- `trade_date`：交易日，格式 `YYYYMMDD`

### data 结构

```json
{
  "trade_date": "20240729",
  "pool_size": 12,
  "analyzed_count": 12,
  "analyze_success_count": 10,
  "analyze_error_count": 2,
  "summary_text": "交易日 20240729，自筛选池共 12 只...",
  "candidate_stocks": [
    {
      "ts_code": "000550.SZ",
      "name": "江铃汽车",
      "industry": "汽车整车",
      "overall_score": 73,
      "action_signal": "持有",
      "risk_level": "中",
      "selection_reason": "基本面稳健增长且估值合理...",
      "analyze_error": null
    }
  ],
  "top_stocks": [
    {
      "ts_code": "000550.SZ",
      "name": "江铃汽车",
      "industry": "汽车整车",
      "overall_score": 73,
      "action_signal": "持有",
      "risk_level": "中",
      "selection_reason": "基本面稳健增长且估值合理...",
      "analyze_error": null
    }
  ],
  "per_stock": [
    {
      "ts_code": "000550.SZ",
      "name": "江铃汽车",
      "industry": "汽车整车",
      "stock_manager_summary": {
        "overall_score": 73,
        "confidence": "中",
        "action_signal": "持有",
        "risk_level": "中",
        "key_points": ["2025年营收与净利润增速超40%，增长韧性较强。"],
        "risks": ["盈利能力仍偏薄，成本波动敏感。"],
        "summary": "基本面与技术面共振偏强，建议持有并观察阻力位突破有效性。"
      },
      "error": null
    }
  ]
}
```

### 数据来源与规则

- 数据文件：`data/artifacts/manager/stock_pool_manager/{trade_date}/result.json`
- `action_signal` 已做中文映射（如 `buy/watch/sell/hold`）
- `trade_date` 非 `YYYYMMDD` 时返回 400
- 无对应 `result.json` 时返回 404

## GET /api/v1/data/analyst/macro/economist/dates

### 用途

`/data/macro/macro-economist` 页面日期下拉框。

### 请求参数

无。

### 请求示例

```http
GET /api/v1/data/analyst/macro/economist/dates
```

### data 结构

```json
{
  "dates": ["20260421", "20260420", "20260419"]
}
```

### 字段说明

- `dates`：可选交易日列表（降序，最新日期在前）

### 数据来源与规则

- 数据目录：`data/artifacts/analyst/macro_analyst/macro_economist`
- 仅返回包含 `result.json` 的日期目录
- 日期格式：`YYYYMMDD`

## GET /api/v1/data/analyst/macro/economist/{trade_date}

### 用途

`/data/macro/macro-economist` 页面按日期读取真实分析结果（不使用 mock）。

### 路径参数

- `trade_date`：交易日，格式 `YYYYMMDD`

### 请求示例

```http
GET /api/v1/data/analyst/macro/economist/20260422
```

### data 结构（按页面渲染格式）

```json
{
  "trade_date": "20260421",
  "lpr_data": [
    { "month": "11月", "value": 3.55 },
    { "month": "12月", "value": 3.5 }
  ],
  "cpi_data": [
    { "month": "11月", "value": 0.6 },
    { "month": "12月", "value": 0.4 }
  ],
  "sf_data": [
    { "month": "11月", "value": 1.8 },
    { "month": "12月", "value": 2.1 }
  ],
  "pmi_data": [
    { "month": "11月", "value": 49.8 },
    { "month": "12月", "value": 49.5 }
  ],
  "m2_data": [
    { "month": "11月", "value": 9.8 },
    { "month": "12月", "value": 9.5 }
  ],
  "gdp_data": [
    { "quarter": "Q2", "value": 5.1 },
    { "quarter": "Q3", "value": 5.0 }
  ],
  "llm_output": {
    "gdp_trend": "down",
    "lpr_trend": "stable",
    "cpi_trend": "up",
    "sf_trend": "up",
    "m2_trend": "up",
    "pmi_status": "contraction",
    "growth_signal": "weakening",
    "inflation_signal": "rising",
    "liquidity_signal": "loose",
    "macro_regime": "slowdown",
    "equity_market_bias": "neutral",
    "bond_market_bias": "bullish",
    "commodity_bias": "neutral",
    "liquidity_summary": "M2同比增速维持高位且社融增量回升，显示流动性环境整体宽松，但LPR保持稳定表明政策利率未进一步下调。",
    "conclusion": "经济保持稳健增长，通胀压力较低，流动性环境总体宽松。"
  }
}
```

### 字段说明

- `trade_date`：当前数据交易日
- `lpr_data/cpi_data/sf_data/pmi_data/m2_data`：月度序列
  - `month`：月份标签（格式如 `11月`）
  - `value`：指标值（number）
- `gdp_data`：季度同比序列
  - `quarter`：季度标签（如 `Q1`）
  - `value`：指标值（number）
- `llm_output`：模型结论输出
  - `growth_signal`：增长信号
  - `inflation_signal`：通胀信号
  - `liquidity_signal`：流动性信号
  - `macro_regime`：宏观状态
  - `equity_market_bias`：权益偏好
  - `bond_market_bias`：债券偏好
  - `commodity_bias`：商品偏好
  - `liquidity_summary`：流动性摘要
  - `conclusion`：综合结论

### 错误示例

当指定日期没有产出数据时：

```json
{
  "detail": "result file not found: E:\\Code\\Stock-M\\data\\artifacts\\analyst\\macro_analyst\\macro_economist\\20260401\\result.json"
}
```

### 数据来源说明

- `llm_output`：来自 `data/artifacts/analyst/macro_analyst/macro_economist/{trade_date}/result.json`
- 六组图表序列：通过宏观数据源实时拉取并按页面结构转换；若拉取失败，对应序列返回空数组 `[]`

## GET /api/v1/data/analyst/macro/market-sentiment/dates

### 用途

`/data/macro/market-sentiment-analyst` 页面日期选择。

### 请求参数

无。

### 请求示例

```http
GET /api/v1/data/analyst/macro/market-sentiment/dates
```

### data 结构

```json
{
  "dates": ["20260421", "20260420", "20260419"]
}
```

### 字段说明

- `dates`：可选交易日列表（降序，最新日期在前）

### 数据来源与规则

- 数据目录：`data/artifacts/analyst/macro_analyst/market_sentiment_analyst`
- 仅返回包含 `result.json` 的日期目录
- 日期格式：`YYYYMMDD`

## GET /api/v1/data/analyst/macro/market-sentiment/{trade_date}

### 用途

渲染信号卡、指数列表与综合情绪摘要。

### 路径参数

- `trade_date`：交易日，格式 `YYYYMMDD`

### data 结构

```json
{
  "trade_date": "20260421",
  "index_items": [
    {
      "code": "000001.SH",
      "name": "上证综指",
      "index_trend": "down",
      "turnover_summary": "近期成交量整体平稳...",
      "volatility_summary": "近期波动率有所放大...",
      "market_conclusion": "市场情绪偏空且脆弱...",
      "start_price": 3030,
      "end_price": 2878,
      "start_volume": 255,
      "end_volume": 270,
      "market_series": [
        {
          "date": "2026-02-01",
          "open": 3021.3,
          "high": 3040.5,
          "low": 3002.1,
          "close": 3033.7,
          "volume": 268.5
        }
      ]
    }
  ],
  "sentiment_output": {
    "index_trend": "down",
    "market_sentiment": "bearish",
    "volume_signal": "contracting",
    "volatility_signal": "high",
    "sentiment_summary": "市场整体情绪极度悲观..."
  }
}
```

### 字段说明

- `trade_date`：当前数据交易日
- `index_items`：指数维度分析列表
  - `code`：指数代码
  - `name`：指数名称
  - `index_trend`：指数趋势（`up/down/neutral`）
  - `turnover_summary`：成交量解读
  - `volatility_summary`：波动率解读
  - `market_conclusion`：该指数市场结论
  - `start_price/end_price`：窗口起止价格
  - `start_volume/end_volume`：窗口起止成交量（单位：亿）
  - `market_series`：行情序列（用于 K 线与成交量图）
    - `date`：交易日，格式 `YYYY-MM-DD`
    - `open/high/low/close`：当日 OHLC
    - `volume`：当日成交量（单位：亿）
- `sentiment_output`：综合情绪输出
  - `index_trend`：指数总体趋势
  - `market_sentiment`：市场情绪
  - `volume_signal`：成交量信号
  - `volatility_signal`：波动率信号
  - `sentiment_summary`：综合情绪摘要

### 补充约定

- `market_series` 默认返回最近 60 个交易日
- `volume` 固定单位为“亿”

## GET /api/v1/data/analyst/sector/trend/dates

### 用途

`/data/sector/sector-trend-analyst` 页面日期选择。

### 请求参数

无。

### 请求示例

```http
GET /api/v1/data/analyst/sector/trend/dates
```

### data 结构

```json
{
  "dates": ["20250320", "20250319", "20250318"]
}
```

### 字段说明

- `dates`：可选交易日列表（降序，最新日期在前）

### 数据来源与规则

- 数据目录：`data/artifacts/analyst/sector_analyst/sector_trend_analyst`
- 仅返回包含 `result.json` 的日期目录
- 日期格式：`YYYYMMDD`

## GET /api/v1/data/analyst/sector/trend/{trade_date}

### 用途

渲染趋势主线、修复机会、风险板块与结论摘要（不含 K 线明细）。

### 路径参数

- `trade_date`：交易日，格式 `YYYYMMDD`

### data 结构

```json
{
  "trade_date": "20250320",
  "summary": "市场呈现明显的结构性分化...",
  "conclusion": "建议继续跟踪强势主线，同时关注底部修复机会。",
  "leading_themes": ["无线电信业务Ⅲ", "保健护理机构"],
  "reversal_opportunities": ["冰雪产业", "火电"],
  "top_risk_sectors": ["多种化学制品", "商品化工"],
  "highlights": ["趋势主线呈现多周期动量共振。"],
  "market_regime": "mixed",
  "series_list": [
    {
      "ts_code": "865001.TI",
      "name": "无线电信业务Ⅲ"
    }
  ]
}
```

### 字段说明

- `trade_date`：当前数据交易日
- `summary`：行业趋势摘要
- `conclusion`：结论建议
- `leading_themes`：趋势主线板块列表
- `reversal_opportunities`：修复机会板块列表
- `top_risk_sectors`：风险板块列表
- `highlights`：关键要点列表
- `market_regime`：市场状态（已做映射：`up/down/mixed/trend_following/rotation/repair/risk_off/neutral/stable/unknown` -> `上行/下行/分化/趋势延续/轮动/修复/避险/中性/稳定/未知`）
- `series_list`：板块列表（仅用于前端选择板块）
  - `ts_code`：行业指数代码（同花顺指数代码）
  - `name`：行业名称

### 数据来源与规则

- 结构化分析字段来源：`data/artifacts/analyst/sector_analyst/sector_trend_analyst/{trade_date}/result.json`
- `series_list` 会基于主题名称自动匹配同花顺行业指数代码
- 若本地未配置行情数据依赖或未匹配到指数代码，`series_list` 可能为空数组

## GET /api/v1/data/analyst/sector/trend/series/{code}

### 用途

点击板块后按需拉取 K 线与成交量序列，降低主接口返回体积。

### 路径参数

- `code`：板块代码（如 `865001.TI`）

### data 结构

```json
{
  "ts_code": "865001.TI",
  "name": "无线电信业务Ⅲ",
  "rows": [
    {
      "trade_date": "20250320",
      "open": 1660.706,
      "high": 1671.229,
      "low": 1649.42,
      "close": 1664.753,
      "pct_change": 0.5646,
      "vol": 13224.26
    }
  ]
}
```

### 字段说明

- `ts_code`：板块代码
- `name`：板块名称
- `rows`：时间序列数据（用于 K 线）
  - `trade_date`：交易日（`YYYYMMDD`）
  - `open/high/low/close`：OHLC
  - `pct_change`：当日涨跌幅（%）
  - `vol`：成交量

## GET /api/v1/data/analyst/sector/capital-flow/dates

### 用途

`/data/sector/sector-capital-flow-analyst` 页面日期选择。

### 请求参数

无。

### 请求示例

```http
GET /api/v1/data/analyst/sector/capital-flow/dates
```

### data 结构

```json
{
  "dates": ["20250320", "20250319", "20250318"]
}
```

### 字段说明

- `dates`：可选交易日列表（降序，最新日期在前）

### 数据来源与规则

- 数据目录：`data/artifacts/analyst/sector_analyst/sector_capital_flow_analyst`
- 仅返回包含 `result.json` 的日期目录
- 日期格式：`YYYYMMDD`

## GET /api/v1/data/analyst/sector/capital-flow/{trade_date}

### 用途

渲染资金净额图、热点/风险板块、样本表与结论摘要。

### 路径参数

- `trade_date`：交易日，格式 `YYYYMMDD`

### data 结构

```json
{
  "trade_date": "20250320",
  "summary": "整体市场资金呈净流出状态...",
  "conclusion": "市场资金面偏空，建议谨慎操作。",
  "highlights": ["1日窗口净流出约-882万元。"],
  "market_bias": "偏空",
  "one_day_net_amount": -882,
  "five_day_net_amount": -2529,
  "twenty_day_net_amount": -9564,
  "hot_sectors": ["共封装光学(CPO)", "光纤概念"],
  "risk_sectors": ["融资融券", "深股通"],
  "one_day_sector_flow": [
    { "name": "融资融券", "net_amount": -472 },
    { "name": "共封装光学(CPO)", "net_amount": 186 }
  ],
  "one_day_rows": [
    {
      "trade_date": "20250320",
      "ts_code": "885748.TI",
      "name": "可燃冰",
      "lead_stock": "海默科技",
      "pct_change": 4.76,
      "net_amount": 1
    }
  ]
}
```

### 字段说明

- `trade_date`：当前数据交易日
- `summary`：整体资金流摘要
- `conclusion`：结论建议
- `highlights`：关键要点列表
- `market_bias`：市场偏向（已做映射：`bullish/neutral/bearish/unknown` -> `偏多/中性/偏空/未知`）
- `one_day_net_amount/five_day_net_amount/twenty_day_net_amount`：多窗口资金净额（万元）
- `hot_sectors`：热点板块列表
- `risk_sectors`：风险板块列表
- `one_day_sector_flow`：1 日窗口板块净额分布
  - `name`：板块名称
  - `net_amount`：净额（万元）
- `one_day_rows`：最近一天样本数据
  - `trade_date`：交易日（`YYYYMMDD`）
  - `ts_code`：板块代码
  - `name`：板块名称
  - `lead_stock`：龙头股
  - `pct_change`：涨跌幅（%）
  - `net_amount`：净额（万元）

### 数据来源与规则

- 洞察字段来源：`data/artifacts/analyst/sector_analyst/sector_capital_flow_analyst/{trade_date}/result.json`
- `one_day_net_amount/five_day_net_amount/twenty_day_net_amount`、`one_day_sector_flow`、`one_day_rows` 基于同花顺资金流原始数据实时聚合

## GET /api/v1/data/analyst/macro/news/dates

### 用途

`/data/macro/news-analyst` 页面日期选择。

### 请求参数

无。

### 请求示例

```http
GET /api/v1/data/analyst/macro/news/dates
```

### data 结构

```json
{
  "dates": ["20260422", "20260421", "20260420"]
}
```

### 字段说明

- `dates`：可选交易日列表（降序，最新日期在前）

### 数据来源与规则

- 数据目录：`data/artifacts/analyst/macro_analyst/news_analyst`
- 仅返回包含 `result.json` 的日期目录
- 日期格式：`YYYYMMDD`

## GET /api/v1/data/analyst/macro/news/{trade_date}

### 用途

渲染宏观环境信号卡、新闻事件列表、行业影响分析。

### 路径参数

- `trade_date`：交易日，格式 `YYYYMMDD`

### data 结构

```json
{
  "date": "20260422",
  "events": [
    {
      "source": "Bloomberg",
      "type": "company",
      "summary": "宁德时代发布6分钟快充电池新技术。",
      "industry": ["电池", "汽车零部件"],
      "sentiment": "positive",
      "impact_level": 4
    }
  ],
  "sector_impacts": {
    "电池": {
      "sentiment": "bullish",
      "confidence": 0.85,
      "reason": ["技术创新强化行业景气预期。"]
    }
  },
  "macro_environment": {
    "liquidity": "neutral",
    "policy_bias": "neutral",
    "global_risk": "high",
    "market_sentiment": "neutral"
  }
}
```

## GET /api/v1/data/analyst/macro/commodity/dates

### 用途

`/data/macro/commodity-analyst` 页面日期选择。

### 请求参数

无。

### 请求示例

```http
GET /api/v1/data/analyst/macro/commodity/dates
```

### data 结构

```json
{
  "dates": ["20260421", "20260420", "20260419"]
}
```

### 字段说明

- `dates`：可选交易日列表（降序，最新日期在前）

### 数据来源与规则

- 数据目录：`data/artifacts/analyst/macro_analyst/commodity_analyst`
- 仅返回包含 `result.json` 的日期目录
- 日期格式：`YYYYMMDD`

## GET /api/v1/data/analyst/macro/commodity/{trade_date}

### 用途

渲染商品卡片、K 线/成交量与综合输出。

### 路径参数

- `trade_date`：交易日，格式 `YYYYMMDD`

### data 结构

```json
{
  "trade_date": "20260421",
  "commodity_items": [
    {
      "name": "黄金",
      "trend": "上行",
      "start": 467.73,
      "end": 479.0,
      "price_summary": "价格从...整体涨幅约2.4%",
      "macro_implication": "黄金作为避险资产...",
      "market_series": [
        {
          "date": "2026-02-01",
          "open": 466.2,
          "high": 469.3,
          "low": 464.8,
          "close": 468.9,
          "volume": 32.4
        }
      ]
    }
  ],
  "output_summary": {
    "overall_trend": "下行",
    "commodity_market_trend": "分化",
    "macro_signals": {
      "growth_signal": "走弱",
      "inflation_signal": "回落",
      "risk_sentiment": "风险规避"
    },
    "macro_summary": "经济增长动能减弱，通胀压力下降，市场避险情绪上升。"
  }
}
```

### 字段说明

- `trade_date`：当前数据交易日
- `commodity_items`：商品分析列表
  - `name`：商品名称
  - `trend`：趋势（上行/下行/中性）
  - `start/end`：窗口起止价格
  - `price_summary`：价格行为解读
  - `macro_implication`：宏观含义解读
  - `market_series`：行情序列（用于 K 线与成交量图）
    - `date`：交易日，格式 `YYYY-MM-DD`
    - `open/high/low/close`：当日 OHLC
    - `volume`：当日成交量（单位：亿）
- `output_summary`：综合输出
  - `overall_trend`：总体趋势
  - `commodity_market_trend`：商品市场趋势
  - `macro_signals`：宏观信号汇总
    - `growth_signal`：增长信号
    - `inflation_signal`：通胀信号
    - `risk_sentiment`：风险偏好
  - `macro_summary`：综合摘要

### 补充约定

- `market_series` 默认返回最近 60 个交易日
- `volume` 固定单位为“亿”
