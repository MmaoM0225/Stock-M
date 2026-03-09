# Stock-M 数据库建表数据总结

> 本地开发使用 SQLite，本文档汇总需要建表的数据及字段设计。

## 目录结构

```
database/
├── config.py       # 连接配置、get_db_session
├── models.py       # ORM 模型
├── data_sync/      # 远程拉取并写入本地（可单独运行）
│   ├── stock_list.py
│   └── industry.py
└── data_access/    # 从本地读取，空则 fallback 到 sync
    ├── stock_list.py
    └── industry.py
```

---

## 一、基础/静态数据（已有 JSON，优先迁移）

### 1. 股票列表 `stock_list`

| 字段 | 类型 | 说明 |
|------|------|------|
| ts_code | TEXT PK | 股票代码，如 000001.SZ |
| symbol | TEXT | 6位代码，如 000001 |
| name | TEXT | 股票名称 |
| area | TEXT | 所属地区 |
| industry | TEXT | 申万行业名称（与行业表可关联） |
| market | TEXT | 市场类型：主板/创业板/科创板/北交所 |
| list_date | INTEGER | 上市日期，YYYYMMDD |
| created_at | DATETIME | 记录创建时间 |
| updated_at | DATETIME | 记录更新时间 |

**数据来源**: `data/stock_list.json`，约 5000+ 条  
**更新频率**: 不定期（新股上市、退市时）

---

### 2. 行业分类 `industry`

一级、二级行业统一存储，通过 `level` 和 `parent_code` 区分层级。

| 字段 | 类型 | 说明 |
|------|------|------|
| index_code | TEXT PK | 行业指数代码，如 801010.SI |
| industry_name | TEXT | 行业名称 |
| level | TEXT | 层级：L1 一级 / L2 二级 |
| industry_code | INTEGER | 行业编码 |
| parent_code | INTEGER | 父级行业编码，0 表示一级 |
| is_pub | INTEGER | 是否公开，0/1 |
| src | TEXT | 数据来源，如 SW2021 |
| created_at | DATETIME | 记录创建时间 |

**数据来源**: `data/industry_list.json`、`data/industry_list_l2.json`  
**更新频率**:  rarely（行业分类调整时）

---

### 3. 股票-行业关联 `stock_industry`（可选）

若需按申万行业指数精确归属，可建关联表；当前 `stock_list.industry` 为文本，也可满足简单查询。

| 字段 | 类型 | 说明 |
|------|------|------|
| ts_code | TEXT | 股票代码 |
| index_code | TEXT | 行业指数代码 |
| level | TEXT | L1/L2 |
| created_at | DATETIME | 记录创建时间 |

**数据来源**: `dataflow/industry_data.fetch_stock_industry()`  
**更新频率**: 随股票列表或行业调整

---

## 二、新闻数据

### 4. 财经早餐摘要 `breakfast_news`

| 字段 | 类型 | 说明 |
|------|------|------|
| id | INTEGER PK | 自增主键 |
| title | TEXT | 标题 |
| summary | TEXT | 摘要 |
| pub_date | TEXT | 发布时间，YYYYMMDD |
| url | TEXT UNIQUE | 链接，用于去重 |
| created_at | DATETIME | 记录创建时间 |

**数据来源**: `data/breakfast_news.json`  
**更新频率**: 每日

---

### 5. 财经新闻详情 `news_detail`

| 字段 | 类型 | 说明 |
|------|------|------|
| id | INTEGER PK | 自增主键 |
| pub_date | TEXT | 新闻日期，YYYYMMDD，唯一索引 |
| url | TEXT | 原文链接 |
| title | TEXT | 标题 |
| content | TEXT | 正文全文 |
| created_at | DATETIME | 记录创建时间 |

**数据来源**: `data/news/news_YYYYMMDD.json`  
**更新频率**: 每日（按日期爬取）

---

### 6. 新闻分节 `news_section`（可选）

若需按主题检索新闻片段，可拆分 sections。

| 字段 | 类型 | 说明 |
|------|------|------|
| id | INTEGER PK | 自增主键 |
| news_id | INTEGER FK | 关联 news_detail.id |
| title | TEXT | 小节标题 |
| content | TEXT | 小节内容 |
| sort_order | INTEGER | 排序序号 |

**数据来源**: `news_detail.content` 中的 `sections` 数组

---

## 三、行情数据（建议入库）

### 7. K线日线 `kline_daily`

| 字段 | 类型 | 说明 |
|------|------|------|
| ts_code | TEXT | 股票代码 |
| trade_date | TEXT | 交易日期 YYYYMMDD |
| open | REAL | 开盘价 |
| high | REAL | 最高价 |
| low | REAL | 最低价 |
| close | REAL | 收盘价 |
| pre_close | REAL | 昨收价 |
| change | REAL | 涨跌额 |
| pct_chg | REAL | 涨跌幅(%) |
| vol | REAL | 成交量(手) |
| amount | REAL | 成交额(千元) |
| adj_factor | REAL | 复权因子 |

**主键**: (ts_code, trade_date)  
**数据来源**: `dataflow/kline_data.fetch_daily_data()`  
**更新频率**: 每日收盘后

---

### 8. 指数日线 `index_daily`（可选）

| 字段 | 类型 | 说明 |
|------|------|------|
| ts_code | TEXT | 指数代码，如 000001.SH |
| trade_date | TEXT | 交易日期 |
| open | REAL | 开盘 |
| high | REAL | 最高 |
| low | REAL | 最低 |
| close | REAL | 收盘 |
| pre_close | REAL | 昨收 |
| change | REAL | 涨跌额 |
| pct_chg | REAL | 涨跌幅 |
| vol | REAL | 成交量 |
| amount | REAL | 成交额 |

**主键**: (ts_code, trade_date)

---

### 9. 每日基本面指标 `daily_basic`

| 字段 | 类型 | 说明 |
|------|------|------|
| ts_code | TEXT | 股票代码 |
| trade_date | TEXT | 交易日期 |
| close | REAL | 收盘价 |
| turnover_rate | REAL | 换手率(%) |
| turnover_rate_f | REAL | 换手率(自由流通股) |
| volume_ratio | REAL | 量比 |
| pe | REAL | 市盈率 |
| pe_ttm | REAL | 市盈率TTM |
| pb | REAL | 市净率 |
| ps | REAL | 市销率 |
| ps_ttm | REAL | 市销率TTM |
| dv_ratio | REAL | 股息率(%) |
| dv_ttm | REAL | 股息率TTM(%) |
| total_share | REAL | 总股本(万股) |
| float_share | REAL | 流通股本(万股) |
| free_share | REAL | 自由流通股本(万) |
| total_mv | REAL | 总市值(万元) |
| circ_mv | REAL | 流通市值(万元) |

**主键**: (ts_code, trade_date)  
**数据来源**: `dataflow/fundamental_data.fetch_daily_basic()`  
**更新频率**: 每日

---

## 四、财务报表（可选，数据量大）

### 10. 利润表 `income_statement`

| 字段 | 类型 | 说明 |
|------|------|------|
| ts_code | TEXT | 股票代码 |
| ann_date | TEXT | 公告日期 |
| f_ann_date | TEXT | 实际公告日期 |
| end_date | TEXT | 报告期 YYYYMMDD |
| report_type | TEXT | 报告类型 |
| revenue | REAL | 营业总收入 |
| n_income | REAL | 净利润 |
| ... | ... | 其他财务字段 |

**主键**: (ts_code, end_date, report_type)  
**数据来源**: `dataflow/fundamental_data.fetch_income_statement()`

---

### 11. 资产负债表 `balance_sheet`

**主键**: (ts_code, end_date, report_type)  
**数据来源**: `dataflow/fundamental_data.fetch_balance_sheet()`

---

### 12. 现金流量表 `cashflow_statement`

**主键**: (ts_code, end_date, report_type)  
**数据来源**: `dataflow/fundamental_data.fetch_cashflow_statement()`

---

### 13. 财务指标 `financial_indicators`

**主键**: (ts_code, end_date, report_type)  
**数据来源**: `dataflow/fundamental_data.fetch_financial_indicators()`

---

## 五、市场数据（可选）

| 表名 | 说明 | 数据来源 |
|------|------|----------|
| money_flow | 资金流向 | market_data.fetch_money_flow |
| margin_detail | 融资融券明细 | market_data.fetch_margin_detail |
| margin_target | 融资融券标的 | market_data.fetch_margin_target |
| dragon_tiger_list | 龙虎榜 | market_data.fetch_dragon_tiger_list |
| top10_holders | 十大股东 | market_data.fetch_top10_holders |
| block_trade | 大宗交易 | market_data.fetch_block_trade |

---

## 六、宏观经济指数（可选）

| 表名 | 说明 | 数据来源 |
|------|------|----------|
| shibor_lpr | Shibor/LPR | market_data.fetch_shibor_lpr |
| cpi | CPI | market_data.fetch_cpi |
| sf_month | 社会融资 | market_data.fetch_sf_month |

---

## 七、建表优先级建议

| 优先级 | 表 | 理由 |
|--------|-----|------|
| P0 | stock_list, industry | 基础数据，当前 JSON 直接对应 |
| P0 | breakfast_news, news_detail | 新闻已用 JSON，迁移后便于查询 |
| P1 | kline_daily | 行情核心，分析必备 |
| P1 | daily_basic | 估值指标常用 |
| P2 | news_section | 若需按主题检索新闻 |
| P2 | index_daily | 若需指数行情 |
| P3 | 财务报表 | 数据量大，按需建表 |
| P3 | 市场数据 | 按需建表 |

---

## 八、数据量估算（供参考）

| 表 | 预估行数 | 说明 |
|----|----------|------|
| stock_list | ~5,000 | 全市场股票 |
| industry | ~400 | L1+L2 |
| breakfast_news | 持续增长 | 每日 1 条 |
| news_detail | 持续增长 | 每日 1 条 |
| kline_daily | 百万级 | 5000 股 × 250 天/年 × N 年 |
| daily_basic | 百万级 | 同上 |

---

## 九、通用字段约定

所有表建议包含（按需）：

- `created_at` DATETIME：记录创建时间
- `updated_at` DATETIME：记录更新时间

便于后续审计和增量同步。
