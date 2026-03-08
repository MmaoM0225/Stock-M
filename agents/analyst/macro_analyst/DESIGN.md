# 宏观经济分析师 - 设计文档（节点 / 图 / 数据结构）

## 一、分析维度与可获取数据

| 维度 | 分析内容 | 数据来源（dataflow 已有接口） |
|------|----------|------------------------------|
| **货币环境** | LPR、CPI、社融 | `MarketDataFetcher.get_shibor_lpr()`、`get_cpi()`、`get_sf_month()` |
| **全球环境** | 美股趋势（可选）、大宗商品 | 美股：`get_yahoo_index_daily()`（SP500/NASDAQ/DJI）；大宗：`get_sge_daily()`（黄金）、`get_fut_daily()`（原油等期货） |
| **市场本身** | A 股、指数趋势、成交额、波动率等 | 指数日线：`KLineDataFetcher.get_index_daily_data()`；指数每日指标：`get_index_dailybasic()`（换手、市值等）；成交额/成交量在 index_daily 的 amount/vol；波动率由日线 close 计算 |

---

## 二、配置扩展（config）

### 2.1 配置项

在 **agents/config** 或宏观分析师专属配置中增加：

- **`macro_use_us_stock_trend`**（bool）：是否在「全球环境」中纳入美股趋势判断。  
  - `True`：拉取美股指数并参与全球环境分析；  
  - `False`：不拉美股、不输出美股趋势结论，仅大宗商品等。

（其他可选：宏观分析回溯区间、默认指数列表、大宗品种列表等，可在实现时再定。）

### 2.2 配置传递方式

**配置不作为 State 的一部分**。配置是「控制参数」，不随图执行而演进，建议通过以下方式传递：

1. **RunnableConfig**（推荐）：调用时通过 `graph.invoke(input, config={"configurable": {"macro_config": {...}}})` 传入，节点通过 `config["configurable"]["macro_config"]` 读取。
2. **图构建时注入**：在 `build_macro_graph(macro_config)` 中通过闭包注入，节点在定义时捕获 config。

State 仅承载 `trade_date`、原始数据、分析结果等业务数据。

---

## 三、节点（Nodes）

| 节点名 | 职责 | 输入（State） | 配置（非 State） | 输出（写回 State） |
|--------|------|---------------|------------------|--------------------|
| **macro_fetch** | 根据 trade_date 与 config 拉取所有原始数据 | `trade_date` | `macro_config`（RunnableConfig 或闭包） | `lpr_data`, `cpi_data`, `sf_data`, `us_stock_data`（可选）, `commodity_data`, `index_data`, `index_dailybasic`（或合并为 `market_index_data`） |
| **monetary_analysis** | 货币环境分析 | `lpr_data`, `cpi_data`, `sf_data` | — | `monetary_analysis`（结构化结论） |
| **global_analysis** | 全球环境分析 | `us_stock_data`（若启用）、`commodity_data` | `macro_config`（决定是否含美股趋势） | `global_analysis`（含美股趋势与否由 config 决定） |
| **market_analysis** | A 股/指数/成交额/波动率分析 | `index_data`, `index_dailybasic`（或等价的市场日线与指标） | `market_analysis`（结构化结论） |
| **macro_reduce** | 汇总三块结论，生成最终宏观报告 | `monetary_analysis`, `global_analysis`, `market_analysis`, `trade_date` | — | `macro_analysis`（最终报告） |

说明：

- **macro_fetch**：统一负责调用 dataflow（MarketDataFetcher + KLineDataFetcher），按日期与 config 决定是否拉美股；大宗可固定品种（如黄金、原油主力合约等）或由配置指定。
- **monetary / global / market**：三节点只做「解读」，不拉数据；输入为空或缺失时，可输出「数据缺失」或跳过该维度。
- **macro_reduce**：纯汇总 + 可选 LLM 润色，输出一份 `macro_analysis` 供上层使用。

---

## 四、图结构（Graph）

- **推荐：数据拉取 → 三路并行分析 → 汇总**

```
                    START
                       │
                       ▼
               ┌───────────────┐
               │  macro_fetch  │
               └───────┬───────┘
                       │
         ┌─────────────┼─────────────┐
         ▼             ▼             ▼
  monetary_analysis  global_analysis  market_analysis
         │             │             │
         └─────────────┼─────────────┘
                       ▼
               ┌───────────────┐
               │ macro_reduce  │
               └───────┬───────┘
                       │
                       ▼
                      END
```

- **实现方式**：  
  - 从 `macro_fetch` 出来后用 **conditional_edges** 或 **Send** 并行到 `monetary_analysis`、`global_analysis`、`market_analysis` 三个节点（LangGraph 支持多路并行）。  
  - 三路都完成后汇聚到 `macro_reduce`，再到 END。  
  - 若框架更习惯「串行」：可改为  
    `macro_fetch → monetary_analysis → global_analysis → market_analysis → macro_reduce → END`，逻辑等价，仅无并行。

---

## 五、数据结构（State）

### 5.1 状态定义（TypedDict，total=False）

```text
MacroState
├── trade_date: str                    # 分析基准日 YYYYMMDD（入口参数）
│
├── # === 原始数据（由 macro_fetch 写入）===
├── lpr_data: Optional[DataFrame/List]  # LPR 序列
├── cpi_data: Optional[DataFrame/List]  # CPI 序列
├── sf_data: Optional[DataFrame/List]   # 社融月度
├── us_stock_data: Optional[DataFrame/List]  # 美股指数日线（SP500/NASDAQ/DJI 等）
├── commodity_data: Optional[Dict]     # 大宗：如 {"gold": df, "crude": df} 或列表
├── index_data: Optional[DataFrame/List]     # A 股指数日线（如 000001.SH, 399001.SZ 等）
├── index_dailybasic: Optional[DataFrame/List]  # 指数每日指标（换手、市值等），可与 index_data 合并
│
├── # === 各维度分析结果（由三个 analysis 节点写入）===
├── monetary_analysis: Optional[Dict]   # 货币环境结论
├── global_analysis: Optional[Dict]      # 全球环境结论（含/不含美股趋势）
├── market_analysis: Optional[Dict]     # 市场本身结论
│
├── # === 最终输出（由 macro_reduce 写入）===
├── macro_analysis: Optional[Dict]      # 汇总后的宏观分析报告
│
└── messages: List[Any]                # 可选，日志/错误信息
```

（实际实现时 DataFrame 在 State 里可用「可序列化」的 dict/list 或约定好的结构代替，以便 LangGraph 持久化。）

**说明**：`macro_config` 不在 MacroState 中，通过 RunnableConfig 或图构建时闭包注入传递给 macro_fetch、global_analysis 等需要它的节点。

### 5.2 各分析结果子结构（建议）

- **monetary_analysis**  
  - 字段建议：`lpr_trend`, `cpi_trend`, `sf_trend`, `liquidity_summary`, `conclusion`（简短文字结论）等。

- **global_analysis**  
  - 若 `macro_use_us_stock_trend=True`：`us_stock_trend`（如 bullish/bearish/neutral）、`us_stock_comment`。  
  - 大宗：`commodity_summary`（黄金、原油等方向/结论）。  
  - 汇总：`global_risk` 或 `global_conclusion`。

- **market_analysis**  
  - 指数趋势、成交额变化、波动率水平等；字段如：`index_trend`, `turnover_summary`, `volatility_summary`, `market_conclusion`。

- **macro_analysis**  
  - 至少包含：`date`, `monetary`, `global`, `market`，以及顶层 `summary` 或 `conclusion`；可再带 `raw` 引用上述三块。

---

## 六、与新闻分析师的对比

| 项目 | 新闻分析师 | 宏观经济分析师 |
|------|------------|----------------|
| 入口 | trade_date | trade_date（config 单独传入） |
| 数据来源 | 新闻 API/本地 | dataflow（LPR/CPI/社融/美股/大宗/指数） |
| 并行 | map-reduce（按 section 并行 extract） | 三路并行（货币/全球/市场） |
| 汇总 | news_reduce → news_analysis | macro_reduce → macro_analysis |
| 配置 | 无特别开关 | macro_use_us_stock_trend 等 |

---

## 七、小结

- **节点**：macro_fetch → monetary_analysis、global_analysis、market_analysis（可并行）→ macro_reduce。  
- **图**：START → macro_fetch → [三条分析边] → macro_reduce → END。  
- **状态**：MacroState 含 trade_date、原始数据槽位、三块分析结果、macro_analysis 与可选 messages；config 不纳入 State。  
- **配置**：通过 `macro_use_us_stock_trend` 控制是否拉取并判断美股趋势；数据均来自现有 dataflow 接口。

本文档仅定义节点、图结构与数据结构，不涉及具体代码实现。
