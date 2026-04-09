# Macro Manager 设计文档

## 1. 目标与职责

**Macro Manager（宏观管理器）** 负责在给定交易日下，编排并运行多个**微观分析师**子图，控制并发度，并汇总各分析师输出，为后续宏观报告或策略提供统一入口。

- **输入**：`trade_date`（YYYYMMDD）
- **输出**：合并后的状态，包含各分析师的结构化结果（新闻、市场情绪、流动性、大宗商品、宏观经济等）

---

## 2. 子图与分析师

当前纳入编排的微观分析师（均为独立 LangGraph 子图）：

| 分析师 | 子图模块 | 输出在 state 中的键名 |
|--------|----------|------------------------|
| 新闻分析师 | `agents.analyst.macro_analyst.news_analyst` | `news_analysis` |
| 市场情绪分析师 | `agents.analyst.macro_analyst.market_sentiment_analyst` | `market_sentiment_analyst_summary` |
| 流动性分析师 | `agents.analyst.macro_analyst.liquidity_analyst` | `liquidity_analyst_summary` |
| 大宗商品分析师 | `agents.analyst.macro_analyst.commodity_analyst` | `commodity_analyst_summary` |
| 宏观经济分析师 | `agents.analyst.macro_analyst.macro_economist` | `macro_economist_analysis` |

每个子图均支持 `invoke({"trade_date": "YYYYMMDD"})`，返回的 state 中包含上表对应键名的结果。

---

## 3. 最大并发子图数量

### 3.1 为何要限制

- 各分析师内部可能有多路并行（如新闻 map-reduce、商品按品种并行、市场情绪按指数并行），且会调用 LLM、数据接口，同时跑满 5 个子图易导致：
  - API 限流或超时
  - 内存与 CPU 峰值过高
  - 日志交错难以排查
- 通过**限制同一时刻正在运行的子图数量**，在耗时与稳定性之间取得平衡。

### 3.2 设计要点

- **配置项**：在最外层 `agents.config` 中提供 `MACRO_MANAGER_MAX_CONCURRENT_SUBGRAPHS`（如 3），表示最多允许同时运行的分析师子图个数。
- **实现方式**（二选一或组合）：
  - **线程池**：使用 `ThreadPoolExecutor(max_workers=MAX_CONCURRENT_SUBGRAPHS)`，将每个子图的 `graph.invoke(...)` 作为任务提交，自然形成“最多 N 个同时在跑”。
  - **信号量**：在单个“并行执行节点”内，用 `threading.Semaphore(MAX_CONCURRENT_SUBGRAPHS)` 或 `asyncio.Semaphore`（若改为异步）控制同时进入 invoke 的子图数量。
- **建议默认值**：3（可根据机器与 API 能力调节为 2～5）。

---

## 4. 并行运行多个微观分析师

### 4.1 策略

- **宏观层面**：5 个分析师子图在“逻辑上”并行执行，由并发控制限制实际同时运行数。
- **实现**：
  - 在一个**编排节点**（如 `run_analysts`）内：
    - 准备输入：`{"trade_date": state["trade_date"]}`。
    - 为每个分析师构造任务：`(name, graph, output_key)`，其中 `output_key` 即上表中的 state 键名。
    - 使用线程池或信号量控制的并行执行：对每个任务执行 `result = graph.invoke(input)`，再从 `result` 中取出 `result.get(output_key)`。
    - 将各 `output_key` 与取出的值合并进当前 state，作为本节点输出。
  - 不要求各子图内部实现异步；子图保持现有同步 `invoke` 即可，由管理器用线程池做“多子图并行”。

### 4.2 依赖与共享

- **数据依赖**：各分析师仅依赖 `trade_date`，彼此无数据依赖，因此可以任意顺序或并行执行。
- **共享资源**：
  - LLM：可共用一个实例，多线程调用时需确认所用 SDK 是否线程安全（多数 HTTP 客户端可接受）。
  - Fetcher（如 `NewsSentimentFetcher`、`MarketDataFetcher`、`KLineDataFetcher`）：若实现为无状态或线程安全，可共享；否则可在各子图内独立创建（当前各 analyst demo 多为节点内自建）。
- **新闻分析师**：唯一需要额外依赖 `NewsSentimentFetcher` 的子图，由 macro_manager 在构建新闻子图时注入。

### 4.3 错误与部分失败

- 单个子图 `invoke` 失败时建议：捕获异常，将该分析师对应的 `output_key` 置为 `None` 或占位结构（如 `{"error": "..."}`），不阻塞其余子图。
- 最终 state 中仍包含所有键名，便于上游统一判断“哪些分析师有结果、哪些失败”。

---

## 5. 图结构（Macro Manager 主图）

- **状态**：使用 `dict` 兼容各子图；至少包含：
  - `trade_date`：输入交易日
  - 以及上述 5 个 `output_key`（由 `run_analysts` 节点写入）
- **节点**：
  - `run_analysts`：并行运行 5 个分析师子图（带最大并发限制），合并结果到 state。
- **边**：`START → run_analysts → END`。
- **可选扩展**：
  - 增加 `macro_summary` 节点：以 5 个分析师结果为输入，再做一次 LLM 综合摘要或生成宏观日报；边为 `run_analysts → macro_summary → END`。可在后续迭代中实现。

---

## 6. 配置与入口

- **配置**：统一放在最外层 `agents.config` 中，例如 `MACRO_MANAGER_MAX_CONCURRENT_SUBGRAPHS`，实现时从 `agents.config` 读取；如需可被环境变量或上层配置覆盖，可在 demo 或图构建处做覆盖逻辑。
- **入口**：提供 `demo.py`，支持：
  - `python -m agents.manager.macro_manager.demo [YYYYMMDD]`
  - 解析日期 → 构建 LLM、各 Fetcher、5 个子图 → 构建 macro_manager 图 → `invoke({"trade_date": ...})` → 打印或保存合并结果。

---

## 7. 文件结构（规划）

```
agents/manager/macro_manager/
├── DESIGN.md          # 本设计文档
├── __init__.py        # 导出 create_macro_manager_graph、main 等
├── node.py            # run_analysts 节点（并行调用子图并合并结果）
├── graph.py           # 主图编排（START → run_analysts → END）
└── demo.py            # 命令行入口，日期解析与 invoke
```

宏观管理器相关配置（如 `MACRO_MANAGER_MAX_CONCURRENT_SUBGRAPHS`）统一写在最外层 `agents/config.py`。

---

## 8. 小结

| 项 | 设计要点 |
|----|----------|
| 最大并发子图数 | 配置项 `MACRO_MANAGER_MAX_CONCURRENT_SUBGRAPHS`（在 `agents.config`），用线程池或信号量限制同时运行的子图数量 |
| 多分析师并行 | 单节点内用线程池执行多个 `graph.invoke()`，按 `output_key` 合并到 state |
| 图结构 | 单节点编排，可选后续增加 macro_summary 综合节点 |
| 依赖 | 仅 trade_date；新闻子图需注入 NewsSentimentFetcher；LLM/Fetcher 可共享或按需创建 |

以上为设计阶段结论，实现时按此文档编写代码即可。
