# 策略经理 - 设计文档（节点 / 图 / 数据结构）

## 一、角色定位与职责

**策略经理（Strategy Manager）** 是交易团队中的**决策整合者**，负责将宏观经济分析师和新闻分析师的输出综合为可执行的交易策略建议。

| 职责 | 说明 |
|------|------|
| **信息整合** | 综合 `macro_analysis` 与 `news_analysis`，识别一致性与冲突点 |
| **方向判断** | 输出市场整体方向（看多/看空/中性）及置信度 |
| **板块配置** | 给出行业/板块的配置建议（超配/标配/低配）及理由 |
| **风险提示** | 标注主要风险因素与需关注的不确定性 |
| **可执行输出** | 生成结构化策略报告，供风险管理师、执行层或人工决策使用 |

---

## 二、输入数据来源

| 输入 | 来源 | 结构概要 |
|------|------|----------|
| **macro_analysis** | 宏观经济分析师 | `{ date, monetary, global, market, summary }`；含货币环境、大宗商品、全球风险、A 股技术面 |
| **news_analysis** | 新闻分析师 | `{ date, events, sector_impacts, macro_environment }`；含事件列表、板块情绪、流动性/政策/风险/市场情绪 |
| **trade_date** | 调用方传入 | 策略基准日 YYYYMMDD |

**说明**：策略经理**不拉取原始数据**，仅消费上游分析师的输出。若 `macro_analysis` 或 `news_analysis` 缺失，可降级处理（仅基于可用输入生成策略，或输出「数据不足」提示）。

---

## 三、输出结构（strategy_analysis）

### 3.1 顶层字段

| 字段 | 类型 | 说明 |
|------|------|------|
| **date** | str | 策略基准日 |
| **market_direction** | str | 市场整体方向：`bullish` / `bearish` / `neutral` |
| **direction_confidence** | float | 方向判断置信度 (0-1) |
| **direction_reason** | str | 方向判断的主要理由（1-3 句话） |
| **position_control** | PositionControl | 仓位控制建议 |
| **sector_allocation** | Dict[str, SectorAllocation] | 各板块配置建议 |
| **key_risks** | List[str] | 需关注的主要风险 |
| **key_opportunities** | List[str] | 主要机会点 |
| **summary** | str | 策略日报一句话摘要 |

### 3.2 PositionControl 子结构（仓位控制，百分数 0-100）

| 字段 | 类型 | 说明 |
|------|------|------|
| **position_level** | float | 建议仓位百分数 (0-100)，如 60 表示 60% |
| **position_action** | str | 仓位操作：`add`(加仓) / `reduce`(减仓) / `hold`(维持) |
| **max_position** | float | 建议最大仓位百分数 (0-100)，风控上限 |
| **min_position** | float | 建议最小仓位百分数 (0-100)，风控下限 |
| **reason** | str | 仓位控制理由（1-2 句话） |

### 3.3 SectorAllocation 子结构

| 字段 | 类型 | 说明 |
|------|------|------|
| **allocation** | str | `overweight` / `neutral` / `underweight` |
| **sentiment** | str | `bullish` / `bearish` / `neutral` |
| **confidence** | float | 置信度 (0-1) |
| **reason** | List[str] | 配置理由（简要） |

### 3.4 示例

```json
{
  "date": "20260305",
  "market_direction": "neutral",
  "direction_confidence": 0.65,
  "direction_reason": "宏观货币宽松与政策支持利好，但技术面偏弱、地缘风险高企，市场处于震荡寻底阶段，短期方向不明朗。",
  "position_control": {
    "position_level": 60,
    "position_action": "hold",
    "max_position": 75,
    "min_position": 40,
    "reason": "多空交织，建议中性仓位，保留弹性应对波动。"
  },
  "sector_allocation": {
    "贵金属": {
      "allocation": "overweight",
      "sentiment": "bullish",
      "confidence": 0.8,
      "reason": ["避险情绪升温", "黄金白银价格大涨"]
    },
    "石油石化": {
      "allocation": "overweight",
      "sentiment": "bullish",
      "confidence": 0.75,
      "reason": ["地缘风险推高油价", "能源供需趋紧"]
    },
    "国防军工": {
      "allocation": "neutral",
      "sentiment": "bullish",
      "confidence": 0.6,
      "reason": ["事件性催化", "持续性待观察"]
    },
    "航运": {
      "allocation": "underweight",
      "sentiment": "bearish",
      "confidence": 0.7,
      "reason": ["关税冲击", "中东航线风险"]
    }
  },
  "key_risks": [
    "美国加征关税对全球贸易的冲击",
    "中东地缘冲突升级",
    "A 股技术面偏弱，支撑位有效性待验证"
  ],
  "key_opportunities": [
    "AI 算力与智能经济政策红利",
    "超跌后的技术性反弹机会"
  ],
  "summary": "避险与主题并存，建议超配贵金属、能源，低配航运，关注地缘与关税风险。"
}
```

---

## 四、配置扩展（config）

### 4.1 配置项

| 配置项 | 类型 | 默认值 | 说明 |
|--------|------|--------|------|
| **generate_markdown** | bool | True | 是否生成 Markdown 报告并写入 `data/analysis/YYYYMMDD_strategy_analysis.md` |
| **use_llm_for_markdown** | bool | False | 是否用 LLM 润色 Markdown（多一次调用） |
| **min_sectors** | int | 3 | 至少输出的板块配置数量 |
| **max_sectors** | int | 10 | 最多输出的板块配置数量 |

### 4.2 配置传递方式

与宏观分析师一致：通过 **RunnableConfig** 传入。

```python
config = {
    "configurable": {
        "strategy_config": {
            "generate_markdown": True,
            "use_llm_for_markdown": False,
        }
    }
}
graph.invoke(input, config=config)
```

---

## 五、节点（Nodes）

| 节点名 | 职责 | 输入（State） | 输出（写回 State） |
|--------|------|---------------|-------------------|
| **strategy_synthesize** | 综合 macro + news，生成策略结论 | `macro_analysis`, `news_analysis`, `trade_date` | `strategy_analysis` |
| **strategy_markdown_write** | 将策略报告写入 Markdown 文件 | `strategy_analysis`, `trade_date` | 无（仅写文件） |

**说明**：

- **strategy_synthesize**：核心节点。接收 `macro_analysis` 与 `news_analysis`，通过 LLM 进行综合推理，输出结构化的 `strategy_analysis`。需使用 `with_structured_output` 或严格的 JSON Schema 约束输出格式。
- **strategy_markdown_write**：可选节点。根据 `strategy_config.generate_markdown` 决定是否执行，将 `strategy_analysis` 格式化为 Markdown 并写入 `data/analysis/{trade_date}_strategy_analysis.md`。

---

## 六、图结构（Graph）

策略经理为**单节点主流程 + 条件分支**，结构简单：

```
                    START
                       │
                       ▼
               ┌───────────────────────┐
               │  strategy_synthesize  │
               └───────────┬───────────┘
                           │
                           ▼
               ┌───────────────────────┐
               │  generate_markdown?   │
               └───────────┬───────────┘
                           │
              ┌────────────┴────────────┐
              │ 是                     │ 否
              ▼                         ▼
    strategy_markdown_write            END
              │
              ▼
             END
```

**实现方式**：

- `strategy_synthesize` 为唯一业务节点，完成策略综合。
- 条件边：若 `strategy_config.generate_markdown == True`，则进入 `strategy_markdown_write`；否则直接 END。
- `strategy_markdown_write` 不修改 State，仅写文件，然后 END。

---

## 七、数据结构（State）

### 7.1 状态定义（TypedDict，total=False）

```text
StrategyState
├── trade_date: str                    # 策略基准日 YYYYMMDD（入口参数）
│
├── # === 上游输入（由调用方或父图传入）===
├── macro_analysis: Optional[Dict]      # 宏观经济分析师输出
├── news_analysis: Optional[Dict]       # 新闻分析师输出
│
├── # === 输出（由 strategy_synthesize 写入）===
├── strategy_analysis: Optional[Dict]   # 策略报告（见第三节结构）
│
└── messages: List[Any]                # 可选，日志/错误信息
```

**说明**：`strategy_config` 不纳入 State，通过 `RunnableConfig["configurable"]["strategy_config"]` 传递。

### 7.2 调用入口

策略经理可作为**独立图**调用，也可作为**父工作流子图**被编排。

**独立调用**（需调用方先执行 macro + news，再传入结果）：

```python
input_state = {
    "trade_date": "20260305",
    "macro_analysis": {...},   # 来自 macro_analyst
    "news_analysis": {...},   # 来自 news_analyst
}
result = strategy_graph.invoke(input_state, config=config)
```

**父工作流编排**（推荐）：在更高层图中，macro_analyst 与 news_analyst 并行执行，完成后将 `macro_analysis`、`news_analysis` 传入 strategy_manager 子图。

---

## 八、与上下游的衔接

| 上游 | 输出 | 策略经理消费 |
|------|------|--------------|
| 宏观经济分析师 | macro_analysis | monetary、global、market、summary |
| 新闻分析师 | news_analysis | sector_impacts、macro_environment、events |

| 下游 | 策略经理输出 | 下游消费 |
|------|--------------|----------|
| 风险管理师 | strategy_analysis | market_direction、sector_allocation、key_risks |
| 人工决策 | strategy_analysis / Markdown | 全文 |
| 执行层（未来） | strategy_analysis | sector_allocation、direction |

---

## 九、LLM 提示设计要点

**strategy_synthesize** 的 system prompt 应明确：

1. **角色**：资深策略经理，综合宏观与新闻，输出可执行策略。
2. **输入格式**：说明 macro_analysis 与 news_analysis 的字段含义。
3. **输出约束**：必须返回符合 `strategy_analysis` 结构的 JSON；`market_direction`、`allocation`、`sentiment` 等使用指定枚举。
4. **一致性处理**：当宏观与新闻结论冲突时，需说明权衡逻辑并给出综合判断。
5. **置信度**：要求对方向与板块配置给出 0-1 置信度，反映不确定性。

---

## 十、小结

- **节点**：strategy_synthesize（核心）→ 条件分支 → strategy_markdown_write（可选）。
- **图**：START → strategy_synthesize → [generate_markdown?] → strategy_markdown_write | END。
- **状态**：StrategyState 含 trade_date、macro_analysis、news_analysis、strategy_analysis；config 通过 RunnableConfig 传入。
- **输出**：结构化的 strategy_analysis，可选 Markdown 报告文件。
- **定位**：决策整合层，不拉数据，仅综合上游分析师的结论，输出可执行的策略建议。

本文档仅定义节点、图结构与数据结构，不涉及具体代码实现。
