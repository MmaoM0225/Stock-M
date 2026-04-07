# 个股层 (Stock Manager) 架构设计

## 一、层级定位

个股层是 Stock-M 系统的第三层，承接板块层的输出，负责从候选板块中筛选出具体的投资标的。

**上游输入**: 板块层的 `favored_sectors` (推荐板块)、`watchlist_sectors` (观察板块)
**下游输出**: `candidate_stocks` (候选股票池)、`top_stocks` (优先推荐股票)

---

## 二、分析师团队设计

### 2.1 Stock Screener Analyst（股票筛选分析师）

**职责**: 从全市场或指定板块中初步筛选符合条件的股票

**Input**:
- `stock_pool`: 初始股票池（如全市场、某个板块的成分股）
- `sector_filter`: 板块过滤条件（来自上游板块层）
- `basic_criteria`: 基础筛选条件（市值、上市时间、ST剔除等）

**Output**:
```json
{
  "screener_result": {
    "filtered_stocks": ["000001.SZ", "000002.SZ", ...],
    "total_count": 100,
    "filter_criteria_applied": ["市值>50亿", "非ST", ...],
    "sector_distribution": {
      "半导体": 15,
      "人工智能": 12,
      ...
    }
  }
}
```

**Node 实现**: `create_stock_screener_node()`

---

### 2.2 Fundamental Analyst（基本面分析师）

**职责**: 分析个股的财务健康和估值水平

**Input**:
- `ts_code`: 股票代码
- `trade_date`: 分析日期
- `financial_data`: 财务数据（ROE、营收增速、净利润、负债率等）
- `valuation_data`: 估值数据（PE、PB、PS、PEG等）

**Output**:
```json
{
  "fundamental_analysis": {
    "ts_code": "000001.SZ",
    "fundamental_score": 7.5,
    "quality_rating": "良好",
    "valuation_rating": "合理",
    "key_metrics": {
      "roe_ttm": 12.5,
      "revenue_growth_yoy": 15.2,
      "profit_growth_yoy": 8.3,
      "debt_ratio": 45.2,
      "pe_ttm": 15.8,
      "pb_mrq": 1.2,
      "peg": 1.9
    },
    "red_flags": ["应收账款增长过快"],
    "highlights": ["ROE连续三年>10%", "现金流健康"],
    "recommendation": "适合价值投资者"
  }
}
```

**Node 实现**: `create_fundamental_analysis_node()`
- 内部使用 Map-Reduce 模式批量处理多只股票

---

### 2.3 Technical Analyst（技术分析师）

**职责**: 分析个股的技术面走势和买卖时机

**Input**:
- `ts_code`: 股票代码
- `trade_date`: 分析日期
- `kline_data`: K线数据（日线/周线）
- `indicators`: 技术指标参数

**Output**:
```json
{
  "technical_analysis": {
    "ts_code": "000001.SZ",
    "technical_score": 8.2,
    "trend_signal": "uptrend",
    "trend_strength": "strong",
    "support_levels": [12.5, 11.8],
    "resistance_levels": [14.2, 15.0],
    "technical_indicators": {
      "ma_trend": "多头排列",
      "macd_signal": "金叉",
      "rsi": 65,
      "bollinger_position": "中轨上方",
      "volume_trend": "放量上涨"
    },
    "pattern_recognition": ["突破平台", "量价齐升"],
    "short_term_outlook": "看涨",
    "risk_reminder": "接近前期高点，注意回调"
  }
}
```

**Node 实现**: `create_technical_analysis_node()`
- 内部使用 Map-Reduce 模式批量处理多只股票

---

### 2.4 Sentiment Analyst（个股情绪分析师）

**职责**: 分析个股的市场情绪和市场关注度

**Input**:
- `ts_code`: 股票代码
- `trade_date`: 分析日期
- `news_data`: 个股相关新闻
- `social_data`: 社交媒体数据（雪球、东方财富等）
- `analyst_ratings`: 券商研报评级

**Output**:
```json
{
  "sentiment_analysis": {
    "ts_code": "000001.SZ",
    "sentiment_score": 6.8,
    "sentiment_trend": "improving",
    "news_sentiment": "正面",
    "social_sentiment": "乐观",
    "analyst_consensus": "买入",
    "attention_level": "高",
    "hot_topics": ["业绩预增", "新产品发布"],
    "unusual_activity": ["机构调研增加", "北向资金增持"],
    "risk_alerts": ["大股东减持计划"]
  }
}
```

**Node 实现**: `create_sentiment_analysis_node()`

---

### 2.5 Capital Flow Analyst（个股资金分析师）【补充】

**职责**: 分析个股的资金流入流出情况

**Input**:
- `ts_code`: 股票代码
- `trade_date`: 分析日期
- `capital_flow_data`: 资金流向数据

**Output**:
```json
{
  "capital_flow_analysis": {
    "ts_code": "000001.SZ",
    "flow_score": 7.0,
    "main_force_flow": "净流入",
    "retail_flow": "净流出",
    "net_inflow_5d": 12000000,
    "net_inflow_20d": 45000000,
    "flow_trend": "持续流入",
    "large_order_ratio": 0.35,
    "institutional_activity": "活跃"
  }
}
```

---

## 三、Stock Manager（个股研究经理）

**职责**: 整合各分析师结果，生成最终的选股建议

**Input**:
- `trade_date`: 交易日期
- `screener_result`: 筛选结果（股票池）
- `fundamental_scores`: 基本面评分列表
- `technical_scores`: 技术面评分列表
- `sentiment_scores`: 情绪面评分列表
- `capital_flow_scores`: 资金流评分列表（可选）

**Output**:
```json
{
  "stock_manager_summary": {
    "trade_date": "20260330",
    "market_regime": "trend_following",
    "selection_strategy": "quality_growth",
    
    "candidate_stocks": [
      {
        "ts_code": "000001.SZ",
        "name": "平安银行",
        "overall_score": 8.5,
        "component_scores": {
          "fundamental": 7.5,
          "technical": 8.2,
          "sentiment": 6.8,
          "capital_flow": 7.0
        },
        "sector": "银行",
        "selection_reason": "技术突破+基本面稳健",
        "risk_level": "低",
        "suggested_position": "5-8%"
      },
      ...
    ],
    
    "top_stocks": [
      {
        "rank": 1,
        "ts_code": "002594.SZ",
        "name": "比亚迪",
        "overall_score": 9.2,
        "priority": "high",
        "action": "重点关注",
        "key_drivers": ["业绩超预期", "技术强势", "资金持续流入"]
      },
      ...
    ],
    
    "watchlist": [
      {
        "ts_code": "300750.SZ",
        "name": "宁德时代",
        "watch_reason": "等待回调买入机会",
        "trigger_price": 180.0
      }
    ],
    
    "avoid_stocks": [
      {
        "ts_code": "000XXX.SZ",
        "name": "XXX",
        "avoid_reason": "业绩暴雷+技术破位+资金流出"
      }
    ],
    
    "sector_rotation_candidates": {
      "进攻型": [...],
      "防御型": [...],
      "主题型": [...]
    },
    
    "stock_selection_summary": "当前市场处于趋势跟踪状态，建议优先配置技术强势+基本面优质的成长股...",
    "confidence": 0.75,
    "risk_reminder": "注意控制仓位，避免追高"
  },
  
  "stock_report_path": "data/analysis/20260330_stock_report.md"
}
```

---

## 四、图结构 (Graph Flow)

```
START
  │
  ▼
┌─────────────────────────────────────┐
│  stock_screener                     │  ← 筛选初始股票池
│  (可配置是否基于板块层输出)          │
└─────────────────────────────────────┘
  │
  ▼
┌─────────────────────────────────────┐
│  run_analysts (并行4个子图)          │
│  ├─ fundamental_analyst            │
│  ├─ technical_analyst                │
│  ├─ sentiment_analyst                │
│  └─ capital_flow_analyst             │
└─────────────────────────────────────┘
  │
  ▼
┌─────────────────────────────────────┐
│  stock_summary                      │  ← LLM综合或规则汇总
└─────────────────────────────────────┘
  │
  ▼
┌─────────────────────────────────────┐
│  write_stock_report (可选)           │
└─────────────────────────────────────┘
  │
  ▼
END
```

---

## 五、配置参数

```python
# agents/config.py 新增配置

# 个股层配置
STOCK_GENERATE_MARKDOWN = True
STOCK_USE_LLM_FOR_MARKDOWN = False
STOCK_MANAGER_MAX_CONCURRENT_SUBGRAPHS = 3

# 股票筛选默认参数
STOCK_DEFAULT_MIN_MARKET_CAP = 50e8  # 最小市值50亿
STOCK_DEFAULT_MIN_LISTING_DAYS = 180  # 最小上市时间180天
STOCK_DEFAULT_EXCLUDE_ST = True  # 剔除ST股票
STOCK_DEFAULT_MAX_STOCKS = 100  # 最大分析股票数量

# 评分权重配置
STOCK_SCORE_WEIGHTS = {
    "fundamental": 0.30,
    "technical": 0.35,
    "sentiment": 0.20,
    "capital_flow": 0.15
}
```

---

## 六、文件结构

```
agents/manager/stock_manager/
├── __init__.py
├── graph.py           # 图结构定义
├── node.py            # 节点实现
├── state.py           # 状态类型定义（可选）
├── PLAN.md            # 本设计文档
└── demo.py            # 演示脚本
```

---

## 七、后续扩展建议

1. **个股风险分析师**: 分析个股特有风险（质押风险、解禁风险、业绩变脸风险等）
2. **事件驱动分析师**: 捕捉个股特定事件（业绩公告、分红、股权激励等）
3. **产业链分析师**: 分析个股在产业链中的位置和上下游关系
4. **对标分析师**: 与同行业竞品进行对标分析

---

## 八、实现优先级

1. P0: 基础框架（graph.py + node.py骨架）
2. P0: Stock Manager 汇总节点
3. P1: Stock Screener Analyst
4. P1: Technical Analyst（技术相对成熟）
5. P2: Fundamental Analyst
6. P2: Sentiment Analyst
7. P2: Capital Flow Analyst
