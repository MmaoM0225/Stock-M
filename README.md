# Stock-M

基于 `LangGraph + LLM` 的股票研究与组合决策系统。  
当前仓库以「多智能体流水线 + 本地任务调度 + 结果落盘」为核心，支持：

- 宏观/行业/个股筛选与股票池分析
- 常规组合决策（`portfolio_decision`）
- 长周期满仓组合决策（`full_position_decision`）
- 定时触发日常运行与满仓运行
- 结果可视化（净值曲线、组合表现等）

## 目录说明（当前实际结构）

```text
Stock-M/
├── agents/                         # 多智能体定义（analyst/manager/decision）
├── dataflow/                       # 行情、新闻等数据获取
├── api/                            # FastAPI 接口
├── visualization/                  # 可视化脚本
├── data/artifacts/                 # 各模块执行产物（result.json / manifest.json）
├── live_trading.py                 # 常规链路执行入口
├── long_cycle_full_position_runner.py   # 长周期满仓执行入口
├── schedule_live_trading.py        # 常规定时任务
├── schedule_full_position_trading.py    # 满仓定时任务
└── run_api.py                      # API 启动入口
```

## 执行链路

默认执行依赖顺序（不可并行）：

1. `Macro Manager`
2. `Sector Manager`（依赖宏观输出）
3. `Stock Screener`（依赖行业输出）
4. `Stock Pool Manager`（依赖筛选股票池）
5. `Decision`（读取上游全部结果生成组合）

其中决策层支持两套：

- 常规组合：`agents/decision/portfolio_decision`
- 满仓组合：`agents/decision/full_position_decision`

## 环境准备

### 1) Python 与依赖

- 建议 `Python 3.11+`
- 安装依赖：

```bash
pip install -r requirements.txt
```

### 2) 环境变量

复制并配置：

```bash
cp .env.example .env
```

至少确保以下配置可用（名称以代码实际读取为准）：

- LLM 网关地址 / Key（`langchain-openai`）
- 行情数据源相关 Token（如 Tushare）
- 新闻数据源配置（如已启用）

## 快速开始

### 1) 常规链路运行（portfolio 决策）

```bash
# 单日
python live_trading.py --single-date 20250102

# 区间 + 每 N 个交易日执行一次
python live_trading.py --start-date 20240108 --end-date 20260424 --interval 7

# 仅查看计划交易日
python live_trading.py --dry-run
```

### 2) 长周期满仓运行（full position 决策）

```bash
# 单日
python long_cycle_full_position_runner.py --single-date 20240206

# 区间 + 长周期（示例 21 个交易日）
python long_cycle_full_position_runner.py --start-date 20240108 --end-date 20260424 --interval 21

# 仅查看计划交易日
python long_cycle_full_position_runner.py --dry-run
```

## 定时任务

### 常规链路定时

```bash
# 默认每天多个时点执行（详见脚本默认值）
python schedule_live_trading.py

# 指定多个时间
python schedule_live_trading.py --run-times 20:00,20:30

# 测试：只执行一次
python schedule_live_trading.py --once
```

### 满仓链路定时

```bash
# 默认每天多个时点执行（详见脚本默认值）
python schedule_full_position_trading.py

# 指定时间
python schedule_full_position_trading.py --run-times 21:00,21:30

# 测试：只执行一次
python schedule_full_position_trading.py --once
```

## 产物与可视化

- 主要产物位于 `data/artifacts/`，按模块和交易日落盘
- 决策结果常见路径：
  - `data/artifacts/decision/.../portfolio/<trade_date>/result.json`
- 可视化脚本示例：
  - `visualization/portfolio_nav_visualization.py`

## API（可选）

```bash
python run_api.py
```

如需联调接口，请查看 `api/` 与 `docs/` 下文档。
