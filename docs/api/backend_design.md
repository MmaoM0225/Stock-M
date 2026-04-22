# Stock-M 后端设计文档

## 1. 目标

本设计用于将现有 `Stock-M` 的命令行分析流程升级为可调用的后端 API 服务，满足以下目标：

- 对外提供统一接口，支持 manager / analyst / decision 调用
- 提供对 `data/artifacts` 历史结果的查询能力
- 保持与现有 `agents`、`dataflow`、`database` 模块兼容
- 先快速落地，再逐步演进为异步任务架构

## 2. 技术栈

- Web 框架：`FastAPI`
- 服务运行：`Uvicorn`
- 模型校验：`Pydantic v2`
- 业务执行：复用 `LangGraph` 的 `graph.invoke(...)`
- 数据存储：`SQLite/PostgreSQL + artifacts(JSON)`

## 3. 目录设计

```text
Stock-M/
├─ api/
│  ├─ __init__.py
│  ├─ main.py
│  ├─ config.py
│  ├─ dependencies.py
│  ├─ routers/
│  │  ├─ __init__.py
│  │  ├─ agents.py
│  │  ├─ data.py
│  │  ├─ stocks.py
│  │  └─ portfolio.py
│  ├─ schemas/
│  │  ├─ __init__.py
│  │  ├─ base.py
│  │  ├─ agent.py
│  │  ├─ data.py
│  │  └─ portfolio.py
│  └─ services/
│     ├─ __init__.py
│     ├─ agent_service.py
│     └─ data_service.py
├─ run_api.py
└─ docs/api/
   ├─ README.md
   ├─ agents.md
   ├─ data.md
   ├─ portfolio.md
   └─ backend_design.md
```

## 4. 分层职责

### 4.1 Router 层

- 接收 HTTP 请求参数
- 调用 service 层能力
- 返回统一响应结构
- 不编写复杂业务逻辑

### 4.2 Service 层

- 复用现有 graph 工厂并执行分析流程
- 管理 artifacts 文件读写与路径解析
- 聚合查询与业务规则处理

### 4.3 Schema 层

- 统一请求/响应结构
- 参数验证（日期格式、股票代码、分页参数）
- 对外契约固定，减少前后端耦合

### 4.4 Dependency 层

- 提供单例 LLM 实例
- 提供数据库会话
- 注入 service 实例

## 5. 核心流程设计

### 5.1 执行流程（agents）

执行依赖固定为：

`macro -> sector -> screener -> stock_pool -> portfolio_decision`

规则：

- 当 `force=false` 且结果已存在，直接复用历史 artifact
- 当 `force=true`，强制重算并覆盖/新增结果
- 全流程接口必须串行执行，避免依赖数据缺失

### 5.2 查询流程（data）

- 只读 `data/artifacts/.../result.json`
- 读取失败返回 `404`（不存在）或 `500`（结构错误）
- 对组合查询支持日期范围与分页

## 6. 接口域划分

### 6.1 Agent 执行接口

- 前缀：`/api/v1/agents`
- 覆盖 manager、analyst、full-pipeline
- analyst 接口要求“每个 analyst 都有独立入口”

### 6.2 数据查询接口

- 前缀：`/api/v1/data`
- 覆盖 manager/decision 查询
- analyst 查询要求“每个 analyst 都有独立读取接口”

### 6.3 股票与组合接口

- 股票基础数据：`/api/v1/stocks`、`/api/v1/industries`
- 组合管理：`/api/v1/portfolio`

## 7. 统一响应规范

成功：

```json
{
  "success": true,
  "message": "ok",
  "data": {}
}
```

失败：

```json
{
  "success": false,
  "message": "result file not found",
  "error_code": "NOT_FOUND"
}
```

## 8. 异常与状态码

- `400` 参数错误
- `404` 结果不存在
- `409` 任务冲突（可选）
- `422` 请求体校验失败
- `500` 服务内部错误

## 9. 迭代计划

### Phase 1（当前）

- 完成 FastAPI 基础骨架
- 完成 data 只读接口
- 完成 agent 同步执行接口

### Phase 2

- 引入任务队列（Celery/RQ）改造长任务
- 增加任务状态查询接口
- 增加执行日志与成本统计

### Phase 3

- 增加鉴权与访问控制
- 增加缓存层与性能优化
- 增加观测与告警能力

## 10. 风险与约束

- Agent 耗时长，HTTP 直连可能超时
- LLM 成本需限制频率和并发
- artifacts 版本目录需要统一配置，避免硬编码
- 并发执行同一交易日任务可能导致数据竞争

## 11. 开发建议

- 先实现 `data_service + data_router` 作为最小可运行链路
- 再实现 `agent_service + agents_router`
- 最后补齐 `stocks/portfolio` 与部署配置
- 全部路径常量收敛到 `config.py`，减少魔法字符串
