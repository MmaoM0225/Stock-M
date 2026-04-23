# Artifact 关键表规划（V1）

## 当前进度

- 已完成：`commodity_analyst` 关键表落库
- 已建表：`agent_commodity_analyst_key`
- 已接入：`database/models.py`、`database/__init__.py`
- 已执行：`python scripts/init_db.py` 完成建表

---

## 1. 目标与范围

本方案用于解决 `data/artifacts` 下 JSON 结果不易高效筛选的问题。  
核心方案：

- 数据库仅保存高频查询的关键字段
- 保留完整 `result.json` 文件
- 表内通过 `run_id` + `result_path` 指向完整结果

---

## 2. 设计原则

- 低冗余：不全量复制 JSON 入库
- 高可查：常用筛选字段必须结构化
- 可回溯：每条关键记录可定位原始结果
- 可演进：字段变更优先追加，不破坏历史
- 幂等写入：按 `run_id` 做 upsert

---

## 3. `commodity_analyst` 已落地表设计

表名：`agent_commodity_analyst_key`

### 3.1 字段（已实现）

- `id`：主键，自增
- `run_id`：业务唯一键（UNIQUE）
- `trade_date`：交易日（`YYYYMMDD`）
- `commodity_market_trend`：商品市场趋势
- `overall_trend`：整体趋势
- `growth_signal`：增长信号
- `inflation_signal`：通胀信号
- `risk_sentiment`：风险情绪
- `macro_summary`：宏观一句话总结
- `combined_summary`：各品种汇总摘要
- `commodity_count`：分析品种数
- `result_path`：`result.json` 相对路径
- `result_hash`：结果文件哈希（预留）
- `created_at` / `updated_at`：时间戳

### 3.2 索引（已实现）

- `idx_commodity_analyst_trade_date`
- `idx_commodity_analyst_market_trend`
- `idx_commodity_analyst_growth_signal`

---

## 4. 运行键与指针约定

建议 `run_id` 规则：

`commodity_analyst:{trade_date}`

如后续出现多版本回测，可扩展为：

`commodity_analyst:{trade_date}:{version}`

`result_path` 示例：

`data/artifacts/analyst/macro_analyst/commodity_analyst/20260423/result.json`

---

## 5. 查询与使用模式

- 列表和筛选：查询 `agent_commodity_analyst_key`
- 详情和深度分析：根据 `result_path` 加载 `result.json`
- API 返回：建议列表接口返回关键字段，详情接口返回完整 JSON

---

## 6. 下一步（待实现）

1. 编写 `commodity_analyst` 增量同步脚本  
   - 扫描 `data/artifacts/analyst/macro_analyst/commodity_analyst/*/result.json`
   - 解析关键字段
   - 按 `run_id` upsert 入表

2. 在 `api/services/data_service.py` 增加“优先查表”的读取方法  
   - 列表查表
   - 详情按 `result_path` 回源 JSON

3. 统一异常记录  
   - 解析失败时记录日志并跳过
   - 后续可补 `status/error_message` 字段

---

## 7. 分阶段推进建议

- 阶段 A：先打通 `commodity_analyst`（已完成建表）
- 阶段 B：接入同步脚本与 1 个查询接口
- 阶段 C：按同样模式逐个扩展到其他 agent

