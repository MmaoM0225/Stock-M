# Stock-M API 文档总览

## 1. 简介

Stock-M 后端 API 用于两类能力：

- 触发各类 Agent 执行分析与决策流程
- 查询 `data/artifacts` 中已经生成的分析结果与组合结果

当前接口版本：`v1`。

## 2. 基础信息

- Base URL: `http://127.0.0.1:8000`
- API 前缀: `/api/v1`
- 数据格式: `application/json`
- 日期格式: `YYYYMMDD`（例如 `20260420`）

## 3. 快速开始

1) 安装依赖

```bash
pip install -r requirements.txt
```

2) 启动服务

```bash
python run_api.py
```

3) 访问文档

- Swagger UI: `http://127.0.0.1:8000/docs`
- ReDoc: `http://127.0.0.1:8000/redoc`

## 4. 通用响应结构

成功响应示例：

```json
{
  "success": true,
  "message": "ok",
  "data": {},
  "trace_id": "4d0f44f8d9db4c4d8fe3d93cf7f8c7ee"
}
```

失败响应示例：

```json
{
  "success": false,
  "message": "artifact not found",
  "error_code": "NOT_FOUND",
  "trace_id": "f3bf74558e9d415e8dbd6f4d92416f6f"
}
```

## 5. 错误码约定

- `400 BAD_REQUEST`：参数校验失败
- `404 NOT_FOUND`：目标数据不存在
- `409 CONFLICT`：资源状态冲突（例如正在执行）
- `422 UNPROCESSABLE_ENTITY`：请求体格式错误
- `500 INTERNAL_SERVER_ERROR`：服务内部错误

## 6. 模块导航

- Agent 执行接口：`docs/api/agents.md`
- 数据查询接口：`docs/api/data.md`
- 组合管理接口：`docs/api/portfolio.md`

## 7. 说明

- Agent 相关接口通常耗时较长，建议前端使用轮询或任务状态提示。
- 数据查询接口只读取本地 artifacts，不会触发计算流程。
