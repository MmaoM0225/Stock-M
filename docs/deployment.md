# Stock-M API 部署文档

## 1. 环境要求

- Python `3.9+`
- Windows / Linux / macOS
- 可访问 LLM 服务与行情数据源

## 2. 安装步骤

1) 克隆项目并进入目录

```bash
git clone <repo_url>
cd Stock-M
```

2) 创建并激活虚拟环境

```bash
python -m venv .venv
# Windows
.venv\Scripts\activate
# Linux/macOS
source .venv/bin/activate
```

3) 安装依赖

```bash
pip install -r requirements.txt
```

## 3. 环境变量配置

复制模板并填写实际值：

```bash
cp .env.example .env
```

重点变量（示例）：

- `OPENAI_API_KEY` / 或项目使用的 LLM Key
- `OPENAI_BASE_URL`
- `OPENAI_MODEL`
- `DATABASE_URL`（默认可用 SQLite）
- `TUSHARE_TOKEN`

## 4. 启动 API 服务

开发模式：

```bash
python run_api.py
```

或：

```bash
uvicorn api.main:app --host 0.0.0.0 --port 8000 --reload
```

生产模式：

```bash
uvicorn api.main:app --host 0.0.0.0 --port 8000 --workers 2
```

## 5. 反向代理（Nginx 示例）

```nginx
server {
    listen 80;
    server_name your_domain;

    location / {
        proxy_pass http://127.0.0.1:8000;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
    }
}
```

## 6. systemd（Linux 可选）

`/etc/systemd/system/stockm-api.service`：

```ini
[Unit]
Description=Stock-M API Service
After=network.target

[Service]
User=ubuntu
WorkingDirectory=/opt/Stock-M
Environment="PYTHONUNBUFFERED=1"
ExecStart=/opt/Stock-M/.venv/bin/uvicorn api.main:app --host 0.0.0.0 --port 8000 --workers 2
Restart=always

[Install]
WantedBy=multi-user.target
```

启用：

```bash
sudo systemctl daemon-reload
sudo systemctl enable stockm-api
sudo systemctl start stockm-api
sudo systemctl status stockm-api
```

## 7. 运维建议

- 为 Agent 执行接口增加超时和并发限制。
- 对查询接口添加只读缓存层，降低磁盘读取压力。
- 对 `full-pipeline` 建议改为任务队列模式（如 Celery/RQ）。
- 生产环境建议接入统一日志采集与告警。
