# 本地 PostgreSQL 测试

## 概述

本节介绍如何设置本地 PostgreSQL 测试环境。

## 使用 Docker 运行 PostgreSQL

```bash
# 运行 PostgreSQL 容器
docker run -d \
  --name postgres-test \
  -e POSTGRES_USER=test_user \
  -e POSTGRES_PASSWORD=test \
  -e POSTGRES_DB=test \
  -p 5432:5432 \
  postgres:16

# 等待 PostgreSQL 启动
docker exec postgres-test pg_isready -U test_user
```

## 使用 Docker Compose

```yaml
# docker-compose.yml
version: '3.8'

services:
  postgres:
    image: postgres:16
    environment:
      POSTGRES_USER: test_user
      POSTGRES_PASSWORD: test
      POSTGRES_DB: test
    ports:
      - "5432:5432"
    volumes:
      - postgres_data:/var/lib/postgresql/data

volumes:
  postgres_data:
```

```bash
docker-compose up -d
```

## 运行测试

```bash
# 设置环境变量
export PG_HOST=localhost
export PG_PORT=5432
export PG_DATABASE=test
export PG_USER=test_user
export PG_PASSWORD=test

# 运行测试（串行 - 不要并行执行）
pytest tests/
```

## 重要：禁止并行测试执行

**测试必须串行执行。** 测试套件使用固定的表名，并行执行会导致冲突和失败。

```bash
# 不要使用并行执行
pytest -n auto          # ❌ 会导致失败
pytest -n 4             # ❌ 会导致失败

# 始终串行运行测试（默认行为）
pytest                  # ✅ 正确
```

💡 *AI 提示词：* "Docker 和 Docker Compose 有什么区别？"
