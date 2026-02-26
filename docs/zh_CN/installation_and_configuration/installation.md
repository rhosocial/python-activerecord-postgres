# 安装指南

## 环境要求

- Python 3.8 或更高版本
- PostgreSQL 8.0 或更高版本（推荐 12+）
- psycopg 3.2.12 或更高版本

## 安装

### 使用 pip

```bash
pip install rhosocial-activerecord-postgres
```

### 安装可选依赖

```bash
# 安装连接池支持
pip install rhosocial-activerecord-postgres[pooling]

# 安装开发依赖
pip install rhosocial-activerecord-postgres[test,dev,docs]
```

## 验证安装

```python
from rhosocial.activerecord.backend.impl.postgres import PostgresBackend

print("PostgreSQL 后端安装成功！")
```

## psycopg 与 psycopg-binary

本包使用 `psycopg`（psycopg3）作为 PostgreSQL 适配器。您可以选择安装 `psycopg-binary` 以获取预编译二进制文件：

```bash
pip install psycopg-binary
```

**注意**：`psycopg-binary` 是平台特定的。如果您的平台没有预编译版本，psycopg 将自动从源码编译。

💡 *AI 提示词：* "psycopg 和 psycopg-binary 之间的性能差异是什么？"
