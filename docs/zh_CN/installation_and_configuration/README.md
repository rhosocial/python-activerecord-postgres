# 安装与配置

本节介绍如何安装和配置 rhosocial-activerecord 的 PostgreSQL 后端。

## 主题

- **[安装指南](./installation.md)**: 通过 pip 安装及环境要求
- **[连接配置](./configuration.md)**: 数据库连接设置
- **[SSL/TLS 配置](./ssl.md)**: 安全连接选项
- **[连接管理](./pool.md)**: 连接生命周期管理
- **[客户端编码](./encoding.md)**: 字符编码配置

## 快速开始

```bash
pip install rhosocial-activerecord-postgres
```

```python
from rhosocial.activerecord.backend.impl.postgres.backend import PostgresBackend
from rhosocial.activerecord.backend.impl.postgres.config import PostgresConnectionConfig

config = PostgresConnectionConfig(
    host="localhost",
    port=5432,
    database="mydb",
    username="user",
    password="password"
)

backend = PostgresBackend(connection_config=config)
```

💡 *AI 提示词：* "在生产应用中管理数据库凭据的最佳实践是什么？"
