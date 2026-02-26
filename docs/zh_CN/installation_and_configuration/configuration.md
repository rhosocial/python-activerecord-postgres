# 连接配置

## 基础配置

```python
from rhosocial.activerecord.backend.impl.postgres import PostgresConnectionConfig

config = PostgresConnectionConfig(
    host="localhost",
    port=5432,
    database="mydb",
    username="user",
    password="password"
)
```

## 配置选项

| 参数 | 类型 | 默认值 | 描述 |
|-----|------|-------|------|
| `host` | str | "localhost" | 数据库服务器主机名 |
| `port` | int | 5432 | 数据库服务器端口 |
| `database` | str | 必填 | 数据库名称 |
| `username` | str | 必填 | 数据库用户名 |
| `password` | str | None | 数据库密码 |
| `options` | dict | None | 额外连接选项 |

## 高级选项

```python
config = PostgresConnectionConfig(
    host="localhost",
    port=5432,
    database="mydb",
    username="user",
    password="password",
    options={
        "sslmode": "prefer",
        "connect_timeout": 10,
        "application_name": "my_app",
        "client_encoding": "UTF8"
    }
)
```

## 环境变量

出于安全考虑，建议使用环境变量：

```python
import os
from rhosocial.activerecord.backend.impl.postgres import PostgresConnectionConfig

config = PostgresConnectionConfig(
    host=os.getenv("PG_HOST", "localhost"),
    port=int(os.getenv("PG_PORT", "5432")),
    database=os.getenv("PG_DATABASE"),
    username=os.getenv("PG_USER"),
    password=os.getenv("PG_PASSWORD")
)
```

💡 *AI 提示词：* "在生产环境中如何安全地管理数据库凭据？"
