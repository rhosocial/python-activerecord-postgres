# SSL/TLS 配置

## SSL 模式选项

| 模式 | 描述 |
|-----|------|
| `disable` | 不使用 SSL（生产环境不推荐） |
| `allow` | 先尝试非 SSL，失败后回退到 SSL |
| `prefer` | 先尝试 SSL，失败后回退到非 SSL（默认） |
| `require` | 要求 SSL，不验证证书 |
| `verify-ca` | 要求 SSL 并验证 CA 证书 |
| `verify-full` | 要求 SSL 并完整验证证书 |

## 基础 SSL 配置

```python
config = PostgresConnectionConfig(
    host="localhost",
    port=5432,
    database="mydb",
    username="user",
    password="password",
    options={
        "sslmode": "require"
    }
)
```

## 带证书验证

```python
config = PostgresConnectionConfig(
    host="prod-db.example.com",
    port=5432,
    database="mydb",
    username="user",
    password="password",
    options={
        "sslmode": "verify-full",
        "sslrootcert": "/path/to/ca-cert.pem",
        "sslcert": "/path/to/client-cert.pem",
        "sslkey": "/path/to/client-key.pem"
    }
)
```

💡 *AI 提示词：* "不同 SSL 模式的安全影响是什么？"
