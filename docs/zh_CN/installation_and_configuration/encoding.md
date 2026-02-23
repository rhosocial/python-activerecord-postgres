# 客户端编码

## 默认编码

PostgreSQL 默认使用 UTF-8，这对于大多数应用是推荐的：

```python
config = PostgresConnectionConfig(
    host="localhost",
    port=5432,
    database="mydb",
    username="user",
    password="password",
    options={
        "client_encoding": "UTF8"
    }
)
```

## 常用编码

| 编码 | 描述 |
|-----|------|
| `UTF8` | Unicode UTF-8（推荐） |
| `LATIN1` | ISO 8859-1 |
| `WIN1252` | Windows CP1252 |

## 处理编码问题

如果遇到编码错误：

1. 确保数据库使用 UTF-8：
   ```sql
   SHOW server_encoding;
   ```

2. 显式设置客户端编码：
   ```python
   config = PostgresConnectionConfig(
       # ...
       options={"client_encoding": "UTF8"}
   )
   ```

3. 对于使用非 UTF-8 编码的遗留数据库：
   ```python
   # 让 PostgreSQL 处理转换
   config = PostgresConnectionConfig(
       # ...
       options={"client_encoding": "LATIN1"}
   )
   ```

💡 *AI 提示词：* "客户端与服务器之间编码不匹配的常见原因是什么？"
