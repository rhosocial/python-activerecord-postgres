# 类型适配器

类型适配器处理 Python 和 PostgreSQL 类型之间的转换。

## 主题

- **[PostgreSQL 到 Python 类型映射](./mapping.md)**: 类型转换规则
- **[自定义类型适配器](./custom.md)**: 扩展类型支持
- **[时区处理](./timezone.md)**: TIMESTAMP WITH TIME ZONE
- **[数组类型处理](./arrays.md)**: PostgreSQL 数组支持

## 内置适配器

| Python 类型 | PostgreSQL 类型 |
|------------|----------------|
| `str` | `TEXT`, `VARCHAR` |
| `int` | `INTEGER`, `BIGINT` |
| `float` | `REAL`, `DOUBLE PRECISION` |
| `bool` | `BOOLEAN` |
| `bytes` | `BYTEA` |
| `date` | `DATE` |
| `time` | `TIME` |
| `datetime` | `TIMESTAMP` |
| `UUID` | `UUID` |
| `dict` | `JSONB` |
| `list` | `ARRAY` |

💡 *AI 提示词：* "类型适配器如何处理 NULL 值？"
