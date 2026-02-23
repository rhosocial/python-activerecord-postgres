# PostgreSQL 到 Python 类型映射

## 标准映射

| PostgreSQL 类型 | Python 类型 |
|----------------|------------|
| `SMALLINT` | `int` |
| `INTEGER` | `int` |
| `BIGINT` | `int` |
| `REAL` | `float` |
| `DOUBLE PRECISION` | `float` |
| `NUMERIC`, `DECIMAL` | `Decimal` |
| `BOOLEAN` | `bool` |
| `TEXT`, `VARCHAR` | `str` |
| `BYTEA` | `bytes` |
| `DATE` | `date` |
| `TIME` | `time` |
| `TIMESTAMP` | `datetime` |
| `TIMESTAMPTZ` | `datetime` (带时区) |
| `UUID` | `UUID` |
| `JSON`, `JSONB` | `dict` 或 `list` |
| `ARRAY` | `list` |

## 特殊处理

### JSONB

```python
class Product(ActiveRecord):
    attributes: dict  # 映射到 JSONB

product = Product(attributes={"key": "value"})
# 自动转换为 JSONB
```

### 数组

```python
class Article(ActiveRecord):
    tags: list  # 映射到 TEXT[]

article = Article(tags=["python", "database"])
# 自动转换为 PostgreSQL 数组
```

### UUID

```python
from uuid import UUID

class User(ActiveRecord):
    id: UUID

user = User(id=UUID("..."))
# 使用 PostgreSQL 原生 UUID 类型
```

💡 *AI 提示词：* "当 PostgreSQL 类型没有 Python 等价物时会发生什么？"
