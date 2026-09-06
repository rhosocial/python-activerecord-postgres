# 数组类型处理

## PostgreSQL 数组

PostgreSQL 支持任意类型的数组：

```sql
INTEGER[], TEXT[], UUID[], JSONB[]
```

## Python 集成

```python
from rhosocial.activerecord.model import ActiveRecord

class Article(ActiveRecord):
    __table_name__ = "articles"
    title: str
    tags: list[str]      # TEXT[]
    scores: list[int]    # INTEGER[]
```

## 数组操作

```python
from rhosocial.activerecord.backend.impl.postgres.functions import array_length
from rhosocial.activerecord.backend.expression import Column, QueryExpression, TableExpression
from rhosocial.activerecord.backend.expression.core import Literal
from rhosocial.activerecord.backend.expression.operators import BinaryExpression

# 包含：tags 包含 'python'
query = QueryExpression(
    dialect=dialect,
    select=[Column(dialect, "*")],
    from_=TableExpression(dialect, "articles"),
    where=BinaryExpression(dialect, "@>", Column(dialect, "tags"), Literal(dialect, "{python}")),
)
sql, params = query.to_sql()
# sql: SELECT "*" FROM "articles" WHERE "tags" @> %s
# params: ('{python}',)

# 包含多个：tags 同时包含 'python' 和 'database'
query = QueryExpression(
    dialect=dialect,
    select=[Column(dialect, "*")],
    from_=TableExpression(dialect, "articles"),
    where=BinaryExpression(dialect, "@>", Column(dialect, "tags"), Literal(dialect, "{python,database}")),
)
sql, params = query.to_sql()
# sql: SELECT "*" FROM "articles" WHERE "tags" @> %s
# params: ('{python,database}',)

# 任意元素匹配（通过 array_length）
func = array_length(dialect, Column(dialect, "tags"), 1)
query = QueryExpression(
    dialect=dialect,
    select=[Column(dialect, "*")],
    from_=TableExpression(dialect, "articles"),
    where=BinaryExpression(dialect, ">", func, Literal(dialect, 0)),
)
sql, params = query.to_sql()
# sql: SELECT "*" FROM "articles" WHERE ARRAY_LENGTH("tags", %s) > %s
# params: (1, 0)

# 数组长度检查
func = array_length(dialect, Column(dialect, "tags"), 1)
query = QueryExpression(
    dialect=dialect,
    select=[Column(dialect, "*")],
    from_=TableExpression(dialect, "articles"),
    where=BinaryExpression(dialect, "=", func, Literal(dialect, 3)),
)
sql, params = query.to_sql()
# sql: SELECT "*" FROM "articles" WHERE ARRAY_LENGTH("tags", %s) = %s
# params: (1, 3)
```

> **注意**：详细示例和测试验证请参阅[数组类型对比](./array_comparison.md)。

## 多维数组

```python
class Matrix(ActiveRecord):
    __table_name__ = "matrices"
    data: list[list[int]]  # INTEGER[][]
```

💡 *AI 提示词：* "数组列有什么性能考虑？"
