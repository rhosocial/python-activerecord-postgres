# Array Type Handling

## PostgreSQL Arrays

PostgreSQL supports arrays of any type:

```sql
INTEGER[], TEXT[], UUID[], JSONB[]
```

## Python Integration

```python
from rhosocial.activerecord.model import ActiveRecord

class Article(ActiveRecord):
    __table_name__ = "articles"
    title: str
    tags: list[str]      # TEXT[]
    scores: list[int]    # INTEGER[]
```

## Array Operations

```python
from rhosocial.activerecord.backend.impl.postgres.functions import array_length
from rhosocial.activerecord.backend.expression import Column, QueryExpression, TableExpression
from rhosocial.activerecord.backend.expression.core import Literal
from rhosocial.activerecord.backend.expression.operators import BinaryExpression

# Contains: tags contains 'python'
query = QueryExpression(
    dialect=dialect,
    select=[Column(dialect, "*")],
    from_=TableExpression(dialect, "articles"),
    where=BinaryExpression(dialect, "@>", Column(dialect, "tags"), Literal(dialect, "{python}")),
)
sql, params = query.to_sql()
# sql: SELECT "*" FROM "articles" WHERE "tags" @> %s
# params: ('{python}',)

# Contains multiple: tags contains both 'python' AND 'database'
query = QueryExpression(
    dialect=dialect,
    select=[Column(dialect, "*")],
    from_=TableExpression(dialect, "articles"),
    where=BinaryExpression(dialect, "@>", Column(dialect, "tags"), Literal(dialect, "{python,database}")),
)
sql, params = query.to_sql()
# sql: SELECT "*" FROM "articles" WHERE "tags" @> %s
# params: ('{python,database}',)

# Any element match (via array_position)
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

# Array length check
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

> **Note**: See [Array Type Comparison](./array_comparison.md) for detailed examples and test verification.

## Multi-dimensional Arrays

```python
class Matrix(ActiveRecord):
    __table_name__ = "matrices"
    data: list[list[int]]  # INTEGER[][]
```

💡 *AI Prompt:* "What are the performance considerations for array columns?"
