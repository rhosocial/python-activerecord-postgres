# PostgreSQL 特性

PostgreSQL 提供了许多超越标准 SQL 的高级功能。本节介绍 PostgreSQL 独有或优化的特性。

## 主题

- **[PostgreSQL 特定字段类型](./field_types.md)**: ARRAY, JSONB, UUID, Range 类型
- **[PostgreSQL Dialect 表达式](./dialect.md)**: PostgreSQL 特定的 SQL 语法
- **[高级索引](./indexing.md)**: GIN, GiST, BRIN 索引
- **[RETURNING 子句](./returning.md)**: 从 DML 操作获取返回数据

## 功能亮点

```python
from rhosocial.activerecord.model import ActiveRecord

class Article(ActiveRecord):
    __table_name__ = "articles"
    title: str
    tags: list        # PostgreSQL ARRAY
    metadata: dict    # PostgreSQL JSONB
    id: UUID         # PostgreSQL UUID
```

💡 *AI 提示词：* "PostgreSQL 的 JSONB 相比普通 JSON 存储有什么优势？"
