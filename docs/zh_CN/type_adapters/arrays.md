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
# 创建带数组的记录
article = Article(
    title="PostgreSQL 数组",
    tags=["python", "database", "arrays"]
)

# 使用数组操作符查询
# 包含：tags 包含 'python'
Article.query().where("tags @> ARRAY[?]", ('python',)).all()

# 包含多个：tags 同时包含 'python' 和 'database'
Article.query().where("tags @> ARRAY[?, ?]", ('python', 'database')).all()

# 任意元素匹配
Article.query().where("? = ANY(tags)", ('python',)).all()

# 所有元素满足条件
Article.query().where("? = ALL(tags)", ('python',)).all()
```

> **注意**：详细示例和测试验证请参阅[数组类型对比](./array_comparison.md)。

## 多维数组

```python
class Matrix(ActiveRecord):
    __table_name__ = "matrices"
    data: list[list[int]]  # INTEGER[][]
```

💡 *AI 提示词：* "数组列有什么性能考虑？"
