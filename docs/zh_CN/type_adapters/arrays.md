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
Article.query().where("tags @> ?", (['python', 'database'],))

# 任意元素匹配
Article.query().where("? = ANY(tags)", ('python',))

# 所有元素匹配
Article.query().where("tags <@ ?", (['python'],))
```

## 多维数组

```python
class Matrix(ActiveRecord):
    __table_name__ = "matrices"
    data: list[list[int]]  # INTEGER[][]
```

💡 *AI 提示词：* "数组列有什么性能考虑？"
