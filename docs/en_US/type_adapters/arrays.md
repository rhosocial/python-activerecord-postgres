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
# Create with array
article = Article(
    title="PostgreSQL Arrays",
    tags=["python", "database", "arrays"]
)

# Query with array operators
# Contains: tags contains 'python'
Article.query().where("tags @> ARRAY[?]", ('python',)).all()

# Contains multiple: tags contains both 'python' AND 'database'
Article.query().where("tags @> ARRAY[?, ?]", ('python', 'database')).all()

# Any element match
Article.query().where("? = ANY(tags)", ('python',)).all()

# All elements satisfy condition
Article.query().where("? = ALL(tags)", ('python',)).all()
```

> **Note**: See [Array Type Comparison](./array_comparison.md) for detailed examples and test verification.

## Multi-dimensional Arrays

```python
class Matrix(ActiveRecord):
    __table_name__ = "matrices"
    data: list[list[int]]  # INTEGER[][]
```

💡 *AI Prompt:* "What are the performance considerations for array columns?"
