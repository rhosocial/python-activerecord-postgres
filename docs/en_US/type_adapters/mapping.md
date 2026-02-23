# PostgreSQL to Python Type Mapping

## Standard Mappings

| PostgreSQL Type | Python Type |
|-----------------|-------------|
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
| `TIMESTAMPTZ` | `datetime` (timezone-aware) |
| `UUID` | `UUID` |
| `JSON`, `JSONB` | `dict` or `list` |
| `ARRAY` | `list` |

## Special Handling

### JSONB

```python
class Product(ActiveRecord):
    attributes: dict  # Maps to JSONB

product = Product(attributes={"key": "value"})
# Automatically converted to JSONB
```

### Arrays

```python
class Article(ActiveRecord):
    tags: list  # Maps to TEXT[]

article = Article(tags=["python", "database"])
# Automatically converted to PostgreSQL array
```

### UUID

```python
from uuid import UUID

class User(ActiveRecord):
    id: UUID

user = User(id=UUID("..."))
# Uses PostgreSQL native UUID type
```

💡 *AI Prompt:* "What happens when a PostgreSQL type has no Python equivalent?"
