# Array Type Comparison: PostgreSQL vs Other Databases

## Overview

PostgreSQL is unique among commonly used databases in providing native array type support. This document explains the key differences and how PostgreSQL's array capabilities compare to databases without native array support.

## Native Array Support

### PostgreSQL

PostgreSQL supports arrays of any built-in or user-defined type:

```sql
-- Native array columns
CREATE TABLE articles (
    id SERIAL PRIMARY KEY,
    tags TEXT[],           -- Array of text
    scores INTEGER[],      -- Array of integers
    uuids UUID[]           -- Array of UUIDs
);
```

**Model Definition (No Adapter Needed):**

```python
from rhosocial.activerecord.model import ActiveRecord
from typing import List, Optional

class Article(ActiveRecord):
    __table_name__ = "articles"
    
    title: str
    tags: Optional[List[str]] = None      # Direct mapping to TEXT[]
    scores: Optional[List[int]] = None    # Direct mapping to INTEGER[]
```

### Databases Without Native Array Support

Many databases do not have native array types. In such cases, lists must be stored as serialized strings:

```sql
-- Store as TEXT with serialized format
CREATE TABLE articles (
    id INTEGER PRIMARY KEY,
    tags TEXT,    -- Serialized list: "tag1,tag2,tag3"
    scores TEXT   -- Serialized list: "1,2,3"
);
```

**Model Definition (Requires Adapter):**

```python
from rhosocial.activerecord.model import ActiveRecord
from rhosocial.activerecord.backend.type_adapter import BaseSQLTypeAdapter
from rhosocial.activerecord.base.fields import UseAdapter
from typing import List, Optional

class ListToStringAdapter(BaseSQLTypeAdapter):
    """Converts a Python list to a comma-separated string."""
    
    def __init__(self):
        super().__init__()
        self._register_type(list, str)

    def _do_to_database(self, value: List, target_type: type, options=None) -> Optional[str]:
        if value is None:
            return None
        return ",".join(str(v) for v in value)

    def _do_from_database(self, value: str, target_type: type, options=None) -> Optional[List]:
        if value is None:
            return None
        return value.split(",") if value else []

class Article(ActiveRecord):
    __table_name__ = "articles"
    
    title: str
    tags: Optional[List[str]] = None  # Requires adapter for serialization
```

## Key Differences

| Feature | PostgreSQL | Databases Without Native Arrays |
|---------|------------|--------------------------------|
| Native array type | Yes (`TEXT[]`, `INTEGER[]`, etc.) | No |
| Type adapter required | No | Yes |
| Array operators | `@>`, `<@`, `&&`, `ANY()`, `ALL()` | Not available |
| Indexing | GIN indexes on arrays | Not applicable |
| Multi-dimensional | Yes (`INTEGER[][]`) | No |
| Query performance | Optimized with indexes | Requires LIKE or string functions |

## PostgreSQL Array Operators

PostgreSQL provides powerful array-specific operators that are not available in databases without native array support:

```python
# Contains: tags contains 'python'
Article.query().where("tags @> ARRAY[?]", ('python',)).all()

# Contains multiple: tags contains both 'python' AND 'database'
Article.query().where("tags @> ARRAY[?, ?]", ('python', 'database')).all()

# Overlaps: tags contains any of these
Article.query().where("tags && ARRAY[?, ?]", ('python', 'database')).all()

# Any element equals
Article.query().where("? = ANY(tags)", ('python',)).all()

# All elements satisfy condition
Article.query().where("? = ALL(tags)", ('python',)).all()

# Array length
Article.query().where("array_length(tags, 1) > ?", (3,)).all()
```

**Equivalent in Databases Without Arrays (Limited):**

```python
# Must use string matching (less efficient, less precise)
Article.query().where("tags LIKE ?", ('%python%')).all()
```

## Writing Portable Code

### Option 1: Backend-Specific Models

Define separate models for PostgreSQL vs other backends:

```python
# Generic version (for databases without native arrays)
class MixedAnnotationModel(ActiveRecord):
    tags: Annotated[List[str], UseAdapter(ListToStringAdapter(), str)]

# PostgreSQL-specific version
class PostgresMixedAnnotationModel(ActiveRecord):
    tags: Optional[List[str]] = None  # Native array, no adapter
```

### Option 2: Use JSON Arrays

For portability across databases, use JSON arrays instead:

```python
# Most databases support JSON
class Article(ActiveRecord):
    __table_name__ = "articles"
    tags: Optional[List[str]] = None  # Stored as JSON
```

### Option 3: Feature Detection

Detect array support at runtime:

```python
from rhosocial.activerecord.backend.interface import SupportsArrayQueries

if isinstance(model.__backend__, SupportsArrayQueries):
    # Use native array operators
    results = model.query().where("tags @> ARRAY[?]", ('python',)).all()
else:
    # Fall back to string matching
    results = model.query().where("tags LIKE ?", ('%python%')).all()
```

## Schema Comparison Example

### PostgreSQL Schema

```sql
-- Native array support
CREATE TABLE "mixed_annotation_items" (
    "id" SERIAL PRIMARY KEY,
    "name" VARCHAR(255) NOT NULL,
    "tags" TEXT[],           -- Native array
    "meta" TEXT,
    "description" TEXT,
    "status" TEXT
);
```

### Schema for Databases Without Native Arrays

```sql
-- Must use TEXT with serialization
CREATE TABLE "mixed_annotation_items" (
    "id" INTEGER PRIMARY KEY,
    "name" VARCHAR(255) NOT NULL,
    "tags" TEXT,             -- Serialized string
    "meta" TEXT,
    "description" TEXT,
    "status" TEXT
);
```

## Performance Considerations

### PostgreSQL Arrays

- **Pros**: Native indexing with GIN, efficient queries, type safety
- **Cons**: PostgreSQL-specific feature, larger storage for small arrays

### Serialized Strings

- **Pros**: Portable across databases, simpler schema
- **Cons**: No indexing, string parsing overhead, limited query capabilities

## Recommendation

1. **PostgreSQL-only projects**: Use native arrays for better performance and query capabilities
2. **Multi-database projects**: Use JSON arrays for portability
3. **Simple string lists**: Serialization adapter is acceptable across all backends

💡 *AI Prompt:* "How do I create a GIN index on a PostgreSQL array column?"
