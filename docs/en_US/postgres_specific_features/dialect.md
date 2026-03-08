# PostgreSQL Dialect Expressions

## Type Casting

PostgreSQL uses the `::` operator for type casting, which is PostgreSQL-specific concise syntax. The PostgresDialect automatically uses this syntax.

### Basic Usage

```python
from rhosocial.activerecord.backend.expression import Column
from rhosocial.activerecord.backend.impl.postgres import PostgresDialect
from rhosocial.activerecord.backend.impl.postgres.types.constants import INTEGER, NUMERIC, MONEY

dialect = PostgresDialect()

# Basic type casting
col = Column(dialect, "price")
expr = col.cast(INTEGER)
sql, params = expr.to_sql()
# Generates: ("price"::integer, ())

# With type modifiers
col2 = Column(dialect, "name")
expr2 = col2.cast("VARCHAR(100)")
sql, params = expr2.to_sql()
# Generates: ("name"::VARCHAR(100), ())
```

### Chained Type Conversions

PostgreSQL supports chained type conversions:

```python
from rhosocial.activerecord.backend.expression import Column
from rhosocial.activerecord.backend.impl.postgres import PostgresDialect
from rhosocial.activerecord.backend.impl.postgres.types.constants import MONEY, NUMERIC, FLOAT8

dialect = PostgresDialect()
col = Column(dialect, "amount")

# Multi-level type conversion
expr = col.cast(MONEY).cast(NUMERIC).cast(FLOAT8)
sql, params = expr.to_sql()
# Generates: ("amount"::money::numeric::float8, ())
```

### Type Compatibility

Some type conversions require intermediate types. For example, `money` type cannot be directly converted to `float8`, it needs to go through `numeric`:

```python
# Not recommended - direct conversion (will trigger a warning)
col.cast("float8")  # From money directly

# Recommended - chained conversion
col.cast("money").cast("numeric").cast("float8")
```

When using incompatible type conversions, the system will emit a warning to guide users to use the correct intermediate type.

### PostgreSQL Type Constants

For convenience, PostgreSQL type constants are provided:

```python
from rhosocial.activerecord.backend.impl.postgres.types.constants import (
    # Numeric types
    SMALLINT, INTEGER, BIGINT, NUMERIC, DECIMAL,
    REAL, DOUBLE_PRECISION, FLOAT8, FLOAT4,
    SERIAL, BIGSERIAL,
    
    # Monetary type
    MONEY,
    
    # Character types
    VARCHAR, TEXT, CHAR,
    
    # Temporal types
    DATE, TIME, TIMESTAMP, TIMESTAMPTZ, INTERVAL,
    
    # Boolean type
    BOOLEAN,
    
    # JSON types
    JSON, JSONB, JSONPATH,
    
    # UUID type
    UUID,
    
    # Network address types
    INET, CIDR, MACADDR, MACADDR8,
    
    # Geometric types
    POINT, LINE, BOX, PATH, POLYGON, CIRCLE,
    
    # Range types
    INT4RANGE, INT8RANGE, NUMRANGE, DATERANGE,
)

# Usage examples
col.cast(INTEGER)  # Equivalent to col.cast("integer")
col.cast(MONEY)    # Equivalent to col.cast("money")
col.cast(JSONB)    # Equivalent to col.cast("jsonb")
```

### Using in Queries

```python
from rhosocial.activerecord.backend.expression import Column

# Type casting in WHERE clause
col = Column(dialect, "amount")
predicate = col.cast(NUMERIC) > 100
sql, params = predicate.to_sql()
# Generates: ("amount"::numeric > %s, (100,))

# Type casting in arithmetic expressions
col1 = Column(dialect, "price1")
col2 = Column(dialect, "price2")
expr = col1.cast(NUMERIC) + col2.cast(NUMERIC)
sql, params = expr.to_sql()
# Generates: ("price1"::numeric + "price2"::numeric, ())
```

### PostgreSQL vs Standard SQL Syntax

| Feature | Standard SQL | PostgreSQL |
|---------|--------------|------------|
| Basic cast | `CAST(x AS integer)` | `x::integer` |
| With modifier | `CAST(x AS VARCHAR(100))` | `x::VARCHAR(100)` |
| Chained cast | `CAST(CAST(x AS money) AS numeric)` | `x::money::numeric` |
| Numeric precision | `CAST(x AS NUMERIC(10,2))` | `x::NUMERIC(10,2)` |

**Note**: While PostgreSQL supports the `::` syntax, the `CAST()` function is still available. The system defaults to using `::` syntax to generate more concise SQL.

## DDL Statements

### CREATE TABLE ... LIKE

PostgreSQL supports copying table structure using the `LIKE` clause with fine-grained control over what gets copied via INCLUDING/EXCLUDING options.

```python
from rhosocial.activerecord.backend.expression import CreateTableExpression
from rhosocial.activerecord.backend.impl.postgres.dialect import PostgresDialect

# Basic usage - copy table structure
create_expr = CreateTableExpression(
    dialect=PostgresDialect(),
    table_name="users_copy",
    columns=[],
    dialect_options={'like_table': 'users'}
)
# Generates: CREATE TABLE "users_copy" (LIKE "users")

# With INCLUDING options (dictionary format - recommended)
create_expr = CreateTableExpression(
    dialect=PostgresDialect(),
    table_name="users_copy",
    columns=[],
    dialect_options={
        'like_table': 'users',
        'like_options': {
            'including': ['DEFAULTS', 'CONSTRAINTS', 'INDEXES'],
            'excluding': ['COMMENTS']
        }
    }
)
# Generates: CREATE TABLE "users_copy" (LIKE "users", INCLUDING DEFAULTS, 
#            INCLUDING CONSTRAINTS, INCLUDING INDEXES, EXCLUDING COMMENTS)

# With schema-qualified source table
create_expr = CreateTableExpression(
    dialect=PostgresDialect(),
    table_name="users_copy",
    columns=[],
    dialect_options={'like_table': ('public', 'users')}
)
# Generates: CREATE TABLE "users_copy" (LIKE "public"."users")

# With TEMPORARY and IF NOT EXISTS
create_expr = CreateTableExpression(
    dialect=PostgresDialect(),
    table_name="temp_users",
    columns=[],
    temporary=True,
    if_not_exists=True,
    dialect_options={'like_table': 'users'}
)
# Generates: CREATE TEMPORARY TABLE IF NOT EXISTS "temp_users" (LIKE "users")

# Including everything
create_expr = CreateTableExpression(
    dialect=PostgresDialect(),
    table_name="users_copy",
    columns=[],
    dialect_options={
        'like_table': 'users',
        'like_options': {'including': ['ALL']}
    }
)
# Generates: CREATE TABLE "users_copy" (LIKE "users", INCLUDING ALL)
```

**Available INCLUDING/EXCLUDING Options:**
- `DEFAULTS` - Copy default values
- `CONSTRAINTS` - Copy table constraints
- `INDEXES` - Copy indexes
- `IDENTITY` - Copy identity columns
- `GENERATED` - Copy generated columns
- `COMMENTS` - Copy column/table comments
- `STORAGE` - Copy storage parameters
- `COMPRESSION` - Copy compression settings
- `ALL` - Copy everything

**Important Notes:**
- When `like_table` is specified in `dialect_options`, it takes highest priority
- All other parameters (columns, indexes, constraints, etc.) are IGNORED
- Only `temporary` and `if_not_exists` flags are considered
- The `like_options` key supports both dictionary and list formats

## RETURNING Clause

PostgreSQL supports RETURNING for DML operations:

```python
# INSERT RETURNING
user = User(name="John")
user.save()
# Returns the inserted row with generated id

# UPDATE RETURNING
User.query().where(User.c.id == 1).update(name="Jane")
# Returns affected rows
```

## DISTINCT ON

PostgreSQL-specific DISTINCT ON:

```python
# Get the most recent order for each user
orders = Order.query().distinct_on("user_id").order_by(
    "user_id", "created_at DESC"
).all()
```

## ILIKE (Case-Insensitive Match)

```python
users = User.query().where(
    "name ILIKE ?", ("%john%",)
).all()
```

## Array Operators

```python
# Contains (@>)
Article.query().where("tags @> ?", (['python', 'database'],))

# Is contained by (<@)
Article.query().where("tags <@ ?", (['python', 'database', 'web'],))

# Overlaps (&&)
Article.query().where("tags && ?", (['python', 'java'],))

# Any element
Article.query().where("? = ANY(tags)", ('python',))
```

## JSONB Operators

```python
# Get JSON value at path
Product.query().where("attributes->>'brand' = ?", ('Dell',))

# Get nested value
Product.query().where("attributes->'specs'->>'cpu' = ?", ('Intel i7',))

# JSONB contains
Product.query().where("attributes @> ?", ({"brand": "Dell"},))

# Key exists
Product.query().where("attributes ? 'brand'", ())
```

💡 *AI Prompt:* "How do PostgreSQL's ILIKE and standard LIKE differ in performance?"
