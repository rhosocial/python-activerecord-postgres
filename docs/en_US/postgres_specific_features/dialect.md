# PostgreSQL Dialect Expressions

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
