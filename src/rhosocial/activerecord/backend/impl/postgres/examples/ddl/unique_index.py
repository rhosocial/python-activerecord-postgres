"""
CREATE UNIQUE INDEX - PostgreSQL.

This example demonstrates:
1. CREATE UNIQUE INDEX on a single column
2. CREATE UNIQUE INDEX on multiple columns (composite unique)
3. Verify uniqueness constraint by attempting duplicate insert
"""

# ============================================================
# SECTION: Setup (necessary for execution, reference only)
# ============================================================
import os
from rhosocial.activerecord.backend.impl.postgres import PostgresBackend
from rhosocial.activerecord.backend.impl.postgres.config import PostgresConnectionConfig
from rhosocial.activerecord.backend.expression import (
    CreateTableExpression,
    DropTableExpression,
    QueryExpression,
    TableExpression,
    InsertExpression,
    ValuesSource,
    CreateIndexExpression,
)
from rhosocial.activerecord.backend.expression.core import Literal, WildcardExpression
from rhosocial.activerecord.backend.expression.statements import (
    ColumnDefinition,
    ColumnConstraint,
    ColumnConstraintType,
)

config = PostgresConnectionConfig(
    host=os.getenv('PG_HOST', 'localhost'),
    port=int(os.getenv('PG_PORT', '5432')),
    database=os.getenv('PG_DATABASE', 'test'),
    username=os.getenv('PG_USERNAME', 'postgres'),
    password=os.getenv('PG_PASSWORD', ''),
)
backend = PostgresBackend(connection_config=config)
backend.connect()
dialect = backend.dialect

drop_table = DropTableExpression(
    dialect=dialect,
    table_name='users',
    if_exists=True,
    cascade=True,
)
sql, params = drop_table.to_sql()
backend.execute(sql, params)

create_table = CreateTableExpression(
    dialect=dialect,
    table_name='users',
    columns=[
        ColumnDefinition('id', 'SERIAL', constraints=[
            ColumnConstraint(ColumnConstraintType.PRIMARY_KEY),
            ColumnConstraint(ColumnConstraintType.NOT_NULL),
        ]),
        ColumnDefinition('email', 'VARCHAR(200)', constraints=[
            ColumnConstraint(ColumnConstraintType.NOT_NULL),
        ]),
        ColumnDefinition('username', 'VARCHAR(100)', constraints=[
            ColumnConstraint(ColumnConstraintType.NOT_NULL),
        ]),
    ],
    if_not_exists=True,
)
sql, params = create_table.to_sql()
backend.execute(sql, params)

# ============================================================
# SECTION: Business Logic (the pattern to learn)
# ============================================================

# 1. CREATE UNIQUE INDEX on a single column
create_email_idx = CreateIndexExpression(
    dialect=dialect,
    index_name='idx_users_email',
    table_name='users',
    columns=['email'],
    unique=True,
    if_not_exists=True,
)
sql, params = create_email_idx.to_sql()
print(f"SQL: {sql}")
print(f"Params: {params}")

# 2. CREATE UNIQUE INDEX on multiple columns (composite unique)
create_composite_idx = CreateIndexExpression(
    dialect=dialect,
    index_name='idx_users_username_email',
    table_name='users',
    columns=['username', 'email'],
    unique=True,
    if_not_exists=True,
)
sql, params = create_composite_idx.to_sql()
print(f"SQL: {sql}")
print(f"Params: {params}")

# ============================================================
# SECTION: Execution (run the expression)
# ============================================================

# Create the unique indexes
sql, params = create_email_idx.to_sql()
backend.execute(sql, params)
print("Index created: idx_users_email (UNIQUE on email)")

sql, params = create_composite_idx.to_sql()
backend.execute(sql, params)
print("Index created: idx_users_username_email (UNIQUE on username, email)")

# Insert initial row
insert_expr = InsertExpression(
    dialect=dialect,
    into='users',
    columns=['email', 'username'],
    source=ValuesSource(dialect, [
        [Literal(dialect, 'alice@example.com'), Literal(dialect, 'alice')],
    ]),
)
sql, params = insert_expr.to_sql()
backend.execute(sql, params)
print("Inserted: alice@example.com / alice")

# 3. Verify uniqueness: attempt duplicate email (should fail)
try:
    duplicate_insert = InsertExpression(
        dialect=dialect,
        into='users',
        columns=['email', 'username'],
        source=ValuesSource(dialect, [
            [Literal(dialect, 'alice@example.com'), Literal(dialect, 'alice2')],
        ]),
    )
    sql, params = duplicate_insert.to_sql()
    backend.execute(sql, params)
    print("ERROR: Duplicate email was accepted (should have been rejected)")
except Exception as e:
    print(f"Uniqueness constraint enforced: {e}")

# Verify data after failed insert
verify_query = QueryExpression(
    dialect=dialect,
    select=[WildcardExpression(dialect)],
    from_=TableExpression(dialect, 'users'),
)
sql, params = verify_query.to_sql()
result = backend.execute(sql, params)
print(f"Rows in users table: {result.data}")

# ============================================================
# SECTION: Teardown (necessary for execution, reference only)
# ============================================================
drop_table = DropTableExpression(
    dialect=dialect,
    table_name='users',
    if_exists=True,
    cascade=True,
)
sql, params = drop_table.to_sql()
backend.execute(sql, params)
backend.disconnect()

# ============================================================
# SECTION: Summary
# ============================================================
# Key points:
# 1. Use CreateIndexExpression with unique=True for UNIQUE indexes
# 2. Single-column unique index ensures no duplicates in that column
# 3. Composite unique index ensures no duplicate combination across columns
# 4. if_not_exists=True prevents errors if index already exists
# 5. PostgreSQL enforces uniqueness at INSERT/UPDATE time
