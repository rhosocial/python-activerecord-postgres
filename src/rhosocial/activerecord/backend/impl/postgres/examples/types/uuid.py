"""
Native UUID type - PostgreSQL.

This example demonstrates:
1. CREATE TABLE with UUID column type
2. Using gen_random_uuid() as DEFAULT value (PostgreSQL 13+)
3. INSERT with UUID values
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
    InsertExpression,
    ValuesSource,
    QueryExpression,
    TableExpression,
)
from rhosocial.activerecord.backend.expression.core import Literal, Column, FunctionCall
from rhosocial.activerecord.backend.expression.statements import (
    ColumnDefinition,
    ColumnConstraint,
    ColumnConstraintType,
)
from rhosocial.activerecord.backend.options import ExecutionOptions
from rhosocial.activerecord.backend.schema import StatementType

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

dql_options = ExecutionOptions(stmt_type=StatementType.DQL)

drop_table = DropTableExpression(
    dialect=dialect,
    table_name='events',
    if_exists=True,
    cascade=True,
)
sql, params = drop_table.to_sql()
backend.execute(sql, params)

# ============================================================
# SECTION: Business Logic (the pattern to learn)
# ============================================================

# 1. CREATE TABLE with UUID column and gen_random_uuid() DEFAULT
# gen_random_uuid() is built-in since PostgreSQL 13
# For PostgreSQL < 13, use uuid-ossp extension: uuid_generate_v4()
create_table = CreateTableExpression(
    dialect=dialect,
    table_name='events',
    columns=[
        ColumnDefinition('id', 'UUID', constraints=[
            ColumnConstraint(ColumnConstraintType.PRIMARY_KEY),
            ColumnConstraint(
                ColumnConstraintType.DEFAULT,
                default_value=FunctionCall(dialect, 'GEN_RANDOM_UUID'),
            ),
        ]),
        ColumnDefinition('name', 'VARCHAR(200)', constraints=[
            ColumnConstraint(ColumnConstraintType.NOT_NULL),
        ]),
        ColumnDefinition('payload', 'JSONB'),
    ],
    if_not_exists=True,
)

sql, params = create_table.to_sql()
print(f"SQL: {sql}")
print(f"Params: {params}")

# ============================================================
# SECTION: Execution (run the expression)
# ============================================================
backend.execute(sql, params)
print("Table created: events (with UUID primary key)")

# 2. INSERT with auto-generated UUID (using DEFAULT)
insert_default = InsertExpression(
    dialect=dialect,
    into='events',
    columns=['name'],
    source=ValuesSource(dialect, [
        [Literal(dialect, 'user_created')],
        [Literal(dialect, 'order_placed')],
    ]),
)
sql, params = insert_default.to_sql()
backend.execute(sql, params)
print("Inserted rows with auto-generated UUIDs")

# 3. INSERT with explicit UUID value
insert_explicit = InsertExpression(
    dialect=dialect,
    into='events',
    columns=['id', 'name'],
    source=ValuesSource(dialect, [
        [Literal(dialect, '550e8400-e29b-41d4-a716-446655440000'), Literal(dialect, 'explicit_uuid')],
    ]),
)
sql, params = insert_explicit.to_sql()
backend.execute(sql, params)
print("Inserted row with explicit UUID")

# 4. Verify data
verify_query = QueryExpression(
    dialect=dialect,
    select=[Column(dialect, 'id'), Column(dialect, 'name')],
    from_=TableExpression(dialect, 'events'),
)
sql, params = verify_query.to_sql()
result = backend.execute(sql, params, options=dql_options)
print(f"Events: {result.data}")

# ============================================================
# SECTION: Teardown (necessary for execution, reference only)
# ============================================================
drop_table = DropTableExpression(
    dialect=dialect,
    table_name='events',
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
# 1. PostgreSQL has native UUID type (no need for VARCHAR storage)
# 2. gen_random_uuid() available since PostgreSQL 13 (built-in)
# 3. For PostgreSQL < 13, enable uuid-ossp extension: CREATE EXTENSION IF NOT EXISTS "uuid-ossp"
# 4. Use FunctionCall(dialect, 'GEN_RANDOM_UUID') for DEFAULT value
# 5. Explicit UUID values can be inserted as string literals
# 6. UUID type provides validation and efficient storage (16 bytes vs 36 chars)
