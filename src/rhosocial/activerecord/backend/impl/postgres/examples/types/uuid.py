"""
Native UUID type - PostgreSQL.

This example demonstrates:
1. CREATE TABLE with UUID column type
2. Using uuid_default_generator() for version-aware DEFAULT value
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
from rhosocial.activerecord.backend.expression.core import Literal, Column
from rhosocial.activerecord.backend.expression.statements import (
    ColumnDefinition,
    ColumnConstraint,
    ColumnConstraintType,
)
from rhosocial.activerecord.backend.impl.postgres.functions.uuid import uuid_default_generator
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

# Use uuid_default_generator() for version-aware UUID default
# - PostgreSQL 13+: automatically selects gen_random_uuid() (built-in)
# - PostgreSQL < 13: automatically selects uuid_generate_v4() (uuid-ossp extension)
uuid_func = uuid_default_generator(dialect)

server_version = backend.get_server_version()
print(f"PostgreSQL version: {'.'.join(str(v) for v in server_version)}")

# Clean up
for table in ['events_v13', 'events_legacy']:
    drop = DropTableExpression(dialect=dialect, table_name=table, if_exists=True, cascade=True)
    sql, params = drop.to_sql()
    backend.execute(sql, params)

# ============================================================
# SECTION: Business Logic (the pattern to learn)
# ============================================================

# 1. CREATE TABLE with UUID column and auto-generated DEFAULT
# uuid_default_generator(dialect) automatically picks the right function:
#   PostgreSQL 13+: gen_random_uuid() (built-in, no extension)
#   PostgreSQL < 13: uuid_generate_v4() (requires uuid-ossp extension)
create_table = CreateTableExpression(
    dialect=dialect,
    table_name='events',
    columns=[
        ColumnDefinition('id', 'UUID', constraints=[
            ColumnConstraint(ColumnConstraintType.PRIMARY_KEY),
            ColumnConstraint(
                ColumnConstraintType.DEFAULT,
                default_value=uuid_func,
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
print(f"Table created: events (with UUID primary key, auto-generated DEFAULT)")

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
# 2. Use uuid_default_generator(dialect) for DDL column defaults:
#    - PostgreSQL 13+: gen_random_uuid() (built-in, no extension required)
#    - PostgreSQL < 13: uuid_generate_v4() (uuid-ossp extension)
# 3. Explicit UUID values can be inserted as string literals
# 4. UUID type provides validation and efficient storage (16 bytes vs 36 chars)
