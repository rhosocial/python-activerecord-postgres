"""
Insert a record and return the auto-generated ID using RETURNING clause - PostgreSQL.

This example demonstrates:
1. INSERT with RETURNING clause to get auto-generated SERIAL id
2. INSERT with multiple RETURNING columns
3. Batch INSERT with RETURNING
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
    ReturningClause,
    TableExpression,
    Column,
)
from rhosocial.activerecord.backend.expression.core import Literal
from rhosocial.activerecord.backend.expression.statements import (
    ColumnDefinition,
    ColumnConstraint,
    ColumnConstraintType,
)

config = PostgresConnectionConfig(
    host=os.getenv('POSTGRES_HOST', 'localhost'),
    port=int(os.getenv('POSTGRES_PORT', '5432')),
    database=os.getenv('POSTGRES_DATABASE', 'test'),
    username=os.getenv('POSTGRES_USER', 'postgres'),
    password=os.getenv('POSTGRES_PASSWORD', ''),
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
        ColumnDefinition('name', 'VARCHAR(100)', constraints=[
            ColumnConstraint(ColumnConstraintType.NOT_NULL),
        ]),
        ColumnDefinition('email', 'VARCHAR(200)'),
    ],
    if_not_exists=True,
)
sql, params = create_table.to_sql()
backend.execute(sql, params)

# ============================================================
# SECTION: Business Logic (the pattern to learn)
# ============================================================

# 1. INSERT with RETURNING id
insert_expr = InsertExpression(
    dialect=dialect,
    into=TableExpression(dialect, 'users'),
    source=ValuesSource(dialect, [[Literal(dialect, 'Alice'), Literal(dialect, 'alice@example.com')]]),
    columns=['name', 'email'],
    returning=ReturningClause(dialect, [Column(dialect, 'id')]),
    dialect_options={},
)

sql, params = insert_expr.to_sql()
print(f"SQL: {sql}")
print(f"Params: {params}")

# ============================================================
# SECTION: Execution (run the expression)
# ============================================================
result = backend.execute(sql, params)
print(f"Affected rows: {result.affected_rows}")
print(f"Returned data: {result.data}")

# 2. INSERT with multiple RETURNING columns
insert_multi = InsertExpression(
    dialect=dialect,
    into=TableExpression(dialect, 'users'),
    source=ValuesSource(dialect, [[Literal(dialect, 'Bob'), Literal(dialect, 'bob@example.com')]]),
    columns=['name', 'email'],
    returning=ReturningClause(dialect, [Column(dialect, 'id'), Column(dialect, 'name')]),
    dialect_options={},
)

sql, params = insert_multi.to_sql()
result = backend.execute(sql, params)
print(f"Multi-column RETURNING: {result.data}")

# 3. Batch INSERT with RETURNING
batch_insert = InsertExpression(
    dialect=dialect,
    into=TableExpression(dialect, 'users'),
    source=ValuesSource(dialect, [
        [Literal(dialect, 'Charlie'), Literal(dialect, 'charlie@example.com')],
        [Literal(dialect, 'Diana'), Literal(dialect, 'diana@example.com')],
    ]),
    columns=['name', 'email'],
    returning=ReturningClause(dialect, [Column(dialect, 'id'), Column(dialect, 'name')]),
    dialect_options={},
)

sql, params = batch_insert.to_sql()
result = backend.execute(sql, params)
print(f"Batch RETURNING: {result.data}")

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
# 1. PostgreSQL natively supports RETURNING clause on INSERT
# 2. ReturningClause returns data from inserted rows (e.g., auto-generated id)
# 3. Batch INSERT with RETURNING returns all inserted rows
# 4. Use Column() in ReturningClause to specify which columns to return
# 5. result.data contains the returned rows
