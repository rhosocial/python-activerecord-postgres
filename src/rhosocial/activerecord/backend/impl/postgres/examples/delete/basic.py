"""
Delete records and return deleted rows using RETURNING clause - PostgreSQL.

This example demonstrates:
1. DELETE with WHERE clause
2. DELETE with RETURNING clause to get deleted row data
3. Affected rows tracking
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
    DeleteExpression,
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
from rhosocial.activerecord.backend.expression.predicates import ComparisonPredicate

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
        ColumnDefinition('name', 'VARCHAR(100)', constraints=[
            ColumnConstraint(ColumnConstraintType.NOT_NULL),
        ]),
        ColumnDefinition('email', 'VARCHAR(200)'),
    ],
    if_not_exists=True,
)
sql, params = create_table.to_sql()
backend.execute(sql, params)

# Insert sample data
insert_expr = InsertExpression(
    dialect=dialect,
    into='users',
    columns=['name', 'email'],
    source=ValuesSource(dialect, [
        [Literal(dialect, 'Alice'), Literal(dialect, 'alice@example.com')],
        [Literal(dialect, 'Bob'), Literal(dialect, 'bob@example.com')],
        [Literal(dialect, 'Charlie'), Literal(dialect, 'charlie@example.com')],
    ]),
)
sql, params = insert_expr.to_sql()
backend.execute(sql, params)

# ============================================================
# SECTION: Business Logic (the pattern to learn)
# ============================================================

# 1. Simple DELETE with WHERE
delete_expr = DeleteExpression(
    dialect=dialect,
    table=TableExpression(dialect, 'users'),
    where=ComparisonPredicate(
        dialect,
        '=',
        Column(dialect, 'name'),
        Literal(dialect, 'Alice'),
    ),
    dialect_options={},
)

sql, params = delete_expr.to_sql()
print(f"SQL: {sql}")
print(f"Params: {params}")

# ============================================================
# SECTION: Execution (run the expression)
# ============================================================
result = backend.execute(sql, params)
print(f"Affected rows: {result.affected_rows}")

# 2. DELETE with RETURNING clause
delete_returning = DeleteExpression(
    dialect=dialect,
    table=TableExpression(dialect, 'users'),
    where=ComparisonPredicate(
        dialect,
        '=',
        Column(dialect, 'name'),
        Literal(dialect, 'Bob'),
    ),
    returning=ReturningClause(dialect, [Column(dialect, 'id'), Column(dialect, 'name')]),
    dialect_options={},
)

sql, params = delete_returning.to_sql()
print(f"RETURNING SQL: {sql}")
result = backend.execute(sql, params)
print(f"Deleted rows: {result.data}")
print(f"Affected rows: {result.affected_rows}")

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
# 1. PostgreSQL natively supports RETURNING clause on DELETE
# 2. ReturningClause returns data from deleted rows
# 3. Use Column() in ReturningClause to specify which columns to return
# 4. result.data contains the returned rows
# 5. result.affected_rows indicates how many rows were deleted
