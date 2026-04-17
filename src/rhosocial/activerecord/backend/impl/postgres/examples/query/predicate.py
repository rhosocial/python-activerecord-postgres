"""
Complex Predicates - PostgreSQL.

This example demonstrates:
1. LIKE predicate (pattern matching)
2. IN predicate (list membership)
3. BETWEEN predicate (range)
4. IS NULL / IS NOT NULL predicates
"""

# ============================================================
# SECTION: Setup (necessary for execution, reference only)
# ============================================================
import os
from rhosocial.activerecord.backend.impl.postgres import PostgresBackend
from rhosocial.activerecord.backend.impl.postgres.config import PostgresConnectionConfig
from rhosocial.activerecord.backend.expression import (
    CreateTableExpression,
    InsertExpression,
    ValuesSource,
    DropTableExpression,
    QueryExpression,
    TableExpression,
    WhereClause,
)
from rhosocial.activerecord.backend.expression.core import Literal, Column
from rhosocial.activerecord.backend.expression.predicates import (
    LikePredicate,
    InPredicate,
    BetweenPredicate,
    IsNullPredicate,
    LogicalPredicate,
)
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

drop_table = DropTableExpression(
    dialect=dialect,
    table_name='employees',
    if_exists=True,
    cascade=True,
)
sql, params = drop_table.to_sql()
backend.execute(sql, params)

create_table = CreateTableExpression(
    dialect=dialect,
    table_name='employees',
    columns=[
        ColumnDefinition(
            'id',
            'SERIAL',
            constraints=[
                ColumnConstraint(ColumnConstraintType.PRIMARY_KEY),
                ColumnConstraint(ColumnConstraintType.NOT_NULL),
            ],
        ),
        ColumnDefinition(
            'name',
            'VARCHAR(100)',
            constraints=[ColumnConstraint(ColumnConstraintType.NOT_NULL)],
        ),
        ColumnDefinition('department', 'VARCHAR(50)'),
        ColumnDefinition('salary', 'NUMERIC(10,2)'),
        ColumnDefinition('manager_id', 'INTEGER'),
    ],
    if_not_exists=True,
)
sql, params = create_table.to_sql()
backend.execute(sql, params)

insert = InsertExpression(
    dialect=dialect,
    into='employees',
    columns=['name', 'department', 'salary', 'manager_id'],
    source=ValuesSource(
        dialect,
        [
            [
                Literal(dialect, 'Alice Smith'),
                Literal(dialect, 'Engineering'),
                Literal(dialect, 95000.00),
                Literal(dialect, None),
            ],
            [
                Literal(dialect, 'Bob Johnson'),
                Literal(dialect, 'Engineering'),
                Literal(dialect, 85000.00),
                Literal(dialect, 1),
            ],
            [
                Literal(dialect, 'Charlie Brown'),
                Literal(dialect, 'Sales'),
                Literal(dialect, 72000.00),
                Literal(dialect, None),
            ],
            [
                Literal(dialect, 'Diana Prince'),
                Literal(dialect, 'Marketing'),
                Literal(dialect, 88000.00),
                Literal(dialect, None),
            ],
            [
                Literal(dialect, 'Eve Williams'),
                Literal(dialect, 'Sales'),
                Literal(dialect, 68000.00),
                Literal(dialect, 3),
            ],
            [
                Literal(dialect, 'Frank Miller'),
                Literal(dialect, 'Engineering'),
                Literal(dialect, 105000.00),
                Literal(dialect, 1),
            ],
        ],
    ),
)
sql, params = insert.to_sql()
backend.execute(sql, params)

# ============================================================
# SECTION: Business Logic (the pattern to learn)
# ============================================================

# 1. LIKE predicate - find employees whose name starts with 'A'
like_query = QueryExpression(
    dialect=dialect,
    select=[
        Column(dialect, 'id'),
        Column(dialect, 'name'),
        Column(dialect, 'department'),
    ],
    from_=TableExpression(dialect, 'employees'),
    where=WhereClause(
        dialect,
        condition=LikePredicate(
            dialect,
            'LIKE',
            Column(dialect, 'name'),
            Literal(dialect, 'A%'),
        ),
    ),
)
sql, params = like_query.to_sql()
print(f"LIKE SQL: {sql}")
print(f"Params: {params}")

# 2. IN predicate - find employees in specific departments
in_query = QueryExpression(
    dialect=dialect,
    select=[
        Column(dialect, 'id'),
        Column(dialect, 'name'),
        Column(dialect, 'department'),
    ],
    from_=TableExpression(dialect, 'employees'),
    where=WhereClause(
        dialect,
        condition=InPredicate(
            dialect,
            Column(dialect, 'department'),
            Literal(dialect, ['Engineering', 'Sales']),
        ),
    ),
)
sql, params = in_query.to_sql()
print(f"IN SQL: {sql}")
print(f"Params: {params}")

# 3. BETWEEN predicate - find employees with salary in a range
between_query = QueryExpression(
    dialect=dialect,
    select=[
        Column(dialect, 'id'),
        Column(dialect, 'name'),
        Column(dialect, 'salary'),
    ],
    from_=TableExpression(dialect, 'employees'),
    where=WhereClause(
        dialect,
        condition=BetweenPredicate(
            dialect,
            Column(dialect, 'salary'),
            Literal(dialect, 70000.00),
            Literal(dialect, 90000.00),
        ),
    ),
)
sql, params = between_query.to_sql()
print(f"BETWEEN SQL: {sql}")
print(f"Params: {params}")

# 4. IS NULL / IS NOT NULL predicates - find employees with/without managers
is_null_query = QueryExpression(
    dialect=dialect,
    select=[
        Column(dialect, 'id'),
        Column(dialect, 'name'),
        Column(dialect, 'manager_id'),
    ],
    from_=TableExpression(dialect, 'employees'),
    where=WhereClause(
        dialect,
        condition=IsNullPredicate(
            dialect,
            Column(dialect, 'manager_id'),
            is_not=False,
        ),
    ),
)
sql, params = is_null_query.to_sql()
print(f"IS NULL SQL: {sql}")
print(f"Params: {params}")

is_not_null_query = QueryExpression(
    dialect=dialect,
    select=[
        Column(dialect, 'id'),
        Column(dialect, 'name'),
        Column(dialect, 'manager_id'),
    ],
    from_=TableExpression(dialect, 'employees'),
    where=WhereClause(
        dialect,
        condition=IsNullPredicate(
            dialect,
            Column(dialect, 'manager_id'),
            is_not=True,
        ),
    ),
)
sql, params = is_not_null_query.to_sql()
print(f"IS NOT NULL SQL: {sql}")
print(f"Params: {params}")

# 5. Combined predicates - AND with LIKE and BETWEEN
combined_query = QueryExpression(
    dialect=dialect,
    select=[
        Column(dialect, 'id'),
        Column(dialect, 'name'),
        Column(dialect, 'department'),
        Column(dialect, 'salary'),
    ],
    from_=TableExpression(dialect, 'employees'),
    where=WhereClause(
        dialect,
        condition=LogicalPredicate(
            dialect,
            'AND',
            LikePredicate(
                dialect,
                'LIKE',
                Column(dialect, 'name'),
                Literal(dialect, '%e%'),
            ),
            BetweenPredicate(
                dialect,
                Column(dialect, 'salary'),
                Literal(dialect, 70000.00),
                Literal(dialect, 100000.00),
            ),
        ),
    ),
)
sql, params = combined_query.to_sql()
print(f"Combined SQL: {sql}")
print(f"Params: {params}")

# ============================================================
# SECTION: Execution (run the expression)
# ============================================================
options = ExecutionOptions(stmt_type=StatementType.DQL)

print("\n--- LIKE predicate results ---")
sql, params = like_query.to_sql()
result = backend.execute(sql, params, options=options)
print(f"Rows returned: {len(result.data) if result.data else 0}")
for row in result.data or []:
    print(f" {row}")

print("\n--- IN predicate results ---")
sql, params = in_query.to_sql()
result = backend.execute(sql, params, options=options)
print(f"Rows returned: {len(result.data) if result.data else 0}")
for row in result.data or []:
    print(f" {row}")

print("\n--- BETWEEN predicate results ---")
sql, params = between_query.to_sql()
result = backend.execute(sql, params, options=options)
print(f"Rows returned: {len(result.data) if result.data else 0}")
for row in result.data or []:
    print(f" {row}")

print("\n--- IS NULL predicate results ---")
sql, params = is_null_query.to_sql()
result = backend.execute(sql, params, options=options)
print(f"Rows returned: {len(result.data) if result.data else 0}")
for row in result.data or []:
    print(f" {row}")

print("\n--- IS NOT NULL predicate results ---")
sql, params = is_not_null_query.to_sql()
result = backend.execute(sql, params, options=options)
print(f"Rows returned: {len(result.data) if result.data else 0}")
for row in result.data or []:
    print(f" {row}")

print("\n--- Combined predicates results ---")
sql, params = combined_query.to_sql()
result = backend.execute(sql, params, options=options)
print(f"Rows returned: {len(result.data) if result.data else 0}")
for row in result.data or []:
    print(f" {row}")

# ============================================================
# SECTION: Teardown (necessary for execution, reference only)
# ============================================================
backend.disconnect()
