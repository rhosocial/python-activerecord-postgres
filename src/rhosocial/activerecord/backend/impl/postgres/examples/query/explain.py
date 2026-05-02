"""
EXPLAIN and Query Plan analysis - PostgreSQL.

This example demonstrates:
1. Using EXPLAIN to analyze query execution
2. Using ANALYZE to get actual execution times
3. Understanding the EXPLAIN output
4. Interpreting costs and execution plans
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
    QueryExpression,
    TableExpression,
    ExplainExpression,
    CreateIndexExpression,
    DropTableExpression,
)
from rhosocial.activerecord.backend.expression.core import Literal, Column
from rhosocial.activerecord.backend.expression.statements import (
    ColumnDefinition,
    ColumnConstraint,
    ColumnConstraintType,
)
from rhosocial.activerecord.backend.expression.statements.explain import ExplainOptions
from rhosocial.activerecord.backend.expression.predicates import ComparisonPredicate

config = PostgresConnectionConfig(
    host=os.getenv('POSTGRES_HOST', 'localhost'),
    port=int(os.getenv('POSTGRES_PORT', 5432)),
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

create_index = CreateIndexExpression(
    dialect=dialect,
    index_name='idx_users_email',
    table_name='users',
    columns=['email'],
    if_not_exists=True,
)
sql, params = create_index.to_sql()
backend.execute(sql, params)

users = [
    ('Alice', 'alice@example.com'),
    ('Bob', 'bob@example.com'),
    ('Charlie', 'charlie@example.com'),
]
for name, email in users:
    insert_expr = InsertExpression(
        dialect=dialect,
        into='users',
        columns=['name', 'email'],
        source=ValuesSource(dialect, [[Literal(dialect, name), Literal(dialect, email)]]),
    )
    sql, params = insert_expr.to_sql()
    backend.execute(sql, params)

# ============================================================
# SECTION: Business Logic (the pattern to learn)
# ============================================================

# 1. EXPLAIN for table scan (no index on name column)
query1 = QueryExpression(
    dialect=dialect,
    select=[Column(dialect, '*')],
    from_=TableExpression(dialect, 'users'),
    where=ComparisonPredicate(
        dialect, '=', Column(dialect, 'name'), Literal(dialect, 'Alice'),
    ),
)
explain_scan = ExplainExpression(
    dialect=dialect,
    statement=query1,
    options=ExplainOptions(),
)
sql, params = explain_scan.to_sql()
print("1. EXPLAIN (no index on name):")
print(f"SQL: {sql}")
result = backend.execute(sql, params)
for row in result.data:
    print(f"  {row}")

# 2. EXPLAIN for index scan (email has index)
query2 = QueryExpression(
    dialect=dialect,
    select=[Column(dialect, '*')],
    from_=TableExpression(dialect, 'users'),
    where=ComparisonPredicate(
        dialect, '=', Column(dialect, 'email'), Literal(dialect, 'alice@example.com'),
    ),
)
explain_search = ExplainExpression(
    dialect=dialect,
    statement=query2,
    options=ExplainOptions(),
)
sql, params = explain_search.to_sql()
print("\n2. EXPLAIN (using idx_users_email):")
print(f"SQL: {sql}")
result = backend.execute(sql, params)
for row in result.data:
    print(f"  {row}")

# 3. EXPLAIN ANALYZE for actual execution time (executes the query)
explain_analyze = ExplainExpression(
    dialect=dialect,
    statement=query2,
    options=ExplainOptions(analyze=True, timing=True, costs=True),
)
sql, params = explain_analyze.to_sql()
print("\n3. EXPLAIN ANALYZE (actual execution):")
print(f"SQL: {sql}")
result = backend.execute(sql, params)
for row in result.data:
    print(f"  {row}")

# 4. EXPLAIN with FORMAT JSON
explain_json = ExplainExpression(
    dialect=dialect,
    statement=query2,
    options=ExplainOptions(format='JSON'),
)
sql, params = explain_json.to_sql()
print("\n4. EXPLAIN FORMAT JSON:")
print(f"SQL: {sql}")
result = backend.execute(sql, params)
for row in result.data:
    print(f"  {row}")

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
# 1. EXPLAIN shows estimated query plan without executing
# 2. EXPLAIN ANALYZE executes the query and shows actual times
# 3. "cost" shows estimated startup and total cost
# 4. "rows" shows estimated rows
# 5. "actual time" shows actual execution time (ms)
# 6. FORMAT JSON provides detailed structured output