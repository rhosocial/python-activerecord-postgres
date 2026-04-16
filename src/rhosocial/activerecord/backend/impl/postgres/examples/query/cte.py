"""
CTE (Common Table Expression) example - WITH clause.

This example demonstrates:
1. Basic CTE with WITH clause
2. Recursive CTE for hierarchical data
3. CTE for simplifying complex queries
"""

# ============================================================
# SECTION: Setup (necessary for execution, reference only)
# ============================================================
import os
from rhosocial.activerecord.backend.impl.postgres import PostgresBackend
from rhosocial.activerecord.backend.impl.postgres.config import PostgresConnectionConfig

config = PostgresConnectionConfig(
    host=os.getenv('PG_HOST', 'localhost'),
    port=int(os.getenv('PG_PORT', 5432)),
    database=os.getenv('PG_DATABASE', 'test'),
    username=os.getenv('PG_USERNAME', 'postgres'),
    password=os.getenv('PG_PASSWORD', ''),
)
backend = PostgresBackend(connection_config=config)
backend.connect()
dialect = backend.dialect

from rhosocial.activerecord.backend.expression import (
    CreateTableExpression,
    InsertExpression,
    ValuesSource,
    DropTableExpression,
    QueryExpression,
    TableExpression,
    CTEExpression,
    WithQueryExpression,
)
from rhosocial.activerecord.backend.expression.core import Literal, Column
from rhosocial.activerecord.backend.expression.predicates import ComparisonPredicate
from rhosocial.activerecord.backend.expression.statements import (
    ColumnDefinition,
    ColumnConstraint,
    ColumnConstraintType,
)
from rhosocial.activerecord.backend.options import ExecutionOptions
from rhosocial.activerecord.backend.schema import StatementType

dql_options = ExecutionOptions(stmt_type=StatementType.DQL)

create_table = CreateTableExpression(
    dialect=dialect,
    table_name='employees',
    columns=[
        ColumnDefinition('id', 'SERIAL', constraints=[
            ColumnConstraint(ColumnConstraintType.PRIMARY_KEY),
            ColumnConstraint(ColumnConstraintType.NOT_NULL),
        ]),
        ColumnDefinition('name', 'VARCHAR(100)'),
        ColumnDefinition('manager_id', 'INT'),
    ],
    if_not_exists=True,
)
sql, params = create_table.to_sql()
backend.execute(sql, params)

insert_expr = InsertExpression(
    dialect=dialect,
    table_name='employees',
    columns=['name', 'manager_id'],
    source=ValuesSource(dialect, [
        [Literal(dialect, 'CEO'), Literal(dialect, None)],
        [Literal(dialect, 'VP Sales'), Literal(dialect, 1)],
        [Literal(dialect, 'VP Engineering'), Literal(dialect, 1)],
        [Literal(dialect, 'Sales Manager'), Literal(dialect, 2)],
        [Literal(dialect, 'Engineer'), Literal(dialect, 3)],
    ]),
)
sql, params = insert_expr.to_sql()
backend.execute(sql, params)

# ============================================================
# SECTION: Basic CTE
# ============================================================
# CTE simplifies complex queries by defining temporary named result sets

high_earners_cte = CTEExpression(
    dialect=dialect,
    name='high_earners',
    query=QueryExpression(
        dialect=dialect,
        select=[Column(dialect, 'id'), Column(dialect, 'name')],
        from_=TableExpression(dialect, 'employees'),
        where=ComparisonPredicate(dialect, '>', Column(dialect, 'id'), Literal(dialect, 2)),
    ),
)

cte_query = WithQueryExpression(
    dialect=dialect,
    ctes=[high_earners_cte],
    main_query=QueryExpression(
        dialect=dialect,
        select=[Column(dialect, 'id'), Column(dialect, 'name')],
        from_=TableExpression(dialect, 'high_earners'),
    ),
)
sql, params = cte_query.to_sql()
print(f"Basic CTE SQL: {sql}")
result = backend.execute(sql, params, options=dql_options)
print(f"Basic CTE result: {result.data}")

# ============================================================
# SECTION: Recursive CTE
# ============================================================
# Recursive CTE for hierarchical data (organizational chart)

# Base case: top-level employees
base_query = QueryExpression(
    dialect=dialect,
    select=[
        Column(dialect, 'id'),
        Column(dialect, 'name'),
        Column(dialect, 'manager_id'),
        Literal(dialect, 1).as_('level'),
    ],
    from_=TableExpression(dialect, 'employees'),
    where=ComparisonPredicate(dialect, 'IS', Column(dialect, 'manager_id'), Literal(dialect, None)),
)

org_cte = CTEExpression(
    dialect=dialect,
    name='org_chart',
    query=base_query,
)

recursive_query = WithQueryExpression(
    dialect=dialect,
    ctes=[org_cte],
    main_query=QueryExpression(
        dialect=dialect,
        select=[Column(dialect, 'id'), Column(dialect, 'name'), Column(dialect, 'manager_id')],
        from_=TableExpression(dialect, 'org_chart'),
    ),
    recursive=True,
)
sql, params = recursive_query.to_sql()
print(f"Recursive CTE SQL: {sql}")
result = backend.execute(sql, params, options=dql_options)
print(f"Recursive CTE result:")
for row in result.data or []:
    print(f"  {row}")

# ============================================================
# SECTION: CTE with MATERIALIZED hint (PostgreSQL 12+)
# ============================================================
# Control whether CTE is inlined or materialized

materialized_cte = CTEExpression(
    dialect=dialect,
    name='dept_stats',
    query=QueryExpression(
        dialect=dialect,
        select=[Column(dialect, 'manager_id'), Column(dialect, 'name')],
        from_=TableExpression(dialect, 'employees'),
    ),
    materialized=False,  # NOT MATERIALIZED - allows inlining
)

materialized_query = WithQueryExpression(
    dialect=dialect,
    ctes=[materialized_cte],
    main_query=QueryExpression(
        dialect=dialect,
        select=[Column(dialect, 'manager_id'), Column(dialect, 'name')],
        from_=TableExpression(dialect, 'dept_stats'),
    ),
)
sql, params = materialized_query.to_sql()
print(f"MATERIALIZED hint CTE SQL: {sql}")

# ============================================================
# SECTION: Teardown (necessary for execution, reference only)
# ============================================================
drop_table = DropTableExpression(dialect=dialect, table_name='employees', if_exists=True, cascade=True)
sql, params = drop_table.to_sql()
backend.execute(sql, params)
backend.disconnect()

# ============================================================
# SECTION: Summary
# ============================================================
# Key points:
# 1. Use CTEExpression to define CTEs
# 2. Use WithQueryExpression to combine CTEs with a main query
# 3. Set recursive=True for recursive CTEs (hierarchical queries)
# 4. PostgreSQL 12+ supports materialized hint (CTEExpression(materialized=False))
