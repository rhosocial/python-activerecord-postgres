"""
Subquery expressions - WHERE and FROM subqueries.
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
)
from rhosocial.activerecord.backend.expression.core import Literal
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

for table in ['employees', 'departments']:
    drop_table = DropTableExpression(
        dialect=dialect,
        table_name=table,
        if_exists=True,
        cascade=True,
    )
    sql, params = drop_table.to_sql()
    backend.execute(sql, params)

create_departments = CreateTableExpression(
    dialect=dialect,
    table_name='departments',
    columns=[
        ColumnDefinition(
            'id',
            'SERIAL',
            constraints=[
                ColumnConstraint(ColumnConstraintType.PRIMARY_KEY),
                ColumnConstraint(ColumnConstraintType.NOT_NULL),
            ],
        ),
        ColumnDefinition('name', 'VARCHAR(100)'),
        ColumnDefinition('budget', 'DECIMAL(15,2)'),
    ],
)
backend.execute(*create_departments.to_sql())

create_employees = CreateTableExpression(
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
        ColumnDefinition('name', 'VARCHAR(100)'),
        ColumnDefinition('salary', 'DECIMAL(10,2)'),
        ColumnDefinition('department_id', 'INTEGER'),
    ],
)
backend.execute(*create_employees.to_sql())

insert_departments = InsertExpression(
    dialect=dialect,
    into='departments',
    columns=['name', 'budget'],
    source=ValuesSource(
        dialect,
        [
            [Literal(dialect, 'Engineering'), Literal(dialect, 1000000)],
            [Literal(dialect, 'Sales'), Literal(dialect, 500000)],
        ],
    ),
)
backend.execute(*insert_departments.to_sql())

insert_employees = InsertExpression(
    dialect=dialect,
    into='employees',
    columns=['name', 'salary', 'department_id'],
    source=ValuesSource(
        dialect,
        [
            [Literal(dialect, 'Alice'), Literal(dialect, 80000), Literal(dialect, 1)],
            [Literal(dialect, 'Bob'), Literal(dialect, 90000), Literal(dialect, 1)],
            [Literal(dialect, 'Charlie'), Literal(dialect, 60000), Literal(dialect, 2)],
            [Literal(dialect, 'David'), Literal(dialect, 70000), Literal(dialect, 2)],
        ],
    ),
)
backend.execute(*insert_employees.to_sql())

# ============================================================
# SECTION: Business Logic (the pattern to learn)
# ============================================================
from rhosocial.activerecord.backend.expression import (
    QueryExpression,
    TableExpression,
    Column,
    WhereClause,
    Subquery,
)
from rhosocial.activerecord.backend.expression.core import FunctionCall
from rhosocial.activerecord.backend.expression.predicates import ComparisonPredicate

subquery_query = QueryExpression(
    dialect=dialect,
    select=[FunctionCall(dialect, 'AVG', Column(dialect, 'salary'))],
    from_=TableExpression(dialect, 'employees'),
)
sql, params = subquery_query.to_sql()
subquery = Subquery(dialect, sql, params)

query = QueryExpression(
    dialect=dialect,
    select=[
        Column(dialect, 'name'),
        Column(dialect, 'salary'),
    ],
    from_=TableExpression(dialect, 'employees'),
    where=WhereClause(
        dialect,
        condition=ComparisonPredicate(
            dialect,
            '>',
            Column(dialect, 'salary'),
            subquery,
        ),
    ),
)

sql, params = query.to_sql()
print(f"SQL: {sql}")
print(f"Params: {params}")

# ============================================================
# SECTION: Execution (run the expression)
# ============================================================
options = ExecutionOptions(stmt_type=StatementType.DQL)
result = backend.execute(sql, params, options=options)
print(f"Rows returned: {len(result.data) if result.data else 0}")
for row in result.data or []:
    print(f" {row}")

# ============================================================
# SECTION: Teardown (necessary for execution, reference only)
# ============================================================
backend.disconnect()
