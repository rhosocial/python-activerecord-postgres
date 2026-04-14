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
    TableExpression,
)
from rhosocial.activerecord.backend.expression.core import Literal
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
    drop_table = dialect.format_drop_table_statement(
        table_name=table,
        if_exists=True,
        cascade=True,
    )
    backend.execute(drop_table[0], drop_table[1])

create_departments = CreateTableExpression(
    dialect=dialect,
    table_name='departments',
    columns=[
        {'name': 'id', 'data_type': 'SERIAL', 'primary_key': True},
        {'name': 'name', 'data_type': 'VARCHAR(100)'},
        {'name': 'budget', 'data_type': 'DECIMAL(15,2)'},
    ],
)
backend.execute(*create_departments.to_sql())

create_employees = CreateTableExpression(
    dialect=dialect,
    table_name='employees',
    columns=[
        {'name': 'id', 'data_type': 'SERIAL', 'primary_key': True},
        {'name': 'name', 'data_type': 'VARCHAR(100)'},
        {'name': 'salary', 'data_type': 'DECIMAL(10,2)'},
        {'name': 'department_id', 'data_type': 'INT'},
    ],
    foreign_keys=[{
        'columns': ['department_id'],
        'ref_table': 'departments',
        'ref_columns': ['id'],
    }],
)
backend.execute(*create_employees.to_sql())

insert_departments = InsertExpression(
    dialect=dialect,
    into=TableExpression(dialect, 'departments'),
    source=ValuesSource(
        dialect,
        [
            [Literal(dialect, 'Engineering'), Literal(dialect, 1000000)],
            [Literal(dialect, 'Sales'), Literal(dialect, 500000)],
        ],
    ),
    columns=['name', 'budget'],
    dialect_options={},
)
backend.execute(*insert_departments.to_sql())

insert_employees = InsertExpression(
    dialect=dialect,
    into=TableExpression(dialect, 'employees'),
    source=ValuesSource(
        dialect,
        [
            [Literal(dialect, 'Alice'), Literal(dialect, 80000), Literal(dialect, 1)],
            [Literal(dialect, 'Bob'), Literal(dialect, 90000), Literal(dialect, 1)],
            [Literal(dialect, 'Charlie'), Literal(dialect, 60000), Literal(dialect, 2)],
            [Literal(dialect, 'David'), Literal(dialect, 70000), Literal(dialect, 2)],
        ],
    ),
    columns=['name', 'salary', 'department_id'],
    dialect_options={},
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
    SubqueryExpression,
)
from rhosocial.activerecord.backend.expression.core import FunctionCall
from rhosocial.activerecord.backend.expression.predicates import ComparisonPredicate

subquery = QueryExpression(
    dialect=dialect,
    select=[FunctionCall(dialect, 'AVG', Column(dialect, 'salary'))],
    from_=TableExpression(dialect, 'employees'),
)

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
            SubqueryExpression(dialect, subquery),
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
