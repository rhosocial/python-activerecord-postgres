"""
Aggregate queries with GROUP BY and HAVING.
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

drop_table = dialect.format_drop_table_statement(
    table_name='sales',
    if_exists=True,
    cascade=True,
)
backend.execute(drop_table[0], drop_table[1])

create_table = CreateTableExpression(
    dialect=dialect,
    table_name='sales',
    columns=[
        {'name': 'id', 'data_type': 'SERIAL', 'primary_key': True},
        {'name': 'product', 'data_type': 'VARCHAR(100)'},
        {'name': 'quantity', 'data_type': 'INT'},
        {'name': 'price', 'data_type': 'DECIMAL(10,2)'},
        {'name': 'region', 'data_type': 'VARCHAR(50)'},
    ],
)
backend.execute(*create_table.to_sql())

insert_expr = InsertExpression(
    dialect=dialect,
    into=TableExpression(dialect, 'sales'),
    source=ValuesSource(
        dialect,
        [
            [Literal(dialect, 'Widget'), Literal(dialect, 10), Literal(dialect, 5.00), Literal(dialect, 'North')],
            [Literal(dialect, 'Widget'), Literal(dialect, 5), Literal(dialect, 5.00), Literal(dialect, 'South')],
            [Literal(dialect, 'Gadget'), Literal(dialect, 8), Literal(dialect, 10.00), Literal(dialect, 'North')],
            [Literal(dialect, 'Gadget'), Literal(dialect, 3), Literal(dialect, 10.00), Literal(dialect, 'South')],
            [Literal(dialect, 'Widget'), Literal(dialect, 7), Literal(dialect, 5.00), Literal(dialect, 'East')],
        ],
    ),
    columns=['product', 'quantity', 'price', 'region'],
    dialect_options={},
)
backend.execute(*insert_expr.to_sql())

# ============================================================
# SECTION: Business Logic (the pattern to learn)
# ============================================================
from rhosocial.activerecord.backend.expression import (
    QueryExpression,
    TableExpression,
    Column,
    GroupByClause,
    HavingClause,
)
from rhosocial.activerecord.backend.expression.core import Literal, FunctionCall
from rhosocial.activerecord.backend.expression.predicates import ComparisonPredicate

query = QueryExpression(
    dialect=dialect,
    select=[
        Column(dialect, 'product'),
        FunctionCall(dialect, 'SUM', Column(dialect, 'quantity')).as_('total_qty'),
        FunctionCall(dialect, 'AVG', Column(dialect, 'price')).as_('avg_price'),
    ],
    from_=TableExpression(dialect, 'sales'),
    group_by=GroupByClause(
        dialect,
        expressions=[Column(dialect, 'product')],
    ),
    having=HavingClause(
        dialect,
        condition=ComparisonPredicate(
            dialect,
            '>',
            FunctionCall(dialect, 'SUM', Column(dialect, 'quantity')),
            Literal(dialect, 10),
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
