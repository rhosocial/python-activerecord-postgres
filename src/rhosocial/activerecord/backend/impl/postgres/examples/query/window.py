"""
Window functions - ROW_NUMBER, RANK, and aggregate over windows.
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
    table_name='sales_data',
    if_exists=True,
    cascade=True,
)
backend.execute(drop_table[0], drop_table[1])

create_table = CreateTableExpression(
    dialect=dialect,
    table_name='sales_data',
    columns=[
        {'name': 'id', 'data_type': 'SERIAL', 'primary_key': True},
        {'name': 'salesperson', 'data_type': 'VARCHAR(100)'},
        {'name': 'region', 'data_type': 'VARCHAR(50)'},
        {'name': 'amount', 'data_type': 'DECIMAL(10,2)'},
    ],
)
backend.execute(*create_table.to_sql())

insert_expr = InsertExpression(
    dialect=dialect,
    into=TableExpression(dialect, 'sales_data'),
    source=ValuesSource(
        dialect,
        [
            [Literal(dialect, 'Alice'), Literal(dialect, 'North'), Literal(dialect, 1000)],
            [Literal(dialect, 'Alice'), Literal(dialect, 'North'), Literal(dialect, 1500)],
            [Literal(dialect, 'Bob'), Literal(dialect, 'North'), Literal(dialect, 1200)],
            [Literal(dialect, 'Bob'), Literal(dialect, 'South'), Literal(dialect, 1800)],
            [Literal(dialect, 'Charlie'), Literal(dialect, 'South'), Literal(dialect, 2000)],
        ],
    ),
    columns=['salesperson', 'region', 'amount'],
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
    WindowSpecification,
)
from rhosocial.activerecord.backend.expression.core import FunctionCall

query = QueryExpression(
    dialect=dialect,
    select=[
        Column(dialect, 'salesperson'),
        Column(dialect, 'region'),
        Column(dialect, 'amount'),
        FunctionCall(dialect, 'ROW_NUMBER').over(
            WindowSpecification(
                dialect,
                partition_by=[Column(dialect, 'region')],
                order_by='amount DESC',
            )
        ).as_('row_num'),
        FunctionCall(dialect, 'SUM', Column(dialect, 'amount')).over(
            WindowSpecification(
                dialect,
                partition_by=[Column(dialect, 'region')],
            )
        ).as_('region_total'),
    ],
    from_=TableExpression(dialect, 'sales_data'),
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
