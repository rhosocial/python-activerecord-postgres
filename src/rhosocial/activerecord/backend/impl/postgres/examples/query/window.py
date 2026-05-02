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
    table_name='sales_data',
    if_exists=True,
    cascade=True,
)
sql, params = drop_table.to_sql()
backend.execute(sql, params)

create_table = CreateTableExpression(
    dialect=dialect,
    table_name='sales_data',
    columns=[
        ColumnDefinition(
            'id',
            'SERIAL',
            constraints=[
                ColumnConstraint(ColumnConstraintType.PRIMARY_KEY),
                ColumnConstraint(ColumnConstraintType.NOT_NULL),
            ],
        ),
        ColumnDefinition('salesperson', 'VARCHAR(100)'),
        ColumnDefinition('region', 'VARCHAR(50)'),
        ColumnDefinition('amount', 'DECIMAL(10,2)'),
    ],
)
backend.execute(*create_table.to_sql())

insert_expr = InsertExpression(
    dialect=dialect,
    into='sales_data',
    columns=['salesperson', 'region', 'amount'],
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
    OrderByClause,
)
from rhosocial.activerecord.backend.expression.advanced_functions import WindowFunctionCall

query = QueryExpression(
    dialect=dialect,
    select=[
        Column(dialect, 'salesperson'),
        Column(dialect, 'region'),
        Column(dialect, 'amount'),
        WindowFunctionCall(
            dialect,
            'ROW_NUMBER',
            window_spec=WindowSpecification(
                dialect,
                partition_by=[Column(dialect, 'region')],
                order_by=OrderByClause(dialect, expressions=[(Column(dialect, 'amount'), 'DESC')]),
            ),
            alias='row_num',
        ),
        WindowFunctionCall(
            dialect,
            'SUM',
            args=[Column(dialect, 'amount')],
            window_spec=WindowSpecification(
                dialect,
                partition_by=[Column(dialect, 'region')],
            ),
            alias='region_total',
        ),
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
