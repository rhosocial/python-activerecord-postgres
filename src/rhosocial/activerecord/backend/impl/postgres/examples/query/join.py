"""
JOIN queries - INNER JOIN and LEFT JOIN.
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

for table in ['orders', 'customers']:
    drop_table = DropTableExpression(
        dialect=dialect,
        table_name=table,
        if_exists=True,
        cascade=True,
    )
    sql, params = drop_table.to_sql()
    backend.execute(sql, params)

create_customers = CreateTableExpression(
    dialect=dialect,
    table_name='customers',
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
        ColumnDefinition('email', 'VARCHAR(100)'),
    ],
)
backend.execute(*create_customers.to_sql())

create_orders = CreateTableExpression(
    dialect=dialect,
    table_name='orders',
    columns=[
        ColumnDefinition(
            'id',
            'SERIAL',
            constraints=[
                ColumnConstraint(ColumnConstraintType.PRIMARY_KEY),
                ColumnConstraint(ColumnConstraintType.NOT_NULL),
            ],
        ),
        ColumnDefinition('customer_id', 'INTEGER'),
        ColumnDefinition('total', 'DECIMAL(10,2)'),
        ColumnDefinition('status', 'VARCHAR(20)'),
    ],
)
backend.execute(*create_orders.to_sql())

insert_customers = InsertExpression(
    dialect=dialect,
    into='customers',
    columns=['name', 'email'],
    source=ValuesSource(
        dialect,
        [
            [Literal(dialect, 'Alice'), Literal(dialect, 'alice@example.com')],
            [Literal(dialect, 'Bob'), Literal(dialect, 'bob@example.com')],
        ],
    ),
)
backend.execute(*insert_customers.to_sql())

insert_orders = InsertExpression(
    dialect=dialect,
    into='orders',
    columns=['customer_id', 'total', 'status'],
    source=ValuesSource(
        dialect,
        [
            [Literal(dialect, 1), Literal(dialect, 100.00), Literal(dialect, 'completed')],
            [Literal(dialect, 1), Literal(dialect, 50.00), Literal(dialect, 'pending')],
            [Literal(dialect, 2), Literal(dialect, 75.00), Literal(dialect, 'completed')],
        ],
    ),
)
backend.execute(*insert_orders.to_sql())

# ============================================================
# SECTION: Business Logic (the pattern to learn)
# ============================================================
from rhosocial.activerecord.backend.expression import (
    QueryExpression,
    TableExpression,
    Column,
    WhereClause,
)
from rhosocial.activerecord.backend.expression.core import Literal
from rhosocial.activerecord.backend.expression.predicates import ComparisonPredicate

customers = TableExpression(dialect, 'customers', alias='c')
orders = TableExpression(dialect, 'orders', alias='o')

query = QueryExpression(
    dialect=dialect,
    select=[
        Column(dialect, 'name', table='c'),
        Column(dialect, 'total', table='o'),
        Column(dialect, 'status', table='o'),
    ],
    from_=[
        customers,
        orders,
    ],
    where=WhereClause(
        dialect,
        condition=ComparisonPredicate(
            dialect,
            'AND',
            ComparisonPredicate(
                dialect,
                '=',
                Column(dialect, 'id', table='c'),
                Column(dialect, 'customer_id', table='o'),
            ),
            ComparisonPredicate(
                dialect,
                '=',
                Column(dialect, 'status', table='o'),
                Literal(dialect, 'completed'),
            ),
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
