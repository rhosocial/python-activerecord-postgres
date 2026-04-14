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

for table in ['orders', 'customers']:
    drop_table = dialect.format_drop_table_statement(
        table_name=table,
        if_exists=True,
        cascade=True,
    )
    backend.execute(drop_table[0], drop_table[1])

create_customers = CreateTableExpression(
    dialect=dialect,
    table_name='customers',
    columns=[
        {'name': 'id', 'data_type': 'SERIAL', 'primary_key': True},
        {'name': 'name', 'data_type': 'VARCHAR(100)', 'nullable': False},
        {'name': 'email', 'data_type': 'VARCHAR(100)'},
    ],
)
backend.execute(*create_customers.to_sql())

create_orders = CreateTableExpression(
    dialect=dialect,
    table_name='orders',
    columns=[
        {'name': 'id', 'data_type': 'SERIAL', 'primary_key': True},
        {'name': 'customer_id', 'data_type': 'INT'},
        {'name': 'total', 'data_type': 'DECIMAL(10,2)'},
        {'name': 'status', 'data_type': 'VARCHAR(20)'},
    ],
    foreign_keys=[{
        'columns': ['customer_id'],
        'ref_table': 'customers',
        'ref_columns': ['id'],
    }],
)
backend.execute(*create_orders.to_sql())

insert_customers = InsertExpression(
    dialect=dialect,
    into=TableExpression(dialect, 'customers'),
    source=ValuesSource(
        dialect,
        [
            [Literal(dialect, 'Alice'), Literal(dialect, 'alice@example.com')],
            [Literal(dialect, 'Bob'), Literal(dialect, 'bob@example.com')],
        ],
    ),
    columns=['name', 'email'],
    dialect_options={},
)
backend.execute(*insert_customers.to_sql())

insert_orders = InsertExpression(
    dialect=dialect,
    into=TableExpression(dialect, 'orders'),
    source=ValuesSource(
        dialect,
        [
            [Literal(dialect, 1), Literal(dialect, 100.00), Literal(dialect, 'completed')],
            [Literal(dialect, 1), Literal(dialect, 50.00), Literal(dialect, 'pending')],
            [Literal(dialect, 2), Literal(dialect, 75.00), Literal(dialect, 'completed')],
        ],
    ),
    columns=['customer_id', 'total', 'status'],
    dialect_options={},
)
backend.execute(*insert_orders.to_sql())

# ============================================================
# SECTION: Business Logic (the pattern to learn)
# ============================================================
from rhosocial.activerecord.backend.expression import (
    QueryExpression,
    TableExpression,
    Column,
    JoinClause,
    WhereClause,
)
from rhosocial.activerecord.backend.expression.core import Literal
from rhosocial.activerecord.backend.expression.predicates import ComparisonPredicate

query = QueryExpression(
    dialect=dialect,
    select=[
        Column(dialect, 'customers', 'name'),
        Column(dialect, 'orders', 'total'),
        Column(dialect, 'orders', 'status'),
    ],
    from_=TableExpression(dialect, 'customers'),
    join=[
        JoinClause(
            dialect,
            join_type='INNER',
            target=TableExpression(dialect, 'orders'),
            condition=ComparisonPredicate(
                dialect,
                '=',
                Column(dialect, 'customers', 'id'),
                Column(dialect, 'orders', 'customer_id'),
            ),
        ),
    ],
    where=WhereClause(
        dialect,
        condition=ComparisonPredicate(
            dialect,
            '=',
            Column(dialect, 'orders', 'status'),
            Literal(dialect, 'completed'),
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
