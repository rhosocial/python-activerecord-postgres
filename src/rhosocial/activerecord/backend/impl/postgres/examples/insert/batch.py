"""
Batch insert using INSERT with multiple VALUES.
"""

# ============================================================
# SECTION: Setup (necessary for execution, reference only)
# ============================================================
import os
from rhosocial.activerecord.backend.impl.postgres import PostgresBackend
from rhosocial.activerecord.backend.impl.postgres.config import PostgresConnectionConfig
from rhosocial.activerecord.backend.expression import (
    CreateTableExpression,
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

drop_table = dialect.format_drop_table_statement(
    table_name='logs',
    if_exists=True,
    cascade=True,
)
backend.execute(drop_table[0], drop_table[1])

create_table = CreateTableExpression(
    dialect=dialect,
    table_name='logs',
    columns=[
        {'name': 'id', 'data_type': 'SERIAL', 'primary_key': True},
        {'name': 'level', 'data_type': 'VARCHAR(20)'},
        {'name': 'message', 'data_type': 'TEXT'},
        {'name': 'created_at', 'data_type': 'TIMESTAMP', 'default': 'CURRENT_TIMESTAMP'},
    ],
)
backend.execute(*create_table.to_sql())

# ============================================================
# SECTION: Business Logic (the pattern to learn)
# ============================================================
from rhosocial.activerecord.backend.expression import (
    InsertExpression,
    ValuesSource,
    TableExpression,
)
from rhosocial.activerecord.backend.expression.core import Literal

insert = InsertExpression(
    dialect=dialect,
    into=TableExpression(dialect, 'logs'),
    source=ValuesSource(
        dialect,
        [
            [Literal(dialect, 'INFO'), Literal(dialect, 'System started')],
            [Literal(dialect, 'DEBUG'), Literal(dialect, 'Loading configuration')],
            [Literal(dialect, 'INFO'), Literal(dialect, 'Application ready')],
            [Literal(dialect, 'WARNING'), Literal(dialect, 'Memory usage high')],
        ],
    ),
    columns=['level', 'message'],
    dialect_options={},
)

sql, params = insert.to_sql()
print(f"SQL: {sql}")
print(f"Params: {params}")

# ============================================================
# SECTION: Execution (run the expression)
# ============================================================
backend.execute(sql, params)
print("Batch insert completed")

# Verify
options = ExecutionOptions(stmt_type=StatementType.DQL)
result = backend.execute("SELECT * FROM logs ORDER BY id", options=options)
print(f"Rows inserted: {len(result.data) if result.data else 0}")
for row in result.data or []:
    print(f" {row}")

# ============================================================
# SECTION: Teardown (necessary for execution, reference only)
# ============================================================
backend.disconnect()
