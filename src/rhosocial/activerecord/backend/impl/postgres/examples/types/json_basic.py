"""
PostgreSQL JSONB operations - PostgreSQL-specific feature.
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
    table_name='documents',
    if_exists=True,
    cascade=True,
)
backend.execute(drop_table[0], drop_table[1])

create_table = CreateTableExpression(
    dialect=dialect,
    table_name='documents',
    columns=[
        {'name': 'id', 'data_type': 'SERIAL', 'primary_key': True},
        {'name': 'data', 'data_type': 'JSONB'},
    ],
)
backend.execute(*create_table.to_sql())

import json
insert_expr = InsertExpression(
    dialect=dialect,
    into=TableExpression(dialect, 'documents'),
    source=ValuesSource(
        dialect,
        [
            [Literal(dialect, json.dumps({'name': 'Alice', 'age': 30, 'tags': ['a', 'b']}))],
            [Literal(dialect, json.dumps({'name': 'Bob', 'age': 25, 'tags': ['c']}))],
        ],
    ),
    columns=['data'],
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
)
from rhosocial.activerecord.backend.impl.postgres.functions.json import (
    jsonb_path_query,
    json_path_root,
    json_path_key,
)

query = QueryExpression(
    dialect=dialect,
    select=[
        Column(dialect, 'id'),
        Column(dialect, 'data'),
    ],
    from_=TableExpression(dialect, 'documents'),
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
