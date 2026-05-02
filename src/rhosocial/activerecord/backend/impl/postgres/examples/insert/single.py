"""
Single Row INSERT - PostgreSQL.

This example demonstrates:
1. INSERT single row
2. INSERT with SERIAL
"""

# ============================================================
# SECTION: Setup (necessary for execution, reference only)
# ============================================================
import os
from rhosocial.activerecord.backend.impl.postgres import PostgresBackend
from rhosocial.activerecord.backend.impl.postgres.config import PostgresConnectionConfig

config = PostgresConnectionConfig(
    host=os.getenv('POSTGRES_HOST', 'localhost'),
    port=int(os.getenv('POSTGRES_PORT', 5432)),
    database=os.getenv('POSTGRES_DATABASE', 'test'),
    username=os.getenv('POSTGRES_USER', 'postgres'),
    password=os.getenv('POSTGRES_PASSWORD', ''),
)
backend = PostgresBackend(connection_config=config)
backend.connect()
dialect = backend.dialect

from rhosocial.activerecord.backend.expression import CreateTableExpression
from rhosocial.activerecord.backend.expression.statements import (
    ColumnDefinition,
)

create_table = CreateTableExpression(
    dialect=dialect,
    table_name='users',
    columns=[
        ColumnDefinition('id', 'SERIAL'),
        ColumnDefinition('name', 'VARCHAR(100)'),
    ],
    if_not_exists=True,
)
sql, params = create_table.to_sql()
print(f"Create table SQL: {sql}")
backend.execute(sql, params)

# ============================================================
# SECTION: Single INSERT
# ============================================================
from rhosocial.activerecord.backend.expression import InsertExpression, ValuesSource, QueryExpression, TableExpression
from rhosocial.activerecord.backend.expression.core import Literal, WildcardExpression

insert_expr = InsertExpression(
    dialect=dialect,
    into='users',
    columns=['name'],
    source=ValuesSource(dialect, [
        [Literal(dialect, 'Alice')],
    ]),
)
sql, params = insert_expr.to_sql()
print(f"Insert SQL: {sql}")
print(f"Params: {params}")
backend.execute(sql, params)

verify_query = QueryExpression(
    dialect=dialect,
    select=[WildcardExpression(dialect)],
    from_=TableExpression(dialect, 'users'),
)
sql, params = verify_query.to_sql()
result = backend.execute(sql, params)
print(f"Result: {result.data}")

# ============================================================
# SECTION: Teardown
# ============================================================
from rhosocial.activerecord.backend.expression import DropTableExpression

drop_expr = DropTableExpression(dialect=dialect, table_name='users', if_exists=True)
sql, params = drop_expr.to_sql()
backend.execute(sql, params)
backend.disconnect()

# ============================================================
# SECTION: Summary
# ============================================================
# Key points:
# 1. Use InsertExpression with ValuesSource
# 2. SERIAL creates auto-increment