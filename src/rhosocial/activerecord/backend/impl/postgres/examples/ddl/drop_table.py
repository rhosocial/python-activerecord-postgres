"""
DROP TABLE - PostgreSQL.

This example demonstrates:
1. DROP TABLE
2. DROP TABLE IF EXISTS
"""

# ============================================================
# SECTION: Setup
# ============================================================
import os
from rhosocial.activerecord.backend.impl.postgres import PostgresBackend
from rhosocial.activerecord.backend.impl.postgres.config import PostgresConnectionConfig

config = PostgresConnectionConfig(
    host=os.getenv('PG_HOST', 'localhost'),
    port=int(os.getenv('PG_PORT', 5432)),
    database=os.getenv('PG_DATABASE', 'test'),
    username=os.getenv('PG_USERNAME', 'postgres'),
    password=os.getenv('PG_PASSWORD', ''),
)
backend = PostgresBackend(connection_config=config)
backend.connect()
dialect = backend.dialect

from rhosocial.activerecord.backend.expression import CreateTableExpression
from rhosocial.activerecord.backend.expression.statements import (
    ColumnDefinition,
    ColumnConstraint,
    ColumnConstraintType,
)

create_table = CreateTableExpression(
    dialect=dialect,
    table_name='users',
    columns=[
        ColumnDefinition('id', 'INT'),
    ],
    if_not_exists=True,
)
sql, params = create_table.to_sql()
print(f"Create table SQL: {sql}")
backend.execute(sql, params)

# ============================================================
# SECTION: DROP TABLE
# ============================================================
from rhosocial.activerecord.backend.expression import DropTableExpression

drop_expr = DropTableExpression(
    dialect=dialect,
    table_name='users',
)
sql, params = drop_expr.to_sql()
print(f"DROP TABLE SQL: {sql}")
print(f"Params: {params}")
backend.execute(sql, params)

# Already deleted
drop_expr_exists = DropTableExpression(
    dialect=dialect,
    table_name='users',
    if_exists=True,
)
sql, params = drop_expr_exists.to_sql()
print(f"DROP TABLE IF EXISTS SQL: {sql}")
backend.execute(sql, params)

# ============================================================
# SECTION: Summary
# ============================================================
backend.disconnect()

# Key points:
# 1. Use DropTableExpression to drop tables
# 2. if_exists=True prevents error