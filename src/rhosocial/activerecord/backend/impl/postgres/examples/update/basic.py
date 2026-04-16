"""
UPDATE using UpdateExpression - PostgreSQL.

This example demonstrates:
1. UPDATE single row
2. UPDATE with WHERE
3. UPDATE all rows
"""

# ============================================================
# SECTION: Setup (necessary for execution, reference only)
# ============================================================
import os
from rhosocial.activerecord.backend.impl.postgres import PostgresBackend
from rhosocial.activerecord.backend.impl.postgres.config import PostgresConnectionConfig
from rhosocial.activerecord.backend.options import ExecutionOptions
from rhosocial.activerecord.backend.schema import StatementType

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

from rhosocial.activerecord.backend.expression import (
    CreateTableExpression,
    InsertExpression,
    ValuesSource,
    DropTableExpression,
    UpdateExpression,
)
from rhosocial.activerecord.backend.expression.core import Literal, Column
from rhosocial.activerecord.backend.expression.predicates import ComparisonPredicate
from rhosocial.activerecord.backend.expression.statements import (
    ColumnDefinition,
    ColumnConstraint,
    ColumnConstraintType,
)

create_table = CreateTableExpression(
    dialect=dialect,
    table_name='users',
    columns=[
        ColumnDefinition('id', 'SERIAL'),
        ColumnDefinition('name', 'VARCHAR(100)'),
        ColumnDefinition('age', 'INT'),
    ],
    if_not_exists=True,
)
sql, params = create_table.to_sql()
print(f"Create table SQL: {sql}")
backend.execute(sql, params)

insert = InsertExpression(
    dialect=dialect,
    into='users',
    columns=['name', 'age'],
    source=ValuesSource(dialect, [
        [Literal(dialect, 'Alice'), Literal(dialect, 25)],
        [Literal(dialect, 'Bob'), Literal(dialect, 30)],
    ]),
)
sql, params = insert.to_sql()
print(f"Insert SQL: {sql}")
backend.execute(sql, params)

# ============================================================
# SECTION: UPDATE (using UpdateExpression)
# ============================================================
update_expr = UpdateExpression(
    dialect=dialect,
    table='users',
    assignments={'age': Literal(dialect, 26)},
    where=ComparisonPredicate(dialect, '=', Column(dialect, 'name'), Literal(dialect, 'Alice')),
)
sql, params = update_expr.to_sql()
print(f"UPDATE SQL: {sql}")
print(f"Params: {params}")

options = ExecutionOptions(stmt_type=StatementType.DML)
result = backend.execute(sql, params, options=options)
print(f"Updated rows: {result.affected_rows}")

# ============================================================
# SECTION: UPDATE all rows
# ============================================================
update_all = UpdateExpression(
    dialect=dialect,
    table='users',
    assignments={'age': Literal(dialect, 99)},
)
sql, params = update_all.to_sql()
print(f"UPDATE all SQL: {sql}")
result = backend.execute(sql, params, options=options)
print(f"Updated rows: {result.affected_rows}")

# ============================================================
# SECTION: Teardown
# ============================================================
drop_expr = DropTableExpression(dialect=dialect, table_name='users', if_exists=True)
sql, params = drop_expr.to_sql()
backend.execute(sql, params)
backend.disconnect()

# ============================================================
# SECTION: Summary
# ============================================================
# Key points:
# 1. Use UpdateExpression with assignments dict for SET clause
# 2. Use ComparisonPredicate for WHERE
# 3. Omit where to update all rows
