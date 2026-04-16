"""
FOR UPDATE Row Locking - PostgreSQL.

This example demonstrates:
1. SELECT ... FOR UPDATE to lock rows
2. Preventing dirty reads in concurrent scenarios
3. Using SKIP LOCKED for non-blocking locks
4. NOWAIT for immediate failure on lock
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
    QueryExpression,
    TableExpression,
    UpdateExpression,
)
from rhosocial.activerecord.backend.expression.core import Literal, Column
from rhosocial.activerecord.backend.expression.predicates import ComparisonPredicate
from rhosocial.activerecord.backend.expression.query_parts import ForUpdateClause
from rhosocial.activerecord.backend.expression.statements import (
    ColumnDefinition,
    ColumnConstraint,
    ColumnConstraintType,
)
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

dql_options = ExecutionOptions(stmt_type=StatementType.DQL)
dml_options = ExecutionOptions(stmt_type=StatementType.DML)

drop_table = DropTableExpression(
    dialect=dialect,
    table_name='accounts',
    if_exists=True,
    cascade=True,
)
sql, params = drop_table.to_sql()
backend.execute(sql, params)

create_table = CreateTableExpression(
    dialect=dialect,
    table_name='accounts',
    columns=[
        ColumnDefinition(
            'id',
            'SERIAL',
            constraints=[
                ColumnConstraint(ColumnConstraintType.PRIMARY_KEY),
                ColumnConstraint(ColumnConstraintType.NOT_NULL),
            ],
        ),
        ColumnDefinition('name', 'VARCHAR(100)', constraints=[
            ColumnConstraint(ColumnConstraintType.NOT_NULL),
        ]),
        ColumnDefinition('balance', 'DECIMAL(10,2)', default=Literal(dialect, '0')),
    ],
    if_not_exists=True,
)
sql, params = create_table.to_sql()
backend.execute(sql, params)

insert_expr = InsertExpression(
    dialect=dialect,
    into='accounts',
    columns=['name', 'balance'],
    source=ValuesSource(
        dialect,
        [
            [Literal(dialect, 'Alice'), Literal(dialect, '1000')],
            [Literal(dialect, 'Bob'), Literal(dialect, '500')],
        ],
    ),
)
sql, params = insert_expr.to_sql()
backend.execute(sql, params)

# ============================================================
# SECTION: SELECT FOR UPDATE
# ============================================================
# Lock rows to prevent concurrent modifications

with backend.transaction():
    # Lock Alice's row using ForUpdateClause
    lock_query = QueryExpression(
        dialect=dialect,
        select=[Column(dialect, 'id'), Column(dialect, 'name'), Column(dialect, 'balance')],
        from_=TableExpression(dialect, 'accounts'),
        where=ComparisonPredicate(dialect, '=', Column(dialect, 'name'), Literal(dialect, 'Alice')),
        for_update=ForUpdateClause(dialect),
    )
    sql, params = lock_query.to_sql()
    result = backend.execute(sql, params, options=dql_options)
    print(f"Locked row: {result.data}")

    # Update the balance
    update_expr = UpdateExpression(
        dialect=dialect,
        table='accounts',
        assignments={'balance': Literal(dialect, 900)},
        where=ComparisonPredicate(dialect, '=', Column(dialect, 'name'), Literal(dialect, 'Alice')),
    )
    sql, params = update_expr.to_sql()
    backend.execute(sql, params, options=dml_options)

# The lock is released after commit

# ============================================================
# SECTION: FOR UPDATE with WHERE Conditions
# ============================================================
# Lock specific rows based on conditions

with backend.transaction():
    lock_query = QueryExpression(
        dialect=dialect,
        select=[Column(dialect, 'id'), Column(dialect, 'name'), Column(dialect, 'balance')],
        from_=TableExpression(dialect, 'accounts'),
        where=ComparisonPredicate(dialect, '>', Column(dialect, 'balance'), Literal(dialect, 500)),
        for_update=ForUpdateClause(dialect),
    )
    sql, params = lock_query.to_sql()
    result = backend.execute(sql, params, options=dql_options)
    print(f"Locked high balance accounts: {len(result.data)} rows")

# ============================================================
# SECTION: SKIP LOCKED (PostgreSQL 8.4+)
# ============================================================
# Skip locked rows instead of waiting

skip_query = QueryExpression(
    dialect=dialect,
    select=[Column(dialect, 'id'), Column(dialect, 'name'), Column(dialect, 'balance')],
    from_=TableExpression(dialect, 'accounts'),
    for_update=ForUpdateClause(dialect, skip_locked=True),
)
sql, params = skip_query.to_sql()
print(f"SKIP LOCKED SQL: {sql}")
result = backend.execute(sql, params, options=dql_options)
print(f"SKIP LOCKED result: {result.data}")

# ============================================================
# SECTION: NOWAIT
# ============================================================
# Fail immediately if rows are locked

try:
    nowait_query = QueryExpression(
        dialect=dialect,
        select=[Column(dialect, 'id'), Column(dialect, 'name'), Column(dialect, 'balance')],
        from_=TableExpression(dialect, 'accounts'),
        where=ComparisonPredicate(dialect, '=', Column(dialect, 'name'), Literal(dialect, 'Alice')),
        for_update=ForUpdateClause(dialect, nowait=True),
    )
    sql, params = nowait_query.to_sql()
    result = backend.execute(sql, params, options=dql_options)
    print(f"NOWAIT result: {result.data}")
except Exception as e:
    print(f"NOWAIT failed (expected if locked): {e}")

# ============================================================
# SECTION: Lock Modes
# ============================================================
# FOR UPDATE - exclusive lock (write lock)
# FOR UPDATE OF table_name - lock specific table
# FOR SHARE - shared lock (read lock)
# FOR KEY SHARE - for foreign key detection

# Note: FOR SHARE uses ForUpdateClause with different lock mode.
# Currently Expression API provides ForUpdateClause for FOR UPDATE.
# FOR SHARE requires raw SQL or extension of ForUpdateClause.

with backend.transaction():
    # Shared lock - allows other transactions to also read
    result = backend.execute(
        "SELECT * FROM accounts FOR SHARE"
    )
    print(f"FOR SHARE result: {len(result.data)} rows")

# ============================================================
# SECTION: Teardown (necessary for execution, reference only)
# ============================================================
drop_table = DropTableExpression(
    dialect=dialect,
    table_name='accounts',
    if_exists=True,
    cascade=True,
)
sql, params = drop_table.to_sql()
backend.execute(sql, params)
backend.disconnect()

# ============================================================
# SECTION: Summary
# ============================================================
# Key points:
# 1. Use ForUpdateClause with QueryExpression for SELECT ... FOR UPDATE
# 2. ForUpdateClause(dialect, skip_locked=True) for SKIP LOCKED
# 3. ForUpdateClause(dialect, nowait=True) for NOWAIT
# 4. FOR SHARE requires raw SQL (Expression API provides FOR UPDATE only)
# 5. Locks released on COMMIT/ROLLBACK
