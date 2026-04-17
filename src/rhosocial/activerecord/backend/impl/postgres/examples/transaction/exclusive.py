"""
Transaction Isolation Levels - PostgreSQL.

This example demonstrates:
1. READ COMMITTED isolation level (PostgreSQL default)
2. REPEATABLE READ isolation level
3. SERIALIZABLE isolation level
4. How to set isolation level on a transaction
"""

# ============================================================
# SECTION: Setup (necessary for execution, reference only)
# ============================================================
import os
from rhosocial.activerecord.backend.impl.postgres import PostgresBackend
from rhosocial.activerecord.backend.impl.postgres.config import PostgresConnectionConfig
from rhosocial.activerecord.backend.expression import (
    CreateTableExpression,
    DropTableExpression,
    InsertExpression,
    ValuesSource,
    QueryExpression,
    TableExpression,
    UpdateExpression,
)
from rhosocial.activerecord.backend.expression.core import Literal, Column
from rhosocial.activerecord.backend.expression.predicates import ComparisonPredicate
from rhosocial.activerecord.backend.expression.query_parts import OrderByClause
from rhosocial.activerecord.backend.expression.statements import (
    ColumnDefinition,
    ColumnConstraint,
    ColumnConstraintType,
)
from rhosocial.activerecord.backend.options import ExecutionOptions
from rhosocial.activerecord.backend.schema import StatementType
from rhosocial.activerecord.backend.transaction import IsolationLevel

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
        ColumnDefinition('balance', 'NUMERIC(10,2)'),
    ],
    if_not_exists=True,
)
sql, params = create_table.to_sql()
backend.execute(sql, params)

insert_expr = InsertExpression(
    dialect=dialect,
    into='accounts',
    columns=['name', 'balance'],
    source=ValuesSource(dialect, [
        [Literal(dialect, 'Alice'), Literal(dialect, 1000)],
        [Literal(dialect, 'Bob'), Literal(dialect, 500)],
    ]),
)
sql, params = insert_expr.to_sql()
backend.execute(sql, params)

# ============================================================
# SECTION: Business Logic (the pattern to learn)
# ============================================================

# 1. READ COMMITTED isolation level (PostgreSQL default)
# Prevents dirty reads: only committed data is visible.
# Non-repeatable reads and phantom reads can still occur
# because each statement sees a snapshot as of its start.
print("--- READ COMMITTED (PostgreSQL default) ---")
with backend.transaction_manager.transaction(isolation_level=IsolationLevel.READ_COMMITTED):
    # Read Alice's balance
    query = QueryExpression(
        dialect=dialect,
        select=[Column(dialect, 'name'), Column(dialect, 'balance')],
        from_=TableExpression(dialect, 'accounts'),
        where=ComparisonPredicate(
            dialect, '=',
            Column(dialect, 'name'),
            Literal(dialect, 'Alice'),
        ),
    )
    sql, params = query.to_sql()
    result = backend.execute(sql, params, options=dql_options)
    print(f"READ COMMITTED - Alice's balance: {result.data[0]['balance']}")

    # Update within the transaction
    update_expr = UpdateExpression(
        dialect=dialect,
        table=TableExpression(dialect, 'accounts'),
        assignments={'balance': Literal(dialect, 1100)},
        where=ComparisonPredicate(
            dialect, '=',
            Column(dialect, 'name'),
            Literal(dialect, 'Alice'),
        ),
    )
    sql, params = update_expr.to_sql()
    backend.execute(sql, params)

# After commit, the change is visible to all subsequent transactions
query = QueryExpression(
    dialect=dialect,
    select=[Column(dialect, 'name'), Column(dialect, 'balance')],
    from_=TableExpression(dialect, 'accounts'),
    where=ComparisonPredicate(
        dialect, '=',
        Column(dialect, 'name'),
        Literal(dialect, 'Alice'),
    ),
)
sql, params = query.to_sql()
result = backend.execute(sql, params, options=dql_options)
print(f"After commit - Alice's balance: {result.data[0]['balance']}")

# 2. REPEATABLE READ isolation level
# Prevents dirty reads and non-repeatable reads.
# All queries in the transaction see the same snapshot
# taken at the first query's start time.
# Phantom reads are possible in PostgreSQL (unlike MySQL).
print("\n--- REPEATABLE READ ---")
with backend.transaction_manager.transaction(isolation_level=IsolationLevel.REPEATABLE_READ):
    # First read
    query = QueryExpression(
        dialect=dialect,
        select=[Column(dialect, 'name'), Column(dialect, 'balance')],
        from_=TableExpression(dialect, 'accounts'),
        where=ComparisonPredicate(
            dialect, '=',
            Column(dialect, 'name'),
            Literal(dialect, 'Alice'),
        ),
    )
    sql, params = query.to_sql()
    result = backend.execute(sql, params, options=dql_options)
    first_read = result.data[0]['balance']

    # Second read in the same transaction returns the same result
    # even if another transaction committed changes in between
    sql, params = query.to_sql()
    result = backend.execute(sql, params, options=dql_options)
    second_read = result.data[0]['balance']

    print(f"First read: {first_read}, Second read: {second_read}")
    print(f"Consistent reads: {first_read == second_read}")

# 3. SERIALIZABLE isolation level
# The strictest level: prevents dirty reads, non-repeatable reads,
# and phantom reads. Transactions appear to execute sequentially.
# May raise serialization failures that require retry.
print("\n--- SERIALIZABLE ---")
with backend.transaction_manager.transaction(isolation_level=IsolationLevel.SERIALIZABLE):
    # Read all accounts
    query = QueryExpression(
        dialect=dialect,
        select=[Column(dialect, 'name'), Column(dialect, 'balance')],
        from_=TableExpression(dialect, 'accounts'),
        order_by=OrderByClause(dialect, [Column(dialect, 'id')]),
    )
    sql, params = query.to_sql()
    result = backend.execute(sql, params, options=dql_options)
    print(f"SERIALIZABLE - All accounts: {result.data}")

    # Update Bob's balance
    update_expr = UpdateExpression(
        dialect=dialect,
        table=TableExpression(dialect, 'accounts'),
        assignments={'balance': Literal(dialect, 600)},
        where=ComparisonPredicate(
            dialect, '=',
            Column(dialect, 'name'),
            Literal(dialect, 'Bob'),
        ),
    )
    sql, params = update_expr.to_sql()
    backend.execute(sql, params)

# 4. Setting isolation level per transaction
# The isolation_level parameter on transaction() sets the level
# for that specific transaction only, without affecting the
# default level of subsequent transactions.
print("\n--- Per-transaction isolation level ---")

# Transaction 1: SERIALIZABLE
with backend.transaction_manager.transaction(isolation_level=IsolationLevel.SERIALIZABLE):
    update_expr = UpdateExpression(
        dialect=dialect,
        table=TableExpression(dialect, 'accounts'),
        assignments={'balance': Literal(dialect, 1200)},
        where=ComparisonPredicate(
            dialect, '=',
            Column(dialect, 'name'),
            Literal(dialect, 'Alice'),
        ),
    )
    sql, params = update_expr.to_sql()
    backend.execute(sql, params)

# Transaction 2: defaults to READ COMMITTED (PostgreSQL default)
with backend.transaction():
    query = QueryExpression(
        dialect=dialect,
        select=[Column(dialect, 'name'), Column(dialect, 'balance')],
        from_=TableExpression(dialect, 'accounts'),
        order_by=OrderByClause(dialect, [Column(dialect, 'id')]),
    )
    sql, params = query.to_sql()
    result = backend.execute(sql, params, options=dql_options)
    print(f"Default isolation - All accounts: {result.data}")

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
# 1. Use backend.transaction_manager.transaction(isolation_level=...) to set isolation
# 2. READ COMMITTED: PostgreSQL default, prevents dirty reads only
# 3. REPEATABLE READ: prevents dirty and non-repeatable reads, snapshot-based
# 4. SERIALIZABLE: strictest level, may raise serialization failures
# 5. PostgreSQL REPEATABLE READ does NOT prevent phantom reads (unlike MySQL)
# 6. SERIALIZABLE in PostgreSQL uses predicate locking for true serializability
