"""
Basic transaction control - PostgreSQL.

This example demonstrates:
1. Transaction commit and rollback
2. Using transaction context manager
3. Savepoints for nested transactions
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

dql_options = ExecutionOptions(stmt_type=StatementType.DQL)

create_table = CreateTableExpression(
    dialect=dialect,
    table_name='accounts',
    columns=[
        ColumnDefinition('id', 'SERIAL', constraints=[
            ColumnConstraint(ColumnConstraintType.PRIMARY_KEY),
        ]),
        ColumnDefinition('name', 'VARCHAR(100)', constraints=[
            ColumnConstraint(ColumnConstraintType.NOT_NULL),
        ]),
        ColumnDefinition('balance', 'NUMERIC(10,2)', default_value='0'),
    ],
    if_not_exists=True,
)
sql, params = create_table.to_sql()
backend.execute(sql, params)

# Insert initial data
insert = InsertExpression(
    dialect=dialect,
    into=TableExpression(dialect, 'accounts'),
    source=ValuesSource(dialect, [
        [Literal(dialect, 'Alice'), Literal(dialect, 1000.00)],
        [Literal(dialect, 'Bob'), Literal(dialect, 500.00)],
    ]),
    columns=['name', 'balance'],
)
sql, params = insert.to_sql()
backend.execute(sql, params)

# ============================================================
# SECTION: Business Logic (the pattern to learn)
# ============================================================

# 1. Transaction with commit
print("--- Transaction with commit ---")
with backend.transaction():
    from rhosocial.activerecord.backend.expression import UpdateExpression
    from rhosocial.activerecord.backend.expression.predicates import ComparisonPredicate
    from rhosocial.activerecord.backend.expression.core import Column

    update = UpdateExpression(
        dialect=dialect,
        table=TableExpression(dialect, 'accounts'),
        set_values={'balance': Literal(dialect, 1500.00)},
        where=ComparisonPredicate(
            dialect, '=',
            Column(dialect, 'name'),
            Literal(dialect, 'Alice'),
        ),
    )
    sql, params = update.to_sql()
    backend.execute(sql, params)
    print("Updated Alice's balance within transaction")

# Verify after commit
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
print(f"After commit: {result.data}")

# 2. Transaction with rollback
print("\n--- Transaction with rollback ---")
try:
    with backend.transaction():
        update = UpdateExpression(
            dialect=dialect,
            table=TableExpression(dialect, 'accounts'),
            set_values={'balance': Literal(dialect, 0)},
            where=ComparisonPredicate(
                dialect, '=',
                Column(dialect, 'name'),
                Literal(dialect, 'Bob'),
            ),
        )
        sql, params = update.to_sql()
        backend.execute(sql, params)
        print("Updated Bob's balance (will be rolled back)")
        raise ValueError("Simulated error - triggering rollback")
except ValueError:
    print("Transaction rolled back due to error")

# Verify rollback
query = QueryExpression(
    dialect=dialect,
    select=[Column(dialect, 'name'), Column(dialect, 'balance')],
    from_=TableExpression(dialect, 'accounts'),
    where=ComparisonPredicate(
        dialect, '=',
        Column(dialect, 'name'),
        Literal(dialect, 'Bob'),
    ),
)
sql, params = query.to_sql()
result = backend.execute(sql, params, options=dql_options)
print(f"After rollback: {result.data}")

# 3. Savepoints for nested transactions
print("\n--- Savepoints (nested transactions) ---")
with backend.transaction():
    # First operation
    update = UpdateExpression(
        dialect=dialect,
        table=TableExpression(dialect, 'accounts'),
        set_values={'balance': Literal(dialect, 2000.00)},
        where=ComparisonPredicate(
            dialect, '=',
            Column(dialect, 'name'),
            Literal(dialect, 'Alice'),
        ),
    )
    sql, params = update.to_sql()
    backend.execute(sql, params)

    # Create savepoint
    sp = backend.savepoint('before_bob_update')
    print(f"Savepoint created: {sp.name}")

    # Second operation (within savepoint)
    update = UpdateExpression(
        dialect=dialect,
        table=TableExpression(dialect, 'accounts'),
        set_values={'balance': Literal(dialect, 9999.00)},
        where=ComparisonPredicate(
            dialect, '=',
            Column(dialect, 'name'),
            Literal(dialect, 'Bob'),
        ),
    )
    sql, params = update.to_sql()
    backend.execute(sql, params)

    # Rollback to savepoint (undo only Bob's update)
    sp.rollback()
    print("Rolled back to savepoint (Bob's update undone)")

# Verify - Alice updated, Bob unchanged
query = QueryExpression(
    dialect=dialect,
    select=[Column(dialect, 'name'), Column(dialect, 'balance')],
    from_=TableExpression(dialect, 'accounts'),
    order_by=[Column(dialect, 'id')],
)
sql, params = query.to_sql()
result = backend.execute(sql, params, options=dql_options)
print(f"Final state: {result.data}")

# ============================================================
# SECTION: Teardown (necessary for execution, reference only)
# ============================================================
drop_table = DropTableExpression(dialect=dialect, table_name='accounts', if_exists=True)
sql, params = drop_table.to_sql()
backend.execute(sql, params)
backend.disconnect()

# ============================================================
# SECTION: Summary
# ============================================================
# Key points:
# 1. Use backend.transaction() context manager for automatic commit/rollback
# 2. Exceptions within the context cause automatic rollback
# 3. Use backend.savepoint(name) for nested transaction control
# 4. Savepoint.rollback() undoes operations after the savepoint
# 5. Outer transaction commits if no exception propagates
