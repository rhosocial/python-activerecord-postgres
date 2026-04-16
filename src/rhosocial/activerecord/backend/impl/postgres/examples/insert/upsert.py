"""
UPSERT (INSERT ... ON CONFLICT) - PostgreSQL 9.5+.

This example demonstrates:
1. INSERT ... ON CONFLICT DO NOTHING
2. INSERT ... ON CONFLICT DO UPDATE
3. UPSERT with conditional updates
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
from rhosocial.activerecord.backend.expression.core import Literal, Column
from rhosocial.activerecord.backend.expression.predicates import ComparisonPredicate
from rhosocial.activerecord.backend.expression.statements import (
    ColumnDefinition,
    ColumnConstraint,
    ColumnConstraintType,
    OnConflictClause,
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

drop_table = DropTableExpression(
    dialect=dialect,
    table_name='users',
    if_exists=True,
    cascade=True,
)
sql, params = drop_table.to_sql()
backend.execute(sql, params)

create_table = CreateTableExpression(
    dialect=dialect,
    table_name='users',
    columns=[
        ColumnDefinition(
            'id',
            'SERIAL',
            constraints=[
                ColumnConstraint(ColumnConstraintType.PRIMARY_KEY),
                ColumnConstraint(ColumnConstraintType.NOT_NULL),
            ],
        ),
        ColumnDefinition('username', 'VARCHAR(100)', constraints=[
            ColumnConstraint(ColumnConstraintType.UNIQUE),
            ColumnConstraint(ColumnConstraintType.NOT_NULL),
        ]),
        ColumnDefinition('email', 'VARCHAR(100)', constraints=[
            ColumnConstraint(ColumnConstraintType.NOT_NULL),
        ]),
        ColumnDefinition('login_count', 'INTEGER'),
    ],
    if_not_exists=True,
)
sql, params = create_table.to_sql()
backend.execute(sql, params)

# ============================================================
# SECTION: INSERT ON CONFLICT DO NOTHING
# ============================================================
# Ignore duplicate key violations

insert_nothing = InsertExpression(
    dialect=dialect,
    into='users',
    columns=['username', 'email', 'login_count'],
    source=ValuesSource(dialect, [
        [Literal(dialect, 'alice'), Literal(dialect, 'alice@example.com'), Literal(dialect, 1)],
    ]),
    on_conflict=OnConflictClause(
        dialect,
        [Column(dialect, 'username')],
        do_nothing=True,
    ),
)
sql, params = insert_nothing.to_sql()
print(f"DO NOTHING SQL: {sql}")
backend.execute(sql, params)

# Try to insert again - will be ignored
insert_nothing2 = InsertExpression(
    dialect=dialect,
    into='users',
    columns=['username', 'email', 'login_count'],
    source=ValuesSource(dialect, [
        [Literal(dialect, 'alice'), Literal(dialect, 'different@example.com'), Literal(dialect, 2)],
    ]),
    on_conflict=OnConflictClause(
        dialect,
        [Column(dialect, 'username')],
        do_nothing=True,
    ),
)
sql, params = insert_nothing2.to_sql()
backend.execute(sql, params)

query = QueryExpression(
    dialect=dialect,
    select=[Column(dialect, 'id'), Column(dialect, 'username'), Column(dialect, 'email')],
    from_=TableExpression(dialect, 'users'),
    where=ComparisonPredicate(dialect, '=', Column(dialect, 'username'), Literal(dialect, 'alice')),
)
sql, params = query.to_sql()
result = backend.execute(sql, params, options=dql_options)
print(f"DO NOTHING result: {result.data}")

# ============================================================
# SECTION: INSERT ON CONFLICT DO UPDATE
# ============================================================
# Update existing rows on conflict

insert_update = InsertExpression(
    dialect=dialect,
    into='users',
    columns=['username', 'email', 'login_count'],
    source=ValuesSource(dialect, [
        [Literal(dialect, 'bob'), Literal(dialect, 'bob@example.com'), Literal(dialect, 1)],
    ]),
    on_conflict=OnConflictClause(
        dialect,
        [Column(dialect, 'username')],
        update_assignments={
            'email': Column(dialect, 'email', table='EXCLUDED'),
            'login_count': Column(dialect, 'login_count', table='EXCLUDED'),
        },
    ),
)
sql, params = insert_update.to_sql()
print(f"DO UPDATE SQL: {sql}")
backend.execute(sql, params)

# Insert again - will update
insert_update2 = InsertExpression(
    dialect=dialect,
    into='users',
    columns=['username', 'email', 'login_count'],
    source=ValuesSource(dialect, [
        [Literal(dialect, 'bob'), Literal(dialect, 'bob_new@example.com'), Literal(dialect, 2)],
    ]),
    on_conflict=OnConflictClause(
        dialect,
        [Column(dialect, 'username')],
        update_assignments={
            'email': Column(dialect, 'email', table='EXCLUDED'),
            'login_count': Column(dialect, 'login_count', table='EXCLUDED'),
        },
    ),
)
sql, params = insert_update2.to_sql()
backend.execute(sql, params)

query = QueryExpression(
    dialect=dialect,
    select=[
        Column(dialect, 'id'), Column(dialect, 'username'),
        Column(dialect, 'email'), Column(dialect, 'login_count'),
    ],
    from_=TableExpression(dialect, 'users'),
    where=ComparisonPredicate(dialect, '=', Column(dialect, 'username'), Literal(dialect, 'bob')),
)
sql, params = query.to_sql()
result = backend.execute(sql, params, options=dql_options)
print(f"DO UPDATE result: {result.data}")

# ============================================================
# SECTION: Teardown (necessary for execution, reference only)
# ============================================================
drop_table = DropTableExpression(
    dialect=dialect,
    table_name='users',
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
# 1. Requires PostgreSQL 9.5+
# 2. Use OnConflictClause with do_nothing=True for DO NOTHING
# 3. Use OnConflictClause with update_assignments for DO UPDATE
# 4. conflict_target specifies the conflict columns (e.g., [Column(dialect, 'username')])
# 5. Use Column(dialect, 'col', table='EXCLUDED') to reference EXCLUDED values
