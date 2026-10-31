"""
Batch insert using INSERT with multiple VALUES.
"""

# ============================================================
# SECTION: Setup (necessary for execution, reference only)
# ============================================================
import os
from rhosocial.activerecord.backend.impl.postgres.backend import PostgresBackend
from rhosocial.activerecord.backend.impl.postgres.config import PostgresConnectionConfig
from rhosocial.activerecord.backend.expression import (
    CreateTableStatement,
    DropTableStatement,
    ColumnNotNullConstraint,
    ColumnPrimaryKeyConstraint,
)
from rhosocial.activerecord.backend.expression.statements import (
    ColumnDefinition,
    ColumnConstraint,
)
from rhosocial.activerecord.backend.options import ExecutionOptions
from rhosocial.activerecord.backend.schema import StatementType

config = PostgresConnectionConfig(
    host=os.getenv('POSTGRES_HOST', 'localhost'),
    port=int(os.getenv('POSTGRES_PORT', '5432')),
    database=os.getenv('POSTGRES_DATABASE', 'test'),
    username=os.getenv('POSTGRES_USER', 'postgres'),
    password=os.getenv('POSTGRES_PASSWORD', ''),
)
backend = PostgresBackend(connection_config=config)
backend.connect()
dialect = backend.dialect

drop_table = DropTableStatement(
    dialect=dialect,
    table=Table(dialect, 'logs'),
    if_exists=True,
    cascade=True,
)
sql, params = drop_table.to_sql()
backend.execute(sql, params)

create_table = CreateTableStatement(
    dialect=dialect,
    table=Table(dialect, 'logs'),
    columns=[
        ColumnDefinition(
            'id',
            'SERIAL',
            constraints=[
                ColumnConstraint(dialect, branch=ColumnPrimaryKeyConstraint(dialect)),
                ColumnConstraint(dialect, branch=ColumnNotNullConstraint(dialect)),
            ],
        ),
        ColumnDefinition('level', 'VARCHAR(20)'),
        ColumnDefinition('message', 'TEXT'),
        ColumnDefinition('created_at', 'TIMESTAMP'),
    ],
)
backend.execute(*create_table.to_sql())

# ============================================================
# SECTION: Business Logic (the pattern to learn)
# ============================================================
from rhosocial.activerecord.backend.expression import (
    InsertStatement, ValuesSource, QueryStatement,
)
from rhosocial.activerecord.backend.expression.objects import Table
from rhosocial.activerecord.backend.expression.core import Literal, AllColumnsWildcard, Column
from rhosocial.activerecord.backend.expression.query_parts import OrderByClause

insert = InsertStatement(
    dialect=dialect,
    into=Table(dialect, 'logs'),
    columns=['level', 'message'],
    source=ValuesSource(
        dialect,
        [
            [Literal(dialect, 'INFO'), Literal(dialect, 'System started')],
            [Literal(dialect, 'DEBUG'), Literal(dialect, 'Loading configuration')],
            [Literal(dialect, 'INFO'), Literal(dialect, 'Application ready')],
            [Literal(dialect, 'WARNING'), Literal(dialect, 'Memory usage high')],
        ],
    ),
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
verify_query = QueryStatement(
    dialect=dialect,
    select=[AllColumnsWildcard(dialect)],
    from_=Table(dialect, 'logs'),
    order_by=OrderByClause(dialect, [Column(dialect, 'id')]),
)
options = ExecutionOptions(stmt_type=StatementType.DQL)
sql, params = verify_query.to_sql()
result = backend.execute(sql, params, options=options)
print(f"Rows inserted: {len(result.data) if result.data else 0}")
for row in result.data or []:
    print(f"  {row}")

# ============================================================
# SECTION: Teardown (necessary for execution, reference only)
# ============================================================
backend.disconnect()
