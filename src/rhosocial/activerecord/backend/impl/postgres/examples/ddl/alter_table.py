"""
Alter table statements - ADD COLUMN, ALTER COLUMN.

Note: PostgreSQL supports multiple actions in a single ALTER TABLE statement.
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
    host=os.getenv('POSTGRES_HOST', 'localhost'),
    port=int(os.getenv('POSTGRES_PORT', '5432')),
    database=os.getenv('POSTGRES_DATABASE', 'test'),
    username=os.getenv('POSTGRES_USER', 'postgres'),
    password=os.getenv('POSTGRES_PASSWORD', ''),
)
backend = PostgresBackend(connection_config=config)
backend.connect()
dialect = backend.dialect

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
        ColumnDefinition('name', 'VARCHAR(100)'),
    ],
)
backend.execute(*create_table.to_sql())

insert_expr = InsertExpression(
    dialect=dialect,
    into='users',
    columns=['name'],
    source=ValuesSource(
        dialect,
        [[Literal(dialect, 'Alice')]],
    ),
)
backend.execute(*insert_expr.to_sql())

# ============================================================
# SECTION: Business Logic (the pattern to learn)
# ============================================================
from rhosocial.activerecord.backend.expression import (
    AlterTableExpression,
    ColumnDefinition,
)
from rhosocial.activerecord.backend.expression.statements.ddl_alter import (
    AddColumn,
    RenameColumn,
)

# Add a new column
add_email_action = AddColumn(
    column=ColumnDefinition(
        name='email',
        data_type='VARCHAR(100)',
    ),
)

add_email_expr = AlterTableExpression(
    dialect=dialect,
    table_name='users',
    actions=[add_email_action],
)

sql, params = add_email_expr.to_sql()
print(f"SQL (Add Column): {sql}")
print(f"Params: {params}")

# ============================================================
# SECTION: Execution (run the expression)
# ============================================================
backend.execute(sql, params)
print("Column added successfully")

# Add another column without default (PostgreSQL ALTER TABLE doesn't support parameterized DEFAULT well)
add_age_action = AddColumn(
    column=ColumnDefinition(
        'age',
        'INT',
    ),
)

add_age_expr = AlterTableExpression(
    dialect=dialect,
    table_name='users',
    actions=[add_age_action],
)

sql, params = add_age_expr.to_sql()
backend.execute(sql, params)
print("Column age added successfully")

# Rename a column
rename_action = RenameColumn(
    old_name='name',
    new_name='full_name',
)

rename_expr = AlterTableExpression(
    dialect=dialect,
    table_name='users',
    actions=[rename_action],
)

sql, params = rename_expr.to_sql()
print(f"SQL (Rename Column): {sql}")
backend.execute(sql, params)
print("Column renamed successfully")

# Verify table structure
options = ExecutionOptions(stmt_type=StatementType.DQL)
result = backend.execute(
    "SELECT column_name, data_type, is_nullable "
    "FROM information_schema.columns "
    "WHERE table_name = 'users' ORDER BY ordinal_position",
    options=options
)
print("Table structure after alterations:")
for row in result.data or []:
    print(f" {row}")

# ============================================================
# SECTION: Teardown (necessary for execution, reference only)
# ============================================================
backend.disconnect()
