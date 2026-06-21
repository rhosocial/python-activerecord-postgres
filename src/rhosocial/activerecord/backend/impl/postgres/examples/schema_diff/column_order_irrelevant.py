"""
Schema diff: PostgreSQL column order has no semantic meaning.

Unlike MySQL, PostgreSQL does not assign semantic significance to
column ordinal position. Reordering columns produces no diff because
PostgresSchemaDiffer only compares type, nullability, and default value.

Supported versions: PostgreSQL 9+
"""

# ============================================================
# SECTION: Setup (necessary for execution, reference only)
# ============================================================
import os
from rhosocial.activerecord.backend.impl.postgres import PostgresBackend
from rhosocial.activerecord.backend.impl.postgres.config import PostgresConnectionConfig
from rhosocial.activerecord.backend.expression import (
    CreateTableExpression, DropTableExpression, ColumnDefinition,
    ColumnConstraint, ColumnConstraintType,
)
from rhosocial.activerecord.backend.expression.types import (
    IntegerType, VarCharType,
)
from rhosocial.activerecord.backend.impl.postgres.expression.types import (
    PostgresSerialType,
)
from rhosocial.activerecord.backend.expression.statements.ddl_alter import (
    AlterTableExpression, AddColumn, DropColumn,
)

config = PostgresConnectionConfig(
    host=os.getenv("POSTGRES_HOST", "localhost"),
    port=int(os.getenv("POSTGRES_PORT", "5432")),
    database=os.getenv("POSTGRES_DATABASE", "test"),
    username=os.getenv("POSTGRES_USER", "postgres"),
    password=os.getenv("POSTGRES_PASSWORD", ""),
)
backend = PostgresBackend(connection_config=config)
backend.connect()
backend.introspect_and_adapt()
dialect = backend.dialect

drop_demo = DropTableExpression(dialect, "demo", if_exists=True, cascade=True)
sql, params = drop_demo.to_sql()
backend.execute(sql, params)

demo_table = CreateTableExpression(
    dialect=dialect, table="demo", columns=[
        ColumnDefinition("id", PostgresSerialType(),
            constraints=[ColumnConstraint(constraint_type=ColumnConstraintType.PRIMARY_KEY)]),
        ColumnDefinition("name", VarCharType(100)),
        ColumnDefinition("email", VarCharType(200)),
    ]
)
sql, params = demo_table.to_sql()
backend.execute(sql, params)

# ============================================================
# SECTION: Business Logic (the pattern to learn)
# ============================================================
from rhosocial.activerecord.backend.schema import (  # noqa: E402
    SyncSchemaSnapshotBuilder,
)
from rhosocial.activerecord.backend.impl.postgres.schema.differ import (  # noqa: E402
    PostgresSchemaDiffer,
)

builder = SyncSchemaSnapshotBuilder(backend.introspector, dialect)
snapshot_before = builder.build(schema="public")

# Add a column between name and email — shifts ordinal positions
# but PostgresSchemaDiffer ignores ordinal_position.
add_age = AlterTableExpression(dialect, "demo", [
    AddColumn(dialect, ColumnDefinition("age", IntegerType()))
])
sql, params = add_age.to_sql()
backend.execute(sql, params)

# Now drop it — column set is identical
drop_age = AlterTableExpression(dialect, "demo", [
    DropColumn(dialect, "age")
])
sql, params = drop_age.to_sql()
backend.execute(sql, params)

snapshot_after = builder.build(schema="public")

differ = PostgresSchemaDiffer()
diff = differ.compare(snapshot_before, snapshot_after)

# ============================================================
# SECTION: Execution (run the expression)
# ============================================================
print(f"Modified tables: {diff.modified_tables}")
print(f"Diff is empty:   {diff.is_empty}")
print("(No diff expected — column set unchanged regardless of position)")

# ============================================================
# SECTION: Teardown (necessary for execution, reference only)
# ============================================================
sql, params = drop_demo.to_sql()
backend.execute(sql, params)
backend.disconnect()
