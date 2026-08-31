"""
Schema diff: basic table add/remove detection in PostgreSQL.

PostgreSQL uses multi-level namespaces (schema.table), and the
PostgresSchemaDiffer treats column order as semantically irrelevant.

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
    ColumnConstraint, ColumnConstraintType, TableConstraint, TableConstraintType,
)
from rhosocial.activerecord.backend.expression.types import (
    IntegerType, VarCharType, DecimalType,
)
from rhosocial.activerecord.backend.impl.postgres.expression.types import (
    PostgresSerialType,
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

drop_users = DropTableExpression(dialect, "users", if_exists=True, cascade=True)
drop_orders = DropTableExpression(dialect, "orders", if_exists=True, cascade=True)
sql, params = drop_users.to_sql()
backend.execute(sql, params)
sql, params = drop_orders.to_sql()
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

users_table = CreateTableExpression(
    dialect=dialect, table="users", columns=[
        ColumnDefinition("id", PostgresSerialType(),
            constraints=[ColumnConstraint(constraint_type=ColumnConstraintType.PRIMARY_KEY)]),
        ColumnDefinition("name", VarCharType(length=100),
            constraints=[ColumnConstraint(constraint_type=ColumnConstraintType.NOT_NULL)]),
    ]
)
sql, params = users_table.to_sql()
backend.execute(sql, params)

orders_table = CreateTableExpression(
    dialect=dialect, table="orders", columns=[
        ColumnDefinition("id", PostgresSerialType(),
            constraints=[ColumnConstraint(constraint_type=ColumnConstraintType.PRIMARY_KEY)]),
        ColumnDefinition("user_id", IntegerType()),
        ColumnDefinition("amount", DecimalType(precision=10, scale=2)),
    ],
    table_constraints=[
        TableConstraint(
            constraint_type=TableConstraintType.FOREIGN_KEY,
            columns=["user_id"],
            foreign_key_table="users",
            foreign_key_columns=["id"],
        )
    ]
)
sql, params = orders_table.to_sql()
backend.execute(sql, params)

snapshot_after = builder.build(schema="public")

differ = PostgresSchemaDiffer()
diff = differ.compare(snapshot_before, snapshot_after)

# ============================================================
# SECTION: Execution (run the expression)
# ============================================================
print(f"Added tables:   {diff.added_tables}")
print(f"Removed tables: {diff.removed_tables}")
print(f"Modified tables:{diff.modified_tables}")
print(f"Diff is empty:  {diff.is_empty}")

# ============================================================
# SECTION: Teardown (necessary for execution, reference only)
# ============================================================
sql, params = drop_users.to_sql()
backend.execute(sql, params)
sql, params = drop_orders.to_sql()
backend.execute(sql, params)
backend.disconnect()
