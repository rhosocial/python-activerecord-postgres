"""
Schema diff: PostgreSQL-specific index types (BTREE, GIN, GiST, HASH).

PostgreSQL supports multiple index access methods. The differ checks
index_type for equivalence, so switching from BTREE to GIN or GiST
will be reported as a removed+added index pair.

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
    VarCharType, ArrayType, IntegerType, TextType,
)
from rhosocial.activerecord.backend.impl.postgres.expression.types import (
    PostgresSerialType,
)
from rhosocial.activerecord.backend.expression.statements.ddl_index import (
    CreateIndexExpression, DropIndexExpression,
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
        ColumnDefinition("name", VarCharType(200)),
        ColumnDefinition("tags", ArrayType(TextType(), dimensions=1)),
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

# Snapshot with BTREE index
idx_btree = CreateIndexExpression(dialect, "idx_name", "demo", ["name"], index_type="BTREE")
sql, params = idx_btree.to_sql()
backend.execute(sql, params)
snap_btree = builder.build(schema="public")

# Drop BTREE, create GIN index on array column — different index type
drop_idx = DropIndexExpression(dialect, "idx_name", "demo", if_exists=True)
sql, params = drop_idx.to_sql()
backend.execute(sql, params)
idx_gin = CreateIndexExpression(dialect, "idx_tags", "demo", ["tags"], index_type="GIN")
sql, params = idx_gin.to_sql()
backend.execute(sql, params)
snap_gin = builder.build(schema="public")

differ = PostgresSchemaDiffer()
diff = differ.compare(snap_btree, snap_gin)

# ============================================================
# SECTION: Execution (run the expression)
# ============================================================
print(f"Modified tables: {diff.modified_tables}")

if "demo" in diff.table_diffs:
    td = diff.table_diffs["demo"]
    print(f"  Removed indexes: {[(idx.name, idx.index_type.value) for idx in td.removed_indexes]}")
    print(f"  Added indexes:   {[(idx.name, idx.index_type.value) for idx in td.added_indexes]}")

# ============================================================
# SECTION: Teardown (necessary for execution, reference only)
# ============================================================
sql, params = drop_demo.to_sql()
backend.execute(sql, params)
backend.disconnect()
