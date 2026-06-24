"""
Schema diff: snapshot serialization roundtrip across sessions.

Serialized snapshots can be stored as JSON files and compared
later — useful for CI/CD pipeline schema validation.

Supported versions: PostgreSQL 9+
"""

# ============================================================
# SECTION: Setup (necessary for execution, reference only)
# ============================================================
import json
import os
from rhosocial.activerecord.backend.impl.postgres import PostgresBackend
from rhosocial.activerecord.backend.impl.postgres.config import PostgresConnectionConfig
from rhosocial.activerecord.backend.expression import (
    CreateTableExpression, DropTableExpression, ColumnDefinition,
    ColumnConstraint, ColumnConstraintType,
)
from rhosocial.activerecord.backend.expression.types import (
    VarCharType,
)
from rhosocial.activerecord.backend.impl.postgres.expression.types import (
    PostgresSerialType,
)
from rhosocial.activerecord.backend.expression.statements.ddl_alter import (
    AlterTableExpression, AddColumn,
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
sql, params = drop_users.to_sql()
backend.execute(sql, params)

users_table = CreateTableExpression(
    dialect=dialect, table="users", columns=[
        ColumnDefinition("id", PostgresSerialType(),
            constraints=[ColumnConstraint(constraint_type=ColumnConstraintType.PRIMARY_KEY)]),
        ColumnDefinition("name", VarCharType(100)),
        ColumnDefinition("email", VarCharType(200)),
    ]
)
sql, params = users_table.to_sql()
backend.execute(sql, params)

# ============================================================
# SECTION: Business Logic (the pattern to learn)
# ============================================================
from rhosocial.activerecord.backend.schema import (  # noqa: E402
    SchemaSnapshot,
    SyncSchemaSnapshotBuilder,
)
from rhosocial.activerecord.backend.impl.postgres.schema.differ import (  # noqa: E402
    PostgresSchemaDiffer,
)

builder = SyncSchemaSnapshotBuilder(backend.introspector, dialect)
snapshot_before = builder.build(schema="public")

# Serialize to JSON and deserialize back
snapshot_json = json.dumps(snapshot_before.to_dict(), default=str)
snapshot_loaded = SchemaSnapshot.from_dict(json.loads(snapshot_json))

# Modify the database
add_phone = AlterTableExpression(dialect, "users", [
    AddColumn(dialect, ColumnDefinition("phone", VarCharType(20)))
])
sql, params = add_phone.to_sql()
backend.execute(sql, params)

snapshot_after = builder.build(schema="public")

# Diff: loaded snapshot (before) vs current (after)
differ = PostgresSchemaDiffer()
diff = differ.compare(snapshot_loaded, snapshot_after)

# ============================================================
# SECTION: Execution (run the expression)
# ============================================================
print(f"Snapshot size (JSON):       {len(snapshot_json)} chars")
print(f"Roundtrip preserves dialect: {snapshot_loaded.dialect_class == snapshot_before.dialect_class}")
# Verify table data survived the roundtrip
assert snapshot_loaded.dialect_class == snapshot_before.dialect_class
assert list(snapshot_loaded.tables.keys()) == list(snapshot_before.tables.keys()), (
    f"table names mismatch: {list(snapshot_loaded.tables.keys())} "
    f"!= {list(snapshot_before.tables.keys())}"
)
for name in snapshot_before.tables:
    before_t = snapshot_before.tables[name]
    after_t = snapshot_loaded.tables[name]
    assert [c.name for c in before_t.columns] == [c.name for c in after_t.columns], (
        f"columns mismatch in table '{name}'"
    )
    for col_b, col_a in zip(before_t.columns, after_t.columns):
        assert col_b.data_type.__class__.__name__ == col_a.data_type.__class__.__name__, (
            f"type mismatch in {name}.{col_b.name}"
        )
print(f"Table data roundtrip:        all columns and types match")
print(f"Added tables:               {diff.added_tables}")
print(f"Removed tables:             {diff.removed_tables}")
print(f"Modified tables:            {diff.modified_tables}")

if "users" in diff.table_diffs:
    td = diff.table_diffs["users"]
    db_info = snapshot_after.database_info
    print(f"Database version:           {db_info.version} (vendor={db_info.vendor})")
    for cd in td.column_diffs:
        print(f"  Column '{cd.column_name}': added")

# ============================================================
# SECTION: Teardown (necessary for execution, reference only)
# ============================================================
sql, params = drop_users.to_sql()
backend.execute(sql, params)
backend.disconnect()
