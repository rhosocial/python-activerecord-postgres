"""
Schema diff: PostgreSQL array type dimension changes.

PostgresArrayType.is_equivalent() handles dimension comparison
internally. Column type changes like INTEGER[] → INTEGER[][]
are detected via parsed_data_type equivalence checks.

Supported versions: PostgreSQL 8.4+ (arrays available since PG 8.4)
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
    IntegerType, ArrayType,
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

drop_v1 = DropTableExpression(dialect, "scores_v1", if_exists=True, cascade=True)
drop_v2 = DropTableExpression(dialect, "scores_v2", if_exists=True, cascade=True)
sql, params = drop_v1.to_sql()
backend.execute(sql, params)
sql, params = drop_v2.to_sql()
backend.execute(sql, params)

# One-dimensional array column
scores_v1_table = CreateTableExpression(
    dialect=dialect, table="scores_v1", columns=[
        ColumnDefinition("id", PostgresSerialType(),
            constraints=[ColumnConstraint(constraint_type=ColumnConstraintType.PRIMARY_KEY)]),
        ColumnDefinition("values", ArrayType(IntegerType(), dimensions=1)),
    ]
)
sql, params = scores_v1_table.to_sql()
backend.execute(sql, params)

# Two-dimensional array column
scores_v2_table = CreateTableExpression(
    dialect=dialect, table="scores_v2", columns=[
        ColumnDefinition("id", PostgresSerialType(),
            constraints=[ColumnConstraint(constraint_type=ColumnConstraintType.PRIMARY_KEY)]),
        ColumnDefinition("values", ArrayType(IntegerType(), dimensions=2)),
    ]
)
sql, params = scores_v2_table.to_sql()
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
snapshot = builder.build(schema="public")

# ============================================================
# SECTION: Execution (run the expression)
# ============================================================
for tbl_name in ("scores_v1", "scores_v2"):
    if tbl_name in snapshot.tables:
        for col in snapshot.tables[tbl_name].columns:
            if col.name == "values":
                pt = col.parsed_data_type
                dims = pt.dimensions if hasattr(pt, "dimensions") else "N/A"
                print(f"{tbl_name}.values: data_type='{col.data_type}', dimensions={dims}")

# Manually construct two snapshots that differ only in array dimensions
from rhosocial.activerecord.backend.introspection.types import (  # noqa: E402
    TableInfo, ColumnInfo, ColumnNullable, TableType, DatabaseInfo,
)
from rhosocial.activerecord.backend.expression.types import IntegerType, ArrayType  # noqa: E402
from datetime import datetime, timezone  # noqa: E402

db_info = DatabaseInfo(name="test", version="16", version_tuple=(16, 0, 0), vendor="postgresql")

# V1: 1-d array
col1d = ColumnInfo(
    name="values", table_name="t", ordinal_position=1,
    data_type="INTEGER[]", parsed_data_type=ArrayType(IntegerType(), dimensions=1),
    nullable=ColumnNullable.NULLABLE,
)
snap1d = snapshot.__class__(
    dialect_class=snapshot.dialect_class,
    captured_at=datetime.now(tz=timezone.utc),
    database_info=db_info,
    tables={"t": TableInfo(name="t", columns=[col1d], table_type=TableType.BASE_TABLE)},
)

# V2: 2-d array
col2d = ColumnInfo(
    name="values", table_name="t", ordinal_position=1,
    data_type="INTEGER[][]", parsed_data_type=ArrayType(IntegerType(), dimensions=2),
    nullable=ColumnNullable.NULLABLE,
)
snap2d = snapshot.__class__(
    dialect_class=snapshot.dialect_class,
    captured_at=datetime.now(tz=timezone.utc),
    database_info=db_info,
    tables={"t": TableInfo(name="t", columns=[col2d], table_type=TableType.BASE_TABLE)},
)

differ = PostgresSchemaDiffer()
diff = differ.compare(snap1d, snap2d)
print("\nDiff 1-d vs 2-d array:")
print(f"  Modified tables: {diff.modified_tables}")
print(f"  Diff is empty:   {diff.is_empty}")

# ============================================================
# SECTION: Teardown (necessary for execution, reference only)
# ============================================================
sql, params = drop_v1.to_sql()
backend.execute(sql, params)
sql, params = drop_v2.to_sql()
backend.execute(sql, params)
backend.disconnect()
