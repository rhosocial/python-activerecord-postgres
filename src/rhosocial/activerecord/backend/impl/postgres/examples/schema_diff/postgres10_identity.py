"""
Schema diff: PostgreSQL 9 vs 10+ — SERIAL vs GENERATED AS IDENTITY.

PostgreSQL 9: SERIAL creates a sequence + DEFAULT nextval().
PostgreSQL 10+: GENERATED AS IDENTITY is the SQL-standard way.

When comparing snapshots, a SERIAL column and an IDENTITY column
will appear differently in introspected metadata:
- SERIAL: default_value = 'nextval(''tab_col_seq''::regclass)'
- IDENTITY: is_auto_increment = True, special metadata

Supported versions: PostgreSQL 9 — SERIAL only.
                     PostgreSQL 10+ — GENERATED AS IDENTITY available.
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

drop_pg9 = DropTableExpression(dialect, "pg9_style", if_exists=True, cascade=True)
drop_pg10 = DropTableExpression(dialect, "pg10_style", if_exists=True, cascade=True)
sql, params = drop_pg9.to_sql()
backend.execute(sql, params)
sql, params = drop_pg10.to_sql()
backend.execute(sql, params)

# PostgreSQL 9-compatible: SERIAL shorthand
pg9_table = CreateTableExpression(
    dialect=dialect, table="pg9_style", columns=[
        ColumnDefinition("id", PostgresSerialType(),
            constraints=[ColumnConstraint(constraint_type=ColumnConstraintType.PRIMARY_KEY)]),
        ColumnDefinition("name", VarCharType(100)),
    ]
)
sql, params = pg9_table.to_sql()
backend.execute(sql, params)

# PostgreSQL 10+: GENERATED AS IDENTITY
# Note: dialect_options={'identity': 'ALWAYS'} triggers GENERATED ALWAYS AS IDENTITY
# in the PostgreSQL dialect's format_column_definition
try:
    pg10_table = CreateTableExpression(
        dialect=dialect, table="pg10_style", columns=[
            ColumnDefinition("id", IntegerType(),
                constraints=[ColumnConstraint(constraint_type=ColumnConstraintType.PRIMARY_KEY)],
                dialect_options={"identity": "ALWAYS"}),
            ColumnDefinition("name", VarCharType(100)),
        ]
    )
    sql, params = pg10_table.to_sql()
    backend.execute(sql, params)
except Exception as e:
    print(f"(GENERATED AS IDENTITY not available on this server: {e})")

# ============================================================
# SECTION: Business Logic (the pattern to learn)
# ============================================================
from rhosocial.activerecord.backend.schema import (  # noqa: E402
    SyncSchemaSnapshotBuilder,
)

builder = SyncSchemaSnapshotBuilder(backend.introspector, dialect)
snapshot = builder.build(schema="public")

# ============================================================
# SECTION: Execution (run the expression)
# ============================================================
for tbl_name in ("pg9_style", "pg10_style"):
    if tbl_name in snapshot.tables:
        tbl = snapshot.tables[tbl_name]
        for col in tbl.columns:
            if col.is_auto_increment or col.is_primary_key:
                print(f"{tbl_name}.{col.name}:")
                print(f"  data_type:       {col.data_type}")
                print(f"  is_auto_increment:{col.is_auto_increment}")
                print(f"  is_generated:    {col.is_generated}")
                print(f"  default_value:   {col.default_value}")
                print(f"  generated_expr:  {col.generated_expression}")

print()
print("Note: SERIAL vs IDENTITY columns produce different metadata.")
print("When diff-ing PG9 vs PG10+ snapshots, these differences will appear.")

# ============================================================
# SECTION: Teardown (necessary for execution, reference only)
# ============================================================
sql, params = drop_pg9.to_sql()
backend.execute(sql, params)
sql, params = drop_pg10.to_sql()
backend.execute(sql, params)
backend.disconnect()
