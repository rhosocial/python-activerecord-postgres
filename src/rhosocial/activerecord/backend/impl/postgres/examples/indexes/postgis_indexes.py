# src/rhosocial/activerecord/backend/impl/postgres/examples/indexes/postgis_indexes.py
"""Spatial indexes for delivery zones — choose the access method on purpose.

GiST is the default for geometry; SP-GiST suits partitioned point sets and BRIN
suits very large, spatially clustered tables. The factory returns an expression
node; the dialect renders it through its one ``format_create_index_statement``.
"""

from rhosocial.activerecord.backend.impl.postgres.expression.types import PostgresGeometryType, PostgresSerialType


# ============================================================
# SECTION: Setup (necessary for execution, reference only)
# ============================================================
import os

from rhosocial.activerecord.backend.impl.postgres.backend import PostgresBackend
from rhosocial.activerecord.backend.impl.postgres.config import PostgresConnectionConfig
from rhosocial.activerecord.backend.impl.postgres.expression import (
    PostgresCreateExtensionStatement,
)
from rhosocial.activerecord.backend.expression import (
    ColumnConstraint,
    ColumnDefinition,
    ColumnPrimaryKeyConstraint,
    CreateTableStatement,
    DropTableStatement,
)
from rhosocial.activerecord.backend.expression.objects import Table  # noqa: E402

config = PostgresConnectionConfig(
    host=os.getenv("PG_HOST", "localhost"),
    port=int(os.getenv("PG_PORT", "5432")),
    database=os.getenv("PG_DATABASE", "test"),
    username=os.getenv("PG_USERNAME", "postgres"),
    password=os.getenv("PG_PASSWORD", ""),
    sslmode=os.getenv("PG_SSLMODE"),
    sslrootcert=os.getenv("PG_SSLROOTCERT"),
)
backend = PostgresBackend(connection_config=config)
backend.connect()
backend.introspect_and_adapt()
dialect = backend.dialect

# ============================================================
# SECTION: Business Logic (the pattern to learn)
# ============================================================
from rhosocial.activerecord.backend.impl.postgres.functions import (  # noqa: E402
    spatial_index,
)

if dialect.is_extension_available("postgis") and not dialect.is_extension_installed("postgis"):
    backend.execute(*PostgresCreateExtensionStatement(
        dialect=dialect, name="postgis"
    ).to_sql())
    backend.introspect_and_adapt()

expressions = []
if dialect.supports_postgis_spatial_index():
    for method, name in (
        ("gist", "idx_zones_geom_gist"),
        ("spgist", "idx_zones_geom_spgist"),
        ("brin", "idx_zones_geom_brin"),
    ):
        statement = spatial_index(
            dialect, "delivery_zones", ["geom"], name, index_type=method
        )
        print(f"SQL: {statement.to_sql()[0]}")
        expressions.append(statement)
else:
    print("PostGIS spatial index is not available on this server")

# ============================================================
# SECTION: Execution (run the expressions)
# ============================================================
if expressions:
    backend.execute(*DropTableStatement(
        dialect=dialect, table=Table(dialect, "delivery_zones"), if_exists=True
    ).to_sql())
    backend.execute(*CreateTableStatement(
        dialect=dialect,
        table=Table(dialect, "delivery_zones"),
        columns=[
            ColumnDefinition(dialect, "id", PostgresSerialType(), constraints=[
                ColumnConstraint(dialect, branch=ColumnPrimaryKeyConstraint(dialect)),
            ]),
            ColumnDefinition(dialect, "geom", PostgresGeometryType()),
        ],
    ).to_sql())
    for statement in expressions:
        backend.execute(*statement.to_sql())

# ============================================================
# SECTION: Teardown (necessary for execution, reference only)
# ============================================================
backend.disconnect()
