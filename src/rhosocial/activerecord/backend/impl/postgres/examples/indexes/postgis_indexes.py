# src/rhosocial/activerecord/backend/impl/postgres/examples/indexes/postgis_indexes.py
"""Spatial indexes for delivery zones — choose the access method on purpose.

GiST is the default for geometry; SP-GiST suits partitioned point sets and BRIN
suits very large, spatially clustered tables. All three are selected through
the shared ``PostgresIndexType`` vocabulary.
"""

# ============================================================
# SECTION: Setup (necessary for execution, reference only)
# ============================================================
import os

from rhosocial.activerecord.backend.impl.postgres.backend import PostgresBackend
from rhosocial.activerecord.backend.impl.postgres.config import PostgresConnectionConfig
from rhosocial.activerecord.backend.impl.postgres.expression import (
    PostgresCreateExtensionStatement,
    PostgresIndexType,
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
if dialect.is_extension_available("postgis") and not dialect.is_extension_installed("postgis"):
    backend.execute(*PostgresCreateExtensionStatement(
        dialect=dialect, name="postgis"
    ).to_sql())
    backend.introspect_and_adapt()

if dialect.supports_postgis_spatial_index():
    # The helper takes an index_type; the enum documents the allowed values.
    statements = []
    for method, name in (
        (PostgresIndexType.GIST, "idx_zones_geom_gist"),
        (PostgresIndexType.SPGIST, "idx_zones_geom_spgist"),
        (PostgresIndexType.BRIN, "idx_zones_geom_brin"),
    ):
        sql, params = dialect.format_spatial_index_statement(
            name, "delivery_zones", "geom", index_type=method.value
        )
        print(f"SQL: {sql}")
        statements.append((sql, params))
else:
    statements = []
    print("PostGIS spatial index is not available on this server")

# ============================================================
# SECTION: Execution (run the expressions)
# ============================================================
if statements:
    backend.execute(*DropTableStatement(
        dialect=dialect, table=Table(dialect, "delivery_zones"), if_exists=True
    ).to_sql())
    backend.execute(*CreateTableStatement(
        dialect=dialect,
        table=Table(dialect, "delivery_zones"),
        columns=[
            ColumnDefinition(dialect, "id", dialect.parse_type("SERIAL"), constraints=[
                ColumnConstraint(dialect, branch=ColumnPrimaryKeyConstraint(dialect)),
            ]),
            ColumnDefinition(dialect, "geom", dialect.parse_type("GEOMETRY")),
        ],
    ).to_sql())
    for sql, params in statements:
        backend.execute(sql, params)

# ============================================================
# SECTION: Teardown (necessary for execution, reference only)
# ============================================================
backend.disconnect()
