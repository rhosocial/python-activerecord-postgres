# src/rhosocial/activerecord/backend/impl/postgres/examples/indexes/pg_trgm_indexes.py
"""Fuzzy product search — GIN vs GiST trigram indexes with the right opclass.

``gin_trgm_ops`` belongs to GIN and ``gist_trgm_ops`` to GiST; the helper now
chooses the operator class from the access method instead of hard-coding one.
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
if dialect.is_extension_available("pg_trgm") and not dialect.is_extension_installed("pg_trgm"):
    backend.execute(*PostgresCreateExtensionStatement(
        dialect=dialect, name="pg_trgm"
    ).to_sql())
    backend.introspect_and_adapt()

if dialect.supports_pg_trgm_index():
    gin_sql, gin_params = dialect.format_trgm_index_statement(
        "idx_products_title_gin", "products", "title",
        index_type=PostgresIndexType.GIN.value,
    )
    gist_sql, gist_params = dialect.format_trgm_index_statement(
        "idx_products_title_gist", "products", "title",
        index_type=PostgresIndexType.GIST.value,
    )
    print(f"SQL: {gin_sql}")
    print(f"SQL: {gist_sql}")
else:
    gin_sql = gist_sql = None
    gin_params = gist_params = ()
    print("pg_trgm index is not available on this server")

# ============================================================
# SECTION: Execution (run the expressions)
# ============================================================
if gin_sql:
    backend.execute(*DropTableStatement(
        dialect=dialect, table=Table(dialect, "products"), if_exists=True
    ).to_sql())
    backend.execute(*CreateTableStatement(
        dialect=dialect,
        table=Table(dialect, "products"),
        columns=[
            ColumnDefinition(dialect, "id", dialect.parse_type("SERIAL"), constraints=[
                ColumnConstraint(dialect, branch=ColumnPrimaryKeyConstraint(dialect)),
            ]),
            ColumnDefinition(dialect, "title", dialect.parse_type("TEXT")),
        ],
    ).to_sql())
    backend.execute(gin_sql, gin_params)
    backend.execute(gist_sql, gist_params)

# ============================================================
# SECTION: Teardown (necessary for execution, reference only)
# ============================================================
backend.disconnect()
