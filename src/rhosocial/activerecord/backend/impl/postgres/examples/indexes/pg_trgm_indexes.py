# src/rhosocial/activerecord/backend/impl/postgres/examples/indexes/pg_trgm_indexes.py
"""Fuzzy product search — GIN vs GiST trigram indexes with the right opclass.

``gin_trgm_ops`` belongs to GIN and ``gist_trgm_ops`` to GiST. The factory
encodes that pairing in an expression node; the dialect only renders nodes.
"""

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
from rhosocial.activerecord.backend.impl.postgres.functions import trgm_index  # noqa: E402

if dialect.is_extension_available("pg_trgm") and not dialect.is_extension_installed("pg_trgm"):
    backend.execute(*PostgresCreateExtensionStatement(
        dialect=dialect, name="pg_trgm"
    ).to_sql())
    backend.introspect_and_adapt()

expressions = []
if dialect.supports_pg_trgm_index():
    for method, name in (
        ("gin", "idx_products_title_gin"),
        ("gist", "idx_products_title_gist"),
    ):
        statement = trgm_index(dialect, "products", ["title"], name, index_type=method)
        print(f"SQL: {statement.to_sql()[0]}")
        expressions.append(statement)
else:
    print("pg_trgm index is not available on this server")

# ============================================================
# SECTION: Execution (run the expressions)
# ============================================================
if expressions:
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
    for statement in expressions:
        backend.execute(*statement.to_sql())

# ============================================================
# SECTION: Teardown (necessary for execution, reference only)
# ============================================================
backend.disconnect()
