# src/rhosocial/activerecord/backend/impl/postgres/examples/indexes/pgvector_indexes.py
"""Vector search with the right access method — a document-retrieval scenario.

Cosine + HNSW for approximate nearest neighbours, L2 + IVFFLAT for a large
corpus, and the ``PostgresIndexType`` enum values used by both.
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
from rhosocial.activerecord.backend.impl.postgres.functions import (  # noqa: E402
    create_vector_index,
    vector_search,
)

print("vector extension:",
      "available =", dialect.is_extension_available("vector"),
      "installed =", dialect.is_extension_installed("vector"))

if dialect.is_extension_available("vector") and not dialect.is_extension_installed("vector"):
    backend.execute(*PostgresCreateExtensionStatement(
        dialect=dialect, name="vector"
    ).to_sql())
    backend.introspect_and_adapt()

if dialect.is_extension_installed("vector"):
    # 1. Cosine search: 1 - cosine distance is a meaningful similarity.
    query = vector_search(
        dialect,
        "documents",
        [0.1, 0.2, 0.9],
        metric="cosine",
        top_k=5,
        columns=["content"],
        include_similarity=True,
    )

    # 2. HNSW index — requires pgvector 0.5.0+, checked by the capability probe.
    if dialect.supports_pgvector_hnsw_index():
        hnsw = create_vector_index(
            dialect, "documents", "embedding",
            metric="cosine",
            index_type=PostgresIndexType.HNSW.value,
            m=16,
            ef_construction=64,
        )
    else:
        hnsw = None

    # 3. IVFFLAT index — build only after the table has data.
    ivfflat = create_vector_index(
        dialect, "documents", "embedding",
        metric="l2",
        index_type=PostgresIndexType.IVFFLAT.value,
        lists=4,
    )

# ============================================================
# SECTION: Execution (run the expressions)
# ============================================================
if dialect.is_extension_installed("vector"):
    backend.execute(*DropTableStatement(
        dialect=dialect, table=Table(dialect, "documents"), if_exists=True
    ).to_sql())
    backend.execute(*CreateTableStatement(
        dialect=dialect,
        table=Table(dialect, "documents"),
        columns=[
            ColumnDefinition(dialect, "id", dialect.parse_type("SERIAL"), constraints=[
                ColumnConstraint(dialect, branch=ColumnPrimaryKeyConstraint(dialect)),
            ]),
            ColumnDefinition(dialect, "content", dialect.parse_type("TEXT")),
            ColumnDefinition(dialect, "embedding", dialect.parse_type("VECTOR(3)")),
        ],
    ).to_sql())

    for statement in (hnsw, ivfflat):
        if statement is not None:
            sql, params = statement.to_sql()
            print(f"SQL: {sql}")
            backend.execute(sql, params)

    sql, params = query.to_sql()
    print(f"QUERY: {sql}")
    print(f"Params: {params}")

# ============================================================
# SECTION: Teardown (necessary for execution, reference only)
# ============================================================
backend.disconnect()
