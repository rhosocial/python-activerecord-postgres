# src/rhosocial/activerecord/backend/impl/postgres/examples/extensions/pgvector.py
"""
pgvector extension - vector similarity search.

This example demonstrates:
1. Check if vector extension is available
2. CREATE EXTENSION and create table with vector column
3. Insert vector data
4. Execute cosine similarity search
5. Create HNSW index for fast vector search
"""

# ============================================================
# SECTION: Setup (necessary for execution, reference only)
# ============================================================
import os
from rhosocial.activerecord.backend.impl.postgres import PostgresBackend
from rhosocial.activerecord.backend.impl.postgres.config import (
    PostgresConnectionConfig,
)

config = PostgresConnectionConfig(
    host=os.getenv("PG_HOST", "localhost"),
    port=int(os.getenv("PG_PORT", "5432")),
    database=os.getenv("PG_DATABASE", "test"),
    username=os.getenv("PG_USERNAME", "postgres"),
    password=os.getenv("PG_PASSWORD", ""),
)
backend = PostgresBackend(connection_config=config)
backend.connect()
backend.introspect_and_adapt()
dialect = backend.dialect

# Clean up for demo
backend.execute("DROP TABLE IF EXISTS documents", ())

# ============================================================
# SECTION: Business Logic (the pattern to learn)
# ============================================================
from rhosocial.activerecord.backend.impl.postgres.expression import (
    CreateExtensionExpression,
)
from rhosocial.activerecord.backend.expression import (
    CreateTableExpression,
    ColumnDefinition,
    ColumnConstraint,
    ColumnConstraintType,
    CreateIndexExpression,
)
from rhosocial.activerecord.backend.expression.core import Literal
from rhosocial.activerecord.backend.expression.statements.dml import (
    InsertExpression,
)
from rhosocial.activerecord.backend.expression.statements import (
    ValuesSource,
)
from rhosocial.activerecord.backend.expression import (
    Column,
    QueryExpression,
    TableExpression,
)
from rhosocial.activerecord.backend.options import ExecutionOptions
from rhosocial.activerecord.backend.schema import StatementType

# Check if vector extension is available
available = dialect.is_extension_available("vector")
installed = dialect.is_extension_installed("vector")
print(f"Extension check: vector available = {available}, installed = {installed}")

# Create extension using expression
if available and not installed:
    create_ext = CreateExtensionExpression(
        dialect=dialect,
        name="vector",
    )
    sql, params = create_ext.to_sql()
    print(f"\n--- CREATE EXTENSION ---")
    print(f"SQL: {sql}")
    backend.execute(sql, params)
    # Re-detect extensions after creation
    backend.introspect_and_adapt()

# Re-check after creation
installed = dialect.is_extension_installed("vector")

if installed:
    # Example 1: Create table with vector column
    columns = [
        ColumnDefinition(
            name="id",
            data_type="SERIAL",
            constraints=[
                ColumnConstraint(ColumnConstraintType.PRIMARY_KEY),
            ],
        ),
        ColumnDefinition(
            name="content",
            data_type="TEXT",
            constraints=[
                ColumnConstraint(ColumnConstraintType.NOT_NULL),
            ],
        ),
        ColumnDefinition(
            name="embedding",
            data_type="VECTOR(3)",
        ),
    ]

    create_expr = CreateTableExpression(
        dialect=dialect,
        table_name="documents",
        columns=columns,
        if_not_exists=True,
    )
    sql, params = create_expr.to_sql()
    print(f"\n--- CREATE TABLE ---")
    print(f"SQL: {sql}")
    backend.execute(sql, params)

    # Example 2: Insert vector data
    # Using literal string representation for vectors
    insert_expr = InsertExpression(
        dialect=dialect,
        into="documents",
        columns=["content", "embedding"],
        source=ValuesSource(
            dialect,
            [
                [
                    Literal(dialect, "cat"),
                    Literal(dialect, "[1.0, 0.5, 0.2]"),
                ],
                [
                    Literal(dialect, "dog"),
                    Literal(dialect, "[0.9, 0.6, 0.3]"),
                ],
                [
                    Literal(dialect, "car"),
                    Literal(dialect, "[0.1, 0.2, 0.9]"),
                ],
                [
                    Literal(dialect, "bicycle"),
                    Literal(dialect, "[0.15, 0.25, 0.85]"),
                ],
            ],
        ),
    )
    sql, params = insert_expr.to_sql()
    print(f"\n--- INSERT vector data ---")
    print(f"SQL: {sql}")
    print(f"Params: {params}")
    backend.execute(sql, params)

    # Example 3: Cosine similarity search
    # Find documents most similar to "kitten" represented as [1.0, 0.55, 0.18]
    opts = ExecutionOptions(stmt_type=StatementType.DQL)
    result = backend.execute(
        "SELECT content, 1 - (embedding <=> '[1.0, 0.55, 0.18]') AS cosine_similarity "
        "FROM documents "
        "ORDER BY embedding <=> '[1.0, 0.55, 0.18]' "
        "LIMIT 3",
        (),
        options=opts,
    )
    print(f"\n--- Cosine similarity search ---")
    print(f"Query vector: [1.0, 0.55, 0.18] (similar to 'cat')")
    print(f"Results: {result.data}")

    # Example 4: L2 distance search
    result = backend.execute(
        "SELECT content, embedding <-> '[0.1, 0.2, 0.9]' AS l2_distance "
        "FROM documents "
        "ORDER BY embedding <-> '[0.1, 0.2, 0.9]' "
        "LIMIT 3",
        (),
        options=opts,
    )
    print(f"\n--- L2 distance search ---")
    print(f"Query vector: [0.1, 0.2, 0.9] (similar to 'car')")
    print(f"Results: {result.data}")

    # Example 5: Create HNSW index for fast vector search
    # HNSW index provides approximate nearest neighbor search
    create_idx = CreateIndexExpression(
        dialect=dialect,
        index_name="idx_documents_embedding",
        table_name="documents",
        columns=["embedding"],
        index_type="HNSW",
        if_not_exists=True,
    )
    sql, params = create_idx.to_sql()
    print(f"\n--- CREATE HNSW INDEX ---")
    print(f"SQL: {sql}")
    backend.execute(sql, params)
    print("HNSW index created: idx_documents_embedding")

    # Note: HNSW index options can be set via:
    # SET hnsw.ef_search = 100;  -- query-time parameter
    # The index creation supports operator classes:
    #   vector_cosine_ops (default for <=>)
    #   vector_l2_ops (for <->)
    #   vector_ip_ops (for <#> inner product)

else:
    print("\nSkipping execution - vector extension not available on this server")
    print("To enable pgvector, install pgvector and run: CREATE EXTENSION vector;")

# ============================================================
# SECTION: Teardown (necessary for execution, reference only)
# ============================================================
backend.execute("DROP TABLE IF EXISTS documents", ())
backend.disconnect()
