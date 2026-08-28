# src/rhosocial/activerecord/backend/impl/postgres/examples/extensions/pgvector.py
"""
pgvector extension - vector similarity search.

This example demonstrates:
1. Check if vector extension is available
2. CREATE EXTENSION and create table with vector column
3. Insert vector data
4. Cosine similarity search (via the vector_search helper)
5. L2 distance search (via the vector_search helper)
6. Create HNSW index (via the create_vector_index helper)
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
from rhosocial.activerecord.backend.expression import DropTableExpression

drop_expr = DropTableExpression(dialect=dialect, table="documents", if_exists=True)
sql, params = drop_expr.to_sql()
backend.execute(sql, params)

# ============================================================
# SECTION: Business Logic (the pattern to learn)
# ============================================================
from rhosocial.activerecord.backend.impl.postgres.expression import (
    PostgresCreateExtensionExpression,
)
from rhosocial.activerecord.backend.expression import (
    CreateTableExpression,
    ColumnDefinition,
    ColumnConstraint,
    ColumnConstraintType,
)
from rhosocial.activerecord.backend.expression.statements.dml import (
    InsertExpression,
)
from rhosocial.activerecord.backend.expression.statements import (
    ValuesSource,
)
from rhosocial.activerecord.backend.expression.core import Literal
from rhosocial.activerecord.backend.options import ExecutionOptions
from rhosocial.activerecord.backend.schema import StatementType
from rhosocial.activerecord.backend.impl.postgres.functions import (
    vector_search,
    create_vector_index,
)

# Check if vector extension is available
available = dialect.is_extension_available("vector")
installed = dialect.is_extension_installed("vector")
print(f"Extension check: vector available = {available}, installed = {installed}")

# Create extension using expression
if available and not installed:
    create_ext = PostgresCreateExtensionExpression(
        dialect=dialect,
        name="vector",
    )
    sql, params = create_ext.to_sql()
    print("\n--- CREATE EXTENSION ---")
    print(f"SQL: {sql}")
    backend.execute(sql, params)
    # Re-detect extensions after creation
    backend.introspect_and_adapt()

# Re-check after creation
installed = dialect.is_extension_installed("vector")

if installed:
    # Example 1: Create table with vector column
    # Column data types are built with dialect.parse_type() (DataType instance).
    columns = [
        ColumnDefinition(
            name="id",
            data_type=dialect.parse_type("SERIAL"),
            constraints=[
                ColumnConstraint(ColumnConstraintType.PRIMARY_KEY),
            ],
        ),
        ColumnDefinition(
            name="content",
            data_type=dialect.parse_type("TEXT"),
            constraints=[
                ColumnConstraint(ColumnConstraintType.NOT_NULL),
            ],
        ),
        ColumnDefinition(
            name="embedding",
            data_type=dialect.parse_type("VECTOR(3)"),
        ),
    ]

    create_expr = CreateTableExpression(
        dialect=dialect,
        table="documents",
        columns=columns,
        if_not_exists=True,
    )
    sql, params = create_expr.to_sql()
    print("\n--- CREATE TABLE ---")
    print(f"SQL: {sql}")
    backend.execute(sql, params)

    # Example 2: Insert vector data
    # Use Literal with .cast("vector") for type-safe vector insertion.
    insert_expr = InsertExpression(
        dialect=dialect,
        into="documents",
        columns=["content", "embedding"],
        source=ValuesSource(
            dialect,
            [
                [
                    Literal(dialect, "cat"),
                    Literal(dialect, "[1.0, 0.5, 0.2]").cast("vector"),
                ],
                [
                    Literal(dialect, "dog"),
                    Literal(dialect, "[0.9, 0.6, 0.3]").cast("vector"),
                ],
                [
                    Literal(dialect, "car"),
                    Literal(dialect, "[0.1, 0.2, 0.9]").cast("vector"),
                ],
                [
                    Literal(dialect, "bicycle"),
                    Literal(dialect, "[0.15, 0.25, 0.85]").cast("vector"),
                ],
            ],
        ),
    )
    sql, params = insert_expr.to_sql()
    print("\n--- INSERT vector data ---")
    print(f"SQL: {sql}")
    backend.execute(sql, params)

    # Example 3: Cosine similarity search (query similar to "cat")
    opts = ExecutionOptions(stmt_type=StatementType.DQL)
    query = vector_search(
        dialect,
        "documents",
        [1.0, 0.55, 0.18],
        metric="cosine",
        top_k=3,
        columns=["content"],
        include_similarity=True,
    )
    sql, params = query.to_sql()
    print("\n--- Cosine similarity search ---")
    print("Query vector: [1.0, 0.55, 0.18] (similar to 'cat')")
    print(f"SQL: {sql}")
    print(f"Params: {params}")
    result = backend.execute(sql, params, options=opts)
    print(f"Results: {result.data}")

    # Example 4: L2 distance search (query vector equal to "car" row)
    query = vector_search(
        dialect,
        "documents",
        [0.1, 0.2, 0.9],
        metric="l2",
        top_k=3,
        columns=["content"],
        include_distance=True,
    )
    sql, params = query.to_sql()
    print("\n--- L2 distance search ---")
    print("Query vector: [0.1, 0.2, 0.9] (similar to 'car')")
    print(f"SQL: {sql}")
    result = backend.execute(sql, params, options=opts)
    print(f"Results: {result.data}")

    # Example 5: Create HNSW index for fast vector search
    # Metric 'cosine' maps to the vector_cosine_ops operator class.
    index_expr = create_vector_index(
        dialect,
        "documents",
        metric="cosine",
        index_type="hnsw",
        m=16,
        ef_construction=64,
    )
    sql, params = index_expr.to_sql()
    print("\n--- CREATE HNSW INDEX ---")
    print(f"SQL: {sql}")
    backend.execute(sql, params)
    print("HNSW index created")

    # Note: HNSW query-time parameter can be set via:
    # SET hnsw.ef_search = 100;
    # Supported operator classes (metric -> opclass):
    #   cosine -> vector_cosine_ops (<=>)
    #   l2 -> vector_l2_ops (<->)
    #   ip -> vector_ip_ops (<#> inner product)

else:
    print("\nSkipping execution - vector extension not available on this server")
    print("To enable pgvector, install pgvector and run: CREATE EXTENSION vector;")

# ============================================================
# SECTION: Teardown (necessary for execution, reference only)
# ============================================================
drop_expr = DropTableExpression(dialect=dialect, table="documents", if_exists=True)
sql, params = drop_expr.to_sql()
backend.execute(sql, params)
backend.disconnect()
