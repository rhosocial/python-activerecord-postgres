# src/rhosocial/activerecord/backend/impl/postgres/examples/extensions/pg_trgm.py
"""
pg_trgm extension - trigram-based fuzzy text search.

This example demonstrates:
1. Check if pg_trgm extension is available
2. CREATE EXTENSION and create table with text column
3. Use similarity() function for fuzzy matching
4. Use % operator for trigram similarity search
5. Create GIN index to accelerate fuzzy search
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
    DropTableExpression,
    QueryExpression,
    Column,
    TableExpression,
    FunctionCall,
    BinaryExpression,
    OrderByClause,
    Literal,
)
from rhosocial.activerecord.backend.expression.statements.dml import (
    InsertExpression,
)
from rhosocial.activerecord.backend.expression.statements import (
    ValuesSource,
)
from rhosocial.activerecord.backend.options import ExecutionOptions
from rhosocial.activerecord.backend.schema import StatementType

# Clean up for demo
drop_expr = DropTableExpression(
    dialect=dialect,
    table_name="articles",
    if_exists=True,
)
sql, params = drop_expr.to_sql()
backend.execute(sql, params)

# Check if pg_trgm extension is available
available = dialect.is_extension_available("pg_trgm")
installed = dialect.is_extension_installed("pg_trgm")
print(f"Extension check: pg_trgm available = {available}, installed = {installed}")

# Create extension using expression
if available and not installed:
    create_ext = CreateExtensionExpression(
        dialect=dialect,
        name="pg_trgm",
    )
    sql, params = create_ext.to_sql()
    print(f"\n--- CREATE EXTENSION ---")
    print(f"SQL: {sql}")
    backend.execute(sql, params)
    # Re-detect extensions after creation
    backend.introspect_and_adapt()

# Re-check after creation
installed = dialect.is_extension_installed("pg_trgm")

if installed:
    # Example 1: Create table with text column
    columns = [
        ColumnDefinition(
            name="id",
            data_type="SERIAL",
            constraints=[
                ColumnConstraint(ColumnConstraintType.PRIMARY_KEY),
            ],
        ),
        ColumnDefinition(
            name="title",
            data_type="TEXT",
            constraints=[
                ColumnConstraint(ColumnConstraintType.NOT_NULL),
            ],
        ),
    ]

    create_expr = CreateTableExpression(
        dialect=dialect,
        table_name="articles",
        columns=columns,
        if_not_exists=True,
    )
    sql, params = create_expr.to_sql()
    print(f"\n--- CREATE TABLE ---")
    print(f"SQL: {sql}")
    backend.execute(sql, params)

    # Example 2: Insert text data
    insert_expr = InsertExpression(
        dialect=dialect,
        into="articles",
        columns=["title"],
        source=ValuesSource(
            dialect,
            [
                [Literal(dialect, "Introduction to PostgreSQL")],
                [Literal(dialect, "PostgreSQL Performance Tuning")],
                [Literal(dialect, "Advanced SQL Queries")],
                [Literal(dialect, "Database Design Principles")],
                [Literal(dialect, "PostgreSQL Replication Guide")],
                [Literal(dialect, "MySQL vs PostgreSQL Comparison")],
            ],
        ),
    )
    sql, params = insert_expr.to_sql()
    print(f"\n--- INSERT text data ---")
    print(f"SQL: {sql}")
    print(f"Params: {params}")
    backend.execute(sql, params)

    opts = ExecutionOptions(stmt_type=StatementType.DQL)

    # Example 3: Similarity search using similarity() function
    # similarity(a, b) returns float [0, 1], higher = more similar
    # Note: as_() modifies the object in-place, so create separate instances
    # for SELECT (with alias) and WHERE (without alias)
    sim_select = FunctionCall(
        dialect, "similarity", Column(dialect, "title"), Literal(dialect, "PostgreSQL intro")
    ).as_("sim_score")
    sim_where = FunctionCall(
        dialect, "similarity", Column(dialect, "title"), Literal(dialect, "PostgreSQL intro")
    )
    sim_order = FunctionCall(
        dialect, "similarity", Column(dialect, "title"), Literal(dialect, "PostgreSQL intro")
    )
    query = QueryExpression(
        dialect=dialect,
        select=[
            Column(dialect, "title"),
            sim_select,
        ],
        from_=TableExpression(dialect, "articles"),
        where=sim_where > Literal(dialect, 0.1),
        order_by=OrderByClause(dialect, [(sim_order, "DESC")]),
    )
    sql, params = query.to_sql()
    print(f"\n--- similarity() fuzzy search ---")
    print(f"SQL: {sql}")
    print(f"Params: {params}")
    result = backend.execute(sql, params, options=opts)
    print(f"Query: 'PostgreSQL intro'")
    print(f"Results: {result.data}")

    # Example 4: Using % operator (similarity threshold)
    # The % operator returns true if similarity exceeds the threshold
    # Default threshold: 0.3 (set by pg_trgm.similarity_threshold)
    query = QueryExpression(
        dialect=dialect,
        select=[Column(dialect, "title")],
        from_=TableExpression(dialect, "articles"),
        where=BinaryExpression(
            dialect, "%", Column(dialect, "title"), Literal(dialect, "Postgres guide")
        ),
        order_by=OrderByClause(dialect, [(Column(dialect, "title"), "ASC")]),
    )
    sql, params = query.to_sql()
    print(f"\n--- %% operator search ---")
    print(f"SQL: {sql}")
    print(f"Params: {params}")
    result = backend.execute(sql, params, options=opts)
    print(f"Query: 'Postgres guide' (above default threshold 0.3)")
    print(f"Results: {result.data}")

    # Example 5: Show similarity scores for comparison
    sim_pg_select = FunctionCall(
        dialect, "similarity", Column(dialect, "title"), Literal(dialect, "PostgreSQL")
    ).as_("sim_postgresql")
    sim_design_select = FunctionCall(
        dialect, "similarity", Column(dialect, "title"), Literal(dialect, "database design")
    ).as_("sim_design")
    sim_pg_order = FunctionCall(
        dialect, "similarity", Column(dialect, "title"), Literal(dialect, "PostgreSQL")
    )
    query = QueryExpression(
        dialect=dialect,
        select=[
            Column(dialect, "title"),
            sim_pg_select,
            sim_design_select,
        ],
        from_=TableExpression(dialect, "articles"),
        order_by=OrderByClause(dialect, [(sim_pg_order, "DESC")]),
    )
    sql, params = query.to_sql()
    print(f"\n--- Multiple similarity scores ---")
    print(f"SQL: {sql}")
    print(f"Params: {params}")
    result = backend.execute(sql, params, options=opts)
    print(f"Results: {result.data}")

    # Example 6: Create GIN index with trigram operator class
    # GIN index with gin_trgm_ops accelerates %, similarity, LIKE queries
    create_idx = CreateIndexExpression(
        dialect=dialect,
        index_name="idx_articles_title_trgm",
        table_name="articles",
        columns=["title"],
        index_type="GIN",
        if_not_exists=True,
        dialect_options={"opclasses": {"title": "gin_trgm_ops"}},
    )
    sql, params = create_idx.to_sql()
    print(f"\n--- CREATE GIN INDEX ---")
    print(f"SQL: {sql}")
    backend.execute(sql, params)
    print("GIN trigram index created: idx_articles_title_trgm")
    print("Note: Use 'gin_trgm_ops' operator class for trigram GIN indexes")

else:
    print("\nSkipping execution - pg_trgm not available on this server")
    print("To enable pg_trgm, run: CREATE EXTENSION pg_trgm;")

# ============================================================
# SECTION: Teardown (necessary for execution, reference only)
# ============================================================
drop_expr = DropTableExpression(
    dialect=dialect,
    table_name="articles",
    if_exists=True,
)
sql, params = drop_expr.to_sql()
backend.execute(sql, params)
backend.disconnect()
