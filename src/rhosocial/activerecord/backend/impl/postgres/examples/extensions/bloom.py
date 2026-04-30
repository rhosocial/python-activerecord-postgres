# src/rhosocial/activerecord/backend/impl/postgres/examples/extensions/bloom.py
"""
bloom extension - bloom filter index access method.

This example demonstrates:
1. Check if bloom extension is available
2. CREATE EXTENSION and create table with multiple text columns
3. Insert sample data
4. Create bloom index for efficient equality searches
5. Query data using the bloom index
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

drop_expr = DropTableExpression(dialect=dialect, table_name="customers", if_exists=True)
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
    CreateIndexExpression,
    InsertExpression,
    QueryExpression,
    TableExpression,
    Column,
)
from rhosocial.activerecord.backend.expression.statements import ValuesSource
from rhosocial.activerecord.backend.expression.core import Literal
from rhosocial.activerecord.backend.expression.predicates import ComparisonPredicate
from rhosocial.activerecord.backend.options import ExecutionOptions
from rhosocial.activerecord.backend.schema import StatementType

# Check if bloom extension is available
available = dialect.is_extension_available("bloom")
installed = dialect.is_extension_installed("bloom")
print(f"Extension check: bloom available = {available}, installed = {installed}")

# Create extension using expression
if available and not installed:
    create_ext = PostgresCreateExtensionExpression(
        dialect=dialect,
        name="bloom",
    )
    sql, params = create_ext.to_sql()
    print("\n--- CREATE EXTENSION ---")
    print(f"SQL: {sql}")
    backend.execute(sql, params)
    backend.introspect_and_adapt()

# Re-check after creation
installed = dialect.is_extension_installed("bloom")

if installed:
    # Example 1: Create table with multiple text columns
    columns = [
        ColumnDefinition(
            name="id",
            data_type="SERIAL",
            constraints=[
                ColumnConstraint(ColumnConstraintType.PRIMARY_KEY),
            ],
        ),
        ColumnDefinition(name="name", data_type="TEXT"),
        ColumnDefinition(name="email", data_type="TEXT"),
        ColumnDefinition(name="city", data_type="TEXT"),
    ]

    create_expr = CreateTableExpression(
        dialect=dialect,
        table_name="customers",
        columns=columns,
        if_not_exists=True,
    )
    sql, params = create_expr.to_sql()
    print("\n--- CREATE TABLE ---")
    print(f"SQL: {sql}")
    backend.execute(sql, params)

    # Example 2: Insert sample data
    rows = [
        [Literal(dialect, "Alice"), Literal(dialect, "alice@example.com"), Literal(dialect, "New York")],
        [Literal(dialect, "Bob"), Literal(dialect, "bob@example.com"), Literal(dialect, "Los Angeles")],
        [Literal(dialect, "Charlie"), Literal(dialect, "charlie@example.com"), Literal(dialect, "Chicago")],
        [Literal(dialect, "Diana"), Literal(dialect, "diana@example.com"), Literal(dialect, "New York")],
        [Literal(dialect, "Eve"), Literal(dialect, "eve@example.com"), Literal(dialect, "Boston")],
    ]
    insert_expr = InsertExpression(
        dialect=dialect,
        into="customers",
        columns=["name", "email", "city"],
        source=ValuesSource(dialect, rows),
    )
    sql, params = insert_expr.to_sql()
    print("\n--- INSERT data ---")
    print(f"SQL: {sql}")
    backend.execute(sql, params)

    # Example 3: Create bloom index on multiple columns
    # Bloom index is useful for equality searches on multiple columns
    create_idx = CreateIndexExpression(
        dialect=dialect,
        index_name="idx_customers_bloom",
        table_name="customers",
        columns=["name", "email", "city"],
        index_type="BLOOM",
        if_not_exists=True,
    )
    sql, params = create_idx.to_sql()
    print("\n--- CREATE BLOOM INDEX ---")
    print(f"SQL: {sql}")
    backend.execute(sql, params)
    print("Bloom index created: idx_customers_bloom")

    # Example 4: Query using equality condition (bloom index can help)
    query = QueryExpression(
        dialect=dialect,
        select=[Column(dialect, "name"), Column(dialect, "email"), Column(dialect, "city")],
        from_=TableExpression(dialect, "customers"),
        where=ComparisonPredicate(
            Column(dialect, "city"),
            "=",
            Literal(dialect, "New York"),
        ),
    )
    sql, params = query.to_sql()
    print("\n--- Query with equality condition ---")
    print(f"SQL: {sql}")
    opts = ExecutionOptions(stmt_type=StatementType.DQL)
    result = backend.execute(sql, params, options=opts)
    print(f"Results: {result.data}")

    # Example 5: Create bloom index with custom options
    # length=64 (signature size in bits), col1=2 (number of bits for first column)
    create_idx_opts = CreateIndexExpression(
        dialect=dialect,
        index_name="idx_customers_bloom_opts",
        table_name="customers",
        columns=["name", "email"],
        index_type="BLOOM",
        if_not_exists=True,
        dialect_options={"with": {"length": 64, "col1": 2, "col2": 2}},
    )
    sql, params = create_idx_opts.to_sql()
    print("\n--- CREATE BLOOM INDEX WITH OPTIONS ---")
    print(f"SQL: {sql}")
    backend.execute(sql, params)
    print("Bloom index with custom options created: idx_customers_bloom_opts")

else:
    print("\nSkipping execution - bloom extension not available on this server")
    print("To enable bloom, run: CREATE EXTENSION bloom;")

# ============================================================
# SECTION: Teardown (necessary for execution, reference only)
# ============================================================
drop_expr = DropTableExpression(dialect=dialect, table_name="customers", if_exists=True)
sql, params = drop_expr.to_sql()
backend.execute(sql, params)
backend.disconnect()
