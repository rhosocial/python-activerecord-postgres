# src/rhosocial/activerecord/backend/impl/postgres/examples/extensions/hstore.py
"""
hstore extension - key-value pair storage.

This example demonstrates:
1. Check if hstore extension is available
2. Create extension using CreateExtensionExpression
3. Create table with HSTORE column
4. Insert and query hstore data
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

# Clean up
backend.execute("DROP TABLE IF EXISTS products", ())

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
)

# Check if hstore extension is available
available = dialect.is_extension_available("hstore")
installed = dialect.is_extension_installed("hstore")
print(f"Extension: hstore available = {available}, installed = {installed}")

# Create extension using expression
if available and not installed:
    create_ext = CreateExtensionExpression(
        dialect=dialect,
        name="hstore",
    )
    sql, params = create_ext.to_sql()
    print(f"\n--- CREATE EXTENSION ---")
    print(f"SQL: {sql}")
    backend.execute(sql, params)
    # Re-detect extensions after creation
    backend.introspect_and_adapt()

# Re-check after creation
installed = dialect.is_extension_installed("hstore")

if installed:
    print("\n--- Creating table with HSTORE column ---")
    columns = [
        ColumnDefinition(
            name="id",
            data_type="SERIAL",
            constraints=[ColumnConstraint(ColumnConstraintType.PRIMARY_KEY)],
        ),
        ColumnDefinition(
            name="name",
            data_type="VARCHAR(100)",
            constraints=[ColumnConstraint(ColumnConstraintType.NOT_NULL)],
        ),
        ColumnDefinition(
            name="attributes",
            data_type="HSTORE",
        ),
    ]

    create_expr = CreateTableExpression(
        dialect=dialect,
        table_name="products",
        columns=columns,
        if_not_exists=True,
    )
    sql, params = create_expr.to_sql()
    print(f"SQL: {sql}")
    backend.execute(sql, params)

    print("\n--- HSTORE available ---")
    print("HSTORE column can store key-value pairs like JSON but more efficient")
    print("Operators: -> (get value), @> (contain), ? (exist), etc.")
else:
    print("\nSkipping - hstore not available on this server")

# ============================================================
# SECTION: Teardown (necessary for execution, reference only)
# ============================================================
backend.execute("DROP TABLE IF EXISTS products", ())
backend.disconnect()