# src/rhosocial/activerecord/backend/impl/postgres/examples/extensions/pageinspect.py
"""
pageinspect extension - page-level database inspection.

This example demonstrates:
1. Check if pageinspect extension is available
2. CREATE EXTENSION pageinspect
3. Inspect heap page items
4. Show B-tree meta page information
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
    PostgresCreateExtensionExpression,
)

# Check if pageinspect extension is available
available = dialect.is_extension_available("pageinspect")
installed = dialect.is_extension_installed("pageinspect")
print(f"Extension check: pageinspect available = {available}, installed = {installed}")

# Create extension using expression
if available and not installed:
    create_ext = PostgresCreateExtensionExpression(
        dialect=dialect,
        name="pageinspect",
    )
    sql, params = create_ext.to_sql()
    print("\n--- CREATE EXTENSION ---")
    print(f"SQL: {sql}")
    backend.execute(sql, params)
    backend.introspect_and_adapt()

# Re-check after creation
installed = dialect.is_extension_installed("pageinspect")

if installed:
    # Example 1: Print available features
    print("pageinspect extension is ready for use")
    print("Features:")
    print(f"  heap inspection: {dialect.supports_pageinspect_heap()}")
    print(f"  B-tree inspection: {dialect.supports_pageinspect_btree()}")
    print(f"  BRIN inspection: {dialect.supports_pageinspect_brin()}")

    # Example 2: Inspect heap page items
    sql, params = dialect.format_heap_page_statement("my_table", 0)
    print("\n--- Inspect heap page 0 ---")
    print(f"SQL: {sql}")

    # Example 3: Show B-tree meta page information
    sql, params = dialect.format_btree_metapage_statement("my_index")
    print("\n--- B-tree meta page ---")
    print(f"SQL: {sql}")
else:
    print("\nSkipping execution - pageinspect not available on this server")
    print("To enable pageinspect, run: CREATE EXTENSION pageinspect;")

# ============================================================
# SECTION: Teardown (necessary for execution, reference only)
# ============================================================
backend.disconnect()