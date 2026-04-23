# src/rhosocial/activerecord/backend/impl/postgres/examples/extensions/uuid_ossp.py
"""
uuid-ossp extension - UUID generation functions.

This example demonstrates:
1. Check if uuid-ossp extension is available
2. Create extension using CreateExtensionExpression
3. Generate UUIDs using various algorithms
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

# Check if uuid-ossp extension is available
available = dialect.is_extension_available("uuid-ossp")
installed = dialect.is_extension_installed("uuid-ossp")
print(f"Extension: uuid-ossp available = {available}, installed = {installed}")

# Create extension using expression
if available and not installed:
    create_ext = CreateExtensionExpression(
        dialect=dialect,
        name="uuid-ossp",
    )
    sql, params = create_ext.to_sql()
    print(f"\n--- CREATE EXTENSION ---")
    print(f"SQL: {sql}")
    backend.execute(sql, params)
    # Re-detect extensions after creation
    backend.introspect_and_adapt()

# Re-check after creation
installed = dialect.is_extension_installed("uuid-ossp")

if installed:
    print("\n--- UUID generation functions ---")
    print("Available functions:")
    print("  - uuid_generate_v1(): MAC address + time-based")
    print("  - uuid_generate_v1mc(): Random node-based v1")
    print("  - uuid_generate_v4(): Random (most common)")
    print("  - uuid_generate_v5(ns, name): Namespace + name based v5")
else:
    print("\nSkipping - uuid-ossp not available on this server")

# ============================================================
# SECTION: Teardown (necessary for execution, reference only)
# ============================================================
backend.disconnect()