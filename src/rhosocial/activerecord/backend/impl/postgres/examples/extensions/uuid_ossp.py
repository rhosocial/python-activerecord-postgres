# src/rhosocial/activerecord/backend/impl/postgres/examples/extensions/uuid_ossp.py
"""
uuid-ossp extension - UUID generation functions.

This example demonstrates:
1. Check if uuid-ossp extension is available
2. Create extension using PostgresCreateExtensionExpression
3. Generate UUIDs using various algorithms with expression-based API
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
from rhosocial.activerecord.backend.impl.postgres.functions import (
    uuid_generate_v1,
    uuid_generate_v1mc,
    uuid_generate_v4,
    uuid_generate_v5,
    uuid_ns_dns,
    uuid_nil,
)
from rhosocial.activerecord.backend.expression import (
    QueryExpression,
)
from rhosocial.activerecord.backend.options import ExecutionOptions
from rhosocial.activerecord.backend.schema import StatementType

opts = ExecutionOptions(stmt_type=StatementType.DQL)

# Check if uuid-ossp extension is available
available = dialect.is_extension_available("uuid-ossp")
installed = dialect.is_extension_installed("uuid-ossp")
print(f"Extension: uuid-ossp available = {available}, installed = {installed}")

# Create extension using expression
if available and not installed:
    create_ext = PostgresCreateExtensionExpression(
        dialect=dialect,
        name="uuid-ossp",
    )
    sql, params = create_ext.to_sql()
    print("\n--- CREATE EXTENSION ---")
    print(f"SQL: {sql}")
    backend.execute(sql, params)
    # Re-detect extensions after creation
    backend.introspect_and_adapt()

# Re-check after creation
installed = dialect.is_extension_installed("uuid-ossp")

if installed:
    # Example 1: Generate UUID v1 (time-based, MAC address)
    # uuid_generate_v1() returns a FunctionCall expression
    uuid_v1_expr = uuid_generate_v1(dialect).as_("uuid_v1")
    query = QueryExpression(
        dialect=dialect,
        select=[uuid_v1_expr],
    )
    sql, params = query.to_sql()
    print("\n--- UUID v1 (time-based) ---")
    print(f"SQL: {sql}")
    result = backend.execute(sql, params, options=opts)
    print(f"Generated UUID v1: {result.data}")

    # Example 2: Generate UUID v1mc (time-based, random multicast MAC)
    uuid_v1mc_expr = uuid_generate_v1mc(dialect).as_("uuid_v1mc")
    query = QueryExpression(
        dialect=dialect,
        select=[uuid_v1mc_expr],
    )
    sql, params = query.to_sql()
    print("\n--- UUID v1mc (random MAC) ---")
    print(f"SQL: {sql}")
    result = backend.execute(sql, params, options=opts)
    print(f"Generated UUID v1mc: {result.data}")

    # Example 3: Generate UUID v4 (random, most common)
    uuid_v4_expr = uuid_generate_v4(dialect).as_("uuid_v4")
    query = QueryExpression(
        dialect=dialect,
        select=[uuid_v4_expr],
    )
    sql, params = query.to_sql()
    print("\n--- UUID v4 (random) ---")
    print(f"SQL: {sql}")
    result = backend.execute(sql, params, options=opts)
    print(f"Generated UUID v4: {result.data}")

    # Example 4: Generate UUID v5 (namespace + SHA-1 name-based)
    # uuid_generate_v5 takes a UUID namespace and a name as parameters
    # Use uuid_ns_dns() to get the DNS namespace UUID constant
    uuid_v5_expr = uuid_generate_v5(dialect, uuid_ns_dns(dialect), "example.com").as_("uuid_v5")
    query = QueryExpression(
        dialect=dialect,
        select=[uuid_v5_expr],
    )
    sql, params = query.to_sql()
    print("\n--- UUID v5 (SHA-1 name-based) ---")
    print(f"SQL: {sql}")
    print(f"Params: {params}")
    result = backend.execute(sql, params, options=opts)
    print(f"Generated UUID v5 (dns, example.com): {result.data}")

    # Example 5: Generate multiple UUIDs in one query
    query = QueryExpression(
        dialect=dialect,
        select=[
            uuid_generate_v1(dialect).as_("v1"),
            uuid_generate_v1mc(dialect).as_("v1mc"),
            uuid_generate_v4(dialect).as_("v4"),
        ],
    )
    sql, params = query.to_sql()
    print("\n--- Multiple UUIDs in one query ---")
    print(f"SQL: {sql}")
    result = backend.execute(sql, params, options=opts)
    print(f"Results: {result.data}")

    # Example 6: Nil UUID constant
    query = QueryExpression(
        dialect=dialect,
        select=[
            uuid_nil(dialect).as_("nil_uuid"),
        ],
    )
    sql, params = query.to_sql()
    print("\n--- Nil UUID constant ---")
    print(f"SQL: {sql}")
    result = backend.execute(sql, params, options=opts)
    print(f"Results: {result.data}")

else:
    print("\nSkipping - uuid-ossp not available on this server")

# ============================================================
# SECTION: Teardown (necessary for execution, reference only)
# ============================================================
backend.disconnect()
