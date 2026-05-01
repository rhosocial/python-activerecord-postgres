# src/rhosocial/activerecord/backend/impl/postgres/examples/extensions/address_standardizer.py
"""
address_standardizer extension - address parsing and standardization.

This example demonstrates:
1. Check if address_standardizer extension is available
2. CREATE EXTENSION using PostgresCreateExtensionExpression
3. Use standardize_address() to normalize an address string
4. Use parse_address() to decompose an address into components

No table is needed - the functions operate on string literals directly.
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
from rhosocial.activerecord.backend.expression import QueryExpression
from rhosocial.activerecord.backend.expression.core import Literal
from rhosocial.activerecord.backend.options import ExecutionOptions
from rhosocial.activerecord.backend.schema import StatementType
from rhosocial.activerecord.backend.impl.postgres.functions.address_standardizer import (
    standardize_address,
    parse_address,
)

opts = ExecutionOptions(stmt_type=StatementType.DQL)

# Check if extensions are available
postgis_available = dialect.is_extension_available("postgis")
postgis_installed = dialect.is_extension_installed("postgis")
addr_available = dialect.is_extension_available("address_standardizer")
addr_installed = dialect.is_extension_installed("address_standardizer")
print(f"Extension: postgis available = {postgis_available}, installed = {postgis_installed}")
print(f"Extension: address_standardizer available = {addr_available}, installed = {addr_installed}")

# Create extensions if needed
if postgis_available and not postgis_installed:
    create_ext = PostgresCreateExtensionExpression(
        dialect=dialect,
        name="postgis",
    )
    sql, params = create_ext.to_sql()
    print("\n--- CREATE EXTENSION postgis ---")
    print(f"SQL: {sql}")
    backend.execute(sql, params)
    backend.introspect_and_adapt()

if addr_available and not addr_installed:
    create_ext = PostgresCreateExtensionExpression(
        dialect=dialect,
        name="address_standardizer",
    )
    sql, params = create_ext.to_sql()
    print("\n--- CREATE EXTENSION address_standardizer ---")
    print(f"SQL: {sql}")
    backend.execute(sql, params)
    backend.introspect_and_adapt()

# Re-check after creation
addr_installed = dialect.is_extension_installed("address_standardizer")

if addr_installed:
    # Example 1: standardize_address
    # Normalize a free-form address string into a standardized format
    std_func = standardize_address(
        dialect,
        Literal(dialect, "123 Main St, Springfield, IL 62701"),
    )
    query = QueryExpression(
        dialect=dialect,
        select=[std_func.as_("std_addr")],
    )
    sql, params = query.to_sql()
    print("\n--- standardize_address ---")
    print(f"SQL: {sql}")
    print(f"Params: {params}")
    result = backend.execute(sql, params, options=opts)
    print(f"Standardized address: {result.data}")

    # Example 2: parse_address
    # Decompose an address into its component parts
    parse_func = parse_address(
        dialect,
        Literal(dialect, "123 Main St, Springfield, IL 62701"),
    )
    query = QueryExpression(
        dialect=dialect,
        select=[parse_func.as_("parsed_addr")],
    )
    sql, params = query.to_sql()
    print("\n--- parse_address ---")
    print(f"SQL: {sql}")
    print(f"Params: {params}")
    result = backend.execute(sql, params, options=opts)
    print(f"Parsed address: {result.data}")

else:
    print("\nSkipping - address_standardizer not available on this server")
    print("To enable: install PostGIS and run: CREATE EXTENSION address_standardizer;")

# ============================================================
# SECTION: Teardown (necessary for execution, reference only)
# ============================================================
backend.disconnect()
