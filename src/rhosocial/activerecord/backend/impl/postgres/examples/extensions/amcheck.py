"""
amcheck extension - index integrity checking.

This example demonstrates:
1. Check if amcheck extension is available
2. CREATE EXTENSION amcheck
3. Verify a B-tree index using bt_index_check
4. Verify all indexes on a table
"""

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

available = dialect.is_extension_available("amcheck")
installed = dialect.is_extension_installed("amcheck")
print(f"Extension check: amcheck available = {available}, installed = {installed}")

if available and not installed:
    from rhosocial.activerecord.backend.impl.postgres.expression import (
        PostgresCreateExtensionExpression,
    )
    create_ext = PostgresCreateExtensionExpression(dialect=dialect, name="amcheck")
    sql, params = create_ext.to_sql()
    backend.execute(sql, params)
    backend.introspect_and_adapt()

installed = dialect.is_extension_installed("amcheck")

if installed:
    print("amcheck extension is ready for use")
    print("Features:")
    print(f"  bt_index_check: {dialect.supports_amcheck_bt_index_check()}")
    print(f"  bt_index_parent_check: {dialect.supports_amcheck_bt_index_parent_check()}")
    print(f"  heap verification: {dialect.supports_amcheck_heap_verification()}")

    sql, params = dialect.format_verify_index_statement("some_index")
    print(f"\nVerify single index: {sql}")

    sql, params = dialect.format_verify_table_statement("some_table")
    print(f"Verify all indexes on table: {sql}")
else:
    print("amcheck not available on this server")

backend.disconnect()