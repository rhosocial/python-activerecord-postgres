# src/rhosocial/activerecord/backend/impl/postgres/examples/extensions/fuzzystrmatch.py
"""
fuzzystrmatch extension - fuzzy string matching.

This example demonstrates:
1. Check if fuzzystrmatch extension is available
2. Create extension using CreateExtensionExpression
3. Use fuzzystrmatch functions
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

# Check if fuzzystrmatch extension is available
available = dialect.is_extension_available("fuzzystrmatch")
installed = dialect.is_extension_installed("fuzzystrmatch")
print(f"Extension: fuzzystrmatch available = {available}, installed = {installed}")

# Create extension using expression
if available and not installed:
    create_ext = CreateExtensionExpression(
        dialect=dialect,
        name="fuzzystrmatch",
    )
    sql, params = create_ext.to_sql()
    print(f"\n--- CREATE EXTENSION ---")
    print(f"SQL: {sql}")
    backend.execute(sql, params)

# Re-check after creation
installed = dialect.is_extension_installed("fuzzystrmatch")

if installed:
    print("\n--- Fuzzystrmatch functions available ---")
    print("Available functions:")
    print("  - levenshtein(s1, s2): Calculate Levenshtein distance")
    print("  - soundex(text): Soundex encoding")
    print("  - dmetaphone(text): Double Metaphone encoding")
    print("  - metaphone(text): Metaphone encoding")
else:
    print("\nSkipping - fuzzystrmatch not available on this server")

# ============================================================
# SECTION: Teardown (necessary for execution, reference only)
# ============================================================
backend.disconnect()