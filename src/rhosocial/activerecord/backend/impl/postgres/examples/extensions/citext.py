# src/rhosocial/activerecord/backend/impl/postgres/examples/extensions/citext.py
"""
citext extension - case insensitive text type.

This example demonstrates:
1. Check if citext extension is available
2. CREATE TABLE with CITEXT column
3. Query with case-insensitive comparison
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
backend.execute("DROP TABLE IF EXISTS users", ())

# ============================================================
# SECTION: Business Logic (the pattern to learn)
# ============================================================
from rhosocial.activerecord.backend.impl.postgres.expression import (
    CreateExtensionExpression,
    DropExtensionExpression,
)
from rhosocial.activerecord.backend.expression import (
    CreateTableExpression,
    ColumnDefinition,
    ColumnConstraint,
    ColumnConstraintType,
)
from rhosocial.activerecord.backend.expression.core import Literal
from rhosocial.activerecord.backend.expression.statements.dml import (
    InsertExpression,
)
from rhosocial.activerecord.backend.expression.statements import (
    ValuesSource,
)
from rhosocial.activerecord.backend.expression import (
    Column,
    QueryExpression,
    TableExpression,
)

# Check if citext extension is available
available = dialect.is_extension_available("citext")
installed = dialect.is_extension_installed("citext")
print(f"Extension check: citext available = {available}, installed = {installed}")

# Create extension using expression
if not installed:
    create_ext = CreateExtensionExpression(
        dialect=dialect,
        name="citext",
    )
    sql, params = create_ext.to_sql()
    print(f"\n--- CREATE EXTENSION ---")
    print(f"SQL: {sql}")
    backend.execute(sql, params)
    # Re-detect extensions after creation
    backend.introspect_and_adapt()

# Example 1: Create table with CITEXT column
columns = [
    ColumnDefinition(
        name="id",
        data_type="SERIAL",
        constraints=[
            ColumnConstraint(ColumnConstraintType.PRIMARY_KEY),
        ],
    ),
    ColumnDefinition(
        name="email",
        data_type="CITEXT",
        constraints=[
            ColumnConstraint(ColumnConstraintType.NOT_NULL),
        ],
    ),
    ColumnDefinition(
        name="username",
        data_type="VARCHAR(100)",
        constraints=[
            ColumnConstraint(ColumnConstraintType.NOT_NULL),
        ],
    ),
]

create_expr = CreateTableExpression(
    dialect=dialect,
    table_name="users",
    columns=columns,
    if_not_exists=True,
)

sql, params = create_expr.to_sql()
print(f"\n--- CREATE TABLE ---")
print(f"SQL: {sql}")

# Only execute if extension is available
if available:
    backend.execute(sql, params)

    # Example 2: Insert with case-insensitive data
    insert_expr = InsertExpression(
        dialect=dialect,
        into="users",
        columns=["email", "username"],
        source=ValuesSource(
            dialect,
            [
                [Literal(dialect, "Test@Example.COM"), Literal(dialect, "testuser")],
            ],
        ),
    )
    sql, params = insert_expr.to_sql()
    print(f"\n--- INSERT ---")
    print(f"SQL: {sql}")
    print(f"Params: {params}")
    backend.execute(sql, params)

    # Example 3: Case-insensitive query
    # CITEXT automatically handles case-insensitive comparison
    query = QueryExpression(
        dialect=dialect,
        select=[
            Column(dialect, "id"),
            Column(dialect, "email"),
        ],
        from_=TableExpression(dialect, "users"),
        where=Column(dialect, "email", table="users")
        == Literal(dialect, "test@example.com"),
    )
    sql, params = query.to_sql()
    print(f"\n--- SELECT (case-insensitive) ---")
    print(f"SQL: {sql}")
    print(f"Params: {params}")

    # ============================================================
    # SECTION: Execution (run the expression)
    # ============================================================
    result = backend.execute(sql, params)
    print(f"\nResult: {result}")
else:
    print("\nSkipping execution - citext not available on this server")
    print("To enable citext, run: CREATE EXTENSION citext;")

# ============================================================
# SECTION: Teardown (necessary for execution, reference only)
# ============================================================
backend.execute("DROP TABLE IF EXISTS users", ())
backend.disconnect()