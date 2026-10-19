# src/rhosocial/activerecord/backend/impl/postgres/examples/extensions/hstore.py
"""
hstore extension - key-value pair storage.

This example demonstrates:
1. Check if hstore extension is available
2. Create extension using PostgresCreateExtensionExpression
3. Create table with HSTORE column
4. Insert hstore data using InsertStatement + ValuesSource
5. Query hstore data using QueryStatement with hstore operators and functions
6. Update hstore data using UpdateStatement
"""

# ============================================================
# SECTION: Setup (necessary for execution, reference only)
# ============================================================
import os
from rhosocial.activerecord.backend.impl.postgres.backend import PostgresBackend
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

# Clean up using DropTableStatement
from rhosocial.activerecord.backend.expression import DropTableStatement

drop_expr = DropTableStatement(dialect=dialect, table=Table(dialect, 'products'), if_exists=True)
sql, params = drop_expr.to_sql()
backend.execute(sql, params)

# ============================================================
# SECTION: Business Logic (the pattern to learn)
# ============================================================
from rhosocial.activerecord.backend.impl.postgres.expression import (
    PostgresCreateExtensionExpression,
)
from rhosocial.activerecord.backend.expression import (
    CreateTableStatement,
    ColumnDefinition,
    ColumnConstraint,
    InsertStatement,
    ValuesSource,
    QueryStatement,
    UpdateStatement,
    ColumnNotNullConstraint,
    ColumnPrimaryKeyConstraint,
)
from rhosocial.activerecord.backend.expression import (
    Column, Literal, FunctionCall, Subquery,
)
from rhosocial.activerecord.backend.expression.core import CastExpression
from rhosocial.activerecord.backend.expression.objects import Table
from rhosocial.activerecord.backend.expression.operators import (
    BinaryExpression,
)
from rhosocial.activerecord.backend.expression.predicates import ComparisonPredicate
from rhosocial.activerecord.backend.options import ExecutionOptions
from rhosocial.activerecord.backend.schema import StatementType
from rhosocial.activerecord.backend.impl.postgres.expression.types import (
    PostgresHstoreType,
)

# Check if hstore extension is available
available = dialect.is_extension_available("hstore")
installed = dialect.is_extension_installed("hstore")
print(f"Extension: hstore available = {available}, installed = {installed}")

# Create extension using expression
if available and not installed:
    create_ext = PostgresCreateExtensionExpression(
        dialect=dialect,
        name="hstore",
    )
    sql, params = create_ext.to_sql()
    print("\n--- CREATE EXTENSION ---")
    print(f"SQL: {sql}")
    backend.execute(sql, params)
    # Re-detect extensions after creation
    backend.introspect_and_adapt()

# Re-check after creation
installed = dialect.is_extension_installed("hstore")

if installed:
    # Example 1: Create table with HSTORE column
    print("\n--- Creating table with HSTORE column ---")
    columns = [
        ColumnDefinition(
            name="id",
            data_type="SERIAL",
            constraints=[ColumnConstraint(dialect, branch=ColumnPrimaryKeyConstraint(dialect))],
        ),
        ColumnDefinition(
            name="name",
            data_type="VARCHAR(100)",
            constraints=[ColumnConstraint(dialect, branch=ColumnNotNullConstraint(dialect))],
        ),
        ColumnDefinition(
            name="attributes",
            data_type="HSTORE",
        ),
    ]

    create_expr = CreateTableStatement(
        dialect=dialect,
        table=Table(dialect, 'products'),
        columns=columns,
        if_not_exists=True,
    )
    sql, params = create_expr.to_sql()
    print(f"SQL: {sql}")
    backend.execute(sql, params)

    # Example 2: Insert hstore data using InsertStatement + ValuesSource
    # Use CastExpression over a Literal for type-safe hstore insertion
    insert_expr = InsertStatement(
        dialect=dialect,
        into=Table(dialect, 'products'),
        columns=["name", "attributes"],
        source=ValuesSource(
            dialect,
            [
                [
                    Literal(dialect, "Laptop"),
                    CastExpression(
                        dialect, Literal(dialect, "color=>silver, weight=>2.5"),
                        PostgresHstoreType(dialect),
                    ),
                ],
                [
                    Literal(dialect, "Phone"),
                    CastExpression(
                        dialect, Literal(dialect, "color=>black, weight=>0.2"),
                        PostgresHstoreType(dialect),
                    ),
                ],
                [
                    Literal(dialect, "Tablet"),
                    CastExpression(
                        dialect, Literal(dialect, "color=>silver, weight=>0.5, brand=>Acme"),
                        PostgresHstoreType(dialect),
                    ),
                ],
            ],
        ),
    )
    sql, params = insert_expr.to_sql()
    print("\n--- INSERT hstore data ---")
    print(f"SQL: {sql}")
    print(f"Params: {params}")
    backend.execute(sql, params)

    opts = ExecutionOptions(stmt_type=StatementType.DQL)

    # Example 3: Get value by key using -> operator
    # attributes -> 'color' retrieves the value for key 'color'
    query = QueryStatement(
        dialect=dialect,
        select=[
            Column(dialect, "name"),
            Subquery(dialect, BinaryExpression(
                dialect, "->",
                Column(dialect, "attributes"),
                Literal(dialect, "color"),
            )).as_("color"),
        ],
        from_=Table(dialect, "products"),
    )
    sql, params = query.to_sql()
    print("\n--- Get value by key (-> operator) ---")
    print(f"SQL: {sql}")
    print(f"Params: {params}")
    result = backend.execute(sql, params, options=opts)
    print(f"Results: {result.data}")

    # Example 4: Check if key exists using ? operator
    # attributes ? 'weight' checks whether the key 'weight' exists
    query = QueryStatement(
        dialect=dialect,
        select=[Column(dialect, "name"), Column(dialect, "attributes")],
        from_=Table(dialect, "products"),
        where=BinaryExpression(
            dialect, "?",
            Column(dialect, "attributes"),
            Literal(dialect, "weight"),
        ),
    )
    sql, params = query.to_sql()
    print("\n--- Key exists (? operator) ---")
    print(f"SQL: {sql}")
    print(f"Params: {params}")
    result = backend.execute(sql, params, options=opts)
    print(f"Products with 'weight' key: {result.data}")

    # Example 5: Contains operator @>
    # attributes @> 'color=>silver' checks if hstore contains the given key-value pair
    query = QueryStatement(
        dialect=dialect,
        select=[Column(dialect, "name"), Column(dialect, "attributes")],
        from_=Table(dialect, "products"),
        where=BinaryExpression(
            dialect, "@>",
            Column(dialect, "attributes"),
            CastExpression(dialect, Literal(dialect, "color=>silver"), PostgresHstoreType(dialect)),
        ),
    )
    sql, params = query.to_sql()
    print("\n--- Contains operator (@>) ---")
    print(f"SQL: {sql}")
    result = backend.execute(sql, params, options=opts)
    print(f"Products with color=silver: {result.data}")

    # Example 6: Concatenation operator ||
    # attributes || 'brand=>Acme' merges new key-value pairs into the hstore
    query = QueryStatement(
        dialect=dialect,
        select=[
            Column(dialect, "name"),
            Subquery(dialect, BinaryExpression(
                dialect, "||",
                Column(dialect, "attributes"),
                CastExpression(
                    dialect, Literal(dialect, "brand=>Acme"),
                    PostgresHstoreType(dialect),
                ),
            )).as_("with_brand"),
        ],
        from_=Table(dialect, "products"),
    )
    sql, params = query.to_sql()
    print("\n--- Concatenation operator (||) ---")
    print(f"SQL: {sql}")
    result = backend.execute(sql, params, options=opts)
    print(f"Results: {result.data}")

    # Example 7: Get all keys using akeys() function
    # akeys(attributes) returns all keys as an array
    query = QueryStatement(
        dialect=dialect,
        select=[
            Column(dialect, "name"),
            FunctionCall(
                dialect, "akeys",
                Column(dialect, "attributes"),
            ).as_("keys"),
        ],
        from_=Table(dialect, "products"),
    )
    sql, params = query.to_sql()
    print("\n--- Get all keys (akeys function) ---")
    print(f"SQL: {sql}")
    result = backend.execute(sql, params, options=opts)
    print(f"Results: {result.data}")

    # Example 8: Get all values using avals() function
    # avals(attributes) returns all values as an array
    query = QueryStatement(
        dialect=dialect,
        select=[
            Column(dialect, "name"),
            FunctionCall(
                dialect, "avals",
                Column(dialect, "attributes"),
            ).as_("values"),
        ],
        from_=Table(dialect, "products"),
    )
    sql, params = query.to_sql()
    print("\n--- Get all values (avals function) ---")
    print(f"SQL: {sql}")
    result = backend.execute(sql, params, options=opts)
    print(f"Results: {result.data}")

    # Example 9: Get keys as set using skeys() function
    # skeys(attributes) returns all keys as a set (one per row)
    query = QueryStatement(
        dialect=dialect,
        select=[
            Column(dialect, "name"),
            FunctionCall(
                dialect, "skeys",
                Column(dialect, "attributes"),
            ).as_("key"),
        ],
        from_=Table(dialect, "products"),
    )
    sql, params = query.to_sql()
    print("\n--- Get keys as set (skeys function) ---")
    print(f"SQL: {sql}")
    result = backend.execute(sql, params, options=opts)
    print(f"Results: {result.data}")

    # Example 10: Delete a key using delete() function
    # delete(attributes, 'weight') removes the key 'weight' from the hstore
    query = QueryStatement(
        dialect=dialect,
        select=[
            Column(dialect, "name"),
            FunctionCall(
                dialect, "delete",
                Column(dialect, "attributes"),
                Literal(dialect, "weight"),
            ).as_("without_weight"),
        ],
        from_=Table(dialect, "products"),
    )
    sql, params = query.to_sql()
    print("\n--- Delete key (delete function) ---")
    print(f"SQL: {sql}")
    print(f"Params: {params}")
    result = backend.execute(sql, params, options=opts)
    print(f"Results: {result.data}")

    # Example 11: Update hstore data using UpdateStatement
    # Add/update a key using || operator in UPDATE
    dml_opts = ExecutionOptions(stmt_type=StatementType.DML)
    update_expr = UpdateStatement(
        dialect=dialect,
        table=Table(dialect, 'products'),
        assignments={
            "attributes": BinaryExpression(
                dialect, "||",
                Column(dialect, "attributes"),
                CastExpression(
                    dialect, Literal(dialect, "discount=>true"),
                    PostgresHstoreType(dialect),
                ),
            ),
        },
        where=ComparisonPredicate(
            dialect, "=",
            BinaryExpression(
                dialect, "->",
                Column(dialect, "attributes"),
                Literal(dialect, "color"),
            ),
            Literal(dialect, "silver"),
        ),
    )
    sql, params = update_expr.to_sql()
    print("\n--- UPDATE with hstore concatenation ---")
    print(f"SQL: {sql}")
    print(f"Params: {params}")
    result = backend.execute(sql, params, options=dml_opts)
    print(f"Updated rows: {result.affected_rows}")

    # Verify the update
    query = QueryStatement(
        dialect=dialect,
        select=[
            Column(dialect, "name"),
            Subquery(dialect, BinaryExpression(
                dialect, "->",
                Column(dialect, "attributes"),
                Literal(dialect, "discount"),
            )).as_("discount"),
        ],
        from_=Table(dialect, "products"),
        where=ComparisonPredicate(
            dialect, "=",
            BinaryExpression(
                dialect, "->",
                Column(dialect, "attributes"),
                Literal(dialect, "color"),
            ),
            Literal(dialect, "silver"),
        ),
    )
    sql, params = query.to_sql()
    result = backend.execute(sql, params, options=opts)
    print(f"Updated products with discount key: {result.data}")

else:
    print("\nSkipping - hstore not available on this server")
    print("To enable hstore, run: CREATE EXTENSION hstore;")

# ============================================================
# SECTION: Teardown (necessary for execution, reference only)
# ============================================================
drop_expr = DropTableStatement(dialect=dialect, table=Table(dialect, 'products'), if_exists=True)
sql, params = drop_expr.to_sql()
backend.execute(sql, params)
backend.disconnect()
