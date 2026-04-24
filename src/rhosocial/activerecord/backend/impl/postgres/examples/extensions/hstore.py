# src/rhosocial/activerecord/backend/impl/postgres/examples/extensions/hstore.py
"""
hstore extension - key-value pair storage.

This example demonstrates:
1. Check if hstore extension is available
2. Create extension using CreateExtensionExpression
3. Create table with HSTORE column
4. Insert hstore data using InsertExpression + ValuesSource
5. Query hstore data using QueryExpression with hstore operators and functions
6. Update hstore data using UpdateExpression
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

# Clean up using DropTableExpression
from rhosocial.activerecord.backend.expression import DropTableExpression

drop_expr = DropTableExpression(dialect=dialect, table_name="products", if_exists=True)
sql, params = drop_expr.to_sql()
backend.execute(sql, params)

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
    InsertExpression,
    ValuesSource,
    QueryExpression,
    UpdateExpression,
)
from rhosocial.activerecord.backend.expression.core import (
    Column,
    Literal,
    FunctionCall,
    Subquery,
    TableExpression,
)
from rhosocial.activerecord.backend.expression.operators import (
    BinaryExpression,
)
from rhosocial.activerecord.backend.expression.predicates import ComparisonPredicate
from rhosocial.activerecord.backend.options import ExecutionOptions
from rhosocial.activerecord.backend.schema import StatementType

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
    # Example 1: Create table with HSTORE column
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

    # Example 2: Insert hstore data using InsertExpression + ValuesSource
    # Use Literal with .cast("hstore") for type-safe hstore insertion
    insert_expr = InsertExpression(
        dialect=dialect,
        into="products",
        columns=["name", "attributes"],
        source=ValuesSource(
            dialect,
            [
                [
                    Literal(dialect, "Laptop"),
                    Literal(dialect, "color=>silver, weight=>2.5").cast("hstore"),
                ],
                [
                    Literal(dialect, "Phone"),
                    Literal(dialect, "color=>black, weight=>0.2").cast("hstore"),
                ],
                [
                    Literal(dialect, "Tablet"),
                    Literal(dialect, "color=>silver, weight=>0.5, brand=>Acme").cast("hstore"),
                ],
            ],
        ),
    )
    sql, params = insert_expr.to_sql()
    print(f"\n--- INSERT hstore data ---")
    print(f"SQL: {sql}")
    print(f"Params: {params}")
    backend.execute(sql, params)

    opts = ExecutionOptions(stmt_type=StatementType.DQL)

    # Example 3: Get value by key using -> operator
    # attributes -> 'color' retrieves the value for key 'color'
    query = QueryExpression(
        dialect=dialect,
        select=[
            Column(dialect, "name"),
            Subquery(dialect, BinaryExpression(
                dialect, "->",
                Column(dialect, "attributes"),
                Literal(dialect, "color"),
            )).as_("color"),
        ],
        from_=TableExpression(dialect, "products"),
    )
    sql, params = query.to_sql()
    print(f"\n--- Get value by key (-> operator) ---")
    print(f"SQL: {sql}")
    print(f"Params: {params}")
    result = backend.execute(sql, params, options=opts)
    print(f"Results: {result.data}")

    # Example 4: Check if key exists using ? operator
    # attributes ? 'weight' checks whether the key 'weight' exists
    query = QueryExpression(
        dialect=dialect,
        select=[Column(dialect, "name"), Column(dialect, "attributes")],
        from_=TableExpression(dialect, "products"),
        where=BinaryExpression(
            dialect, "?",
            Column(dialect, "attributes"),
            Literal(dialect, "weight"),
        ),
    )
    sql, params = query.to_sql()
    print(f"\n--- Key exists (? operator) ---")
    print(f"SQL: {sql}")
    print(f"Params: {params}")
    result = backend.execute(sql, params, options=opts)
    print(f"Products with 'weight' key: {result.data}")

    # Example 5: Contains operator @>
    # attributes @> 'color=>silver' checks if hstore contains the given key-value pair
    query = QueryExpression(
        dialect=dialect,
        select=[Column(dialect, "name"), Column(dialect, "attributes")],
        from_=TableExpression(dialect, "products"),
        where=BinaryExpression(
            dialect, "@>",
            Column(dialect, "attributes"),
            Literal(dialect, "color=>silver").cast("hstore"),
        ),
    )
    sql, params = query.to_sql()
    print(f"\n--- Contains operator (@>) ---")
    print(f"SQL: {sql}")
    result = backend.execute(sql, params, options=opts)
    print(f"Products with color=silver: {result.data}")

    # Example 6: Concatenation operator ||
    # attributes || 'brand=>Acme' merges new key-value pairs into the hstore
    query = QueryExpression(
        dialect=dialect,
        select=[
            Column(dialect, "name"),
            Subquery(dialect, BinaryExpression(
                dialect, "||",
                Column(dialect, "attributes"),
                Literal(dialect, "brand=>Acme").cast("hstore"),
            )).as_("with_brand"),
        ],
        from_=TableExpression(dialect, "products"),
    )
    sql, params = query.to_sql()
    print(f"\n--- Concatenation operator (||) ---")
    print(f"SQL: {sql}")
    result = backend.execute(sql, params, options=opts)
    print(f"Results: {result.data}")

    # Example 7: Get all keys using akeys() function
    # akeys(attributes) returns all keys as an array
    query = QueryExpression(
        dialect=dialect,
        select=[
            Column(dialect, "name"),
            FunctionCall(
                dialect, "akeys",
                Column(dialect, "attributes"),
            ).as_("keys"),
        ],
        from_=TableExpression(dialect, "products"),
    )
    sql, params = query.to_sql()
    print(f"\n--- Get all keys (akeys function) ---")
    print(f"SQL: {sql}")
    result = backend.execute(sql, params, options=opts)
    print(f"Results: {result.data}")

    # Example 8: Get all values using avals() function
    # avals(attributes) returns all values as an array
    query = QueryExpression(
        dialect=dialect,
        select=[
            Column(dialect, "name"),
            FunctionCall(
                dialect, "avals",
                Column(dialect, "attributes"),
            ).as_("values"),
        ],
        from_=TableExpression(dialect, "products"),
    )
    sql, params = query.to_sql()
    print(f"\n--- Get all values (avals function) ---")
    print(f"SQL: {sql}")
    result = backend.execute(sql, params, options=opts)
    print(f"Results: {result.data}")

    # Example 9: Get keys as set using skeys() function
    # skeys(attributes) returns all keys as a set (one per row)
    query = QueryExpression(
        dialect=dialect,
        select=[
            Column(dialect, "name"),
            FunctionCall(
                dialect, "skeys",
                Column(dialect, "attributes"),
            ).as_("key"),
        ],
        from_=TableExpression(dialect, "products"),
    )
    sql, params = query.to_sql()
    print(f"\n--- Get keys as set (skeys function) ---")
    print(f"SQL: {sql}")
    result = backend.execute(sql, params, options=opts)
    print(f"Results: {result.data}")

    # Example 10: Delete a key using delete() function
    # delete(attributes, 'weight') removes the key 'weight' from the hstore
    query = QueryExpression(
        dialect=dialect,
        select=[
            Column(dialect, "name"),
            FunctionCall(
                dialect, "delete",
                Column(dialect, "attributes"),
                Literal(dialect, "weight"),
            ).as_("without_weight"),
        ],
        from_=TableExpression(dialect, "products"),
    )
    sql, params = query.to_sql()
    print(f"\n--- Delete key (delete function) ---")
    print(f"SQL: {sql}")
    print(f"Params: {params}")
    result = backend.execute(sql, params, options=opts)
    print(f"Results: {result.data}")

    # Example 11: Update hstore data using UpdateExpression
    # Add/update a key using || operator in UPDATE
    dml_opts = ExecutionOptions(stmt_type=StatementType.DML)
    update_expr = UpdateExpression(
        dialect=dialect,
        table="products",
        assignments={
            "attributes": BinaryExpression(
                dialect, "||",
                Column(dialect, "attributes"),
                Literal(dialect, "discount=>true").cast("hstore"),
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
    print(f"\n--- UPDATE with hstore concatenation ---")
    print(f"SQL: {sql}")
    print(f"Params: {params}")
    result = backend.execute(sql, params, options=dml_opts)
    print(f"Updated rows: {result.affected_rows}")

    # Verify the update
    query = QueryExpression(
        dialect=dialect,
        select=[
            Column(dialect, "name"),
            Subquery(dialect, BinaryExpression(
                dialect, "->",
                Column(dialect, "attributes"),
                Literal(dialect, "discount"),
            )).as_("discount"),
        ],
        from_=TableExpression(dialect, "products"),
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
drop_expr = DropTableExpression(dialect=dialect, table_name="products", if_exists=True)
sql, params = drop_expr.to_sql()
backend.execute(sql, params)
backend.disconnect()
