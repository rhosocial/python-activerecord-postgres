# src/rhosocial/activerecord/backend/impl/postgres/examples/extensions/intarray.py
"""
intarray extension - integer array operations and indexing.

This example demonstrates:
1. Check if intarray extension is available
2. CREATE EXTENSION and create table with integer array column
3. Use @> (contains) and && (overlap) operators
4. Use idx(), sort(), uniq() functions
5. Create GiST index for fast array queries
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
from rhosocial.activerecord.backend.expression import DropTableExpression

drop_expr = DropTableExpression(dialect=dialect, table_name="tags", if_exists=True)
sql, params = drop_expr.to_sql()
backend.execute(sql, params)

# ============================================================
# SECTION: Business Logic (the pattern to learn)
# ============================================================
from rhosocial.activerecord.backend.impl.postgres.expression import (
    PostgresCreateExtensionExpression,
)
from rhosocial.activerecord.backend.expression import (
    CreateTableExpression,
    ColumnDefinition,
    ColumnConstraint,
    ColumnConstraintType,
    CreateIndexExpression,
    QueryExpression,
    TableExpression,
    Column,
    OrderByClause,
)
from rhosocial.activerecord.backend.expression.core import Literal, FunctionCall
from rhosocial.activerecord.backend.expression.operators import (
    BinaryExpression,
)
from rhosocial.activerecord.backend.expression.statements.dml import (
    InsertExpression,
)
from rhosocial.activerecord.backend.expression.statements import (
    ValuesSource,
)
from rhosocial.activerecord.backend.options import ExecutionOptions
from rhosocial.activerecord.backend.schema import StatementType

# Check if intarray extension is available
available = dialect.is_extension_available("intarray")
installed = dialect.is_extension_installed("intarray")
print(f"Extension check: intarray available = {available}, installed = {installed}")

# Create extension using expression
if available and not installed:
    create_ext = PostgresCreateExtensionExpression(
        dialect=dialect,
        name="intarray",
    )
    sql, params = create_ext.to_sql()
    print("\n--- CREATE EXTENSION ---")
    print(f"SQL: {sql}")
    backend.execute(sql, params)
    # Re-detect extensions after creation
    backend.introspect_and_adapt()

# Re-check after creation
installed = dialect.is_extension_installed("intarray")

if installed:
    # Example 1: Create table with integer array column
    columns = [
        ColumnDefinition(
            name="id",
            data_type="SERIAL",
            constraints=[
                ColumnConstraint(ColumnConstraintType.PRIMARY_KEY),
            ],
        ),
        ColumnDefinition(
            name="name",
            data_type="VARCHAR(100)",
            constraints=[
                ColumnConstraint(ColumnConstraintType.NOT_NULL),
            ],
        ),
        ColumnDefinition(
            name="tag_ids",
            data_type="INTEGER[]",
        ),
    ]

    create_expr = CreateTableExpression(
        dialect=dialect,
        table_name="tags",
        columns=columns,
        if_not_exists=True,
    )
    sql, params = create_expr.to_sql()
    print("\n--- CREATE TABLE ---")
    print(f"SQL: {sql}")
    backend.execute(sql, params)

    # Example 2: Insert integer array data
    insert_expr = InsertExpression(
        dialect=dialect,
        into="tags",
        columns=["name", "tag_ids"],
        source=ValuesSource(
            dialect,
            [
                [Literal(dialect, "Article A"), Literal(dialect, "{1,2,3}")],
                [Literal(dialect, "Article B"), Literal(dialect, "{2,3,4}")],
                [Literal(dialect, "Article C"), Literal(dialect, "{5,6,7}")],
                [Literal(dialect, "Article D"), Literal(dialect, "{1,3,5,7}")],
                [Literal(dialect, "Article E"), Literal(dialect, "{2,4,6}")],
            ],
        ),
    )
    sql, params = insert_expr.to_sql()
    print("\n--- INSERT integer array data ---")
    print(f"SQL: {sql}")
    print(f"Params: {params}")
    backend.execute(sql, params)

    opts = ExecutionOptions(stmt_type=StatementType.DQL)

    # Example 3: Contains operator (@>)
    # Find articles that contain ALL of the specified tag IDs
    query = QueryExpression(
        dialect=dialect,
        select=[Column(dialect, "name"), Column(dialect, "tag_ids")],
        from_=TableExpression(dialect, "tags"),
        where=BinaryExpression(
            dialect, "@>",
            Column(dialect, "tag_ids"),
            Literal(dialect, "{1,3}").cast("integer[]"),
        ),
    )
    sql, params = query.to_sql()
    result = backend.execute(sql, params, options=opts)
    print("\n--- Contains operator (@>) ---")
    print("Find articles with BOTH tag 1 AND 3:")
    print(f"SQL: {sql}")
    print(f"Results: {result.data}")

    # Example 4: Overlap operator (&&)
    # Find articles that have ANY of the specified tag IDs
    query = QueryExpression(
        dialect=dialect,
        select=[Column(dialect, "name"), Column(dialect, "tag_ids")],
        from_=TableExpression(dialect, "tags"),
        where=BinaryExpression(
            dialect, "&&",
            Column(dialect, "tag_ids"),
            Literal(dialect, "{1,6}").cast("integer[]"),
        ),
    )
    sql, params = query.to_sql()
    result = backend.execute(sql, params, options=opts)
    print("\n--- Overlap operator (&&) ---")
    print("Find articles with tag 1 OR tag 6:")
    print(f"SQL: {sql}")
    print(f"Results: {result.data}")

    # Example 5: idx() function - find position of element in array
    # idx(array, element) returns 0 if not found (1-based index)
    idx_func = FunctionCall(dialect, "idx", Column(dialect, "tag_ids"), Literal(dialect, 3)).as_("pos_of_3")

    query = QueryExpression(
        dialect=dialect,
        select=[Column(dialect, "name"), idx_func],
        from_=TableExpression(dialect, "tags"),
        order_by=OrderByClause(dialect, expressions=[Column(dialect, "name")]),
    )
    sql, params = query.to_sql()
    result = backend.execute(sql, params, options=opts)
    print("\n--- idx() function ---")
    print("Position of element 3 in each array:")
    print(f"SQL: {sql}")
    print(f"Results: {result.data}")

    # Example 6: sort() and uniq() functions
    # sort() sorts the array, uniq() removes duplicates (works on sorted arrays)
    sort_func = FunctionCall(dialect, "sort", Column(dialect, "tag_ids")).as_("sorted")
    unique_sorted_func = FunctionCall(
        dialect, "uniq", FunctionCall(dialect, "sort", Column(dialect, "tag_ids"))
    ).as_("unique_sorted")

    query = QueryExpression(
        dialect=dialect,
        select=[
            Column(dialect, "name"),
            sort_func,
            unique_sorted_func,
        ],
        from_=TableExpression(dialect, "tags"),
        where=Column(dialect, "id") == Literal(dialect, 4),
    )
    sql, params = query.to_sql()
    result = backend.execute(sql, params, options=opts)
    print("\n--- sort() and uniq() functions ---")
    print(f"SQL: {sql}")
    print(f"Results: {result.data}")

    # Example 7: Create GiST index for fast intarray queries
    # gist__int_ops operator class enables fast @>, &&, = queries
    create_idx = CreateIndexExpression(
        dialect=dialect,
        index_name="idx_tags_tag_ids",
        table_name="tags",
        columns=["tag_ids"],
        index_type="GIST",
        if_not_exists=True,
    )
    sql, params = create_idx.to_sql()
    print("\n--- CREATE GIST INDEX ---")
    print(f"SQL: {sql}")
    backend.execute(sql, params)
    print("GiST index created: idx_tags_tag_ids")

else:
    print("\nSkipping execution - intarray not available on this server")
    print("To enable intarray, run: CREATE EXTENSION intarray;")

# ============================================================
# SECTION: Teardown (necessary for execution, reference only)
# ============================================================
drop_expr = DropTableExpression(dialect=dialect, table_name="tags", if_exists=True)
sql, params = drop_expr.to_sql()
backend.execute(sql, params)
backend.disconnect()
