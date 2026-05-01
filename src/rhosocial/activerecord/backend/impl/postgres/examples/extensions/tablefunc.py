# src/rhosocial/activerecord/backend/impl/postgres/examples/extensions/tablefunc.py
"""
tablefunc extension - pivot tables, tree traversal, and random data.

This example demonstrates:
1. Check if tablefunc extension is available
2. Create extension using PostgresCreateExtensionExpression
3. Create table with sales data and hierarchical tree data
4. Use normal_rand() to generate normally distributed random values
5. Use crosstab() to produce pivot table displays
6. Use connectby() to traverse hierarchical tree structures
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

for table_name in ["monthly_sales", "org_tree"]:
    drop_expr = DropTableExpression(dialect=dialect, table_name=table_name, if_exists=True)
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
    InsertExpression,
    QueryExpression,
)
from rhosocial.activerecord.backend.expression.statements import ValuesSource
from rhosocial.activerecord.backend.expression.core import Literal
from rhosocial.activerecord.backend.impl.postgres.functions.tablefunc import (
    normal_rand,
    crosstab,
    connectby,
)
from rhosocial.activerecord.backend.options import ExecutionOptions
from rhosocial.activerecord.backend.schema import StatementType

# Check if tablefunc extension is available
available = dialect.is_extension_available("tablefunc")
installed = dialect.is_extension_installed("tablefunc")
print(f"Extension: tablefunc available = {available}, installed = {installed}")

# Create extension using expression
if available and not installed:
    create_ext = PostgresCreateExtensionExpression(
        dialect=dialect,
        name="tablefunc",
    )
    sql, params = create_ext.to_sql()
    print("\n--- CREATE EXTENSION ---")
    print(f"SQL: {sql}")
    backend.execute(sql, params)
    # Re-detect extensions after creation
    backend.introspect_and_adapt()

# Re-check after creation
installed = dialect.is_extension_installed("tablefunc")

if installed:
    opts = ExecutionOptions(stmt_type=StatementType.DQL)

    # Example 1: Create table with monthly sales data
    print("\n--- Creating monthly_sales table ---")
    columns = [
        ColumnDefinition(
            name="id",
            data_type="SERIAL",
            constraints=[ColumnConstraint(ColumnConstraintType.PRIMARY_KEY)],
        ),
        ColumnDefinition(name="month", data_type="TEXT"),
        ColumnDefinition(name="category", data_type="TEXT"),
        ColumnDefinition(name="amount", data_type="INT"),
    ]
    create_expr = CreateTableExpression(
        dialect=dialect,
        table_name="monthly_sales",
        columns=columns,
        if_not_exists=True,
    )
    sql, params = create_expr.to_sql()
    print(f"SQL: {sql}")
    backend.execute(sql, params)

    # Insert sales data
    insert_expr = InsertExpression(
        dialect=dialect,
        into="monthly_sales",
        columns=["month", "category", "amount"],
        source=ValuesSource(
            dialect,
            [
                [Literal(dialect, "Jan"), Literal(dialect, "Electronics"), Literal(dialect, 100)],
                [Literal(dialect, "Jan"), Literal(dialect, "Clothing"), Literal(dialect, 200)],
                [Literal(dialect, "Feb"), Literal(dialect, "Electronics"), Literal(dialect, 150)],
                [Literal(dialect, "Feb"), Literal(dialect, "Clothing"), Literal(dialect, 250)],
                [Literal(dialect, "Mar"), Literal(dialect, "Electronics"), Literal(dialect, 120)],
                [Literal(dialect, "Mar"), Literal(dialect, "Clothing"), Literal(dialect, 180)],
            ],
        ),
    )
    sql, params = insert_expr.to_sql()
    print("\n--- INSERT sales data ---")
    print(f"SQL: {sql}")
    print(f"Params: {params}")
    backend.execute(sql, params)

    # Example 2: Create hierarchical tree table
    print("\n--- Creating org_tree table ---")
    tree_columns = [
        ColumnDefinition(
            name="id",
            data_type="SERIAL",
            constraints=[ColumnConstraint(ColumnConstraintType.PRIMARY_KEY)],
        ),
        ColumnDefinition(name="name", data_type="TEXT"),
        ColumnDefinition(name="parent_id", data_type="INT"),
    ]
    create_tree = CreateTableExpression(
        dialect=dialect,
        table_name="org_tree",
        columns=tree_columns,
        if_not_exists=True,
    )
    sql, params = create_tree.to_sql()
    print(f"SQL: {sql}")
    backend.execute(sql, params)

    # Insert hierarchical data (CEO -> VP Engineering, VP Sales -> Engineers)
    insert_tree = InsertExpression(
        dialect=dialect,
        into="org_tree",
        columns=["name", "parent_id"],
        source=ValuesSource(
            dialect,
            [
                [Literal(dialect, "CEO"), Literal(dialect, None)],
                [Literal(dialect, "VP Engineering"), Literal(dialect, 1)],
                [Literal(dialect, "VP Sales"), Literal(dialect, 1)],
                [Literal(dialect, "Engineer A"), Literal(dialect, 2)],
                [Literal(dialect, "Engineer B"), Literal(dialect, 2)],
            ],
        ),
    )
    sql, params = insert_tree.to_sql()
    print("\n--- INSERT org tree data ---")
    print(f"SQL: {sql}")
    print(f"Params: {params}")
    backend.execute(sql, params)

    # Example 3: normal_rand - Generate normally distributed random values
    # normal_rand(num_values, mean, stddev) returns a set of random values
    # following a normal (Gaussian) distribution
    rand_func = normal_rand(dialect, 5, 100.0, 15.0)
    query = QueryExpression(
        dialect=dialect,
        select=[rand_func.as_("random_value")],
    )
    sql, params = query.to_sql()
    print("\n--- normal_rand: Generate 5 random values (mean=100, stddev=15) ---")
    print(f"SQL: {sql}")
    result = backend.execute(sql, params, options=opts)
    print(f"Results: {result.data}")

    # Example 4: crosstab - Produce pivot table display
    # crosstab(source_sql) takes a SQL query that returns 3 columns:
    # row_name, category, value. It pivots the data into a crosstab format.
    # Note: crosstab returns SETOF record, so in practice you would use:
    #   SELECT * FROM crosstab('...') AS ct(month TEXT, electronics INT, clothing INT)
    # Here we demonstrate the function call generation; the composite
    # result type is returned as a single column.
    crosstab_func = crosstab(
        dialect,
        "SELECT month, category, amount FROM monthly_sales ORDER BY 1",
    )
    query = QueryExpression(
        dialect=dialect,
        select=[crosstab_func.as_("pivot_row")],
    )
    sql, params = query.to_sql()
    print("\n--- crosstab: Pivot sales data by month and category ---")
    print(f"SQL: {sql}")
    result = backend.execute(sql, params, options=opts)
    print(f"Results: {result.data}")

    # Example 5: crosstab with categories - Two-argument form
    # The two-argument form ensures consistent column ordering by providing
    # a query that defines the category set and order.
    crosstab_func2 = crosstab(
        dialect,
        "SELECT month, category, amount FROM monthly_sales ORDER BY 1",
        "SELECT DISTINCT category FROM monthly_sales ORDER BY 1",
    )
    query = QueryExpression(
        dialect=dialect,
        select=[crosstab_func2.as_("pivot_row")],
    )
    sql, params = query.to_sql()
    print("\n--- crosstab (with categories): Consistent column ordering ---")
    print(f"SQL: {sql}")
    result = backend.execute(sql, params, options=opts)
    print(f"Results: {result.data}")

    # Example 6: connectby - Traverse hierarchical tree structure
    # connectby(table_name, key_column, parent_column, start_value, max_depth)
    # traverses the tree starting from the specified root, following
    # parent-child relationships up to the given depth.
    connectby_func = connectby(
        dialect,
        "org_tree",
        "id",
        "parent_id",
        "1",
        max_depth=3,
    )
    query = QueryExpression(
        dialect=dialect,
        select=[connectby_func.as_("tree_row")],
    )
    sql, params = query.to_sql()
    print("\n--- connectby: Traverse org tree from CEO (id=1), depth=3 ---")
    print(f"SQL: {sql}")
    result = backend.execute(sql, params, options=opts)
    print(f"Results: {result.data}")

    # Example 7: connectby with branch delimiter
    # Adding branch_delim produces an additional branch column showing
    # the path from root to each node, with keys separated by the delimiter.
    connectby_func2 = connectby(
        dialect,
        "org_tree",
        "id",
        "parent_id",
        "1",
        max_depth=3,
        branch_delim="~",
    )
    query = QueryExpression(
        dialect=dialect,
        select=[connectby_func2.as_("tree_row")],
    )
    sql, params = query.to_sql()
    print("\n--- connectby (with branch delim): Show path from root ---")
    print(f"SQL: {sql}")
    result = backend.execute(sql, params, options=opts)
    print(f"Results: {result.data}")

else:
    print("\nSkipping - tablefunc not available on this server")
    print("To enable tablefunc, run: CREATE EXTENSION tablefunc;")

# ============================================================
# SECTION: Teardown (necessary for execution, reference only)
# ============================================================
for table_name in ["monthly_sales", "org_tree"]:
    drop_expr = DropTableExpression(dialect=dialect, table_name=table_name, if_exists=True)
    sql, params = drop_expr.to_sql()
    backend.execute(sql, params)
backend.disconnect()
