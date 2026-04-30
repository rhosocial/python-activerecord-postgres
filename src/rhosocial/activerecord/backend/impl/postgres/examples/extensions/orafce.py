# src/rhosocial/activerecord/backend/impl/postgres/examples/extensions/orafce.py
"""
orafce extension - Oracle-compatible functions for PostgreSQL.

This example demonstrates:
1. Check if orafce extension is available
2. CREATE EXTENSION and create table with date/numeric/text columns
3. Insert data with NULL values
4. Use ADD_MONTHS, LAST_DAY, NVL, DECODE, INSTR functions
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

drop_expr = DropTableExpression(dialect=dialect, table_name="employees", if_exists=True)
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
from rhosocial.activerecord.backend.options import ExecutionOptions
from rhosocial.activerecord.backend.schema import StatementType
from rhosocial.activerecord.backend.impl.postgres.functions.orafce import (
    add_months,
    last_day,
    nvl,
    decode,
    instr,
)

# Check if orafce extension is available
available = dialect.is_extension_available("orafce")
installed = dialect.is_extension_installed("orafce")
print(f"Extension check: orafce available = {available}, installed = {installed}")

# Create extension using expression
if available and not installed:
    create_ext = PostgresCreateExtensionExpression(
        dialect=dialect,
        name="orafce",
    )
    sql, params = create_ext.to_sql()
    print("\n--- CREATE EXTENSION ---")
    print(f"SQL: {sql}")
    backend.execute(sql, params)
    # Re-detect extensions after creation
    backend.introspect_and_adapt()

# Re-check after creation
installed = dialect.is_extension_installed("orafce")

if installed:
    # Example 1: Create table with date, numeric, and text columns
    columns = [
        ColumnDefinition(
            name="id",
            data_type="SERIAL",
            constraints=[
                ColumnConstraint(ColumnConstraintType.PRIMARY_KEY),
            ],
        ),
        ColumnDefinition(name="name", data_type="TEXT"),
        ColumnDefinition(name="hire_date", data_type="DATE"),
        ColumnDefinition(name="bonus", data_type="NUMERIC"),
        ColumnDefinition(name="description", data_type="TEXT"),
    ]

    create_expr = CreateTableExpression(
        dialect=dialect,
        table_name="employees",
        columns=columns,
        if_not_exists=True,
    )
    sql, params = create_expr.to_sql()
    print("\n--- CREATE TABLE ---")
    print(f"SQL: {sql}")
    backend.execute(sql, params)

    # Example 2: Insert data with NULL values
    insert_expr = InsertExpression(
        dialect=dialect,
        into="employees",
        columns=["name", "hire_date", "bonus", "description"],
        source=ValuesSource(
            dialect,
            [
                [
                    Literal(dialect, "Alice"),
                    Literal(dialect, "2024-01-15"),
                    Literal(dialect, 5000.00),
                    Literal(dialect, "senior engineer"),
                ],
                [
                    Literal(dialect, "Bob"),
                    Literal(dialect, "2024-06-30"),
                    Literal(dialect, None),
                    Literal(dialect, "junior developer"),
                ],
                [
                    Literal(dialect, "Charlie"),
                    Literal(dialect, "2024-12-25"),
                    Literal(dialect, 3000.00),
                    Literal(dialect, None),
                ],
            ],
        ),
    )
    sql, params = insert_expr.to_sql()
    print("\n--- INSERT data ---")
    print(f"SQL: {sql}")
    print(f"Params: {params}")
    backend.execute(sql, params)

    opts = ExecutionOptions(stmt_type=StatementType.DQL)

    # Example 3: ADD_MONTHS — add 6 months to each hire date
    add_months_expr = add_months(
        dialect,
        Column(dialect, "hire_date"),
        Literal(dialect, 6),
    ).as_("review_date")

    query = QueryExpression(
        dialect=dialect,
        select=[Column(dialect, "name"), add_months_expr],
        from_=TableExpression(dialect, "employees"),
        order_by=[Column(dialect, "id")],
    )
    sql, params = query.to_sql()
    print("\n--- ADD_MONTHS ---")
    print(f"SQL: {sql}")
    result = backend.execute(sql, params, options=opts)
    print(f"Results: {result.data}")

    # Example 4: LAST_DAY — get last day of the month for each hire date
    last_day_expr = last_day(
        dialect,
        Column(dialect, "hire_date"),
    ).as_("month_end")

    query = QueryExpression(
        dialect=dialect,
        select=[Column(dialect, "name"), last_day_expr],
        from_=TableExpression(dialect, "employees"),
        order_by=[Column(dialect, "id")],
    )
    sql, params = query.to_sql()
    print("\n--- LAST_DAY ---")
    print(f"SQL: {sql}")
    result = backend.execute(sql, params, options=opts)
    print(f"Results: {result.data}")

    # Example 5: NVL — replace NULL bonus with 0
    nvl_expr = nvl(
        dialect,
        Column(dialect, "bonus"),
        Literal(dialect, 0.0),
    ).as_("safe_bonus")

    query = QueryExpression(
        dialect=dialect,
        select=[Column(dialect, "name"), nvl_expr],
        from_=TableExpression(dialect, "employees"),
        order_by=[Column(dialect, "id")],
    )
    sql, params = query.to_sql()
    print("\n--- NVL ---")
    print(f"SQL: {sql}")
    result = backend.execute(sql, params, options=opts)
    print(f"Results: {result.data}")

    # Example 6: DECODE — map names to categories
    decode_expr = decode(
        dialect,
        Column(dialect, "name"),
        Literal(dialect, "Alice"),
        Literal(dialect, "Senior"),
        Literal(dialect, "Bob"),
        Literal(dialect, "Junior"),
        default=Literal(dialect, "Other"),
    ).as_("category")

    query = QueryExpression(
        dialect=dialect,
        select=[Column(dialect, "name"), decode_expr],
        from_=TableExpression(dialect, "employees"),
        order_by=[Column(dialect, "id")],
    )
    sql, params = query.to_sql()
    print("\n--- DECODE ---")
    print(f"SQL: {sql}")
    result = backend.execute(sql, params, options=opts)
    print(f"Results: {result.data}")

    # Example 7: INSTR — find position of substring in description
    instr_expr = instr(
        dialect,
        Column(dialect, "description"),
        Literal(dialect, "engineer"),
    ).as_("pos")

    query = QueryExpression(
        dialect=dialect,
        select=[
            Column(dialect, "name"),
            Column(dialect, "description"),
            instr_expr,
        ],
        from_=TableExpression(dialect, "employees"),
        order_by=[Column(dialect, "id")],
    )
    sql, params = query.to_sql()
    print("\n--- INSTR ---")
    print(f"SQL: {sql}")
    result = backend.execute(sql, params, options=opts)
    print(f"Results: {result.data}")

else:
    print("\nSkipping execution - orafce extension not available on this server")
    print("To enable orafce, install it and run: CREATE EXTENSION orafce;")

# ============================================================
# SECTION: Teardown (necessary for execution, reference only)
# ============================================================
drop_expr = DropTableExpression(dialect=dialect, table_name="employees", if_exists=True)
sql, params = drop_expr.to_sql()
backend.execute(sql, params)
backend.disconnect()
