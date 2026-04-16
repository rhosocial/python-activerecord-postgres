"""
Materialized Views - PostgreSQL 9.3+.

This example demonstrates:
1. Creating a materialized view
2. Refreshing materialized views
3. Using WITH NO DATA for fast definition
4. Materialized view with aggregations
"""

# ============================================================
# SECTION: Setup (necessary for execution, reference only)
# ============================================================
import os
from rhosocial.activerecord.backend.impl.postgres import PostgresBackend
from rhosocial.activerecord.backend.impl.postgres.config import PostgresConnectionConfig
from rhosocial.activerecord.backend.expression import (
    CreateTableExpression,
    InsertExpression,
    ValuesSource,
    DropTableExpression,
)
from rhosocial.activerecord.backend.expression.core import Literal
from rhosocial.activerecord.backend.expression.statements import (
    ColumnDefinition,
    ColumnConstraint,
    ColumnConstraintType,
)

config = PostgresConnectionConfig(
    host=os.getenv('PG_HOST', 'localhost'),
    port=int(os.getenv('PG_PORT', 5432)),
    database=os.getenv('PG_DATABASE', 'test'),
    username=os.getenv('PG_USERNAME', 'postgres'),
    password=os.getenv('PG_PASSWORD', ''),
)
backend = PostgresBackend(connection_config=config)
backend.connect()
dialect = backend.dialect

drop_table = DropTableExpression(
    dialect=dialect,
    table_name='sales',
    if_exists=True,
    cascade=True,
)
sql, params = drop_table.to_sql()
backend.execute(sql, params)

create_table = CreateTableExpression(
    dialect=dialect,
    table_name='sales',
    columns=[
        ColumnDefinition(
            'id',
            'SERIAL',
            constraints=[
                ColumnConstraint(ColumnConstraintType.PRIMARY_KEY),
                ColumnConstraint(ColumnConstraintType.NOT_NULL),
            ],
        ),
        ColumnDefinition('product_id', 'INT', constraints=[
            ColumnConstraint(ColumnConstraintType.NOT_NULL),
        ]),
        ColumnDefinition('amount', 'DECIMAL(10,2)', constraints=[
            ColumnConstraint(ColumnConstraintType.NOT_NULL),
        ]),
        ColumnDefinition('sale_date', 'DATE', constraints=[
            ColumnConstraint(ColumnConstraintType.NOT_NULL),
        ]),
    ],
    if_not_exists=True,
)
sql, params = create_table.to_sql()
backend.execute(sql, params)

insert_expr = InsertExpression(
    dialect=dialect,
    into='sales',
    columns=['product_id', 'amount', 'sale_date'],
    source=ValuesSource(
        dialect,
        [
            [Literal(dialect, '1'), Literal(dialect, '100.00'), Literal(dialect, "'2024-01-01'")],
            [Literal(dialect, '1'), Literal(dialect, '150.00'), Literal(dialect, "'2024-01-02'")],
            [Literal(dialect, '2'), Literal(dialect, '200.00'), Literal(dialect, "'2024-01-01'")],
            [Literal(dialect, '2'), Literal(dialect, '250.00'), Literal(dialect, "'2024-01-03'")],
            [Literal(dialect, '1'), Literal(dialect, '180.00'), Literal(dialect, "'2024-01-04'")],
        ],
    ),
)
sql, params = insert_expr.to_sql()
backend.execute(sql, params)

# ============================================================
# SECTION: Create Materialized View
# ============================================================
# Materialized views store the query result physically
# Note: Expression API does not yet cover CREATE MATERIALIZED VIEW.
# The following uses raw SQL for MV operations.

backend.execute("""
    CREATE MATERIALIZED VIEW sales_summary AS
    SELECT
        product_id,
        COUNT(*) AS total_sales,
        SUM(amount) AS total_amount,
        AVG(amount) AS avg_amount
    FROM sales
    GROUP BY product_id
""")

result = backend.execute("SELECT * FROM sales_summary ORDER BY product_id")
print(f"Materialized view result:")
for row in result.data or []:
    print(f"  {row}")

# ============================================================
# SECTION: Refresh Materialized View
# ============================================================
# Refresh to update the data

backend.execute("""
    INSERT INTO sales (product_id, amount, sale_date)
    VALUES (1, 300.00, '2024-01-05')
""")

# Data in view is stale - refresh to see new data
# Note: REFRESH MATERIALIZED VIEW also requires raw SQL.
backend.execute("REFRESH MATERIALIZED VIEW sales_summary")

result = backend.execute("SELECT * FROM sales_summary ORDER BY product_id")
print(f"After refresh:")
for row in result.data or []:
    print(f"  {row}")

# ============================================================
# SECTION: Create with NO DATA
# ============================================================
# Use WITH NO DATA to create without populating

backend.execute("""
    CREATE MATERIALIZED VIEW sales_daily AS
    SELECT sale_date, SUM(amount) AS daily_total
    FROM sales
    GROUP BY sale_date
    WITH NO DATA
""")

result = backend.execute("SELECT * FROM sales_daily")
print(f"WITH NO DATA result: {result.data}")

# ============================================================
# SECTION: Drop Materialized View
# ============================================================
backend.execute("DROP MATERIALIZED VIEW IF EXISTS sales_summary")
backend.execute("DROP MATERIALIZED VIEW IF EXISTS sales_daily")

# ============================================================
# SECTION: Teardown (necessary for execution, reference only)
# ============================================================
drop_table = DropTableExpression(
    dialect=dialect,
    table_name='sales',
    if_exists=True,
    cascade=True,
)
sql, params = drop_table.to_sql()
backend.execute(sql, params)
backend.disconnect()

# ============================================================
# SECTION: Summary
# ============================================================
# Key points:
# 1. Requires PostgreSQL 9.3+
# 2. Data is materialized (physically stored)
# 3. Use REFRESH MATERIALIZED VIEW to update
# 4. Use WITH NO DATA for lazy population
# 5. Cannot be queried with other tables (not like regular views)
# 6. Use CREATE UNIQUE MATERIALIZED VIEW for concurrent refresh