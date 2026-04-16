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
    QueryExpression,
    TableExpression,
    CreateMaterializedViewExpression,
    DropMaterializedViewExpression,
    RefreshMaterializedViewExpression,
    GroupByHavingClause,
    FunctionCall,
)
from rhosocial.activerecord.backend.expression.core import Literal, Column, WildcardExpression
from rhosocial.activerecord.backend.expression.statements import (
    ColumnDefinition,
    ColumnConstraint,
    ColumnConstraintType,
)
from rhosocial.activerecord.backend.options import ExecutionOptions
from rhosocial.activerecord.backend.schema import StatementType

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

dql_options = ExecutionOptions(stmt_type=StatementType.DQL)

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

# Build the summary query with aggregations
summary_query = QueryExpression(
    dialect=dialect,
    select=[
        Column(dialect, 'product_id'),
        FunctionCall(dialect, 'COUNT', WildcardExpression(dialect)).as_('total_sales'),
        FunctionCall(dialect, 'SUM', Column(dialect, 'amount')).as_('total_amount'),
        FunctionCall(dialect, 'AVG', Column(dialect, 'amount')).as_('avg_amount'),
    ],
    from_=TableExpression(dialect, 'sales'),
    group_by_having=GroupByHavingClause(
        dialect,
        group_by=[Column(dialect, 'product_id')],
    ),
)

create_mv = CreateMaterializedViewExpression(
    dialect=dialect,
    view_name='sales_summary',
    query=summary_query,
)
sql, params = create_mv.to_sql()
print(f"Create MV SQL: {sql}")
backend.execute(sql, params)

# Verify the materialized view content
verify_query = QueryExpression(
    dialect=dialect,
    select=[WildcardExpression(dialect)],
    from_=TableExpression(dialect, 'sales_summary'),
    order_by=[Column(dialect, 'product_id')],
)
sql, params = verify_query.to_sql()
result = backend.execute(sql, params, options=dql_options)
print("Materialized view result:")
for row in result.data or []:
    print(f"  {row}")

# ============================================================
# SECTION: Refresh Materialized View
# ============================================================
# Refresh to update the data

# Insert new data
insert_new = InsertExpression(
    dialect=dialect,
    into='sales',
    columns=['product_id', 'amount', 'sale_date'],
    source=ValuesSource(
        dialect,
        [[Literal(dialect, '1'), Literal(dialect, '300.00'), Literal(dialect, "'2024-01-05'")]],
    ),
)
sql, params = insert_new.to_sql()
backend.execute(sql, params)

# Data in view is stale - refresh to see new data
refresh_mv = RefreshMaterializedViewExpression(
    dialect=dialect,
    view_name='sales_summary',
)
sql, params = refresh_mv.to_sql()
backend.execute(sql, params)

# Verify after refresh
sql, params = verify_query.to_sql()
result = backend.execute(sql, params, options=dql_options)
print("After refresh:")
for row in result.data or []:
    print(f"  {row}")

# ============================================================
# SECTION: Create with NO DATA
# ============================================================
# Use WITH NO DATA to create without populating

daily_query = QueryExpression(
    dialect=dialect,
    select=[
        Column(dialect, 'sale_date'),
        FunctionCall(dialect, 'SUM', Column(dialect, 'amount')).as_('daily_total'),
    ],
    from_=TableExpression(dialect, 'sales'),
    group_by_having=GroupByHavingClause(
        dialect,
        group_by=[Column(dialect, 'sale_date')],
    ),
)

create_mv_nodata = CreateMaterializedViewExpression(
    dialect=dialect,
    view_name='sales_daily',
    query=daily_query,
    with_data=False,
)
sql, params = create_mv_nodata.to_sql()
backend.execute(sql, params)

# Verify WITH NO DATA - query should fail or return empty
nodata_query = QueryExpression(
    dialect=dialect,
    select=[WildcardExpression(dialect)],
    from_=TableExpression(dialect, 'sales_daily'),
)
sql, params = nodata_query.to_sql()
try:
    result = backend.execute(sql, params, options=dql_options)
    print(f"WITH NO DATA result: {result.data}")
except Exception as e:
    print(f"WITH NO DATA query error (expected): {e}")

# ============================================================
# SECTION: Drop Materialized View
# ============================================================
drop_mv1 = DropMaterializedViewExpression(
    dialect=dialect,
    view_name='sales_summary',
    if_exists=True,
)
sql, params = drop_mv1.to_sql()
backend.execute(sql, params)

drop_mv2 = DropMaterializedViewExpression(
    dialect=dialect,
    view_name='sales_daily',
    if_exists=True,
)
sql, params = drop_mv2.to_sql()
backend.execute(sql, params)

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
# 1. Use CreateMaterializedViewExpression to create MVs
# 2. Use RefreshMaterializedViewExpression to update MV data
# 3. Use with_data=False for WITH NO DATA (lazy population)
# 4. Use DropMaterializedViewExpression to drop MVs
# 5. Requires PostgreSQL 9.3+
# 6. Use concurrent=True for CONCURRENTLY refresh (PG 9.4+)
