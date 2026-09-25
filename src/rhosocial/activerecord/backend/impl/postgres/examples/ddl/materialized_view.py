"""
Materialized Views - PostgreSQL 9.3+.

This example demonstrates:
1. Creating a materialized view with aggregations
2. Column aliases for the materialized view
3. Storage options (WITH clause) for physical storage tuning
4. TABLESPACE assignment
5. WITH DATA vs WITH NO DATA population modes
6. REFRESH MATERIALIZED VIEW (normal)
7. REFRESH MATERIALIZED VIEW CONCURRENTLY (PostgreSQL 9.4+)
8. REFRESH MATERIALIZED VIEW WITH DATA (after NO DATA)
9. Schema-qualified refresh (PostgresRefreshMaterializedViewExpression)
10. DROP MATERIALIZED VIEW with CASCADE and IF EXISTS
11. IF NOT EXISTS (PostgreSQL 9.4+) via PostgresCreateMaterializedViewExpression
12. Schema-qualified create/drop
13. ALTER MATERIALIZED VIEW: RENAME TO / SET SCHEMA / SET () / RESET () / OWNER TO
14. Introspection: list_materialized_views() with is_populated / has_unique_index
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
    CreateIndexExpression,
    GroupByHavingClause,
    FunctionCall,
)
from rhosocial.activerecord.backend.expression.core import Literal, Column, WildcardExpression
from rhosocial.activerecord.backend.expression.query_parts import OrderByClause
from rhosocial.activerecord.backend.expression.statements import (
    ColumnDefinition,
    ColumnConstraint,
    ColumnConstraintType,
)
from rhosocial.activerecord.backend.impl.postgres import PostgresStorageParameter
from rhosocial.activerecord.backend.impl.postgres.expression.ddl import (
    PostgresAlterMaterializedViewExpression,
    PostgresChangeMaterializedViewOwnerAction,
    PostgresCreateMaterializedViewExpression,
    PostgresDropMaterializedViewExpression,
    PostgresRefreshMaterializedViewExpression,
    PostgresRenameMaterializedViewAction,
    PostgresResetMaterializedViewPropertiesAction,
    PostgresSetMaterializedViewPropertiesAction,
    PostgresSetMaterializedViewSchemaAction,
)
from rhosocial.activerecord.backend.options import ExecutionOptions
from rhosocial.activerecord.backend.schema import StatementType

config = PostgresConnectionConfig(
    host=os.getenv('POSTGRES_HOST', 'localhost'),
    port=int(os.getenv('POSTGRES_PORT', 5432)),
    database=os.getenv('POSTGRES_DATABASE', 'test'),
    username=os.getenv('POSTGRES_USER', 'postgres'),
    password=os.getenv('POSTGRES_PASSWORD', ''),
)
backend = PostgresBackend(connection_config=config)
backend.connect()
backend.introspect_and_adapt()
dialect = backend.dialect

dql_options = ExecutionOptions(stmt_type=StatementType.DQL)
ddl_options = ExecutionOptions(stmt_type=StatementType.DDL)

_drop_mv = DropMaterializedViewExpression(
    dialect=dialect, view_name='sales_summary', if_exists=True, cascade=True,
)
backend.execute(*_drop_mv.to_sql())
_drop_mv = DropMaterializedViewExpression(
    dialect=dialect, view_name='sales_daily', if_exists=True, cascade=True,
)
backend.execute(*_drop_mv.to_sql())
_drop_mv = DropMaterializedViewExpression(
    dialect=dialect, view_name='sales_product_summary', if_exists=True, cascade=True,
)
backend.execute(*_drop_mv.to_sql())
drop_table = DropTableExpression(
    dialect=dialect,
    table='sales',
    if_exists=True,
    cascade=True,
)
backend.execute(*drop_table.to_sql())

create_table = CreateTableExpression(
    dialect=dialect,
    table='sales',
    columns=[
        ColumnDefinition(
            dialect,
            'id',
            dialect.parse_type('SERIAL'),
            constraints=[
                ColumnConstraint(dialect, ColumnConstraintType.PRIMARY_KEY),
                ColumnConstraint(dialect, ColumnConstraintType.NOT_NULL),
            ],
        ),
        ColumnDefinition(dialect, 'product_id', dialect.parse_type('INT'), constraints=[
            ColumnConstraint(dialect, ColumnConstraintType.NOT_NULL),
        ]),
        ColumnDefinition(dialect, 'amount', dialect.parse_type('DECIMAL(10,2)'), constraints=[
            ColumnConstraint(dialect, ColumnConstraintType.NOT_NULL),
        ]),
        ColumnDefinition(dialect, 'sale_date', dialect.parse_type('DATE'), constraints=[
            ColumnConstraint(dialect, ColumnConstraintType.NOT_NULL),
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
# SECTION: 1. Create Materialized View with Column Aliases and Storage Options
# ============================================================
# Column aliases rename the output columns of the materialized view.
# Storage options control physical storage behavior via WITH (param = value).

summary_query = QueryExpression(
    dialect=dialect,
    select=[
        Column(dialect, 'product_id'),
        FunctionCall(dialect, 'COUNT', WildcardExpression(dialect)),
        FunctionCall(dialect, 'SUM', Column(dialect, 'amount')),
        FunctionCall(dialect, 'AVG', Column(dialect, 'amount')),
    ],
    from_=TableExpression(dialect, 'sales'),
    group_by_having=GroupByHavingClause(
        dialect,
        group_by=[Column(dialect, 'product_id')],
    ),
)

create_mv = PostgresCreateMaterializedViewExpression(
    dialect=dialect,
    view_name='sales_summary',
    query=summary_query,
    column_aliases=['product_id', 'total_sales', 'total_amount', 'avg_amount'],
    # Storage parameter names are validated against PostgresStorageParameter;
    # unlisted names (toast.* / table access method options) need
    # allow_unlisted_storage_parameters=True.
    storage_options={
        PostgresStorageParameter.FILLFACTOR: 70,
        PostgresStorageParameter.AUTOVACUUM_ENABLED: 'true',
    },
)
sql, params = create_mv.to_sql()
print("Create MV with column aliases and storage options:")
print(f"  SQL: {sql}")
backend.execute(sql, params)

# Verify
verify_query = QueryExpression(
    dialect=dialect,
    select=[WildcardExpression(dialect)],
    from_=TableExpression(dialect, 'sales_summary'),
    order_by=OrderByClause(dialect, [Column(dialect, 'product_id')]),
)
sql, params = verify_query.to_sql()
result = backend.execute(sql, params, options=dql_options)
print("MV result:")
for row in result.data or []:
    print(f"  {row}")

# ============================================================
# SECTION: 2. TABLESPACE (SQL Generation Demo)
# ============================================================
# Tablespace requires an existing tablespace on the server.
# This section generates the SQL to show the syntax; execution is optional.

print()
sales_query = QueryExpression(
    dialect=dialect,
    select=[
        Column(dialect, 'product_id'),
        Column(dialect, 'amount'),
        Column(dialect, 'sale_date'),
    ],
    from_=TableExpression(dialect, 'sales'),
)

mv_tablespace = CreateMaterializedViewExpression(
    dialect=dialect,
    view_name='sales_on_fast_storage',
    query=sales_query,
    tablespace='fast_ssd',
)
sql, params = mv_tablespace.to_sql()
print(f"TABLESPACE example SQL (not executed): {sql}")

# ============================================================
# SECTION: 3. WITH NO DATA — Fast Definition Without Population
# ============================================================
# Create a materialized view without initial data. The MV is registered
# but cannot be queried until a REFRESH populates it.

daily_query = QueryExpression(
    dialect=dialect,
    select=[
        Column(dialect, 'sale_date'),
        FunctionCall(dialect, 'SUM', Column(dialect, 'amount')),
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
    column_aliases=['sale_date', 'daily_total'],
    with_data=False,
)
sql, params = create_mv_nodata.to_sql()
print(f"\nWITH NO DATA SQL: {sql}")
backend.execute(sql, params)

# Confirm it's unqueriable
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
    print(f"WITH NO DATA query error (expected): {type(e).__name__}: {e}")

# ============================================================
# SECTION: 4. REFRESH MATERIALIZED VIEW (Normal)
# ============================================================
# Refresh recalculates the query and atomically replaces the MV content.

# Insert new data so refresh has an effect
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

# Refresh sales_summary
refresh_mv = RefreshMaterializedViewExpression(
    dialect=dialect,
    view_name='sales_summary',
)
sql, params = refresh_mv.to_sql()
print(f"\nREFRESH SQL: {sql}")
backend.execute(sql, params)

sql, params = verify_query.to_sql()
result = backend.execute(sql, params, options=dql_options)
print("After normal refresh:")
for row in result.data or []:
    print(f"  {row}")

# ============================================================
# SECTION: 5. REFRESH MATERIALIZED VIEW CONCURRENTLY (PG 9.4+)
# ============================================================
# CONCURRENTLY allows the MV to be read during the refresh but
# requires a UNIQUE index on every row of the materialized view.

# Create a unique index prerequisite
create_unique_idx = CreateIndexExpression(
    dialect=dialect,
    index_name='sales_summary_product_id_idx',
    table_name='sales_summary',
    columns=['product_id'],
    unique=True,
)
sql, params = create_unique_idx.to_sql()
backend.execute(sql, params, options=ddl_options)

# Insert more data
insert_more = InsertExpression(
    dialect=dialect,
    into='sales',
    columns=['product_id', 'amount', 'sale_date'],
    source=ValuesSource(
        dialect,
        [[Literal(dialect, '2'), Literal(dialect, '400.00'), Literal(dialect, "'2024-01-06'")]],
    ),
)
sql, params = insert_more.to_sql()
backend.execute(sql, params)

refresh_concurrent = RefreshMaterializedViewExpression(
    dialect=dialect,
    view_name='sales_summary',
    concurrent=True,
)
sql, params = refresh_concurrent.to_sql()
print(f"\nREFRESH CONCURRENTLY SQL: {sql}")
backend.execute(sql, params)

sql, params = verify_query.to_sql()
result = backend.execute(sql, params, options=dql_options)
print("After CONCURRENTLY refresh:")
for row in result.data or []:
    print(f"  {row}")

# ============================================================
# SECTION: 6. REFRESH WITH DATA — Populate a NO DATA View
# ============================================================
# An MV created with WITH NO DATA can be populated later via
# REFRESH ... WITH DATA.

refresh_with_data = RefreshMaterializedViewExpression(
    dialect=dialect,
    view_name='sales_daily',
    with_data=True,
)
sql, params = refresh_with_data.to_sql()
print(f"\nREFRESH WITH DATA SQL: {sql}")
backend.execute(sql, params)

# Now the NO-DATA view is queryable
sql, params = nodata_query.to_sql()
result = backend.execute(sql, params, options=dql_options)
print("After REFRESH WITH DATA (sales_daily):")
for row in result.data or []:
    print(f"  {row}")

# ============================================================
# SECTION: 7. Schema-Qualified Refresh (PostgresRefreshMaterializedViewExpression)
# ============================================================
# The PostgreSQL backend provides an extended refresh expression that
# supports specifying the schema for the materialized view.
# (PostgresRefreshMaterializedViewExpression is imported at the top of this file.)

# Refresh with schema (public is PG's default schema)
pg_refresh = PostgresRefreshMaterializedViewExpression(
    dialect=dialect,
    name='sales_summary',
    schema='public',
)
sql, params = pg_refresh.to_sql()
print(f"\nSchema-qualified REFRESH SQL: {sql}")
backend.execute(sql, params)

# Also supports backward-compatible aliases: name=, concurrently=
pg_refresh_c = PostgresRefreshMaterializedViewExpression(
    dialect=dialect,
    name='sales_summary',
    concurrently=True,
)
sql, params = pg_refresh_c.to_sql()
print(f"Backward-compat CONCURRENTLY SQL: {sql}")

# ============================================================
# SECTION: 8. DROP MATERIALIZED VIEW with CASCADE and IF EXISTS
# ============================================================
# CASCADE drops any objects that depend on the MV.

drop_mv1 = DropMaterializedViewExpression(
    dialect=dialect,
    view_name='sales_summary',
    if_exists=True,
    cascade=True,
)
sql, params = drop_mv1.to_sql()
print(f"\nDROP CASCADE SQL: {sql}")
backend.execute(sql, params)

drop_mv2 = DropMaterializedViewExpression(
    dialect=dialect,
    view_name='sales_daily',
    if_exists=True,
)
sql, params = drop_mv2.to_sql()
backend.execute(sql, params)

# ============================================================
# SECTION: 9. DROP MATERIALIZED VIEW Without IF EXISTS (SQL Generation Demo)
# ============================================================
# Dropping a non-existent view without IF EXISTS will raise an error
# at runtime. This section shows the SQL generation to avoid that.

drop_noexist = DropMaterializedViewExpression(
    dialect=dialect,
    view_name='nonexistent_mv',
)
sql, params = drop_noexist.to_sql()
print(f"DROP without IF EXISTS SQL (not executed): {sql}")
print("  Note: This would fail at runtime if the MV does not exist.")

# ============================================================
# SECTION: 10. IF NOT EXISTS (PostgreSQL 9.4+)
# ============================================================
# IF NOT EXISTS makes creation idempotent: a second run emits a notice
# instead of failing. Requires PostgreSQL 9.4+; the dialect raises
# UnsupportedFeatureError below that version.

if_not_exists_mv = PostgresCreateMaterializedViewExpression(
    dialect=dialect,
    view_name='sales_summary',
    query=summary_query,
    column_aliases=['product_id', 'total_sales', 'total_amount', 'avg_amount'],
    if_not_exists=True,
)
sql, params = if_not_exists_mv.to_sql()
print(f"\nIF NOT EXISTS SQL: {sql}")
backend.execute(sql, params)

# ============================================================
# SECTION: 11. Schema-Qualified Create and Drop
# ============================================================
# Every MV statement accepts a schema; names are quoted per part, so mixed
# case and reserved words are safe.

backend.execute('CREATE SCHEMA IF NOT EXISTS mv_reporting', options=ddl_options)

schema_mv = PostgresCreateMaterializedViewExpression(
    dialect=dialect,
    view_name='Order Summary',
    query=summary_query,
    schema='mv_reporting',
    column_aliases=['product_id', 'total_sales', 'total_amount', 'avg_amount'],
)
sql, params = schema_mv.to_sql()
print(f"\nSchema-qualified CREATE SQL: {sql}")
backend.execute(sql, params)

schema_verify = QueryExpression(
    dialect=dialect,
    select=[WildcardExpression(dialect)],
    from_=TableExpression(
        dialect, 'Order Summary', schema_name='mv_reporting'
    ),
)
sql, params = schema_verify.to_sql()
result = backend.execute(sql, params, options=dql_options)
print("Schema-qualified MV result:")
for row in result.data or []:
    print(f"  {row}")

# ============================================================
# SECTION: 12. ALTER MATERIALIZED VIEW
# ============================================================
# PostgreSQL supports exactly five ALTER MATERIALIZED VIEW actions.
# SET TABLESPACE is NOT one of them: a materialized view cannot be
# relocated to another tablespace after creation.
# Multiple actions render as separate statements joined with ";\n".

alter_mv = PostgresAlterMaterializedViewExpression(
    dialect=dialect,
    view_name='sales_summary',
    actions=[
        PostgresSetMaterializedViewPropertiesAction(
            dialect, {PostgresStorageParameter.FILLFACTOR: 85}
        ),
        PostgresResetMaterializedViewPropertiesAction(
            dialect, [PostgresStorageParameter.AUTOVACUUM_ENABLED]
        ),
    ],
)
sql, params = alter_mv.to_sql()
print(f"\nALTER SET/RESET SQL:\n{sql}")
backend.execute(sql, params, options=ddl_options)

rename_mv = PostgresAlterMaterializedViewExpression(
    dialect=dialect,
    view_name='sales_summary',
    actions=[PostgresRenameMaterializedViewAction(dialect, 'sales_summary_v2')],
)
sql, params = rename_mv.to_sql()
print(f"\nALTER RENAME SQL: {sql}")
backend.execute(sql, params, options=ddl_options)

# Move the schema-qualified view into the public schema.
set_schema_mv = PostgresAlterMaterializedViewExpression(
    dialect=dialect,
    view_name='Order Summary',
    schema='mv_reporting',
    actions=[PostgresSetMaterializedViewSchemaAction(dialect, 'public')],
)
sql, params = set_schema_mv.to_sql()
print(f"\nALTER SET SCHEMA SQL: {sql}")
backend.execute(sql, params, options=ddl_options)

# CURRENT_USER / CURRENT_ROLE / SESSION_USER are emitted verbatim.
owner_mv = PostgresAlterMaterializedViewExpression(
    dialect=dialect,
    view_name='sales_summary_v2',
    actions=[PostgresChangeMaterializedViewOwnerAction(dialect, 'CURRENT_USER')],
)
sql, params = owner_mv.to_sql()
print(f"\nALTER OWNER TO SQL: {sql}")
backend.execute(sql, params, options=ddl_options)

# ============================================================
# SECTION: 13. Introspection
# ============================================================
# list_materialized_views() is a dedicated entry point: list_views() stays
# limited to relkind='v'. Each entry reports whether the view is populated
# and whether it carries the UNIQUE index CONCURRENTLY refresh needs.

backend.introspector.clear_cache()
materialized_views = backend.introspector.list_materialized_views()
print("\nMaterialized views:")
for view in materialized_views:
    print(f"  {view.schema}.{view.name} populated={view.extra.get('is_populated')} "
          f"unique_index={view.extra.get('has_unique_index')}")

info = backend.introspector.get_materialized_view_info('sales_summary_v2')
print(f"\nMV info: {info.name if info else None} definition={info.definition[:60] if info and info.definition else None}")

# ============================================================
# SECTION: 14. Schema-Qualified Drop
# ============================================================

drop_schema_mv = PostgresDropMaterializedViewExpression(
    dialect=dialect,
    view_name='Order Summary',
    schema='mv_reporting',
    if_exists=True,
    cascade=True,
)
sql, params = drop_schema_mv.to_sql()
print(f"\nSchema-qualified DROP SQL: {sql}")
backend.execute(sql, params, options=ddl_options)

# ============================================================
# SECTION: Teardown (necessary for execution, reference only)
# ============================================================
drop_mv = PostgresDropMaterializedViewExpression(
    dialect=dialect, view_name='sales_summary_v2', if_exists=True, cascade=True,
)
backend.execute(*drop_mv.to_sql())
backend.execute('DROP SCHEMA IF EXISTS mv_reporting CASCADE', options=ddl_options)
drop_table = DropTableExpression(
    dialect=dialect,
    table='sales',
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
# 2. Use column_aliases to name the MV output columns
# 3. Use storage_options for physical storage tuning (fillfactor, autovacuum, etc.)
# 4. Use tablespace to assign the MV to a specific storage location
# 5. Use with_data=False for WITH NO DATA (lazy population, MV unqueriable)
# 6. Use RefreshMaterializedViewExpression for standard refresh
# 7. Use RefreshMaterializedViewExpression with concurrent=True for CONCURRENTLY refresh (PG 9.4+, requires UNIQUE index)
# 8. Use RefreshMaterializedViewExpression with with_data=True to populate a NO-DATA MV
# 9. Use PostgresRefreshMaterializedViewExpression for schema-qualified refresh
# 10. Use DropMaterializedViewExpression with cascade=True to drop dependent objects
# 11. Use DropMaterializedViewExpression with if_exists=True for safe drop
# 12. Requires PostgreSQL 9.3+ for basic MV support
# 13. Requires PostgreSQL 9.4+ for CONCURRENTLY refresh and IF NOT EXISTS
# 14. storage_options keys are validated against PostgresStorageParameter
# 15. ALTER MATERIALIZED VIEW supports RENAME/SET SCHEMA/SET()/RESET()/OWNER TO
# 16. list_materialized_views() reports is_populated and has_unique_index