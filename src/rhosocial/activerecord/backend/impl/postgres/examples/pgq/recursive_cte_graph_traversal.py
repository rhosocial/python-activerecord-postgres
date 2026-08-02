"""
Recursive CTE graph traversal — PGQ quantified path alternative
===============================================================

PostgreSQL 19 natively supports SQL/PGQ (MATCH / GRAPH_TABLE), with limitations:
  - supports_quantified_path() → False (no variable-length paths +/*/{n,m})
  - supports_comma_separated_patterns() → False (no comma-separated multi-pattern)

This example demonstrates recursive CTE + SetOperationExpression + UNION ALL
as an equivalent alternative for graph traversal patterns.

Common use cases:
  1. Social network N-degree friend recommendations (replaces MATCH (a)-[e]->+(b))
  2. AML fund source tracing (replaces MATCH (s)-[t*1..5]->(d))

Key components:
  - CTEExpression: defines a CTE (WITH ... AS ...)
  - SetOperationExpression: combines base + recursive queries (UNION ALL)
  - WithQueryExpression(recursive=True): generates WITH RECURSIVE ... SELECT ...
"""

# ============================================================
# SECTION: Setup (reference only — creates tables and data)
# ============================================================
import os
from rhosocial.activerecord.backend.impl.postgres import PostgresBackend
from rhosocial.activerecord.backend.impl.postgres.config import PostgresConnectionConfig

config = PostgresConnectionConfig(
    host=os.getenv('POSTGRES_HOST', 'localhost'),
    port=int(os.getenv('POSTGRES_PORT', 5432)),
    database=os.getenv('POSTGRES_DATABASE', 'test'),
    username=os.getenv('POSTGRES_USER', 'postgres'),
    password=os.getenv('POSTGRES_PASSWORD', ''),
)
backend = PostgresBackend(connection_config=config)
backend.connect()
dialect = backend.dialect

from rhosocial.activerecord.backend.expression import (
    CreateTableExpression,
    InsertExpression,
    ValuesSource,
    DropTableExpression,
    QueryExpression,
    TableExpression,
    CTEExpression,
    WithQueryExpression,
    SetOperationExpression,
    FunctionCall,
    ColumnDefinition,
    ColumnConstraint,
    ColumnConstraintType,
)
from rhosocial.activerecord.backend.expression.core import Literal, Column
from rhosocial.activerecord.backend.expression.predicates import BetweenPredicate
from rhosocial.activerecord.backend.expression.query_parts import (
    WhereClause,
    OrderByClause,
    JoinExpression,
)
from rhosocial.activerecord.backend.options import ExecutionOptions
from rhosocial.activerecord.backend.schema import StatementType

dql_options = ExecutionOptions(stmt_type=StatementType.DQL)

for t in ("transactions", "accounts", "follows", "users"):
    sql, params = DropTableExpression(dialect, t, if_exists=True, cascade=True).to_sql()
    backend.execute(sql, params)

# ---- Social network tables ----
users_cols = [
    ColumnDefinition('id', 'INTEGER', constraints=[
        ColumnConstraint(ColumnConstraintType.PRIMARY_KEY)]),
    ColumnDefinition('name', 'VARCHAR(100)'),
    ColumnDefinition('city', 'VARCHAR(50)'),
]
backend.execute(*CreateTableExpression(dialect, 'users', users_cols, if_not_exists=True).to_sql())

follows_cols = [
    ColumnDefinition('id', 'INTEGER', constraints=[
        ColumnConstraint(ColumnConstraintType.PRIMARY_KEY)]),
    ColumnDefinition('follower_id', 'INTEGER'),
    ColumnDefinition('followed_id', 'INTEGER'),
]
backend.execute(*CreateTableExpression(dialect, 'follows', follows_cols, if_not_exists=True).to_sql())

users_data = ValuesSource(dialect, [
    [Literal(dialect, 1), Literal(dialect, 'Alice'), Literal(dialect, 'NYC')],
    [Literal(dialect, 2), Literal(dialect, 'Bob'), Literal(dialect, 'NYC')],
    [Literal(dialect, 3), Literal(dialect, 'Charlie'), Literal(dialect, 'LA')],
    [Literal(dialect, 4), Literal(dialect, 'Diana'), Literal(dialect, 'NYC')],
    [Literal(dialect, 5), Literal(dialect, 'Eve'), Literal(dialect, 'LA')],
])
backend.execute(*InsertExpression(dialect, 'users', columns=['id', 'name', 'city'], source=users_data).to_sql())

follows_data = ValuesSource(dialect, [
    [Literal(dialect, 1), Literal(dialect, 1), Literal(dialect, 2)],
    [Literal(dialect, 2), Literal(dialect, 2), Literal(dialect, 3)],
    [Literal(dialect, 3), Literal(dialect, 1), Literal(dialect, 3)],
    [Literal(dialect, 4), Literal(dialect, 4), Literal(dialect, 1)],
    [Literal(dialect, 5), Literal(dialect, 3), Literal(dialect, 5)],
])
backend.execute(*InsertExpression(dialect, 'follows', columns=['id', 'follower_id', 'followed_id'], source=follows_data).to_sql())

# ---- AML tables ----
accounts_cols = [
    ColumnDefinition('id', 'INTEGER', constraints=[
        ColumnConstraint(ColumnConstraintType.PRIMARY_KEY)]),
    ColumnDefinition('account_holder', 'VARCHAR(100)'),
    ColumnDefinition('account_type', 'VARCHAR(20)'),
]
backend.execute(*CreateTableExpression(dialect, 'accounts', accounts_cols, if_not_exists=True).to_sql())

txn_cols = [
    ColumnDefinition('id', 'INTEGER', constraints=[
        ColumnConstraint(ColumnConstraintType.PRIMARY_KEY)]),
    ColumnDefinition('source_account_id', 'INTEGER'),
    ColumnDefinition('target_account_id', 'INTEGER'),
    ColumnDefinition('amount', 'NUMERIC(12,2)'),
    ColumnDefinition('ts', 'VARCHAR(20)'),
]
backend.execute(*CreateTableExpression(dialect, 'transactions', txn_cols, if_not_exists=True).to_sql())

accounts_data = ValuesSource(dialect, [
    [Literal(dialect, 1), Literal(dialect, 'Alice Smith'), Literal(dialect, 'checking')],
    [Literal(dialect, 2), Literal(dialect, 'Bob Corp'), Literal(dialect, 'business')],
    [Literal(dialect, 3), Literal(dialect, 'Charlie Ltd'), Literal(dialect, 'business')],
    [Literal(dialect, 4), Literal(dialect, 'Diana Offshore'), Literal(dialect, 'offshore')],
    [Literal(dialect, 5), Literal(dialect, 'Eve Holding'), Literal(dialect, 'offshore')],
    [Literal(dialect, 6), Literal(dialect, 'Foreign Entity'), Literal(dialect, 'offshore')],
])
backend.execute(*InsertExpression(dialect, 'accounts', columns=['id', 'account_holder', 'account_type'], source=accounts_data).to_sql())

txn_data = ValuesSource(dialect, [
    [Literal(dialect, 1), Literal(dialect, 4), Literal(dialect, 3), Literal(dialect, 500000), Literal(dialect, '2024-01-15')],
    [Literal(dialect, 2), Literal(dialect, 5), Literal(dialect, 3), Literal(dialect, 300000), Literal(dialect, '2024-02-01')],
    [Literal(dialect, 3), Literal(dialect, 3), Literal(dialect, 2), Literal(dialect, 600000), Literal(dialect, '2024-03-01')],
    [Literal(dialect, 4), Literal(dialect, 2), Literal(dialect, 1), Literal(dialect, 450000), Literal(dialect, '2024-04-01')],
    [Literal(dialect, 5), Literal(dialect, 6), Literal(dialect, 5), Literal(dialect, 1000000), Literal(dialect, '2024-05-01')],
])
backend.execute(*InsertExpression(dialect, 'transactions', columns=['id', 'source_account_id', 'target_account_id', 'amount', 'ts'], source=txn_data).to_sql())

# ============================================================
# SECTION: Scenario 1 — Social Network N-degree Recommendation
# Equivalent to PGQ: MATCH (a:person {name: 'Alice'})-[e:follows]+{1,3}->(b:person)
# ============================================================
print("=" * 72)
print("Scenario 1: Social Network N-degree Friend Recommendations")
print("Equivalent: MATCH (a)-[e:follows]+{1,3}->(b)")
print("Technique:  Recursive CTE + UNION ALL + depth filter ({1,3})")
print("=" * 72)

# --- Base query: start vertex Alice, depth=0 ---
base_query = QueryExpression(
    dialect=dialect,
    select=[
        Column(dialect, 'id'),
        Column(dialect, 'name'),
        Literal(dialect, 0).as_('depth'),
    ],
    from_=TableExpression(dialect, 'users'),
    where=WhereClause(dialect, condition=Column(dialect, 'name') == Literal(dialect, 'Alice')),
)

# --- Recursive query: follow edges one hop, depth+1, bound depth<4 ---
recursive_join = JoinExpression(
    dialect=dialect,
    left_table=TableExpression(dialect, 'traversal', alias='t'),
    right_table=TableExpression(dialect, 'follows', alias='f'),
    join_type='INNER JOIN',
    condition=Column(dialect, 'id', table='t') == Column(dialect, 'follower_id', table='f'),
).inner_join(
    right_table=TableExpression(dialect, 'users', alias='u'),
    condition=Column(dialect, 'followed_id', table='f') == Column(dialect, 'id', table='u'),
)

recursive_query = QueryExpression(
    dialect=dialect,
    select=[
        Column(dialect, 'id', table='u'),
        Column(dialect, 'name', table='u'),
        FunctionCall(dialect, '+', Column(dialect, 'depth', table='t'), Literal(dialect, 1)).as_('depth'),
    ],
    from_=recursive_join,
    where=WhereClause(dialect, condition=Column(dialect, 'depth', table='t') < Literal(dialect, 4)),
)

# --- Combine with UNION ALL ---
union_expr = SetOperationExpression(
    dialect=dialect,
    left=base_query,
    right=recursive_query,
    operation='UNION ALL',
)

# --- Define CTE ---
traversal_cte = CTEExpression(
    dialect=dialect,
    name='traversal',
    query=union_expr,
    columns=['id', 'name', 'depth'],
)

# --- Main query: filter depth 1~3 (simulating quantified path {1,3}) ---
friend_query = QueryExpression(
    dialect=dialect,
    select=[
        Column(dialect, 'id'),
        Column(dialect, 'name'),
        Column(dialect, 'depth'),
    ],
    from_=TableExpression(dialect, 'traversal'),
    where=WhereClause(dialect, condition=BetweenPredicate(
        dialect, Column(dialect, 'depth'), Literal(dialect, 1), Literal(dialect, 3),
    )),
    order_by=OrderByClause(dialect, [Column(dialect, 'depth'), Column(dialect, 'name')]),
)

with_query = WithQueryExpression(
    dialect=dialect,
    ctes=[traversal_cte],
    main_query=friend_query,
    recursive=True,
)

sql, params = with_query.to_sql()
print(f"\nGenerated SQL:\n{sql}\n")
result = backend.execute(sql, params, options=dql_options)
print("Friend recommendations for Alice (depth 1~3):")
for row in result.data or []:
    print(f"  depth={row['depth']}: {row['name']}")
print()

# ============================================================
# SECTION: Scenario 2 — AML Fund Source Tracing
# Equivalent to PGQ: MATCH (s:account)-[t:transfer*1..5]->(d:account {id: 1})
# ============================================================
print("=" * 72)
print("Scenario 2: AML Fund Source Tracing")
print("Equivalent: MATCH (s)-[t:transfer*1..5]->(d {id: 1})")
print("Technique:  Recursive CTE + UNION ALL + depth limit + amount aggregation")
print("=" * 72)

# --- Base query: direct sources sending to target account ACC-001 ---
aml_base_join = JoinExpression(
    dialect=dialect,
    left_table=TableExpression(dialect, 'transactions', alias='tx'),
    right_table=TableExpression(dialect, 'accounts', alias='a'),
    join_type='INNER JOIN',
    condition=Column(dialect, 'source_account_id', table='tx') == Column(dialect, 'id', table='a'),
)

aml_base = QueryExpression(
    dialect=dialect,
    select=[
        Column(dialect, 'id', table='a'),
        Column(dialect, 'account_holder', table='a'),
        Column(dialect, 'account_type', table='a'),
        Column(dialect, 'amount', table='tx'),
        Literal(dialect, 1).as_('depth'),
    ],
    from_=aml_base_join,
    where=WhereClause(dialect, condition=Column(dialect, 'target_account_id', table='tx') == Literal(dialect, 1)),
)

# --- Recursive query: trace upstream along the transaction chain ---
aml_recursive_join = JoinExpression(
    dialect=dialect,
    left_table=TableExpression(dialect, 'fund_trace', alias='tr'),
    right_table=TableExpression(dialect, 'transactions', alias='tx'),
    join_type='INNER JOIN',
    condition=Column(dialect, 'target_account_id', table='tx') == Column(dialect, 'id', table='tr'),
).inner_join(
    right_table=TableExpression(dialect, 'accounts', alias='a'),
    condition=Column(dialect, 'source_account_id', table='tx') == Column(dialect, 'id', table='a'),
)

aml_recursive = QueryExpression(
    dialect=dialect,
    select=[
        Column(dialect, 'id', table='a'),
        Column(dialect, 'account_holder', table='a'),
        Column(dialect, 'account_type', table='a'),
        Column(dialect, 'amount', table='tx'),
        FunctionCall(dialect, '+', Column(dialect, 'depth', table='tr'), Literal(dialect, 1)).as_('depth'),
    ],
    from_=aml_recursive_join,
    where=WhereClause(dialect, condition=Column(dialect, 'depth', table='tr') < Literal(dialect, 5)),
)

# --- Combine with UNION ALL ---
aml_union = SetOperationExpression(
    dialect=dialect,
    left=aml_base,
    right=aml_recursive,
    operation='UNION ALL',
)

# --- Define CTE ---
fund_trace_cte = CTEExpression(
    dialect=dialect,
    name='fund_trace',
    query=aml_union,
    columns=['id', 'account_holder', 'account_type', 'amount', 'depth'],
)

# --- Main query: list the full fund chain ---
aml_query = QueryExpression(
    dialect=dialect,
    select=[
        Column(dialect, 'id'),
        Column(dialect, 'account_holder'),
        Column(dialect, 'account_type'),
        Column(dialect, 'amount'),
        Column(dialect, 'depth'),
    ],
    from_=TableExpression(dialect, 'fund_trace'),
    where=WhereClause(dialect, condition=Column(dialect, 'depth') >= Literal(dialect, 1)),
    order_by=OrderByClause(dialect, [Column(dialect, 'depth'), Column(dialect, 'id')]),
)

aml_with = WithQueryExpression(
    dialect=dialect,
    ctes=[fund_trace_cte],
    main_query=aml_query,
    recursive=True,
)

sql, params = aml_with.to_sql()
print(f"\nGenerated SQL:\n{sql}\n")
result = backend.execute(sql, params, options=dql_options)
print("Fund source tracing (upstream chain for Alice Smith / ACC-001):")
print(f"{'depth':<6} {'Holder':<20} {'Type':<12} {'Amount':>12}")
print("-" * 52)
for row in result.data or []:
    print(f"  {row['depth']:<4} {row['account_holder']:<20} {row['account_type']:<12} {float(row['amount']):>10,.0f}")

# --- Aggregation query: total suspicious amount per depth level ---
agg_query = QueryExpression(
    dialect=dialect,
    select=[
        Column(dialect, 'depth'),
        FunctionCall(dialect, 'COUNT', Column(dialect, 'id'), alias='account_count'),
        FunctionCall(dialect, 'SUM', Column(dialect, 'amount'), alias='total_amount'),
    ],
    from_=TableExpression(dialect, 'fund_trace'),
    order_by=OrderByClause(dialect, [Column(dialect, 'depth')]),
)

agg_with = WithQueryExpression(
    dialect=dialect,
    ctes=[fund_trace_cte],
    main_query=agg_query,
    recursive=True,
)

sql, params = agg_with.to_sql()
result = backend.execute(sql, params, options=dql_options)
print("\nAggregation: total suspicious amount per depth level")
print(f"{'depth':<8} {'Accounts':<10} {'Total Amount':>14}")
print("-" * 34)
for row in result.data or []:
    print(f"  {row['depth']:<6} {row['account_count']:<10} {float(row['total_amount']):>10,.0f}")

# ============================================================
# SECTION: Summary (key technical points)
# ============================================================
print()
print("=" * 72)
print("Summary / Key Technical Points")
print("=" * 72)
print("""
1. SetOperationExpression(operation='UNION ALL') combines base + recursive queries
2. CTEExpression(name=..., query=..., columns=...) defines the CTE column signature
3. WithQueryExpression(recursive=True) generates WITH RECURSIVE
4. Recursive query joins the CTE reference with physical tables via chained JoinExpression
5. depth field simulates the hop count of quantified path patterns
6. WHERE depth BETWEEN x AND y simulates {x,y} range constraint
7. Use cases: social recommendations, fund tracing, knowledge graph traversal
""")

# ============================================================
# SECTION: Teardown (reference only — cleanup resources)
# ============================================================
for t in ("transactions", "accounts", "follows", "users"):
    sql, params = DropTableExpression(dialect, t, if_exists=True, cascade=True).to_sql()
    backend.execute(sql, params)
backend.disconnect()
