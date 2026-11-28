"""
Recursive CTE graph traversal
=============================

PostgreSQL 19 Beta 4 withdrew SQL/PGQ. This example demonstrates recursive CTE
+ SetOperation + UNION ALL as an alternative for graph traversal.

Common use cases:
  1. Social network N-degree friend recommendations (replaces MATCH (a)-[e]->+(b))
  2. AML fund source tracing (replaces MATCH (s)-[t*1..5]->(d))

Key components:
  - CTEClause: defines a CTE (WITH ... AS ...)
  - SetOperation: combines base + recursive queries (UNION ALL)
  - WithStatement(recursive=True): generates WITH RECURSIVE ... SELECT ...
"""

# ============================================================
# SECTION: Setup (reference only — creates tables and data)
# ============================================================
import os
from rhosocial.activerecord.backend.impl.postgres.backend import PostgresBackend
from rhosocial.activerecord.backend.impl.postgres.config import PostgresConnectionConfig

if __name__ == "__main__":
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
        cast,
        CreateTableStatement,
        InsertStatement,
        ValuesSource,
        DropTableStatement,
        QueryStatement,
        CTEClause,
        WithStatement,
        SetOperation,
        ArithmeticOperation,
        CountFunction,
        SumFunction,
        ColumnDefinition,
        ColumnConstraint,
        ColumnPrimaryKeyConstraint,
    )
    from rhosocial.activerecord.backend.expression.sources import NamedRelationRef
    from rhosocial.activerecord.backend.expression.objects import Table
    from rhosocial.activerecord.backend.expression.value_types.parameter import Parameter
    from rhosocial.activerecord.backend.expression.column_types.column import Column
    from rhosocial.activerecord.backend.expression.predicates import BetweenPredicate, ComparisonPredicate
    from rhosocial.activerecord.backend.expression.query_parts import (
        WhereClause,
        OrderByClause,
        JoinClause,
    )
    from rhosocial.activerecord.backend.options import ExecutionOptions
    from rhosocial.activerecord.backend.schema import StatementType

    dql_options = ExecutionOptions(stmt_type=StatementType.DQL)

    for t in ("transactions", "accounts", "follows", "users"):
        sql, params = DropTableStatement(dialect, t, if_exists=True, cascade=True).to_sql()
        backend.execute(sql, params)

    # ---- Social network tables ----
    users_cols = [
        ColumnDefinition('id', 'INTEGER', constraints=[
            ColumnConstraint(dialect, branch=ColumnPrimaryKeyConstraint(dialect))]),
        ColumnDefinition('name', 'VARCHAR(100)'),
        ColumnDefinition('city', 'VARCHAR(50)'),
    ]
    backend.execute(*CreateTableStatement(dialect, Table(dialect, 'users'), users_cols, if_not_exists=True).to_sql())

    follows_cols = [
        ColumnDefinition('id', 'INTEGER', constraints=[
            ColumnConstraint(dialect, branch=ColumnPrimaryKeyConstraint(dialect))]),
        ColumnDefinition('follower_id', 'INTEGER'),
        ColumnDefinition('followed_id', 'INTEGER'),
    ]
    backend.execute(*CreateTableStatement(dialect, Table(dialect, 'follows'), follows_cols, if_not_exists=True).to_sql())

    users_data = ValuesSource(dialect, [
        [Parameter(dialect, 1), Parameter(dialect, 'Alice'), Parameter(dialect, 'NYC')],
        [Parameter(dialect, 2), Parameter(dialect, 'Bob'), Parameter(dialect, 'NYC')],
        [Parameter(dialect, 3), Parameter(dialect, 'Charlie'), Parameter(dialect, 'LA')],
        [Parameter(dialect, 4), Parameter(dialect, 'Diana'), Parameter(dialect, 'NYC')],
        [Parameter(dialect, 5), Parameter(dialect, 'Eve'), Parameter(dialect, 'LA')],
    ])
    backend.execute(*InsertStatement(dialect, 'users', columns=['id', 'name', 'city'], source=users_data).to_sql())

    follows_data = ValuesSource(dialect, [
        [Parameter(dialect, 1), Parameter(dialect, 1), Parameter(dialect, 2)],
        [Parameter(dialect, 2), Parameter(dialect, 2), Parameter(dialect, 3)],
        [Parameter(dialect, 3), Parameter(dialect, 1), Parameter(dialect, 3)],
        [Parameter(dialect, 4), Parameter(dialect, 4), Parameter(dialect, 1)],
        [Parameter(dialect, 5), Parameter(dialect, 3), Parameter(dialect, 5)],
    ])
    backend.execute(*InsertStatement(dialect, 'follows', columns=['id', 'follower_id', 'followed_id'], source=follows_data).to_sql())

    # ---- AML tables ----
    accounts_cols = [
        ColumnDefinition('id', 'INTEGER', constraints=[
            ColumnConstraint(dialect, branch=ColumnPrimaryKeyConstraint(dialect))]),
        ColumnDefinition('account_holder', 'VARCHAR(100)'),
        ColumnDefinition('account_type', 'VARCHAR(20)'),
    ]
    backend.execute(*CreateTableStatement(dialect, Table(dialect, 'accounts'), accounts_cols, if_not_exists=True).to_sql())

    txn_cols = [
        ColumnDefinition('id', 'INTEGER', constraints=[
            ColumnConstraint(dialect, branch=ColumnPrimaryKeyConstraint(dialect))]),
        ColumnDefinition('source_account_id', 'INTEGER'),
        ColumnDefinition('target_account_id', 'INTEGER'),
        ColumnDefinition('amount', 'NUMERIC(12,2)'),
        ColumnDefinition('ts', 'VARCHAR(20)'),
    ]
    backend.execute(*CreateTableStatement(dialect, Table(dialect, 'transactions'), txn_cols, if_not_exists=True).to_sql())

    accounts_data = ValuesSource(dialect, [
        [Parameter(dialect, 1), Parameter(dialect, 'Alice Smith'), Parameter(dialect, 'checking')],
        [Parameter(dialect, 2), Parameter(dialect, 'Bob Corp'), Parameter(dialect, 'business')],
        [Parameter(dialect, 3), Parameter(dialect, 'Charlie Ltd'), Parameter(dialect, 'business')],
        [Parameter(dialect, 4), Parameter(dialect, 'Diana Offshore'), Parameter(dialect, 'offshore')],
        [Parameter(dialect, 5), Parameter(dialect, 'Eve Holding'), Parameter(dialect, 'offshore')],
        [Parameter(dialect, 6), Parameter(dialect, 'Foreign Entity'), Parameter(dialect, 'offshore')],
    ])
    backend.execute(*InsertStatement(dialect, 'accounts', columns=['id', 'account_holder', 'account_type'], source=accounts_data).to_sql())

    txn_data = ValuesSource(dialect, [
        [Parameter(dialect, 1), Parameter(dialect, 4), Parameter(dialect, 3), Parameter(dialect, 500000), Parameter(dialect, '2024-01-15')],
        [Parameter(dialect, 2), Parameter(dialect, 5), Parameter(dialect, 3), Parameter(dialect, 300000), Parameter(dialect, '2024-02-01')],
        [Parameter(dialect, 3), Parameter(dialect, 3), Parameter(dialect, 2), Parameter(dialect, 600000), Parameter(dialect, '2024-03-01')],
        [Parameter(dialect, 4), Parameter(dialect, 2), Parameter(dialect, 1), Parameter(dialect, 450000), Parameter(dialect, '2024-04-01')],
        [Parameter(dialect, 5), Parameter(dialect, 6), Parameter(dialect, 5), Parameter(dialect, 1000000), Parameter(dialect, '2024-05-01')],
    ])
    backend.execute(*InsertStatement(dialect, 'transactions', columns=['id', 'source_account_id', 'target_account_id', 'amount', 'ts'], source=txn_data).to_sql())

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
    base_query = QueryStatement(
        dialect=dialect,
        select=[
            Column(dialect, 'id'),
            Column(dialect, 'name'),
            cast(dialect, Parameter(dialect, 0), IntegerType(dialect)).as_('depth'),
        ],
        from_=Table(dialect, 'users'),
        where=WhereClause(dialect, condition=ComparisonPredicate(dialect, "=", Column(dialect, 'name'), Parameter(dialect, 'Alice'))),
    )

    # --- Recursive query: follow edges one hop, depth+1, bound depth<4 ---
    recursive_join = JoinClause(
        dialect=dialect,
        left_table=NamedRelationRef(dialect, Table(dialect, 'traversal'), alias='t'),
        right_table=NamedRelationRef(dialect, Table(dialect, 'follows'), alias='f'),
        join_type='INNER JOIN',
        condition=ComparisonPredicate(dialect, "=", Column(dialect, 'id', table='t'), Column(dialect, 'follower_id', table='f')),
    ).inner_join(
        right_table=NamedRelationRef(dialect, Table(dialect, 'users'), alias='u'),
        condition=ComparisonPredicate(dialect, "=", Column(dialect, 'followed_id', table='f'), Column(dialect, 'id', table='u')),
    )

    recursive_query = QueryStatement(
        dialect=dialect,
        select=[
            Column(dialect, 'id', table='u'),
            Column(dialect, 'name', table='u'),
            ArithmeticOperation(dialect, 'add', Column(dialect, 'depth', table='t'), Parameter(dialect, 1)).as_('depth'),
        ],
        from_=recursive_join,
        where=WhereClause(dialect, condition=ComparisonPredicate(dialect, "<", Column(dialect, 'depth', table='t'), Parameter(dialect, 4))),
    )

    # --- Combine with UNION ALL ---
    union_expr = SetOperation(
        dialect=dialect,
        op='union_all',
        left=base_query,
        right=recursive_query,
    )

    # --- Define CTE ---
    traversal_cte = CTEClause(
        dialect=dialect,
        name='traversal',
        query=union_expr,
        columns=['id', 'name', 'depth'],
    )

    # --- Main query: filter depth 1~3 (simulating quantified path {1,3}) ---
    friend_query = QueryStatement(
        dialect=dialect,
        select=[
            Column(dialect, 'id'),
            Column(dialect, 'name'),
            Column(dialect, 'depth'),
        ],
        from_=Table(dialect, 'traversal'),
        where=WhereClause(dialect, condition=BetweenPredicate(
            dialect, Column(dialect, 'depth'), Parameter(dialect, 1), Parameter(dialect, 3),
        )),
        order_by=OrderByClause(dialect, [Column(dialect, 'depth'), Column(dialect, 'name')]),
    )

    with_query = WithStatement(
        dialect=dialect,
        ctes=[traversal_cte],
        query=friend_query,
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
    aml_base_join = JoinClause(
        dialect=dialect,
        left_table=NamedRelationRef(dialect, Table(dialect, 'transactions'), alias='tx'),
        right_table=NamedRelationRef(dialect, Table(dialect, 'accounts'), alias='a'),
        join_type='INNER JOIN',
        condition=ComparisonPredicate(dialect, "=", Column(dialect, 'source_account_id', table='tx'), Column(dialect, 'id', table='a')),
    )

    aml_base = QueryStatement(
        dialect=dialect,
        select=[
            Column(dialect, 'id', table='a'),
            Column(dialect, 'account_holder', table='a'),
            Column(dialect, 'account_type', table='a'),
            Column(dialect, 'amount', table='tx'),
            cast(dialect, Parameter(dialect, 1), IntegerType(dialect)).as_('depth'),
        ],
        from_=aml_base_join,
        where=WhereClause(dialect, condition=ComparisonPredicate(dialect, "=", Column(dialect, 'target_account_id', table='tx'), Parameter(dialect, 1))),
    )

    # --- Recursive query: trace upstream along the transaction chain ---
    aml_recursive_join = JoinClause(
        dialect=dialect,
        left_table=NamedRelationRef(dialect, Table(dialect, 'fund_trace'), alias='tr'),
        right_table=NamedRelationRef(dialect, Table(dialect, 'transactions'), alias='tx'),
        join_type='INNER JOIN',
        condition=ComparisonPredicate(dialect, "=", Column(dialect, 'target_account_id', table='tx'), Column(dialect, 'id', table='tr')),
    ).inner_join(
        right_table=NamedRelationRef(dialect, Table(dialect, 'accounts'), alias='a'),
        condition=ComparisonPredicate(dialect, "=", Column(dialect, 'source_account_id', table='tx'), Column(dialect, 'id', table='a')),
    )

    aml_recursive = QueryStatement(
        dialect=dialect,
        select=[
            Column(dialect, 'id', table='a'),
            Column(dialect, 'account_holder', table='a'),
            Column(dialect, 'account_type', table='a'),
            Column(dialect, 'amount', table='tx'),
            ArithmeticOperation(dialect, 'add', Column(dialect, 'depth', table='tr'), Parameter(dialect, 1)).as_('depth'),
        ],
        from_=aml_recursive_join,
        where=WhereClause(dialect, condition=ComparisonPredicate(dialect, "<", Column(dialect, 'depth', table='tr'), Parameter(dialect, 5))),
    )

    # --- Combine with UNION ALL ---
    aml_union = SetOperation(
        dialect=dialect,
        op='union_all',
        left=aml_base,
        right=aml_recursive,
    )

    # --- Define CTE ---
    fund_trace_cte = CTEClause(
        dialect=dialect,
        name='fund_trace',
        query=aml_union,
        columns=['id', 'account_holder', 'account_type', 'amount', 'depth'],
    )

    # --- Main query: list the full fund chain ---
    aml_query = QueryStatement(
        dialect=dialect,
        select=[
            Column(dialect, 'id'),
            Column(dialect, 'account_holder'),
            Column(dialect, 'account_type'),
            Column(dialect, 'amount'),
            Column(dialect, 'depth'),
        ],
        from_=Table(dialect, 'fund_trace'),
        where=WhereClause(dialect, condition=ComparisonPredicate(dialect, ">=", Column(dialect, 'depth'), Parameter(dialect, 1))),
        order_by=OrderByClause(dialect, [Column(dialect, 'depth'), Column(dialect, 'id')]),
    )

    aml_with = WithStatement(
        dialect=dialect,
        ctes=[fund_trace_cte],
        query=aml_query,
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
    agg_query = QueryStatement(
        dialect=dialect,
        select=[
            Column(dialect, 'depth'),
            CountFunction(dialect, Column(dialect, 'id')).as_('account_count'),
            SumFunction(dialect, Column(dialect, 'amount')).as_('total_amount'),
        ],
        from_=Table(dialect, 'fund_trace'),
        order_by=OrderByClause(dialect, [Column(dialect, 'depth')]),
    )

    agg_with = WithStatement(
        dialect=dialect,
        ctes=[fund_trace_cte],
        query=agg_query,
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
    1. SetOperation(op='union_all') combines base + recursive queries
    2. CTEClause(name=..., query=..., columns=...) defines the CTE column signature
    3. WithStatement(recursive=True) generates WITH RECURSIVE
    4. Recursive query joins the CTE reference with physical tables via chained JoinClause
    5. depth field simulates the hop count of quantified path patterns
    6. WHERE depth BETWEEN x AND y simulates {x,y} range constraint
    7. Use cases: social recommendations, fund tracing, knowledge graph traversal
    """)

    # ============================================================
    # SECTION: Teardown (reference only — cleanup resources)
    # ============================================================
    for t in ("transactions", "accounts", "follows", "users"):
        sql, params = DropTableStatement(dialect, t, if_exists=True, cascade=True).to_sql()
        backend.execute(sql, params)
    backend.disconnect()
