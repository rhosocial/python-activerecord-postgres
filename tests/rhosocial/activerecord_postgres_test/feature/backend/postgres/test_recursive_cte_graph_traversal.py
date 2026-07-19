# tests/rhosocial/activerecord_postgres_test/feature/backend/postgres/test_recursive_cte_graph_traversal.py
"""
Recursive CTE graph traversal tests — PGQ quantified path alternative.

These tests verify that recursive CTE + SetOperationExpression + UNION ALL
correctly simulates PGQ quantified path patterns (not natively supported
in PostgreSQL 19).

Scenarios:
1. Social Network: N-degree friend recommendations
2. Anti-Money Laundering: Fund source tracing
"""
import pytest
import pytest_asyncio

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
from rhosocial.activerecord.backend.expression.types import (
    IntegerType,
    VarCharType,
    DecimalType,
)
from rhosocial.activerecord.backend.expression.core import Literal, Column
from rhosocial.activerecord.backend.expression.predicates import BetweenPredicate
from rhosocial.activerecord.backend.expression.query_parts import (
    WhereClause,
    OrderByClause,
    JoinExpression,
)


@pytest.fixture
def social_network_data(postgres_backend):
    """Fixture: create users + follows tables with test data."""
    backend = postgres_backend
    dialect = backend.dialect

    for t in ("follows", "users"):
        backend.execute(*DropTableExpression(dialect, t, if_exists=True, cascade=True).to_sql())

    backend.execute(*CreateTableExpression(dialect, "users", [
        ColumnDefinition("id", IntegerType(), constraints=[
            ColumnConstraint(ColumnConstraintType.PRIMARY_KEY)]),
        ColumnDefinition("name", VarCharType(100)),
    ]).to_sql())

    backend.execute(*CreateTableExpression(dialect, "follows", [
        ColumnDefinition("id", IntegerType(), constraints=[
            ColumnConstraint(ColumnConstraintType.PRIMARY_KEY)]),
        ColumnDefinition("follower_id", IntegerType()),
        ColumnDefinition("followed_id", IntegerType()),
    ]).to_sql())

    backend.execute(*InsertExpression(dialect, "users", columns=["id", "name"],
        source=ValuesSource(dialect, [
            [Literal(dialect, 1), Literal(dialect, "Alice")],
            [Literal(dialect, 2), Literal(dialect, "Bob")],
            [Literal(dialect, 3), Literal(dialect, "Charlie")],
            [Literal(dialect, 4), Literal(dialect, "Diana")],
            [Literal(dialect, 5), Literal(dialect, "Eve")],
        ])).to_sql())

    backend.execute(*InsertExpression(dialect, "follows", columns=["id", "follower_id", "followed_id"],
        source=ValuesSource(dialect, [
            [Literal(dialect, 1), Literal(dialect, 1), Literal(dialect, 2)],
            [Literal(dialect, 2), Literal(dialect, 2), Literal(dialect, 3)],
            [Literal(dialect, 3), Literal(dialect, 1), Literal(dialect, 3)],
            [Literal(dialect, 4), Literal(dialect, 4), Literal(dialect, 1)],
        ])).to_sql())

    yield

    for t in ("follows", "users"):
        backend.execute(*DropTableExpression(dialect, t, if_exists=True, cascade=True).to_sql())


@pytest.fixture
def aml_data(postgres_backend):
    """Fixture: create accounts + transactions tables with test data."""
    backend = postgres_backend
    dialect = backend.dialect

    for t in ("transactions", "accounts"):
        backend.execute(*DropTableExpression(dialect, t, if_exists=True, cascade=True).to_sql())

    backend.execute(*CreateTableExpression(dialect, "accounts", [
        ColumnDefinition("id", IntegerType(), constraints=[
            ColumnConstraint(ColumnConstraintType.PRIMARY_KEY)]),
        ColumnDefinition("account_holder", VarCharType(100)),
        ColumnDefinition("account_type", VarCharType(20)),
    ]).to_sql())

    backend.execute(*CreateTableExpression(dialect, "transactions", [
        ColumnDefinition("id", IntegerType(), constraints=[
            ColumnConstraint(ColumnConstraintType.PRIMARY_KEY)]),
        ColumnDefinition("source_account_id", IntegerType()),
        ColumnDefinition("target_account_id", IntegerType()),
        ColumnDefinition("amount", DecimalType(12, 2)),
    ]).to_sql())

    backend.execute(*InsertExpression(dialect, "accounts", columns=["id", "account_holder", "account_type"],
        source=ValuesSource(dialect, [
            [Literal(dialect, 1), Literal(dialect, "Alice Smith"), Literal(dialect, "checking")],
            [Literal(dialect, 2), Literal(dialect, "Bob Corp"), Literal(dialect, "business")],
            [Literal(dialect, 3), Literal(dialect, "Charlie Ltd"), Literal(dialect, "business")],
            [Literal(dialect, 4), Literal(dialect, "Diana Offshore"), Literal(dialect, "offshore")],
            [Literal(dialect, 5), Literal(dialect, "Eve Holding"), Literal(dialect, "offshore")],
        ])).to_sql())

    backend.execute(*InsertExpression(dialect, "transactions", columns=[
        "id", "source_account_id", "target_account_id", "amount"],
        source=ValuesSource(dialect, [
            [Literal(dialect, 1), Literal(dialect, 4), Literal(dialect, 3), Literal(dialect, 500000)],
            [Literal(dialect, 2), Literal(dialect, 5), Literal(dialect, 3), Literal(dialect, 300000)],
            [Literal(dialect, 3), Literal(dialect, 3), Literal(dialect, 2), Literal(dialect, 600000)],
            [Literal(dialect, 4), Literal(dialect, 2), Literal(dialect, 1), Literal(dialect, 450000)],
        ])).to_sql())

    yield

    for t in ("transactions", "accounts"):
        backend.execute(*DropTableExpression(dialect, t, if_exists=True, cascade=True).to_sql())


class TestSocialNetworkTraversal:
    """Recursive CTE for N-degree friend recommendations."""

    def _build_traversal_cte(self, dialect):
        """Build recursive CTE expression for social network traversal."""
        base = QueryExpression(
            dialect=dialect,
            select=[
                Column(dialect, "id"),
                Column(dialect, "name"),
                Literal(dialect, 0).as_("depth"),
            ],
            from_=TableExpression(dialect, "users"),
            where=WhereClause(dialect, condition=Column(dialect, "name") == Literal(dialect, "Alice")),
        )

        recursive_join = JoinExpression(
            dialect=dialect,
            left_table=TableExpression(dialect, "traversal", alias="t"),
            right_table=TableExpression(dialect, "follows", alias="f"),
            join_type="INNER JOIN",
            condition=Column(dialect, "id", table="t") == Column(dialect, "follower_id", table="f"),
        ).inner_join(
            right_table=TableExpression(dialect, "users", alias="u"),
            condition=Column(dialect, "followed_id", table="f") == Column(dialect, "id", table="u"),
        )

        recursive = QueryExpression(
            dialect=dialect,
            select=[
                Column(dialect, "id", table="u"),
                Column(dialect, "name", table="u"),
                FunctionCall(dialect, "+", Column(dialect, "depth", table="t"), Literal(dialect, 1)).as_("depth"),
            ],
            from_=recursive_join,
            where=WhereClause(dialect, condition=Column(dialect, "depth", table="t") < Literal(dialect, 4)),
        )

        union = SetOperationExpression(
            dialect=dialect,
            left=base,
            right=recursive,
            operation="UNION ALL",
        )

        return CTEExpression(
            dialect=dialect,
            name="traversal",
            query=union,
            columns=["id", "name", "depth"],
        )

    def test_friend_recommendation_depth_1_to_3(self, postgres_backend, social_network_data):
        """Q1: Friend recommendations at depths 1-3 (simulate {1,3})."""
        dialect = postgres_backend.dialect
        cte = self._build_traversal_cte(dialect)

        main = QueryExpression(
            dialect=dialect,
            select=[
                Column(dialect, "id"),
                Column(dialect, "name"),
                Column(dialect, "depth"),
            ],
            from_=TableExpression(dialect, "traversal"),
            where=WhereClause(dialect, condition=BetweenPredicate(
                dialect, Column(dialect, "depth"), Literal(dialect, 1), Literal(dialect, 3),
            )),
            order_by=OrderByClause(dialect, [Column(dialect, "depth"), Column(dialect, "name")]),
        )

        with_query = WithQueryExpression(
            dialect=dialect,
            ctes=[cte],
            main_query=main,
            recursive=True,
        )

        sql, params = with_query.to_sql()
        rows = postgres_backend.fetch_all(sql, params)
        names_at_depth = {r["name"]: r["depth"] for r in rows}

        # Alice follows Bob (depth 1) and Charlie (depth 1)
        assert names_at_depth.get("Bob") == 1
        assert names_at_depth.get("Charlie") == 1
        # Bob follows Diana (depth 2)
        assert names_at_depth.get("Diana") == 2

    def test_friend_recommendation_depth_2(self, postgres_backend, social_network_data):
        """Q2: Only depth=2 (simulate {2,2})."""
        dialect = postgres_backend.dialect
        cte = self._build_traversal_cte(dialect)

        main = QueryExpression(
            dialect=dialect,
            select=[
                Column(dialect, "id"),
                Column(dialect, "name"),
                Column(dialect, "depth"),
            ],
            from_=TableExpression(dialect, "traversal"),
            where=WhereClause(dialect, condition=Column(dialect, "depth") == Literal(dialect, 2)),
            order_by=OrderByClause(dialect, [Column(dialect, "name")]),
        )

        with_query = WithQueryExpression(
            dialect=dialect,
            ctes=[cte],
            main_query=main,
            recursive=True,
        )

        sql, params = with_query.to_sql()
        rows = postgres_backend.fetch_all(sql, params)
        names = [r["name"] for r in rows]
        assert "Diana" in names
        assert len(names) == 1

    def test_exclude_start_node(self, postgres_backend, social_network_data):
        """Q3: Exclude Alice herself by filtering depth >= 1."""
        dialect = postgres_backend.dialect
        cte = self._build_traversal_cte(dialect)

        main = QueryExpression(
            dialect=dialect,
            select=[
                Column(dialect, "id"),
                Column(dialect, "name"),
                Column(dialect, "depth"),
            ],
            from_=TableExpression(dialect, "traversal"),
            where=WhereClause(dialect, condition=Column(dialect, "depth") >= Literal(dialect, 1)),
            order_by=OrderByClause(dialect, [Column(dialect, "depth"), Column(dialect, "name")]),
        )

        with_query = WithQueryExpression(
            dialect=dialect,
            ctes=[cte],
            main_query=main,
            recursive=True,
        )

        sql, params = with_query.to_sql()
        rows = postgres_backend.fetch_all(sql, params)
        names = [r["name"] for r in rows]
        assert "Alice" not in names
        assert len(names) == 3  # Bob, Charlie, Diana


class TestAMLFundTracing:
    """Recursive CTE for fund source tracing (AML)."""

    def _build_aml_cte(self, dialect):
        """Build recursive CTE expression for fund tracing."""
        base_join = JoinExpression(
            dialect=dialect,
            left_table=TableExpression(dialect, "transactions", alias="tx"),
            right_table=TableExpression(dialect, "accounts", alias="a"),
            join_type="INNER JOIN",
            condition=Column(dialect, "source_account_id", table="tx") == Column(dialect, "id", table="a"),
        )

        base = QueryExpression(
            dialect=dialect,
            select=[
                Column(dialect, "id", table="a"),
                Column(dialect, "account_holder", table="a"),
                Column(dialect, "account_type", table="a"),
                Column(dialect, "amount", table="tx"),
                Literal(dialect, 1).as_("depth"),
            ],
            from_=base_join,
            where=WhereClause(
                dialect,
                condition=Column(dialect, "target_account_id", table="tx")
                          == Literal(dialect, 1),
            ),
        )

        recursive_join = JoinExpression(
            dialect=dialect,
            left_table=TableExpression(dialect, "fund_trace", alias="tr"),
            right_table=TableExpression(dialect, "transactions", alias="tx"),
            join_type="INNER JOIN",
            condition=Column(dialect, "target_account_id", table="tx") == Column(dialect, "id", table="tr"),
        ).inner_join(
            right_table=TableExpression(dialect, "accounts", alias="a"),
            condition=Column(dialect, "source_account_id", table="tx") == Column(dialect, "id", table="a"),
        )

        recursive = QueryExpression(
            dialect=dialect,
            select=[
                Column(dialect, "id", table="a"),
                Column(dialect, "account_holder", table="a"),
                Column(dialect, "account_type", table="a"),
                Column(dialect, "amount", table="tx"),
                FunctionCall(dialect, "+", Column(dialect, "depth", table="tr"), Literal(dialect, 1)).as_("depth"),
            ],
            from_=recursive_join,
            where=WhereClause(dialect, condition=Column(dialect, "depth", table="tr") < Literal(dialect, 5)),
        )

        union = SetOperationExpression(
            dialect=dialect,
            left=base,
            right=recursive,
            operation="UNION ALL",
        )

        return CTEExpression(
            dialect=dialect,
            name="fund_trace",
            query=union,
            columns=["id", "account_holder", "account_type", "amount", "depth"],
        )

    def test_fund_trace_chain(self, postgres_backend, aml_data):
        """Trace fund sources for account ACC-001."""
        dialect = postgres_backend.dialect
        cte = self._build_aml_cte(dialect)

        main = QueryExpression(
            dialect=dialect,
            select=[
                Column(dialect, "id"),
                Column(dialect, "account_holder"),
                Column(dialect, "depth"),
            ],
            from_=TableExpression(dialect, "fund_trace"),
            where=WhereClause(dialect, condition=Column(dialect, "depth") >= Literal(dialect, 1)),
            order_by=OrderByClause(dialect, [Column(dialect, "depth"), Column(dialect, "id")]),
        )

        with_query = WithQueryExpression(
            dialect=dialect,
            ctes=[cte],
            main_query=main,
            recursive=True,
        )

        sql, params = with_query.to_sql()
        rows = postgres_backend.fetch_all(sql, params)
        account_names = [r["account_holder"] for r in rows]
        # Direct source: Bob Corp (depth 1)
        assert "Bob Corp" in account_names
        # Indirect: Charlie Ltd (depth 2) -> Bob Corp
        assert "Charlie Ltd" in account_names
        # Indirect: Diana Offshore (depth 3) -> Charlie Ltd
        assert "Diana Offshore" in account_names

    def test_fund_trace_depth_filter(self, postgres_backend, aml_data):
        """Filter fund sources by minimum depth."""
        dialect = postgres_backend.dialect
        cte = self._build_aml_cte(dialect)

        main = QueryExpression(
            dialect=dialect,
            select=[
                Column(dialect, "id"),
                Column(dialect, "account_holder"),
                Column(dialect, "depth"),
            ],
            from_=TableExpression(dialect, "fund_trace"),
            where=WhereClause(dialect, condition=Column(dialect, "depth") >= Literal(dialect, 2)),
            order_by=OrderByClause(dialect, [Column(dialect, "depth"), Column(dialect, "id")]),
        )

        with_query = WithQueryExpression(
            dialect=dialect,
            ctes=[cte],
            main_query=main,
            recursive=True,
        )

        sql, params = with_query.to_sql()
        rows = postgres_backend.fetch_all(sql, params)
        depths = [r["depth"] for r in rows]
        assert all(d >= 2 for d in depths)
        # Depth 2: Charlie Ltd (via Bob Corp)
        assert any(r["account_holder"] == "Charlie Ltd" for r in rows)

    def test_fund_trace_aggregation(self, postgres_backend, aml_data):
        """Aggregate total amount by depth level."""
        dialect = postgres_backend.dialect
        cte = self._build_aml_cte(dialect)

        agg_main = QueryExpression(
            dialect=dialect,
            select=[
                Column(dialect, "depth"),
                FunctionCall(dialect, "SUM", Column(dialect, "amount"), alias="total"),
            ],
            from_=TableExpression(dialect, "fund_trace"),
            order_by=OrderByClause(dialect, [Column(dialect, "depth")]),
        )

        with_query = WithQueryExpression(
            dialect=dialect,
            ctes=[cte],
            main_query=agg_main,
            recursive=True,
        )

        sql, params = with_query.to_sql()
        rows = postgres_backend.fetch_all(sql, params)
        totals = {r["depth"]: float(r["total"]) for r in rows}
        # Depth 1: Bob Corp sent 450000 to Alice Smith
        assert totals.get(1) == 450000
        # Depth 2: Charlie Ltd sent 600000 to Bob Corp
        assert totals.get(2) == 600000
        # Depth 3: Diana Offshore sent 500000 to Charlie Ltd
        assert totals.get(3) == 500000


class TestAsyncRecursiveCTEGraph:
    """Async versions of recursive CTE graph tests."""

    @pytest_asyncio.fixture
    async def async_social_network_data(self, async_postgres_backend):
        backend = async_postgres_backend
        dialect = backend.dialect

        for t in ("follows", "users"):
            await backend.execute(*DropTableExpression(dialect, t, if_exists=True, cascade=True).to_sql())

        await backend.execute(*CreateTableExpression(dialect, "users", [
            ColumnDefinition("id", IntegerType(), constraints=[
                ColumnConstraint(ColumnConstraintType.PRIMARY_KEY)]),
            ColumnDefinition("name", VarCharType(100)),
        ]).to_sql())

        await backend.execute(*CreateTableExpression(dialect, "follows", [
            ColumnDefinition("id", IntegerType(), constraints=[
                ColumnConstraint(ColumnConstraintType.PRIMARY_KEY)]),
            ColumnDefinition("follower_id", IntegerType()),
            ColumnDefinition("followed_id", IntegerType()),
        ]).to_sql())

        await backend.execute(*InsertExpression(dialect, "users", columns=["id", "name"],
            source=ValuesSource(dialect, [
                [Literal(dialect, 1), Literal(dialect, "Alice")],
                [Literal(dialect, 2), Literal(dialect, "Bob")],
            ])).to_sql())

        await backend.execute(*InsertExpression(dialect, "follows", columns=["id", "follower_id", "followed_id"],
            source=ValuesSource(dialect, [
                [Literal(dialect, 1), Literal(dialect, 1), Literal(dialect, 2)],
            ])).to_sql())

        yield

        for t in ("follows", "users"):
            await backend.execute(*DropTableExpression(dialect, t, if_exists=True, cascade=True).to_sql())

    @pytest.mark.asyncio
    async def test_async_single_hop(self, async_postgres_backend, async_social_network_data):
        """Async: Alice's 1-hop friends."""
        dialect = async_postgres_backend.dialect
        base = QueryExpression(
            dialect=dialect,
            select=[Column(dialect, "id"), Column(dialect, "name"), Literal(dialect, 0).as_("depth")],
            from_=TableExpression(dialect, "users"),
            where=WhereClause(dialect, condition=Column(dialect, "name") == Literal(dialect, "Alice")),
        )
        result = base.to_sql()
        sql, params = result
        rows = await async_postgres_backend.fetch_all(sql, params)
        assert len(rows) == 1
        assert rows[0]["name"] == "Alice"
