# tests/rhosocial/activerecord_postgres_test/feature/backend/postgres/test_property_graph_query_scenarios.py
"""
Real-world PGQ scenario tests for PostgreSQL 19+ using the expression system.

All SQL is constructed via expression classes. No raw SQL strings are used.
"""
import pytest
import pytest_asyncio

from rhosocial.activerecord.backend.dialect.protocols import GraphTableSupport
from rhosocial.activerecord.backend.expression import (
    GraphVertex, GraphEdge, GraphEdgeDirection, MatchClause,
    GraphColumn, ColumnsClause, GraphTableExpression,
    TablePropertiesClause, VertexTable, EdgeTable,
    CreatePropertyGraphExpression, DropPropertyGraphExpression,
    CreateTableExpression, DropTableExpression, ColumnDefinition,
    ColumnConstraint, ColumnConstraintType,
    InsertExpression, ValuesSource, Literal,
    QueryExpression, WildcardExpression,
    FunctionCall, TableExpression,
    ExplainExpression, ExplainOptions,
)
from rhosocial.activerecord.backend.expression.query_parts import (
    WhereClause, OrderByClause, GroupByHavingClause, LimitOffsetClause,
)
from rhosocial.activerecord.backend.expression.core import Column
from rhosocial.activerecord.backend.expression.query_parts import JoinExpression, JoinType
from rhosocial.activerecord.backend.expression.literals import Identifier


GRAPH_NAME = "social_graph"


@pytest.fixture
def social_data(postgres_backend):
    backend = postgres_backend
    dialect = backend.dialect
    if not dialect.supports_graph_table():
        pytest.skip("PGQ not supported in this PostgreSQL version")

    for t in ("likes", "posts", "follows", "people"):
        backend.execute(*DropTableExpression(dialect, t, if_exists=True, cascade=True).to_sql())

    people_cols = [
        ColumnDefinition("id", "INTEGER",
            constraints=[ColumnConstraint(ColumnConstraintType.PRIMARY_KEY)]),
        ColumnDefinition("name", "TEXT"),
        ColumnDefinition("email", "TEXT"),
        ColumnDefinition("city", "TEXT"),
    ]
    backend.execute(*CreateTableExpression(dialect, "people", people_cols).to_sql())

    follows_cols = [
        ColumnDefinition("id", "INTEGER",
            constraints=[ColumnConstraint(ColumnConstraintType.PRIMARY_KEY)]),
        ColumnDefinition("follower_id", "INTEGER",
            constraints=[ColumnConstraint(ColumnConstraintType.FOREIGN_KEY,
                                          foreign_key_reference=("people", ["id"]))]),
        ColumnDefinition("followed_id", "INTEGER",
            constraints=[ColumnConstraint(ColumnConstraintType.FOREIGN_KEY,
                                          foreign_key_reference=("people", ["id"]))]),
        ColumnDefinition("since", "TEXT"),
    ]
    backend.execute(*CreateTableExpression(dialect, "follows", follows_cols).to_sql())

    posts_cols = [
        ColumnDefinition("id", "INTEGER",
            constraints=[ColumnConstraint(ColumnConstraintType.PRIMARY_KEY)]),
        ColumnDefinition("author_id", "INTEGER",
            constraints=[ColumnConstraint(ColumnConstraintType.FOREIGN_KEY,
                                          foreign_key_reference=("people", ["id"]))]),
        ColumnDefinition("content", "TEXT"),
        ColumnDefinition("created_at", "TEXT"),
    ]
    backend.execute(*CreateTableExpression(dialect, "posts", posts_cols).to_sql())

    likes_cols = [
        ColumnDefinition("id", "INTEGER",
            constraints=[ColumnConstraint(ColumnConstraintType.PRIMARY_KEY)]),
        ColumnDefinition("user_id", "INTEGER",
            constraints=[ColumnConstraint(ColumnConstraintType.FOREIGN_KEY,
                                          foreign_key_reference=("people", ["id"]))]),
        ColumnDefinition("post_id", "INTEGER",
            constraints=[ColumnConstraint(ColumnConstraintType.FOREIGN_KEY,
                                          foreign_key_reference=("posts", ["id"]))]),
        ColumnDefinition("created_at", "TEXT"),
    ]
    backend.execute(*CreateTableExpression(dialect, "likes", likes_cols).to_sql())

    people_data = ValuesSource(dialect, [
        [Literal(dialect, 1), Literal(dialect, "Alice"), Literal(dialect, "alice@example.com"), Literal(dialect, "NYC")],
        [Literal(dialect, 2), Literal(dialect, "Bob"), Literal(dialect, "bob@example.com"), Literal(dialect, "NYC")],
        [Literal(dialect, 3), Literal(dialect, "Charlie"), Literal(dialect, "charlie@example.com"), Literal(dialect, "LA")],
        [Literal(dialect, 4), Literal(dialect, "Diana"), Literal(dialect, "diana@example.com"), Literal(dialect, "NYC")],
        [Literal(dialect, 5), Literal(dialect, "Eve"), Literal(dialect, "eve@example.com"), Literal(dialect, "LA")],
    ])
    backend.execute(*InsertExpression(dialect, "people", source=people_data).to_sql())

    follows_data = ValuesSource(dialect, [
        [Literal(dialect, 1), Literal(dialect, 1), Literal(dialect, 2), Literal(dialect, "2024-01-01")],
        [Literal(dialect, 2), Literal(dialect, 2), Literal(dialect, 3), Literal(dialect, "2024-02-01")],
        [Literal(dialect, 3), Literal(dialect, 1), Literal(dialect, 3), Literal(dialect, "2024-03-01")],
        [Literal(dialect, 4), Literal(dialect, 4), Literal(dialect, 1), Literal(dialect, "2024-04-01")],
        [Literal(dialect, 5), Literal(dialect, 3), Literal(dialect, 5), Literal(dialect, "2024-05-01")],
    ])
    backend.execute(*InsertExpression(dialect, "follows", source=follows_data).to_sql())

    posts_data = ValuesSource(dialect, [
        [Literal(dialect, 1), Literal(dialect, 2), Literal(dialect, "Hello world"), Literal(dialect, "2024-06-01")],
        [Literal(dialect, 2), Literal(dialect, 2), Literal(dialect, "PGQ is cool"), Literal(dialect, "2024-06-02")],
        [Literal(dialect, 3), Literal(dialect, 3), Literal(dialect, "Graph databases"), Literal(dialect, "2024-06-03")],
        [Literal(dialect, 4), Literal(dialect, 1), Literal(dialect, "My first post"), Literal(dialect, "2024-06-04")],
    ])
    backend.execute(*InsertExpression(dialect, "posts", source=posts_data).to_sql())

    likes_data = ValuesSource(dialect, [
        [Literal(dialect, 1), Literal(dialect, 1), Literal(dialect, 1), Literal(dialect, "2024-06-02")],
        [Literal(dialect, 2), Literal(dialect, 3), Literal(dialect, 1), Literal(dialect, "2024-06-02")],
        [Literal(dialect, 3), Literal(dialect, 1), Literal(dialect, 2), Literal(dialect, "2024-06-03")],
        [Literal(dialect, 4), Literal(dialect, 4), Literal(dialect, 4), Literal(dialect, "2024-06-05")],
        [Literal(dialect, 5), Literal(dialect, 5), Literal(dialect, 1), Literal(dialect, "2024-06-06")],
    ])
    backend.execute(*InsertExpression(dialect, "likes", source=likes_data).to_sql())

    vt_people = VertexTable(dialect, "people",
                            labels=["person"],
                            properties=TablePropertiesClause(dialect, columns=["id", "name", "city"]))
    vt_posts = VertexTable(dialect, "posts",
                           labels=["post"],
                           properties=TablePropertiesClause(dialect, columns=["id", "content"]))
    et_follows = EdgeTable(dialect, "follows", ["follower_id"], ["followed_id"],
                           references_source=("people", ["id"]),
                           references_destination=("people", ["id"]),
                           labels=["follows"])
    et_posts = EdgeTable(dialect, "posts", ["author_id"], ["id"],
                         references_source=("people", ["id"]),
                         references_destination=("posts", ["id"]),
                         labels=["authored"],
                         alias="authored")
    et_likes = EdgeTable(dialect, "likes", ["user_id"], ["post_id"],
                         references_source=("people", ["id"]),
                         references_destination=("posts", ["id"]),
                         labels=["likes"])
    create_expr = CreatePropertyGraphExpression(
        dialect, GRAPH_NAME, [vt_people, vt_posts], [et_follows, et_posts, et_likes]
    )
    try:
        backend.execute(*create_expr.to_sql())
    except Exception as e:
        for t in ("likes", "posts", "follows", "people"):
            backend.execute(*DropTableExpression(dialect, t, if_exists=True, cascade=True).to_sql())
        raise e

    yield GRAPH_NAME

    backend.execute(*DropPropertyGraphExpression(dialect, GRAPH_NAME, if_exists=True).to_sql())
    for t in ("likes", "posts", "follows", "people"):
        backend.execute(*DropTableExpression(dialect, t, if_exists=True, cascade=True).to_sql())


class TestSocialGraph:
    """Social Network PGQ scenario using expression system."""

    @pytest.mark.requires_protocol((GraphTableSupport, "supports_graph_table"))
    def test_single_hop_followers(self, postgres_backend, social_data):
        """Q1: Who does Alice follow?"""
        dialect = postgres_backend.dialect
        a = GraphVertex(dialect, "a", "person",
                        where=WhereClause(dialect, condition=Column(dialect, "name", table="a") == Literal(dialect, "Alice")))
        f = GraphEdge(dialect, "f", "follows", GraphEdgeDirection.RIGHT)
        b = GraphVertex(dialect, "b", "person")
        match = MatchClause(dialect, a, f, b)
        cols = ColumnsClause(dialect, GraphColumn("b", "name", "b_name"))
        gt = GraphTableExpression(dialect, GRAPH_NAME, match, cols, alias="g")

        query = QueryExpression(dialect,
            select=[Column(dialect, "b_name")],
            from_=gt,
            order_by=OrderByClause(dialect, [Column(dialect, "b_name")]))
        sql, params = query.to_sql()
        rows = postgres_backend.fetch_all(sql, params)
        names = [r["b_name"] for r in rows]
        assert names == ["Bob", "Charlie"]

    @pytest.mark.requires_protocol((GraphTableSupport, "supports_graph_table"))
    def test_two_hop_recommendation(self, postgres_backend, social_data):
        """Q2: Friends of friends (two-hop)."""
        dialect = postgres_backend.dialect
        a = GraphVertex(dialect, "a", "person",
                        where=WhereClause(dialect, condition=Column(dialect, "name", table="a") == Literal(dialect, "Alice")))
        f1 = GraphEdge(dialect, "f1", "follows", GraphEdgeDirection.RIGHT)
        b = GraphVertex(dialect, "b", "person")
        f2 = GraphEdge(dialect, "f2", "follows", GraphEdgeDirection.RIGHT)
        c = GraphVertex(dialect, "c", "person")
        match = MatchClause(dialect, a, f1, b, f2, c)
        cols = ColumnsClause(dialect, GraphColumn("c", "name", "c_name"))
        gt = GraphTableExpression(dialect, GRAPH_NAME, match, cols, alias="g")

        query = QueryExpression(dialect,
            select=[Column(dialect, "c_name")],
            from_=gt,
            where=WhereClause(dialect, condition=Column(dialect, "c_name") != Literal(dialect, "Alice")),
            order_by=OrderByClause(dialect, [Column(dialect, "c_name")]))
        sql, params = query.to_sql()
        rows = postgres_backend.fetch_all(sql, params)
        assert len(rows) >= 1

    @pytest.mark.requires_protocol((GraphTableSupport, "supports_graph_table"))
    def test_likes_on_posts(self, postgres_backend, social_data):
        """Q3: Who liked Alice's posts?"""
        dialect = postgres_backend.dialect
        a = GraphVertex(dialect, "a", "person",
                        where=WhereClause(dialect, condition=Column(dialect, "name", table="a") == Literal(dialect, "Alice")))
        p = GraphEdge(dialect, "p", "authored", GraphEdgeDirection.RIGHT)
        post = GraphVertex(dialect, "post", "post")
        l = GraphEdge(dialect, "l", "likes", GraphEdgeDirection.LEFT)
        liker = GraphVertex(dialect, "liker", "person")
        match = MatchClause(dialect, a, p, post, l, liker)
        cols = ColumnsClause(dialect, GraphColumn("liker", "name", "liker_name"))
        gt = GraphTableExpression(dialect, GRAPH_NAME, match, cols, alias="g")

        query = QueryExpression(dialect,
            select=[Column(dialect, "liker_name")],
            from_=gt,
            order_by=OrderByClause(dialect, [Column(dialect, "liker_name")]))
        sql, params = query.to_sql()
        rows = postgres_backend.fetch_all(sql, params)
        assert len(rows) >= 1

    @pytest.mark.requires_protocol((GraphTableSupport, "supports_graph_table"))
    def test_graph_table_with_group_by(self, postgres_backend, social_data):
        """GRAPH_TABLE + GROUP BY."""
        dialect = postgres_backend.dialect
        a = GraphVertex(dialect, "a", "person")
        p = GraphEdge(dialect, "p", "authored", GraphEdgeDirection.RIGHT)
        post = GraphVertex(dialect, "post", "post")
        l = GraphEdge(dialect, "l", "likes", GraphEdgeDirection.LEFT)
        liker = GraphVertex(dialect, "liker", "person")
        match = MatchClause(dialect, a, p, post, l, liker)
        cols = ColumnsClause(dialect,
                             GraphColumn("a", "name", "author"),
                             GraphColumn("liker", "name", "liker_name"))
        gt = GraphTableExpression(dialect, GRAPH_NAME, match, cols, alias="g")

        query = QueryExpression(dialect,
            select=[
                Column(dialect, "author"),
                FunctionCall(dialect, "COUNT", Column(dialect, "liker_name"), alias="like_count"),
            ],
            from_=gt,
            group_by_having=GroupByHavingClause(dialect, group_by=[Column(dialect, "author")]),
            order_by=OrderByClause(dialect, [(Column(dialect, "like_count"), "DESC")]))
        sql, params = query.to_sql()
        rows = postgres_backend.fetch_all(sql, params)
        assert len(rows) >= 1
        assert rows[0]["like_count"] >= 1


class TestCommerceGraph:
    """Commerce/Supply Chain PGQ scenario."""

    @pytest.mark.requires_protocol((GraphTableSupport, "supports_graph_table"))
    def test_graph_table_combined_with_regular_sql(self, postgres_backend, social_data):
        """Composite: GRAPH_TABLE + JOIN + ORDER BY."""
        dialect = postgres_backend.dialect
        a = GraphVertex(dialect, "a", "person")
        f = GraphEdge(dialect, "f", "follows", GraphEdgeDirection.RIGHT)
        b = GraphVertex(dialect, "b", "person")
        match = MatchClause(dialect, a, f, b)
        cols = ColumnsClause(dialect,
                             GraphColumn("a", "name", "follower"),
                             GraphColumn("b", "name", "followed"),
                             GraphColumn("f", "since", "since"))
        gt = GraphTableExpression(dialect, GRAPH_NAME, match, cols, alias="g")

        join = JoinExpression(dialect,
            left_table=gt,
            right_table=TableExpression(dialect, "people", alias="p"),
            join_type="INNER JOIN",
            condition=Column(dialect, "follower", "g") == Column(dialect, "name", "p"))

        query = QueryExpression(dialect,
            select=[WildcardExpression(dialect)],
            from_=join,
            order_by=OrderByClause(dialect, [Column(dialect, "follower", "g")]),
            limit_offset=LimitOffsetClause(dialect, limit=10))
        sql, params = query.to_sql()
        rows = postgres_backend.fetch_all(sql, params)
        assert len(rows) >= 1

    @pytest.mark.requires_protocol((GraphTableSupport, "supports_graph_table"))
    def test_parameter_binding(self, postgres_backend, social_data):
        """Parameter binding via expression system."""
        dialect = postgres_backend.dialect
        a = GraphVertex(dialect, "a", "person",
                        where=WhereClause(dialect, condition=Column(dialect, "name", table="a") == Literal(dialect, "Alice")))
        f = GraphEdge(dialect, "f", "follows", GraphEdgeDirection.RIGHT)
        b = GraphVertex(dialect, "b", "person")
        match = MatchClause(dialect, a, f, b)
        cols = ColumnsClause(dialect, GraphColumn("b", "name", "b_name"))
        gt = GraphTableExpression(dialect, GRAPH_NAME, match, cols, alias="g")

        query = QueryExpression(dialect,
            select=[Column(dialect, "b_name")],
            from_=gt,
            order_by=OrderByClause(dialect, [Column(dialect, "b_name")]))
        sql, params = query.to_sql()
        rows = postgres_backend.fetch_all(sql, params)
        names = [r["b_name"] for r in rows]
        assert names == ["Bob", "Charlie"]


class TestAsyncSocialGraph:
    """Async versions of social graph scenarios using expression system."""

    @pytest_asyncio.fixture
    async def async_social_data(self, async_postgres_backend):
        backend = async_postgres_backend
        dialect = backend.dialect
        if not dialect.supports_graph_table():
            pytest.skip("PGQ not supported")

        for t in ("likes", "posts", "follows", "people"):
            await backend.execute(*DropTableExpression(dialect, t, if_exists=True, cascade=True).to_sql())

        people_cols = [
            ColumnDefinition("id", "INTEGER",
                constraints=[ColumnConstraint(ColumnConstraintType.PRIMARY_KEY)]),
            ColumnDefinition("name", "TEXT"),
            ColumnDefinition("email", "TEXT"),
            ColumnDefinition("city", "TEXT"),
        ]
        await backend.execute(*CreateTableExpression(dialect, "people", people_cols).to_sql())

        follows_cols = [
            ColumnDefinition("id", "INTEGER",
                constraints=[ColumnConstraint(ColumnConstraintType.PRIMARY_KEY)]),
            ColumnDefinition("follower_id", "INTEGER",
                constraints=[ColumnConstraint(ColumnConstraintType.FOREIGN_KEY,
                                              foreign_key_reference=("people", ["id"]))]),
            ColumnDefinition("followed_id", "INTEGER",
                constraints=[ColumnConstraint(ColumnConstraintType.FOREIGN_KEY,
                                              foreign_key_reference=("people", ["id"]))]),
            ColumnDefinition("since", "TEXT"),
        ]
        await backend.execute(*CreateTableExpression(dialect, "follows", follows_cols).to_sql())

        people_data = ValuesSource(dialect, [
            [Literal(dialect, 1), Literal(dialect, "Alice"), Literal(dialect, "a@x.com"), Literal(dialect, "NYC")],
            [Literal(dialect, 2), Literal(dialect, "Bob"), Literal(dialect, "b@x.com"), Literal(dialect, "NYC")],
        ])
        await backend.execute(*InsertExpression(dialect, "people", source=people_data).to_sql())

        follows_data = ValuesSource(dialect, [
            [Literal(dialect, 1), Literal(dialect, 1), Literal(dialect, 2), Literal(dialect, "2024-01-01")],
        ])
        await backend.execute(*InsertExpression(dialect, "follows", source=follows_data).to_sql())

        vt = VertexTable(dialect, "people", labels=["person"])
        et = EdgeTable(dialect, "follows", ["follower_id"], ["followed_id"],
                       references_source=("people", ["id"]),
                       references_destination=("people", ["id"]),
                       labels=["follows"])
        create_expr = CreatePropertyGraphExpression(dialect, "async_graph", [vt], [et])
        await backend.execute(*create_expr.to_sql())
        yield "async_graph"
        await backend.execute(*DropPropertyGraphExpression(dialect, "async_graph", if_exists=True).to_sql())
        for t in ("follows", "people"):
            await backend.execute(*DropTableExpression(dialect, t, if_exists=True, cascade=True).to_sql())

    @pytest.mark.requires_protocol((GraphTableSupport, "supports_graph_table"))
    @pytest.mark.asyncio
    async def test_async_single_hop(self, async_postgres_backend, async_social_data):
        dialect = async_postgres_backend.dialect
        a = GraphVertex(dialect, "a", "person",
                        where=WhereClause(dialect, condition=Column(dialect, "name", table="a") == Literal(dialect, "Alice")))
        f = GraphEdge(dialect, "f", "follows", GraphEdgeDirection.RIGHT)
        b = GraphVertex(dialect, "b", "person")
        match = MatchClause(dialect, a, f, b)
        cols = ColumnsClause(dialect, GraphColumn("b", "name", "b_name"))
        gt = GraphTableExpression(dialect, GRAPH_NAME, match, cols, alias="g")

        query = QueryExpression(dialect,
            select=[Column(dialect, "b_name")],
            from_=gt)
        sql, params = query.to_sql()
        rows = await async_postgres_backend.fetch_all(sql, params)
        assert len(rows) == 1
        assert rows[0]["b_name"] == "Bob"


class TestPGQExplain:
    """Verify PG 19 rewrites GRAPH_TABLE into relational joins."""

    @pytest.mark.requires_protocol((GraphTableSupport, "supports_graph_table"))
    def test_explain_graph_table(self, postgres_backend, social_data):
        dialect = postgres_backend.dialect
        a = GraphVertex(dialect, "a", "person")
        f = GraphEdge(dialect, "f", "follows", GraphEdgeDirection.RIGHT)
        b = GraphVertex(dialect, "b", "person")
        match = MatchClause(dialect, a, f, b)
        cols = ColumnsClause(dialect, GraphColumn("b", "name", "b_name"))
        gt = GraphTableExpression(dialect, GRAPH_NAME, match, cols, alias="g")

        query = QueryExpression(dialect,
            select=[Column(dialect, "b_name")],
            from_=gt)
        explain = ExplainExpression(dialect, statement=query,
            options=ExplainOptions(costs=False))
        sql, params = explain.to_sql()
        rows = postgres_backend.fetch_all(sql, params)
        explain_text = " ".join(r["QUERY PLAN"] for r in rows)
        assert "Nested Loop" in explain_text or "Hash Join" in explain_text
