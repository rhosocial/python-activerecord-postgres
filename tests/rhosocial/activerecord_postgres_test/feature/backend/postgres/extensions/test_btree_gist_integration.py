# tests/rhosocial/activerecord_postgres_test/feature/backend/postgres/extensions/test_btree_gist_integration.py

"""Integration tests for PostgreSQL btree_gist extension with real database.

These tests require a live PostgreSQL connection with btree_gist extension installed
and test:
- GiST index creation with btree_gist operator class on TIMESTAMP columns (gist_timestamp_ops)
- GiST index creation with btree_gist operator class on INTEGER columns (gist_int4_ops)
- Range queries using ComparisonPredicate on GiST-indexed columns

btree_gist has no function factories. It works through CreateIndexExpression with
index_type="GIST" and dialect_options={"opclasses": {"column": "gist_int4_ops"}} for
specifying btree_gist operator classes that enable B-tree equivalence on GiST indexes.
The btree_gist extension provides opclasses like gist_int4_ops, gist_timestamp_ops,
gist_text_ops, etc. that allow GiST indexes to support B-tree-like operations.
This is particularly useful for exclusion constraints and range queries.
"""

import pytest
import pytest_asyncio

from rhosocial.activerecord_postgres_test.feature.backend.utils import (
    ensure_extension_installed,
    async_ensure_extension_installed,
)
from rhosocial.activerecord.backend.expression import (
    CreateTableExpression,
    ColumnDefinition,
    ColumnConstraint,
    ColumnConstraintType,
    DropTableExpression,
    InsertExpression,
    QueryExpression,
    TableExpression,
    Column,
    CreateIndexExpression,
)
from rhosocial.activerecord.backend.expression.statements import ValuesSource
from rhosocial.activerecord.backend.expression.core import Literal
from rhosocial.activerecord.backend.expression.predicates import ComparisonPredicate
from rhosocial.activerecord.backend.options import ExecutionOptions
from rhosocial.activerecord.backend.schema import StatementType


@pytest.fixture
def btree_gist_env(postgres_backend_single):
    """Independent test environment for btree_gist extension."""
    backend = postgres_backend_single
    ensure_extension_installed(backend, "btree_gist")
    dialect = backend.dialect

    # Setup: create test table using expression
    columns = [
        ColumnDefinition(
            name="id",
            data_type="SERIAL",
            constraints=[
                ColumnConstraint(ColumnConstraintType.PRIMARY_KEY),
            ],
        ),
        ColumnDefinition(name="name", data_type="TEXT"),
        ColumnDefinition(name="created_at", data_type="TIMESTAMP"),
        ColumnDefinition(name="value", data_type="INTEGER"),
    ]
    create_expr = CreateTableExpression(
        dialect=dialect,
        table_name="test_btree_gist",
        columns=columns,
        if_not_exists=True,
    )
    sql, params = create_expr.to_sql()
    backend.execute(sql, params)

    # Setup: insert seed data using expression
    rows = [
        [Literal(dialect, "Event A"), Literal(dialect, "2024-01-15 10:00:00"), Literal(dialect, 10)],
        [Literal(dialect, "Event B"), Literal(dialect, "2024-03-20 14:30:00"), Literal(dialect, 20)],
        [Literal(dialect, "Event C"), Literal(dialect, "2024-06-10 09:00:00"), Literal(dialect, 30)],
        [Literal(dialect, "Event D"), Literal(dialect, "2024-09-05 16:45:00"), Literal(dialect, 40)],
    ]
    insert_expr = InsertExpression(
        dialect=dialect,
        into="test_btree_gist",
        columns=["name", "created_at", "value"],
        source=ValuesSource(dialect, rows),
    )
    sql, params = insert_expr.to_sql()
    backend.execute(sql, params)

    yield backend, dialect

    # Teardown: drop table using expression
    drop_expr = DropTableExpression(
        dialect=dialect,
        table_name="test_btree_gist",
        if_exists=True,
    )
    sql, params = drop_expr.to_sql()
    backend.execute(sql, params)


class TestBtreeGistIntegration:
    """Integration tests for btree_gist extension."""

    def test_btree_gist_timestamp_index(self, btree_gist_env):
        """Test creating a GiST index with btree_gist gist_timestamp_ops operator class."""
        backend, dialect = btree_gist_env
        opts = ExecutionOptions(stmt_type=StatementType.DQL)

        # Create a GiST index with btree_gist operator class on TIMESTAMP column
        create_idx = CreateIndexExpression(
            dialect=dialect,
            index_name="idx_btree_gist_created_at",
            table_name="test_btree_gist",
            columns=["created_at"],
            index_type="GIST",
            if_not_exists=True,
            dialect_options={"opclasses": {"created_at": "gist_timestamp_ops"}},
        )
        sql, params = create_idx.to_sql()
        backend.execute(sql, params)

        # Verify index exists by querying pg_indexes
        query = QueryExpression(
            dialect=dialect,
            select=[Column(dialect, "indexname")],
            from_=TableExpression(dialect, "pg_indexes"),
            where=Column(dialect, "tablename") == Literal(dialect, "test_btree_gist"),
        )
        sql, params = query.to_sql()
        result = backend.execute(sql, params, options=opts)
        assert result.data is not None
        index_names = [row["indexname"] for row in result.data]
        assert "idx_btree_gist_created_at" in index_names

    def test_btree_gist_int4_index(self, btree_gist_env):
        """Test creating a GiST index with btree_gist gist_int4_ops operator class on INTEGER column."""
        backend, dialect = btree_gist_env
        opts = ExecutionOptions(stmt_type=StatementType.DQL)

        # Create a GiST index with btree_gist operator class on INTEGER column
        create_idx = CreateIndexExpression(
            dialect=dialect,
            index_name="idx_btree_gist_value",
            table_name="test_btree_gist",
            columns=["value"],
            index_type="GIST",
            if_not_exists=True,
            dialect_options={"opclasses": {"value": "gist_int4_ops"}},
        )
        sql, params = create_idx.to_sql()
        backend.execute(sql, params)

        # Verify index exists by querying pg_indexes
        query = QueryExpression(
            dialect=dialect,
            select=[Column(dialect, "indexname")],
            from_=TableExpression(dialect, "pg_indexes"),
            where=Column(dialect, "tablename") == Literal(dialect, "test_btree_gist"),
        )
        sql, params = query.to_sql()
        result = backend.execute(sql, params, options=opts)
        assert result.data is not None
        index_names = [row["indexname"] for row in result.data]
        assert "idx_btree_gist_value" in index_names

    def test_btree_gist_range_query(self, btree_gist_env):
        """Test range query on a GiST-indexed TIMESTAMP column using ComparisonPredicate."""
        backend, dialect = btree_gist_env
        opts = ExecutionOptions(stmt_type=StatementType.DQL)

        # Create a GiST index with btree_gist operator class on TIMESTAMP column
        create_idx = CreateIndexExpression(
            dialect=dialect,
            index_name="idx_btree_gist_created_at",
            table_name="test_btree_gist",
            columns=["created_at"],
            index_type="GIST",
            if_not_exists=True,
            dialect_options={"opclasses": {"created_at": "gist_timestamp_ops"}},
        )
        sql, params = create_idx.to_sql()
        backend.execute(sql, params)

        # Query with range condition using ComparisonPredicate
        # Find events where created_at > '2024-03-01'
        query = QueryExpression(
            dialect=dialect,
            select=[Column(dialect, "name"), Column(dialect, "created_at")],
            from_=TableExpression(dialect, "test_btree_gist"),
            where=ComparisonPredicate(
                dialect,
                ">",
                Column(dialect, "created_at"),
                Literal(dialect, "2024-03-01 00:00:00"),
            ),
        )
        sql, params = query.to_sql()
        result = backend.execute(sql, params, options=opts)
        assert result.data is not None
        # Events B (Mar 20), C (Jun 10), D (Sep 5) should match
        assert len(result.data) == 3
        names = [row["name"] for row in result.data]
        assert "Event B" in names
        assert "Event C" in names
        assert "Event D" in names


@pytest_asyncio.fixture
async def async_btree_gist_env(async_postgres_backend_single):
    """Independent async test environment for btree_gist extension."""
    backend = async_postgres_backend_single
    await async_ensure_extension_installed(backend, "btree_gist")
    dialect = backend.dialect

    # Setup: create test table using expression
    columns = [
        ColumnDefinition(
            name="id",
            data_type="SERIAL",
            constraints=[
                ColumnConstraint(ColumnConstraintType.PRIMARY_KEY),
            ],
        ),
        ColumnDefinition(name="name", data_type="TEXT"),
        ColumnDefinition(name="created_at", data_type="TIMESTAMP"),
        ColumnDefinition(name="value", data_type="INTEGER"),
    ]
    create_expr = CreateTableExpression(
        dialect=dialect,
        table_name="test_btree_gist_async",
        columns=columns,
        if_not_exists=True,
    )
    sql, params = create_expr.to_sql()
    await backend.execute(sql, params)

    # Setup: insert seed data using expression
    rows = [
        [Literal(dialect, "Event A"), Literal(dialect, "2024-01-15 10:00:00"), Literal(dialect, 10)],
        [Literal(dialect, "Event B"), Literal(dialect, "2024-03-20 14:30:00"), Literal(dialect, 20)],
        [Literal(dialect, "Event C"), Literal(dialect, "2024-06-10 09:00:00"), Literal(dialect, 30)],
        [Literal(dialect, "Event D"), Literal(dialect, "2024-09-05 16:45:00"), Literal(dialect, 40)],
    ]
    insert_expr = InsertExpression(
        dialect=dialect,
        into="test_btree_gist_async",
        columns=["name", "created_at", "value"],
        source=ValuesSource(dialect, rows),
    )
    sql, params = insert_expr.to_sql()
    await backend.execute(sql, params)

    yield backend, dialect

    # Teardown: drop table using expression
    drop_expr = DropTableExpression(
        dialect=dialect,
        table_name="test_btree_gist_async",
        if_exists=True,
    )
    sql, params = drop_expr.to_sql()
    await backend.execute(sql, params)


class TestAsyncBtreeGistIntegration:
    """Async integration tests for btree_gist extension."""

    @pytest.mark.asyncio
    async def test_async_btree_gist_timestamp_index(self, async_btree_gist_env):
        """Test creating a GiST index with btree_gist gist_timestamp_ops operator class."""
        backend, dialect = async_btree_gist_env
        opts = ExecutionOptions(stmt_type=StatementType.DQL)

        # Create a GiST index with btree_gist operator class on TIMESTAMP column
        create_idx = CreateIndexExpression(
            dialect=dialect,
            index_name="idx_btree_gist_created_at_async",
            table_name="test_btree_gist_async",
            columns=["created_at"],
            index_type="GIST",
            if_not_exists=True,
            dialect_options={"opclasses": {"created_at": "gist_timestamp_ops"}},
        )
        sql, params = create_idx.to_sql()
        await backend.execute(sql, params)

        # Verify index exists by querying pg_indexes
        query = QueryExpression(
            dialect=dialect,
            select=[Column(dialect, "indexname")],
            from_=TableExpression(dialect, "pg_indexes"),
            where=Column(dialect, "tablename") == Literal(dialect, "test_btree_gist_async"),
        )
        sql, params = query.to_sql()
        result = await backend.execute(sql, params, options=opts)
        assert result.data is not None
        index_names = [row["indexname"] for row in result.data]
        assert "idx_btree_gist_created_at_async" in index_names

    @pytest.mark.asyncio
    async def test_async_btree_gist_int4_index(self, async_btree_gist_env):
        """Test creating a GiST index with btree_gist gist_int4_ops operator class on INTEGER column."""
        backend, dialect = async_btree_gist_env
        opts = ExecutionOptions(stmt_type=StatementType.DQL)

        # Create a GiST index with btree_gist operator class on INTEGER column
        create_idx = CreateIndexExpression(
            dialect=dialect,
            index_name="idx_btree_gist_value_async",
            table_name="test_btree_gist_async",
            columns=["value"],
            index_type="GIST",
            if_not_exists=True,
            dialect_options={"opclasses": {"value": "gist_int4_ops"}},
        )
        sql, params = create_idx.to_sql()
        await backend.execute(sql, params)

        # Verify index exists by querying pg_indexes
        query = QueryExpression(
            dialect=dialect,
            select=[Column(dialect, "indexname")],
            from_=TableExpression(dialect, "pg_indexes"),
            where=Column(dialect, "tablename") == Literal(dialect, "test_btree_gist_async"),
        )
        sql, params = query.to_sql()
        result = await backend.execute(sql, params, options=opts)
        assert result.data is not None
        index_names = [row["indexname"] for row in result.data]
        assert "idx_btree_gist_value_async" in index_names

    @pytest.mark.asyncio
    async def test_async_btree_gist_range_query(self, async_btree_gist_env):
        """Test range query on a GiST-indexed TIMESTAMP column using ComparisonPredicate."""
        backend, dialect = async_btree_gist_env
        opts = ExecutionOptions(stmt_type=StatementType.DQL)

        # Create a GiST index with btree_gist operator class on TIMESTAMP column
        create_idx = CreateIndexExpression(
            dialect=dialect,
            index_name="idx_btree_gist_created_at_async",
            table_name="test_btree_gist_async",
            columns=["created_at"],
            index_type="GIST",
            if_not_exists=True,
            dialect_options={"opclasses": {"created_at": "gist_timestamp_ops"}},
        )
        sql, params = create_idx.to_sql()
        await backend.execute(sql, params)

        # Query with range condition using ComparisonPredicate
        # Find events where created_at > '2024-03-01'
        query = QueryExpression(
            dialect=dialect,
            select=[Column(dialect, "name"), Column(dialect, "created_at")],
            from_=TableExpression(dialect, "test_btree_gist_async"),
            where=ComparisonPredicate(
                dialect,
                ">",
                Column(dialect, "created_at"),
                Literal(dialect, "2024-03-01 00:00:00"),
            ),
        )
        sql, params = query.to_sql()
        result = await backend.execute(sql, params, options=opts)
        assert result.data is not None
        # Events B (Mar 20), C (Jun 10), D (Sep 5) should match
        assert len(result.data) == 3
        names = [row["name"] for row in result.data]
        assert "Event B" in names
        assert "Event C" in names
        assert "Event D" in names
