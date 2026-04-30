# tests/rhosocial/activerecord_postgres_test/feature/backend/postgres/extensions/test_btree_gin_integration.py

"""Integration tests for PostgreSQL btree_gin extension with real database.

These tests require a live PostgreSQL connection with btree_gin extension installed
and test:
- GIN index creation with btree_gin operator class on INTEGER columns (int4_ops)
- GIN index creation with btree_gin operator class on TEXT columns (text_ops)
- Query data access through GIN index

btree_gin has no function factories. It works through CreateIndexExpression with
index_type="GIN" and dialect_options={"opclasses": {"column": "int4_ops"}} for
specifying btree_gin operator classes that enable B-tree equivalence on GIN indexes.
The btree_gin extension provides opclasses like int4_ops, int8_ops, text_ops, etc.
that allow GIN indexes to support equality comparisons on scalar data types.
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
from rhosocial.activerecord.backend.options import ExecutionOptions
from rhosocial.activerecord.backend.schema import StatementType


@pytest.fixture
def btree_gin_env(postgres_backend_single):
    """Independent test environment for btree_gin extension."""
    backend = postgres_backend_single
    ensure_extension_installed(backend, "btree_gin")
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
        ColumnDefinition(name="value", data_type="INTEGER"),
        ColumnDefinition(name="tags", data_type="TEXT[]"),
    ]
    create_expr = CreateTableExpression(
        dialect=dialect,
        table_name="test_btree_gin",
        columns=columns,
        if_not_exists=True,
    )
    sql, params = create_expr.to_sql()
    backend.execute(sql, params)

    # Setup: insert seed data using expression
    rows = [
        [Literal(dialect, "Alice"), Literal(dialect, 10), Literal(dialect, "{engineering,backend}")],
        [Literal(dialect, "Bob"), Literal(dialect, 20), Literal(dialect, "{marketing,frontend}")],
        [Literal(dialect, "Charlie"), Literal(dialect, 30), Literal(dialect, "{engineering,frontend}")],
    ]
    insert_expr = InsertExpression(
        dialect=dialect,
        into="test_btree_gin",
        columns=["name", "value", "tags"],
        source=ValuesSource(dialect, rows),
    )
    sql, params = insert_expr.to_sql()
    backend.execute(sql, params)

    yield backend, dialect

    # Teardown: drop table using expression
    drop_expr = DropTableExpression(
        dialect=dialect,
        table_name="test_btree_gin",
        if_exists=True,
    )
    sql, params = drop_expr.to_sql()
    backend.execute(sql, params)


class TestBtreeGinIntegration:
    """Integration tests for btree_gin extension."""

    def test_btree_gin_int4_ops_index(self, btree_gin_env):
        """Test creating a GIN index with btree_gin int4_ops operator class on INTEGER column."""
        backend, dialect = btree_gin_env
        opts = ExecutionOptions(stmt_type=StatementType.DQL)

        # Create a GIN index with btree_gin operator class on INTEGER column
        create_idx = CreateIndexExpression(
            dialect=dialect,
            index_name="idx_btree_gin_value",
            table_name="test_btree_gin",
            columns=["value"],
            index_type="GIN",
            if_not_exists=True,
            dialect_options={"opclasses": {"value": "int4_ops"}},
        )
        sql, params = create_idx.to_sql()
        backend.execute(sql, params)

        # Verify index exists by querying pg_indexes
        query = QueryExpression(
            dialect=dialect,
            select=[Column(dialect, "indexname")],
            from_=TableExpression(dialect, "pg_indexes"),
            where=Column(dialect, "tablename") == Literal(dialect, "test_btree_gin"),
        )
        sql, params = query.to_sql()
        result = backend.execute(sql, params, options=opts)
        assert result.data is not None
        index_names = [row["indexname"] for row in result.data]
        assert "idx_btree_gin_value" in index_names

    def test_btree_gin_text_ops_index(self, btree_gin_env):
        """Test creating a GIN index with btree_gin text_ops operator class on TEXT column."""
        backend, dialect = btree_gin_env
        opts = ExecutionOptions(stmt_type=StatementType.DQL)

        # Create a GIN index with btree_gin operator class on TEXT column
        create_idx = CreateIndexExpression(
            dialect=dialect,
            index_name="idx_btree_gin_name",
            table_name="test_btree_gin",
            columns=["name"],
            index_type="GIN",
            if_not_exists=True,
            dialect_options={"opclasses": {"name": "text_ops"}},
        )
        sql, params = create_idx.to_sql()
        backend.execute(sql, params)

        # Verify index exists by querying pg_indexes
        query = QueryExpression(
            dialect=dialect,
            select=[Column(dialect, "indexname")],
            from_=TableExpression(dialect, "pg_indexes"),
            where=Column(dialect, "tablename") == Literal(dialect, "test_btree_gin"),
        )
        sql, params = query.to_sql()
        result = backend.execute(sql, params, options=opts)
        assert result.data is not None
        index_names = [row["indexname"] for row in result.data]
        assert "idx_btree_gin_name" in index_names

    def test_btree_gin_index_query(self, btree_gin_env):
        """Test querying data after creating a GIN index with btree_gin operator class."""
        backend, dialect = btree_gin_env
        opts = ExecutionOptions(stmt_type=StatementType.DQL)

        # Create a GIN index with btree_gin operator class on INTEGER column
        create_idx = CreateIndexExpression(
            dialect=dialect,
            index_name="idx_btree_gin_value",
            table_name="test_btree_gin",
            columns=["value"],
            index_type="GIN",
            if_not_exists=True,
            dialect_options={"opclasses": {"value": "int4_ops"}},
        )
        sql, params = create_idx.to_sql()
        backend.execute(sql, params)

        # Query the table and verify data is accessible
        query = QueryExpression(
            dialect=dialect,
            select=[Column(dialect, "id"), Column(dialect, "name"), Column(dialect, "value")],
            from_=TableExpression(dialect, "test_btree_gin"),
        )
        sql, params = query.to_sql()
        result = backend.execute(sql, params, options=opts)
        assert result.data is not None
        assert len(result.data) == 3
        names = [row["name"] for row in result.data]
        assert "Alice" in names
        assert "Bob" in names
        assert "Charlie" in names


@pytest_asyncio.fixture
async def async_btree_gin_env(async_postgres_backend_single):
    """Independent async test environment for btree_gin extension."""
    backend = async_postgres_backend_single
    await async_ensure_extension_installed(backend, "btree_gin")
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
        ColumnDefinition(name="value", data_type="INTEGER"),
        ColumnDefinition(name="tags", data_type="TEXT[]"),
    ]
    create_expr = CreateTableExpression(
        dialect=dialect,
        table_name="test_btree_gin_async",
        columns=columns,
        if_not_exists=True,
    )
    sql, params = create_expr.to_sql()
    await backend.execute(sql, params)

    # Setup: insert seed data using expression
    rows = [
        [Literal(dialect, "Alice"), Literal(dialect, 10), Literal(dialect, "{engineering,backend}")],
        [Literal(dialect, "Bob"), Literal(dialect, 20), Literal(dialect, "{marketing,frontend}")],
        [Literal(dialect, "Charlie"), Literal(dialect, 30), Literal(dialect, "{engineering,frontend}")],
    ]
    insert_expr = InsertExpression(
        dialect=dialect,
        into="test_btree_gin_async",
        columns=["name", "value", "tags"],
        source=ValuesSource(dialect, rows),
    )
    sql, params = insert_expr.to_sql()
    await backend.execute(sql, params)

    yield backend, dialect

    # Teardown: drop table using expression
    drop_expr = DropTableExpression(
        dialect=dialect,
        table_name="test_btree_gin_async",
        if_exists=True,
    )
    sql, params = drop_expr.to_sql()
    await backend.execute(sql, params)


class TestAsyncBtreeGinIntegration:
    """Async integration tests for btree_gin extension."""

    @pytest.mark.asyncio
    async def test_async_btree_gin_int4_ops_index(self, async_btree_gin_env):
        """Test creating a GIN index with btree_gin int4_ops operator class on INTEGER column."""
        backend, dialect = async_btree_gin_env
        opts = ExecutionOptions(stmt_type=StatementType.DQL)

        # Create a GIN index with btree_gin operator class on INTEGER column
        create_idx = CreateIndexExpression(
            dialect=dialect,
            index_name="idx_btree_gin_value_async",
            table_name="test_btree_gin_async",
            columns=["value"],
            index_type="GIN",
            if_not_exists=True,
            dialect_options={"opclasses": {"value": "int4_ops"}},
        )
        sql, params = create_idx.to_sql()
        await backend.execute(sql, params)

        # Verify index exists by querying pg_indexes
        query = QueryExpression(
            dialect=dialect,
            select=[Column(dialect, "indexname")],
            from_=TableExpression(dialect, "pg_indexes"),
            where=Column(dialect, "tablename") == Literal(dialect, "test_btree_gin_async"),
        )
        sql, params = query.to_sql()
        result = await backend.execute(sql, params, options=opts)
        assert result.data is not None
        index_names = [row["indexname"] for row in result.data]
        assert "idx_btree_gin_value_async" in index_names

    @pytest.mark.asyncio
    async def test_async_btree_gin_text_ops_index(self, async_btree_gin_env):
        """Test creating a GIN index with btree_gin text_ops operator class on TEXT column."""
        backend, dialect = async_btree_gin_env
        opts = ExecutionOptions(stmt_type=StatementType.DQL)

        # Create a GIN index with btree_gin operator class on TEXT column
        create_idx = CreateIndexExpression(
            dialect=dialect,
            index_name="idx_btree_gin_name_async",
            table_name="test_btree_gin_async",
            columns=["name"],
            index_type="GIN",
            if_not_exists=True,
            dialect_options={"opclasses": {"name": "text_ops"}},
        )
        sql, params = create_idx.to_sql()
        await backend.execute(sql, params)

        # Verify index exists by querying pg_indexes
        query = QueryExpression(
            dialect=dialect,
            select=[Column(dialect, "indexname")],
            from_=TableExpression(dialect, "pg_indexes"),
            where=Column(dialect, "tablename") == Literal(dialect, "test_btree_gin_async"),
        )
        sql, params = query.to_sql()
        result = await backend.execute(sql, params, options=opts)
        assert result.data is not None
        index_names = [row["indexname"] for row in result.data]
        assert "idx_btree_gin_name_async" in index_names

    @pytest.mark.asyncio
    async def test_async_btree_gin_index_query(self, async_btree_gin_env):
        """Test querying data after creating a GIN index with btree_gin operator class."""
        backend, dialect = async_btree_gin_env
        opts = ExecutionOptions(stmt_type=StatementType.DQL)

        # Create a GIN index with btree_gin operator class on INTEGER column
        create_idx = CreateIndexExpression(
            dialect=dialect,
            index_name="idx_btree_gin_value_async",
            table_name="test_btree_gin_async",
            columns=["value"],
            index_type="GIN",
            if_not_exists=True,
            dialect_options={"opclasses": {"value": "int4_ops"}},
        )
        sql, params = create_idx.to_sql()
        await backend.execute(sql, params)

        # Query the table and verify data is accessible
        query = QueryExpression(
            dialect=dialect,
            select=[Column(dialect, "id"), Column(dialect, "name"), Column(dialect, "value")],
            from_=TableExpression(dialect, "test_btree_gin_async"),
        )
        sql, params = query.to_sql()
        result = await backend.execute(sql, params, options=opts)
        assert result.data is not None
        assert len(result.data) == 3
        names = [row["name"] for row in result.data]
        assert "Alice" in names
        assert "Bob" in names
        assert "Charlie" in names
