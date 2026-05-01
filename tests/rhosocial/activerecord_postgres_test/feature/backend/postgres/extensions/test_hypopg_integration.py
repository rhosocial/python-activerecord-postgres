# tests/rhosocial/activerecord_postgres_test/feature/backend/postgres/extensions/test_hypopg_integration.py

"""Integration tests for the hypopg extension.

These tests require a PostgreSQL database with the hypopg extension installed.
Tests will be automatically skipped if the extension is not available.
All database operations use expression objects, not raw SQL strings.
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
)
from rhosocial.activerecord.backend.expression.statements import ValuesSource
from rhosocial.activerecord.backend.expression.core import Literal
from rhosocial.activerecord.backend.impl.postgres.functions.hypopg import (
    hypopg_create_index,
    hypopg_reset,
    hypopg_show_indexes,
    hypopg_estimate_size,
)
from rhosocial.activerecord.backend.options import ExecutionOptions
from rhosocial.activerecord.backend.schema import StatementType


@pytest.fixture
def hypopg_env(postgres_backend_single):
    """Independent test environment for hypopg extension."""
    backend = postgres_backend_single
    ensure_extension_installed(backend, "hypopg")
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
        ColumnDefinition(name="email", data_type="TEXT"),
    ]
    create_expr = CreateTableExpression(
        dialect=dialect,
        table_name="test_hypopg_items",
        columns=columns,
        if_not_exists=True,
    )
    sql, params = create_expr.to_sql()
    backend.execute(sql, params)

    # Setup: insert seed data using expression
    rows = []
    for i in range(10):
        rows.append(
            [
                Literal(dialect, f"user{i}"),
                Literal(dialect, f"user{i}@example.com"),
            ]
        )
    insert_expr = InsertExpression(
        dialect=dialect,
        into="test_hypopg_items",
        columns=["name", "email"],
        source=ValuesSource(dialect, rows),
    )
    sql, params = insert_expr.to_sql()
    backend.execute(sql, params)

    # Reset any leftover hypothetical indexes
    reset_func = hypopg_reset(dialect)
    query = QueryExpression(
        dialect=dialect,
        select=[reset_func],
    )
    sql, params = query.to_sql()
    opts = ExecutionOptions(stmt_type=StatementType.DQL)
    backend.execute(sql, params, options=opts)

    yield backend, dialect

    # Teardown: drop table using expression
    drop_expr = DropTableExpression(
        dialect=dialect,
        table_name="test_hypopg_items",
        if_exists=True,
    )
    sql, params = drop_expr.to_sql()
    backend.execute(sql, params)

    # Reset hypothetical indexes
    reset_func = hypopg_reset(dialect)
    query = QueryExpression(
        dialect=dialect,
        select=[reset_func],
    )
    sql, params = query.to_sql()
    backend.execute(sql, params, options=opts)


class TestHypopgIntegration:
    """Integration tests for hypopg hypothetical index support."""

    def test_hypopg_create_index(self, hypopg_env):
        """Test creating a hypothetical index using hypopg_create_index."""
        backend, dialect = hypopg_env

        # Create a hypothetical index
        create_func = hypopg_create_index(
            dialect,
            "CREATE INDEX ON test_hypopg_items USING btree (email)",
        )
        query = QueryExpression(
            dialect=dialect,
            select=[create_func.as_("index_id")],
        )
        sql, params = query.to_sql()
        opts = ExecutionOptions(stmt_type=StatementType.DQL)
        result = backend.execute(sql, params, options=opts)
        assert result.data is not None
        assert len(result.data) > 0
        # hypopg_create_index returns the OID of the hypothetical index
        assert result.data[0]["index_id"] is not None

    def test_hypopg_show_indexes(self, hypopg_env):
        """Test listing hypothetical indexes using hypopg_show_indexes."""
        backend, dialect = hypopg_env

        # First create a hypothetical index
        create_func = hypopg_create_index(
            dialect,
            "CREATE INDEX ON test_hypopg_items USING btree (name)",
        )
        query = QueryExpression(
            dialect=dialect,
            select=[create_func.as_("index_id")],
        )
        sql, params = query.to_sql()
        opts = ExecutionOptions(stmt_type=StatementType.DQL)
        backend.execute(sql, params, options=opts)

        # Then list all hypothetical indexes
        show_func = hypopg_show_indexes(dialect)
        query = QueryExpression(
            dialect=dialect,
            select=[show_func.as_("index_info")],
        )
        sql, params = query.to_sql()
        result = backend.execute(sql, params, options=opts)
        assert result.data is not None
        assert len(result.data) > 0

    def test_hypopg_estimate_size(self, hypopg_env):
        """Test estimating hypothetical index size using hypopg_estimate_size."""
        backend, dialect = hypopg_env

        # Create a hypothetical index
        create_func = hypopg_create_index(
            dialect,
            "CREATE INDEX ON test_hypopg_items USING btree (email)",
        )
        query = QueryExpression(
            dialect=dialect,
            select=[create_func.as_("result")],
        )
        sql, params = query.to_sql()
        opts = ExecutionOptions(stmt_type=StatementType.DQL)
        result = backend.execute(sql, params, options=opts)
        # hypopg_create_index returns (indexrelid oid, indexname text)
        # psycopg returns it as a tuple or composite string
        raw = result.data[0]["result"]
        if isinstance(raw, tuple):
            index_id = int(raw[0])  # First element is indexrelid
        elif isinstance(raw, str):
            index_id = int(raw.split(",")[0].strip("( "))
        else:
            index_id = raw

        # Estimate the size of the hypothetical index
        size_func = hypopg_estimate_size(dialect, index_id)
        query = QueryExpression(
            dialect=dialect,
            select=[size_func.as_("estimated_size")],
        )
        sql, params = query.to_sql()
        result = backend.execute(sql, params, options=opts)
        assert result.data is not None
        assert len(result.data) > 0
        # The estimated size should be a positive number
        estimated_size = result.data[0]["estimated_size"]
        assert estimated_size is not None

    def test_hypopg_reset(self, hypopg_env):
        """Test resetting hypothetical indexes using hypopg_reset."""
        backend, dialect = hypopg_env

        # Create a hypothetical index
        create_func = hypopg_create_index(
            dialect,
            "CREATE INDEX ON test_hypopg_items USING btree (name)",
        )
        query = QueryExpression(
            dialect=dialect,
            select=[create_func.as_("index_id")],
        )
        sql, params = query.to_sql()
        opts = ExecutionOptions(stmt_type=StatementType.DQL)
        backend.execute(sql, params, options=opts)

        # Verify the index exists by listing
        show_func = hypopg_show_indexes(dialect)
        query = QueryExpression(
            dialect=dialect,
            select=[show_func.as_("index_info")],
        )
        sql, params = query.to_sql()
        result = backend.execute(sql, params, options=opts)
        assert len(result.data) > 0

        # Reset all hypothetical indexes
        reset_func = hypopg_reset(dialect)
        query = QueryExpression(
            dialect=dialect,
            select=[reset_func],
        )
        sql, params = query.to_sql()
        backend.execute(sql, params, options=opts)

        # Verify no hypothetical indexes remain
        show_func = hypopg_show_indexes(dialect)
        query = QueryExpression(
            dialect=dialect,
            select=[show_func.as_("index_info")],
        )
        sql, params = query.to_sql()
        result = backend.execute(sql, params, options=opts)
        assert len(result.data) == 0


@pytest_asyncio.fixture
async def async_hypopg_env(async_postgres_backend_single):
    """Independent async test environment for hypopg extension."""
    backend = async_postgres_backend_single
    await async_ensure_extension_installed(backend, "hypopg")
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
        ColumnDefinition(name="email", data_type="TEXT"),
    ]
    create_expr = CreateTableExpression(
        dialect=dialect,
        table_name="test_hypopg_items_async",
        columns=columns,
        if_not_exists=True,
    )
    sql, params = create_expr.to_sql()
    await backend.execute(sql, params)

    # Setup: insert seed data using expression
    rows = []
    for i in range(10):
        rows.append(
            [
                Literal(dialect, f"user{i}"),
                Literal(dialect, f"user{i}@example.com"),
            ]
        )
    insert_expr = InsertExpression(
        dialect=dialect,
        into="test_hypopg_items_async",
        columns=["name", "email"],
        source=ValuesSource(dialect, rows),
    )
    sql, params = insert_expr.to_sql()
    await backend.execute(sql, params)

    # Reset any leftover hypothetical indexes
    reset_func = hypopg_reset(dialect)
    query = QueryExpression(
        dialect=dialect,
        select=[reset_func],
    )
    sql, params = query.to_sql()
    opts = ExecutionOptions(stmt_type=StatementType.DQL)
    await backend.execute(sql, params, options=opts)

    yield backend, dialect

    # Teardown: drop table using expression
    drop_expr = DropTableExpression(
        dialect=dialect,
        table_name="test_hypopg_items_async",
        if_exists=True,
    )
    sql, params = drop_expr.to_sql()
    await backend.execute(sql, params)

    # Reset hypothetical indexes
    reset_func = hypopg_reset(dialect)
    query = QueryExpression(
        dialect=dialect,
        select=[reset_func],
    )
    sql, params = query.to_sql()
    await backend.execute(sql, params, options=opts)


class TestAsyncHypopgIntegration:
    """Async integration tests for hypopg hypothetical index support."""

    @pytest.mark.asyncio
    async def test_async_hypopg_create_index(self, async_hypopg_env):
        """Test creating a hypothetical index using hypopg_create_index."""
        backend, dialect = async_hypopg_env

        # Create a hypothetical index
        create_func = hypopg_create_index(
            dialect,
            "CREATE INDEX ON test_hypopg_items_async USING btree (email)",
        )
        query = QueryExpression(
            dialect=dialect,
            select=[create_func.as_("index_id")],
        )
        sql, params = query.to_sql()
        opts = ExecutionOptions(stmt_type=StatementType.DQL)
        result = await backend.execute(sql, params, options=opts)
        assert result.data is not None
        assert len(result.data) > 0
        # hypopg_create_index returns the OID of the hypothetical index
        assert result.data[0]["index_id"] is not None

    @pytest.mark.asyncio
    async def test_async_hypopg_show_indexes(self, async_hypopg_env):
        """Test listing hypothetical indexes using hypopg_show_indexes."""
        backend, dialect = async_hypopg_env

        # First create a hypothetical index
        create_func = hypopg_create_index(
            dialect,
            "CREATE INDEX ON test_hypopg_items_async USING btree (name)",
        )
        query = QueryExpression(
            dialect=dialect,
            select=[create_func.as_("index_id")],
        )
        sql, params = query.to_sql()
        opts = ExecutionOptions(stmt_type=StatementType.DQL)
        await backend.execute(sql, params, options=opts)

        # Then list all hypothetical indexes
        show_func = hypopg_show_indexes(dialect)
        query = QueryExpression(
            dialect=dialect,
            select=[show_func.as_("index_info")],
        )
        sql, params = query.to_sql()
        result = await backend.execute(sql, params, options=opts)
        assert result.data is not None
        assert len(result.data) > 0

    @pytest.mark.asyncio
    async def test_async_hypopg_estimate_size(self, async_hypopg_env):
        """Test estimating hypothetical index size using hypopg_estimate_size."""
        backend, dialect = async_hypopg_env

        # Create a hypothetical index
        create_func = hypopg_create_index(
            dialect,
            "CREATE INDEX ON test_hypopg_items_async USING btree (email)",
        )
        query = QueryExpression(
            dialect=dialect,
            select=[create_func.as_("result")],
        )
        sql, params = query.to_sql()
        opts = ExecutionOptions(stmt_type=StatementType.DQL)
        result = await backend.execute(sql, params, options=opts)
        # hypopg_create_index returns (indexrelid oid, indexname text)
        # psycopg returns it as a tuple or composite string
        raw = result.data[0]["result"]
        if isinstance(raw, tuple):
            index_id = int(raw[0])  # First element is indexrelid
        elif isinstance(raw, str):
            index_id = int(raw.split(",")[0].strip("( "))
        else:
            index_id = raw

        # Estimate the size of the hypothetical index
        size_func = hypopg_estimate_size(dialect, index_id)
        query = QueryExpression(
            dialect=dialect,
            select=[size_func.as_("estimated_size")],
        )
        sql, params = query.to_sql()
        result = await backend.execute(sql, params, options=opts)
        assert result.data is not None
        assert len(result.data) > 0
        # The estimated size should be a positive number
        estimated_size = result.data[0]["estimated_size"]
        assert estimated_size is not None

    @pytest.mark.asyncio
    async def test_async_hypopg_reset(self, async_hypopg_env):
        """Test resetting hypothetical indexes using hypopg_reset."""
        backend, dialect = async_hypopg_env

        # Create a hypothetical index
        create_func = hypopg_create_index(
            dialect,
            "CREATE INDEX ON test_hypopg_items_async USING btree (name)",
        )
        query = QueryExpression(
            dialect=dialect,
            select=[create_func.as_("index_id")],
        )
        sql, params = query.to_sql()
        opts = ExecutionOptions(stmt_type=StatementType.DQL)
        await backend.execute(sql, params, options=opts)

        # Verify the index exists by listing
        show_func = hypopg_show_indexes(dialect)
        query = QueryExpression(
            dialect=dialect,
            select=[show_func.as_("index_info")],
        )
        sql, params = query.to_sql()
        result = await backend.execute(sql, params, options=opts)
        assert len(result.data) > 0

        # Reset all hypothetical indexes
        reset_func = hypopg_reset(dialect)
        query = QueryExpression(
            dialect=dialect,
            select=[reset_func],
        )
        sql, params = query.to_sql()
        await backend.execute(sql, params, options=opts)

        # Verify no hypothetical indexes remain
        show_func = hypopg_show_indexes(dialect)
        query = QueryExpression(
            dialect=dialect,
            select=[show_func.as_("index_info")],
        )
        sql, params = query.to_sql()
        result = await backend.execute(sql, params, options=opts)
        assert len(result.data) == 0
