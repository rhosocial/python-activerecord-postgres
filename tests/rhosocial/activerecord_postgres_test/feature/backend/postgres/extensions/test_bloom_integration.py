"""Integration tests for the bloom extension.

These tests require a PostgreSQL database with the bloom extension installed.
Tests will be automatically skipped if the extension is not available.
All database operations use expression objects, not raw SQL strings.

Note: CREATE INDEX ... USING bloom ... WITH (...) uses raw SQL because
the expression system does not structurally support index WITH parameters.
Similarly, pg_indexes queries use raw SQL as they query system catalog views.
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


TABLE_NAME = "test_bloom"
ASYNC_TABLE_NAME = "test_bloom_async"


@pytest.fixture
def bloom_env(postgres_backend_single):
    """Independent test environment for bloom extension."""
    backend = postgres_backend_single
    ensure_extension_installed(backend, "bloom")
    dialect = backend.dialect

    # Clean up residual tables from previous runs
    for table_name in [TABLE_NAME]:
        drop_expr = DropTableExpression(dialect=dialect, table_name=table_name, if_exists=True)
        sql, params = drop_expr.to_sql()
        backend.execute(sql, params)

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
        table_name=TABLE_NAME,
        columns=columns,
        if_not_exists=True,
    )
    sql, params = create_expr.to_sql()
    backend.execute(sql, params)

    # Setup: insert seed data using expression
    rows = []
    for i in range(100):
        rows.append([
            Literal(dialect, f"user{i}"),
            Literal(dialect, f"user{i}@example.com"),
        ])
    insert_expr = InsertExpression(
        dialect=dialect,
        into=TABLE_NAME,
        columns=["name", "email"],
        source=ValuesSource(dialect, rows),
    )
    sql, params = insert_expr.to_sql()
    backend.execute(sql, params)

    # Setup: create bloom index using expression
    create_idx = CreateIndexExpression(
        dialect=dialect,
        index_name="idx_bloom_test",
        table_name=TABLE_NAME,
        columns=["name", "email"],
        index_type="bloom",
        if_not_exists=True,
    )
    sql, params = create_idx.to_sql()
    backend.execute(sql, params)

    yield backend, dialect

    # Teardown: drop table using expression
    drop_expr = DropTableExpression(
        dialect=dialect,
        table_name=TABLE_NAME,
        if_exists=True,
    )
    sql, params = drop_expr.to_sql()
    backend.execute(sql, params)


class TestBloomIntegration:
    """Integration tests for bloom filter index access method."""

    def test_bloom_index_creation(self, bloom_env):
        """Test that bloom index was created successfully."""
        backend, dialect = bloom_env
        opts = ExecutionOptions(stmt_type=StatementType.DQL)

        # Type C: pg_indexes query kept as raw SQL (system catalog view)
        query = QueryExpression(
            dialect=dialect,
            select=[Column(dialect, "indexname")],
            from_=TableExpression(dialect, "pg_indexes"),
            where=Column(dialect, "tablename") == Literal(dialect, TABLE_NAME),
        )
        sql, params = query.to_sql()
        result = backend.execute(sql, params, options=opts)
        index_names = [r['indexname'] for r in result.data]
        assert "idx_bloom_test" in index_names

    def test_bloom_index_query(self, bloom_env):
        """Test querying a table with bloom index."""
        backend, dialect = bloom_env
        opts = ExecutionOptions(stmt_type=StatementType.DQL)

        query = QueryExpression(
            dialect=dialect,
            select=[Column(dialect, "id"), Column(dialect, "name"), Column(dialect, "email")],
            from_=TableExpression(dialect, TABLE_NAME),
            where=Column(dialect, "name") == Literal(dialect, "user25"),
        )
        sql, params = query.to_sql()
        result = backend.execute(sql, params, options=opts)
        assert len(result.data) == 1

    def test_bloom_index_with_options(self, bloom_env):
        """Test creating a bloom index with custom length and signature bits.

        Note: CREATE INDEX ... WITH (...) uses raw SQL because the expression
        system does not structurally support index WITH parameters.
        """
        backend, dialect = bloom_env

        # Create temporary table using expression
        columns = [
            ColumnDefinition(
                name="id",
                data_type="SERIAL",
                constraints=[
                    ColumnConstraint(ColumnConstraintType.PRIMARY_KEY),
                ],
            ),
            ColumnDefinition(name="code", data_type="TEXT"),
        ]
        create_expr = CreateTableExpression(
            dialect=dialect,
            table_name="test_bloom_opts",
            columns=columns,
            if_not_exists=True,
        )
        sql, params = create_expr.to_sql()
        backend.execute(sql, params)

        # Insert data using expression
        rows = []
        for i in range(50):
            rows.append([Literal(dialect, f"CODE{i}")])
        insert_expr = InsertExpression(
            dialect=dialect,
            into="test_bloom_opts",
            columns=["code"],
            source=ValuesSource(dialect, rows),
        )
        sql, params = insert_expr.to_sql()
        backend.execute(sql, params)

        # CREATE INDEX with WITH clause: kept as raw SQL (no structural support)
        backend.execute(
            "CREATE INDEX idx_bloom_opts ON test_bloom_opts "
            "USING bloom (code) WITH (length=64, col1=2)"
        )
        try:
            opts = ExecutionOptions(stmt_type=StatementType.DQL)
            query = QueryExpression(
                dialect=dialect,
                select=[Column(dialect, "indexname")],
                from_=TableExpression(dialect, "pg_indexes"),
                where=Column(dialect, "tablename") == Literal(dialect, "test_bloom_opts"),
            )
            sql, params = query.to_sql()
            result = backend.execute(sql, params, options=opts)
            index_names = [r['indexname'] for r in result.data]
            assert "idx_bloom_opts" in index_names
        finally:
            drop_expr = DropTableExpression(
                dialect=dialect,
                table_name="test_bloom_opts",
                if_exists=True,
            )
            sql, params = drop_expr.to_sql()
            backend.execute(sql, params)


@pytest_asyncio.fixture
async def async_bloom_env(async_postgres_backend_single):
    """Independent async test environment for bloom extension."""
    backend = async_postgres_backend_single
    await async_ensure_extension_installed(backend, "bloom")
    dialect = backend.dialect

    # Clean up residual tables from previous runs
    for table_name in [ASYNC_TABLE_NAME]:
        drop_expr = DropTableExpression(dialect=dialect, table_name=table_name, if_exists=True)
        sql, params = drop_expr.to_sql()
        await backend.execute(sql, params)

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
        table_name=ASYNC_TABLE_NAME,
        columns=columns,
        if_not_exists=True,
    )
    sql, params = create_expr.to_sql()
    await backend.execute(sql, params)

    # Setup: insert seed data using expression
    rows = []
    for i in range(100):
        rows.append([
            Literal(dialect, f"user{i}"),
            Literal(dialect, f"user{i}@example.com"),
        ])
    insert_expr = InsertExpression(
        dialect=dialect,
        into=ASYNC_TABLE_NAME,
        columns=["name", "email"],
        source=ValuesSource(dialect, rows),
    )
    sql, params = insert_expr.to_sql()
    await backend.execute(sql, params)

    # Setup: create bloom index using expression
    create_idx = CreateIndexExpression(
        dialect=dialect,
        index_name="idx_bloom_test_async",
        table_name=ASYNC_TABLE_NAME,
        columns=["name", "email"],
        index_type="bloom",
        if_not_exists=True,
    )
    sql, params = create_idx.to_sql()
    await backend.execute(sql, params)

    yield backend, dialect

    # Teardown: drop table using expression
    drop_expr = DropTableExpression(
        dialect=dialect,
        table_name=ASYNC_TABLE_NAME,
        if_exists=True,
    )
    sql, params = drop_expr.to_sql()
    await backend.execute(sql, params)


class TestAsyncBloomIntegration:
    """Async integration tests for bloom filter index access method."""

    @pytest.mark.asyncio
    async def test_async_bloom_index_creation(self, async_bloom_env):
        """Test that bloom index was created successfully (async)."""
        backend, dialect = async_bloom_env
        opts = ExecutionOptions(stmt_type=StatementType.DQL)

        query = QueryExpression(
            dialect=dialect,
            select=[Column(dialect, "indexname")],
            from_=TableExpression(dialect, "pg_indexes"),
            where=Column(dialect, "tablename") == Literal(dialect, ASYNC_TABLE_NAME),
        )
        sql, params = query.to_sql()
        result = await backend.execute(sql, params, options=opts)
        index_names = [r['indexname'] for r in result.data]
        assert "idx_bloom_test_async" in index_names

    @pytest.mark.asyncio
    async def test_async_bloom_index_query(self, async_bloom_env):
        """Test querying a table with bloom index (async)."""
        backend, dialect = async_bloom_env
        opts = ExecutionOptions(stmt_type=StatementType.DQL)

        query = QueryExpression(
            dialect=dialect,
            select=[Column(dialect, "id"), Column(dialect, "name"), Column(dialect, "email")],
            from_=TableExpression(dialect, ASYNC_TABLE_NAME),
            where=Column(dialect, "name") == Literal(dialect, "user25"),
        )
        sql, params = query.to_sql()
        result = await backend.execute(sql, params, options=opts)
        assert len(result.data) == 1

    @pytest.mark.asyncio
    async def test_async_bloom_index_with_options(self, async_bloom_env):
        """Test creating a bloom index with custom options (async).

        Note: CREATE INDEX ... WITH (...) uses raw SQL (no structural support).
        """
        backend, dialect = async_bloom_env

        columns = [
            ColumnDefinition(
                name="id",
                data_type="SERIAL",
                constraints=[
                    ColumnConstraint(ColumnConstraintType.PRIMARY_KEY),
                ],
            ),
            ColumnDefinition(name="code", data_type="TEXT"),
        ]
        create_expr = CreateTableExpression(
            dialect=dialect,
            table_name="test_bloom_opts_async",
            columns=columns,
            if_not_exists=True,
        )
        sql, params = create_expr.to_sql()
        await backend.execute(sql, params)

        rows = []
        for i in range(50):
            rows.append([Literal(dialect, f"CODE{i}")])
        insert_expr = InsertExpression(
            dialect=dialect,
            into="test_bloom_opts_async",
            columns=["code"],
            source=ValuesSource(dialect, rows),
        )
        sql, params = insert_expr.to_sql()
        await backend.execute(sql, params)

        await backend.execute(
            "CREATE INDEX idx_bloom_opts_async ON test_bloom_opts_async "
            "USING bloom (code) WITH (length=64, col1=2)"
        )
        try:
            opts = ExecutionOptions(stmt_type=StatementType.DQL)
            query = QueryExpression(
                dialect=dialect,
                select=[Column(dialect, "indexname")],
                from_=TableExpression(dialect, "pg_indexes"),
                where=Column(dialect, "tablename") == Literal(dialect, "test_bloom_opts_async"),
            )
            sql, params = query.to_sql()
            result = await backend.execute(sql, params, options=opts)
            index_names = [r['indexname'] for r in result.data]
            assert "idx_bloom_opts_async" in index_names
        finally:
            drop_expr = DropTableExpression(
                dialect=dialect,
                table_name="test_bloom_opts_async",
                if_exists=True,
            )
            sql, params = drop_expr.to_sql()
            await backend.execute(sql, params)
