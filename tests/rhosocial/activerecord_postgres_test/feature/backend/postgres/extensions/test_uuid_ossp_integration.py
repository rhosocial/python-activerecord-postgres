# tests/rhosocial/activerecord_postgres_test/feature/backend/postgres/extensions/test_uuid_ossp_integration.py

"""Integration tests for PostgreSQL uuid-ossp extension with real database.

These tests require a live PostgreSQL connection with uuid-ossp extension installed
and test:
- uuid_generate_v4() function
- UUID column with uuid_generate_v4() default value
- uuid_generate_v1() function
- Explicit UUID insertion
"""

import re

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
)
from rhosocial.activerecord.backend.expression.statements import ValuesSource
from rhosocial.activerecord.backend.expression.core import Literal, FunctionCall
from rhosocial.activerecord.backend.expression.predicates import ComparisonPredicate
from rhosocial.activerecord.backend.options import ExecutionOptions
from rhosocial.activerecord.backend.schema import StatementType


UUID_V4_PATTERN = re.compile(
    r'^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$',
    re.IGNORECASE
)

UUID_V1_PATTERN = re.compile(
    r'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$',
    re.IGNORECASE
)


# --- Sync fixture and tests ---


@pytest.fixture
def uuid_ossp_env(postgres_backend_single):
    """Independent test environment for uuid-ossp extension."""
    backend = postgres_backend_single
    ensure_extension_installed(backend, "uuid-ossp")
    dialect = backend.dialect

    # Clean up residual tables from previous runs
    for table_name in ["test_uuid_ossp", "test_uuid_explicit"]:
        drop_expr = DropTableExpression(dialect=dialect, table_name=table_name, if_exists=True)
        sql, params = drop_expr.to_sql()
        backend.execute(sql, params)

    # Setup: create test_uuid_ossp table (with DEFAULT uuid_generate_v4())
    create_ossp = CreateTableExpression(
        dialect=dialect,
        table_name="test_uuid_ossp",
        columns=[
            ColumnDefinition(
                name="id",
                data_type="UUID",
                constraints=[
                    ColumnConstraint(ColumnConstraintType.PRIMARY_KEY),
                    ColumnConstraint(
                        ColumnConstraintType.DEFAULT,
                        default_value=FunctionCall(dialect, "uuid_generate_v4"),
                    ),
                ],
            ),
            ColumnDefinition(name="name", data_type="TEXT"),
        ],
        if_not_exists=True,
    )
    sql, params = create_ossp.to_sql()
    backend.execute(sql, params)

    # Insert data into test_uuid_ossp (id auto-generated)
    insert_ossp = InsertExpression(
        dialect=dialect,
        into="test_uuid_ossp",
        columns=["name"],
        source=ValuesSource(
            dialect,
            [[Literal(dialect, "test item")]],
        ),
    )
    sql, params = insert_ossp.to_sql()
    backend.execute(sql, params)

    # Setup: create test_uuid_explicit table (no DEFAULT)
    create_explicit = CreateTableExpression(
        dialect=dialect,
        table_name="test_uuid_explicit",
        columns=[
            ColumnDefinition(
                name="id",
                data_type="UUID",
                constraints=[
                    ColumnConstraint(ColumnConstraintType.PRIMARY_KEY),
                ],
            ),
            ColumnDefinition(name="label", data_type="TEXT"),
        ],
        if_not_exists=True,
    )
    sql, params = create_explicit.to_sql()
    backend.execute(sql, params)

    # Generate a UUID and use it explicitly
    gen_func = FunctionCall(dialect, "uuid_generate_v4").as_("uuid_val")
    gen_query = QueryExpression(
        dialect=dialect,
        select=[gen_func],
    )
    sql, params = gen_query.to_sql()
    gen_result = backend.fetch_one(sql, params)
    uuid_val = gen_result['uuid_val']

    # Insert with the generated UUID
    insert_explicit = InsertExpression(
        dialect=dialect,
        into="test_uuid_explicit",
        columns=["id", "label"],
        source=ValuesSource(
            dialect,
            [[Literal(dialect, str(uuid_val)).cast("uuid"), Literal(dialect, "explicit")]],
        ),
    )
    sql, params = insert_explicit.to_sql()
    backend.execute(sql, params)

    yield backend, dialect

    # Teardown: drop tables
    for table_name in ["test_uuid_ossp", "test_uuid_explicit"]:
        drop_expr = DropTableExpression(
            dialect=dialect,
            table_name=table_name,
            if_exists=True,
        )
        sql, params = drop_expr.to_sql()
        backend.execute(sql, params)


class TestUuidOsspIntegration:
    """Integration tests for uuid-ossp extension."""

    def test_uuid_generate_v4(self, uuid_ossp_env):
        """Test uuid_generate_v4() produces valid UUIDs."""
        backend, dialect = uuid_ossp_env
        opts = ExecutionOptions(stmt_type=StatementType.DQL)

        gen_func = FunctionCall(dialect, "uuid_generate_v4").as_("uuid_val")
        query = QueryExpression(
            dialect=dialect,
            select=[gen_func],
        )
        sql, params = query.to_sql()
        result = backend.execute(sql, params, options=opts)
        assert result.data is not None
        assert result.data[0]['uuid_val'] is not None
        # Verify the UUID format (v4 has '4' in the version position)
        assert UUID_V4_PATTERN.match(str(result.data[0]['uuid_val'])) is not None

    def test_uuid_generate_v4_unique(self, uuid_ossp_env):
        """Test that uuid_generate_v4() produces unique values."""
        backend, dialect = uuid_ossp_env
        opts = ExecutionOptions(stmt_type=StatementType.DQL)

        gen_func = FunctionCall(dialect, "uuid_generate_v4").as_("uuid_val")
        query = QueryExpression(
            dialect=dialect,
            select=[gen_func],
        )
        sql, params = query.to_sql()

        result1 = backend.execute(sql, params, options=opts)
        result2 = backend.execute(sql, params, options=opts)
        assert result1.data[0]['uuid_val'] != result2.data[0]['uuid_val']

    def test_uuid_column_default(self, uuid_ossp_env):
        """Test creating a table with UUID column using uuid_generate_v4() as default."""
        backend, dialect = uuid_ossp_env
        opts = ExecutionOptions(stmt_type=StatementType.DQL)

        query = QueryExpression(
            dialect=dialect,
            select=[Column(dialect, "id"), Column(dialect, "name")],
            from_=TableExpression(dialect, "test_uuid_ossp"),
            where=ComparisonPredicate(
                dialect, "=",
                Column(dialect, "name"),
                Literal(dialect, "test item"),
            ),
        )
        sql, params = query.to_sql()
        result = backend.execute(sql, params, options=opts)
        assert result.data is not None
        assert result.data[0]['id'] is not None
        assert result.data[0]['name'] == 'test item'
        # Verify the UUID is a v4
        assert UUID_V4_PATTERN.match(str(result.data[0]['id'])) is not None

    def test_uuid_generate_v1(self, uuid_ossp_env):
        """Test uuid_generate_v1() produces valid UUIDs."""
        backend, dialect = uuid_ossp_env
        opts = ExecutionOptions(stmt_type=StatementType.DQL)

        gen_func = FunctionCall(dialect, "uuid_generate_v1").as_("uuid_val")
        query = QueryExpression(
            dialect=dialect,
            select=[gen_func],
        )
        sql, params = query.to_sql()
        result = backend.execute(sql, params, options=opts)
        assert result.data is not None
        assert result.data[0]['uuid_val'] is not None
        # Verify UUID format (v1)
        assert UUID_V1_PATTERN.match(str(result.data[0]['uuid_val'])) is not None

    def test_uuid_column_explicit_insert(self, uuid_ossp_env):
        """Test inserting explicit UUID values into a UUID column."""
        backend, dialect = uuid_ossp_env
        opts = ExecutionOptions(stmt_type=StatementType.DQL)

        query = QueryExpression(
            dialect=dialect,
            select=[Column(dialect, "id"), Column(dialect, "label")],
            from_=TableExpression(dialect, "test_uuid_explicit"),
            where=ComparisonPredicate(
                dialect, "=",
                Column(dialect, "label"),
                Literal(dialect, "explicit"),
            ),
        )
        sql, params = query.to_sql()
        result = backend.execute(sql, params, options=opts)
        assert result.data is not None
        # The UUID was generated during fixture setup and inserted explicitly
        assert result.data[0]['id'] is not None


# --- Async fixture and tests ---


@pytest_asyncio.fixture
async def async_uuid_ossp_env(async_postgres_backend_single):
    """Independent async test environment for uuid-ossp extension."""
    backend = async_postgres_backend_single
    await async_ensure_extension_installed(backend, "uuid-ossp")
    dialect = backend.dialect

    # Clean up residual tables from previous runs
    for table_name in ["test_uuid_ossp_async", "test_uuid_explicit_async"]:
        drop_expr = DropTableExpression(dialect=dialect, table_name=table_name, if_exists=True)
        sql, params = drop_expr.to_sql()
        await backend.execute(sql, params)

    # Setup: create test_uuid_ossp_async table (with DEFAULT uuid_generate_v4())
    create_ossp = CreateTableExpression(
        dialect=dialect,
        table_name="test_uuid_ossp_async",
        columns=[
            ColumnDefinition(
                name="id",
                data_type="UUID",
                constraints=[
                    ColumnConstraint(ColumnConstraintType.PRIMARY_KEY),
                    ColumnConstraint(
                        ColumnConstraintType.DEFAULT,
                        default_value=FunctionCall(dialect, "uuid_generate_v4"),
                    ),
                ],
            ),
            ColumnDefinition(name="name", data_type="TEXT"),
        ],
        if_not_exists=True,
    )
    sql, params = create_ossp.to_sql()
    await backend.execute(sql, params)

    # Insert data into test_uuid_ossp_async (id auto-generated)
    insert_ossp = InsertExpression(
        dialect=dialect,
        into="test_uuid_ossp_async",
        columns=["name"],
        source=ValuesSource(
            dialect,
            [[Literal(dialect, "test item")]],
        ),
    )
    sql, params = insert_ossp.to_sql()
    await backend.execute(sql, params)

    # Setup: create test_uuid_explicit_async table (no DEFAULT)
    create_explicit = CreateTableExpression(
        dialect=dialect,
        table_name="test_uuid_explicit_async",
        columns=[
            ColumnDefinition(
                name="id",
                data_type="UUID",
                constraints=[
                    ColumnConstraint(ColumnConstraintType.PRIMARY_KEY),
                ],
            ),
            ColumnDefinition(name="label", data_type="TEXT"),
        ],
        if_not_exists=True,
    )
    sql, params = create_explicit.to_sql()
    await backend.execute(sql, params)

    # Generate a UUID and use it explicitly
    gen_func = FunctionCall(dialect, "uuid_generate_v4").as_("uuid_val")
    gen_query = QueryExpression(
        dialect=dialect,
        select=[gen_func],
    )
    sql, params = gen_query.to_sql()
    gen_result = await backend.fetch_one(sql, params)
    uuid_val = gen_result['uuid_val']

    # Insert with the generated UUID
    insert_explicit = InsertExpression(
        dialect=dialect,
        into="test_uuid_explicit_async",
        columns=["id", "label"],
        source=ValuesSource(
            dialect,
            [[Literal(dialect, str(uuid_val)).cast("uuid"), Literal(dialect, "explicit")]],
        ),
    )
    sql, params = insert_explicit.to_sql()
    await backend.execute(sql, params)

    yield backend, dialect

    # Teardown: drop tables
    for table_name in ["test_uuid_ossp_async", "test_uuid_explicit_async"]:
        drop_expr = DropTableExpression(
            dialect=dialect,
            table_name=table_name,
            if_exists=True,
        )
        sql, params = drop_expr.to_sql()
        await backend.execute(sql, params)


class TestAsyncUuidOsspIntegration:
    """Async integration tests for uuid-ossp extension."""

    @pytest.mark.asyncio
    async def test_async_uuid_generate_v4(self, async_uuid_ossp_env):
        """Test uuid_generate_v4() produces valid UUIDs."""
        backend, dialect = async_uuid_ossp_env
        opts = ExecutionOptions(stmt_type=StatementType.DQL)

        gen_func = FunctionCall(dialect, "uuid_generate_v4").as_("uuid_val")
        query = QueryExpression(
            dialect=dialect,
            select=[gen_func],
        )
        sql, params = query.to_sql()
        result = await backend.execute(sql, params, options=opts)
        assert result.data is not None
        assert result.data[0]['uuid_val'] is not None
        # Verify the UUID format (v4 has '4' in the version position)
        assert UUID_V4_PATTERN.match(str(result.data[0]['uuid_val'])) is not None

    @pytest.mark.asyncio
    async def test_async_uuid_generate_v4_unique(self, async_uuid_ossp_env):
        """Test that uuid_generate_v4() produces unique values."""
        backend, dialect = async_uuid_ossp_env
        opts = ExecutionOptions(stmt_type=StatementType.DQL)

        gen_func = FunctionCall(dialect, "uuid_generate_v4").as_("uuid_val")
        query = QueryExpression(
            dialect=dialect,
            select=[gen_func],
        )
        sql, params = query.to_sql()

        result1 = await backend.execute(sql, params, options=opts)
        result2 = await backend.execute(sql, params, options=opts)
        assert result1.data[0]['uuid_val'] != result2.data[0]['uuid_val']

    @pytest.mark.asyncio
    async def test_async_uuid_column_default(self, async_uuid_ossp_env):
        """Test creating a table with UUID column using uuid_generate_v4() as default."""
        backend, dialect = async_uuid_ossp_env
        opts = ExecutionOptions(stmt_type=StatementType.DQL)

        query = QueryExpression(
            dialect=dialect,
            select=[Column(dialect, "id"), Column(dialect, "name")],
            from_=TableExpression(dialect, "test_uuid_ossp_async"),
            where=ComparisonPredicate(
                dialect, "=",
                Column(dialect, "name"),
                Literal(dialect, "test item"),
            ),
        )
        sql, params = query.to_sql()
        result = await backend.execute(sql, params, options=opts)
        assert result.data is not None
        assert result.data[0]['id'] is not None
        assert result.data[0]['name'] == 'test item'
        # Verify the UUID is a v4
        assert UUID_V4_PATTERN.match(str(result.data[0]['id'])) is not None

    @pytest.mark.asyncio
    async def test_async_uuid_generate_v1(self, async_uuid_ossp_env):
        """Test uuid_generate_v1() produces valid UUIDs."""
        backend, dialect = async_uuid_ossp_env
        opts = ExecutionOptions(stmt_type=StatementType.DQL)

        gen_func = FunctionCall(dialect, "uuid_generate_v1").as_("uuid_val")
        query = QueryExpression(
            dialect=dialect,
            select=[gen_func],
        )
        sql, params = query.to_sql()
        result = await backend.execute(sql, params, options=opts)
        assert result.data is not None
        assert result.data[0]['uuid_val'] is not None
        # Verify UUID format (v1)
        assert UUID_V1_PATTERN.match(str(result.data[0]['uuid_val'])) is not None

    @pytest.mark.asyncio
    async def test_async_uuid_column_explicit_insert(self, async_uuid_ossp_env):
        """Test inserting explicit UUID values into a UUID column."""
        backend, dialect = async_uuid_ossp_env
        opts = ExecutionOptions(stmt_type=StatementType.DQL)

        query = QueryExpression(
            dialect=dialect,
            select=[Column(dialect, "id"), Column(dialect, "label")],
            from_=TableExpression(dialect, "test_uuid_explicit_async"),
            where=ComparisonPredicate(
                dialect, "=",
                Column(dialect, "label"),
                Literal(dialect, "explicit"),
            ),
        )
        sql, params = query.to_sql()
        result = await backend.execute(sql, params, options=opts)
        assert result.data is not None
        # The UUID was generated during fixture setup and inserted explicitly
        assert result.data[0]['id'] is not None
