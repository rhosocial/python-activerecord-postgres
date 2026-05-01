# tests/rhosocial/activerecord_postgres_test/feature/backend/postgres/extensions/test_hstore_integration.py

"""Integration tests for PostgreSQL hstore extension with real database.

These tests require a live PostgreSQL connection with hstore extension installed
and test:
- hstore column creation and data retrieval
- hstore operators (@>, ?, ||) using BinaryExpression
- hstore key access (->) using BinaryExpression
- hstore functions (each(), akeys(), avals()) using FunctionCall
- hstore key deletion using delete() function in UpdateExpression

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
    TableExpression,
    Column,
    UpdateExpression,
)
from rhosocial.activerecord.backend.expression.statements import ValuesSource
from rhosocial.activerecord.backend.expression.core import Literal, FunctionCall, Subquery
from rhosocial.activerecord.backend.expression.operators import BinaryExpression
from rhosocial.activerecord.backend.expression.predicates import ComparisonPredicate
from rhosocial.activerecord.backend.options import ExecutionOptions
from rhosocial.activerecord.backend.schema import StatementType


# --- Helper functions ---


def _make_hstore_columns():
    """Return standard column definitions for an hstore test table."""
    return [
        ColumnDefinition(
            name="id",
            data_type="SERIAL",
            constraints=[
                ColumnConstraint(ColumnConstraintType.PRIMARY_KEY),
            ],
        ),
        ColumnDefinition(name="data", data_type="HSTORE"),
    ]


def _setup_hstore_table(backend, dialect, table_name, hstore_value):
    """Create and populate an hstore test table using expressions."""
    create_expr = CreateTableExpression(
        dialect=dialect,
        table_name=table_name,
        columns=_make_hstore_columns(),
        if_not_exists=True,
    )
    sql, params = create_expr.to_sql()
    backend.execute(sql, params)

    insert_expr = InsertExpression(
        dialect=dialect,
        into=table_name,
        columns=["data"],
        source=ValuesSource(
            dialect,
            [[Literal(dialect, hstore_value).cast("hstore")]],
        ),
    )
    sql, params = insert_expr.to_sql()
    backend.execute(sql, params)


def _teardown_table(backend, dialect, table_name):
    """Drop a test table using expression."""
    drop_expr = DropTableExpression(
        dialect=dialect,
        table_name=table_name,
        if_exists=True,
    )
    sql, params = drop_expr.to_sql()
    backend.execute(sql, params)


async def _async_setup_hstore_table(backend, dialect, table_name, hstore_value):
    """Async: create and populate an hstore test table using expressions."""
    create_expr = CreateTableExpression(
        dialect=dialect,
        table_name=table_name,
        columns=_make_hstore_columns(),
        if_not_exists=True,
    )
    sql, params = create_expr.to_sql()
    await backend.execute(sql, params)

    insert_expr = InsertExpression(
        dialect=dialect,
        into=table_name,
        columns=["data"],
        source=ValuesSource(
            dialect,
            [[Literal(dialect, hstore_value).cast("hstore")]],
        ),
    )
    sql, params = insert_expr.to_sql()
    await backend.execute(sql, params)


async def _async_teardown_table(backend, dialect, table_name):
    """Async: drop a test table using expression."""
    drop_expr = DropTableExpression(
        dialect=dialect,
        table_name=table_name,
        if_exists=True,
    )
    sql, params = drop_expr.to_sql()
    await backend.execute(sql, params)


# --- Table name constants ---

T_HSTORE = "test_hstore"
T_HSTORE_OPS = "test_hstore_ops"
T_HSTORE_KEY = "test_hstore_key"
T_HSTORE_CONCAT = "test_hstore_concat"
T_HSTORE_EACH = "test_hstore_each"
T_HSTORE_KEYS_VALS = "test_hstore_keys_vals"
T_HSTORE_UPDATE = "test_hstore_update"
T_HSTORE_DELETE = "test_hstore_delete"

T_HSTORE_ASYNC = "test_hstore_async"
T_HSTORE_OPS_ASYNC = "test_hstore_ops_async"
T_HSTORE_KEY_ASYNC = "test_hstore_key_async"
T_HSTORE_CONCAT_ASYNC = "test_hstore_concat_async"
T_HSTORE_EACH_ASYNC = "test_hstore_each_async"
T_HSTORE_KEYS_VALS_ASYNC = "test_hstore_keys_vals_async"
T_HSTORE_UPDATE_ASYNC = "test_hstore_update_async"
T_HSTORE_DELETE_ASYNC = "test_hstore_delete_async"


# --- Sync fixture and tests ---


@pytest.fixture
def hstore_env(postgres_backend_single):
    """Independent test environment for hstore extension."""
    backend = postgres_backend_single
    ensure_extension_installed(backend, "hstore")
    dialect = backend.dialect

    # Clean up residual tables from previous runs
    for table_name in [T_HSTORE, T_HSTORE_OPS, T_HSTORE_KEY, T_HSTORE_CONCAT,
                       T_HSTORE_EACH, T_HSTORE_KEYS_VALS, T_HSTORE_UPDATE,
                       T_HSTORE_DELETE]:
        _teardown_table(backend, dialect, table_name)

    _setup_hstore_table(backend, dialect, T_HSTORE, "a=>1, b=>2")
    _setup_hstore_table(backend, dialect, T_HSTORE_OPS, "color=>red, size=>large")
    _setup_hstore_table(backend, dialect, T_HSTORE_KEY, "name=>Alice, age=>30")
    _setup_hstore_table(backend, dialect, T_HSTORE_CONCAT, "a=>1")
    _setup_hstore_table(backend, dialect, T_HSTORE_EACH, "x=>10, y=>20, z=>30")
    _setup_hstore_table(backend, dialect, T_HSTORE_KEYS_VALS, "first=>a, second=>b")
    _setup_hstore_table(backend, dialect, T_HSTORE_UPDATE, "status=>active")
    _setup_hstore_table(backend, dialect, T_HSTORE_DELETE, "keep=>yes, remove=>me")

    yield backend, dialect

    for table_name in [T_HSTORE, T_HSTORE_OPS, T_HSTORE_KEY, T_HSTORE_CONCAT,
                       T_HSTORE_EACH, T_HSTORE_KEYS_VALS, T_HSTORE_UPDATE,
                       T_HSTORE_DELETE]:
        _teardown_table(backend, dialect, table_name)


class TestHstoreIntegration:
    """Integration tests for hstore extension."""

    def test_hstore_create_and_query(self, hstore_env):
        """Test creating a table with hstore column, inserting and querying data."""
        backend, dialect = hstore_env
        opts = ExecutionOptions(stmt_type=StatementType.DQL)

        # Query back the data
        query = QueryExpression(
            dialect=dialect,
            select=[Column(dialect, "data")],
            from_=TableExpression(dialect, T_HSTORE),
            where=ComparisonPredicate(
                dialect, "=",
                Column(dialect, "id"),
                Literal(dialect, 1),
            ),
        )
        sql, params = query.to_sql()
        result = backend.execute(sql, params, options=opts)
        assert result.data is not None
        assert result.data[0]['data'] is not None

        # Access individual key using -> operator
        # data->'a' retrieves the value for key 'a'
        val_a_expr = Subquery(dialect, BinaryExpression(
            dialect, "->",
            Column(dialect, "data"),
            Literal(dialect, "a"),
        )).as_("val_a")
        query_a = QueryExpression(
            dialect=dialect,
            select=[val_a_expr],
            from_=TableExpression(dialect, T_HSTORE),
            where=ComparisonPredicate(
                dialect, "=",
                Column(dialect, "id"),
                Literal(dialect, 1),
            ),
        )
        sql, params = query_a.to_sql()
        result = backend.execute(sql, params, options=opts)
        assert result.data is not None
        assert result.data[0]['val_a'] == '1'

        # Access key 'b'
        val_b_expr = Subquery(dialect, BinaryExpression(
            dialect, "->",
            Column(dialect, "data"),
            Literal(dialect, "b"),
        )).as_("val_b")
        query_b = QueryExpression(
            dialect=dialect,
            select=[val_b_expr],
            from_=TableExpression(dialect, T_HSTORE),
            where=ComparisonPredicate(
                dialect, "=",
                Column(dialect, "id"),
                Literal(dialect, 1),
            ),
        )
        sql, params = query_b.to_sql()
        result = backend.execute(sql, params, options=opts)
        assert result.data is not None
        assert result.data[0]['val_b'] == '2'

    def test_hstore_operators_contains(self, hstore_env):
        """Test hstore @> (contains) operator."""
        backend, dialect = hstore_env
        opts = ExecutionOptions(stmt_type=StatementType.DQL)

        # Test @> operator: does data contain 'color=>red'?
        contains_expr = Subquery(dialect, BinaryExpression(
            dialect, "@>",
            Column(dialect, "data"),
            Literal(dialect, "color=>red").cast("hstore"),
        )).as_("contains")
        query = QueryExpression(
            dialect=dialect,
            select=[Column(dialect, "id"), contains_expr],
            from_=TableExpression(dialect, T_HSTORE_OPS),
            where=ComparisonPredicate(
                dialect, "=",
                Column(dialect, "id"),
                Literal(dialect, 1),
            ),
        )
        sql, params = query.to_sql()
        result = backend.execute(sql, params, options=opts)
        assert result.data is not None
        assert result.data[0]['contains'] is True

        # Test @> with non-existent pair
        contains_expr2 = Subquery(dialect, BinaryExpression(
            dialect, "@>",
            Column(dialect, "data"),
            Literal(dialect, "color=>blue").cast("hstore"),
        )).as_("contains")
        query2 = QueryExpression(
            dialect=dialect,
            select=[Column(dialect, "id"), contains_expr2],
            from_=TableExpression(dialect, T_HSTORE_OPS),
            where=ComparisonPredicate(
                dialect, "=",
                Column(dialect, "id"),
                Literal(dialect, 1),
            ),
        )
        sql2, params2 = query2.to_sql()
        result2 = backend.execute(sql2, params2, options=opts)
        assert result2.data is not None
        assert result2.data[0]['contains'] is False

    def test_hstore_operators_key_exists(self, hstore_env):
        """Test hstore ? (key exists) operator."""
        backend, dialect = hstore_env
        opts = ExecutionOptions(stmt_type=StatementType.DQL)

        # Test ? operator: does key 'name' exist?
        has_name_expr = Subquery(dialect, BinaryExpression(
            dialect, "?",
            Column(dialect, "data"),
            Literal(dialect, "name"),
        )).as_("has_name")
        query = QueryExpression(
            dialect=dialect,
            select=[Column(dialect, "id"), has_name_expr],
            from_=TableExpression(dialect, T_HSTORE_KEY),
            where=ComparisonPredicate(
                dialect, "=",
                Column(dialect, "id"),
                Literal(dialect, 1),
            ),
        )
        sql, params = query.to_sql()
        result = backend.execute(sql, params, options=opts)
        assert result.data is not None
        assert result.data[0]['has_name'] is True

        # Test ? operator: does key 'email' exist?
        has_email_expr = Subquery(dialect, BinaryExpression(
            dialect, "?",
            Column(dialect, "data"),
            Literal(dialect, "email"),
        )).as_("has_email")
        query2 = QueryExpression(
            dialect=dialect,
            select=[Column(dialect, "id"), has_email_expr],
            from_=TableExpression(dialect, T_HSTORE_KEY),
            where=ComparisonPredicate(
                dialect, "=",
                Column(dialect, "id"),
                Literal(dialect, 1),
            ),
        )
        sql2, params2 = query2.to_sql()
        result2 = backend.execute(sql2, params2, options=opts)
        assert result2.data is not None
        assert result2.data[0]['has_email'] is False

    def test_hstore_operators_concat(self, hstore_env):
        """Test hstore || (concat) operator."""
        backend, dialect = hstore_env
        opts = ExecutionOptions(stmt_type=StatementType.DQL)

        # Test || operator: merge hstores
        merged_expr = Subquery(dialect, BinaryExpression(
            dialect, "||",
            Column(dialect, "data"),
            Literal(dialect, "b=>2").cast("hstore"),
        )).as_("merged")
        query = QueryExpression(
            dialect=dialect,
            select=[Column(dialect, "id"), merged_expr],
            from_=TableExpression(dialect, T_HSTORE_CONCAT),
            where=ComparisonPredicate(
                dialect, "=",
                Column(dialect, "id"),
                Literal(dialect, 1),
            ),
        )
        sql, params = query.to_sql()
        result = backend.execute(sql, params, options=opts)
        assert result.data is not None
        assert result.data[0]['merged'] is not None

    def test_hstore_functions_each(self, hstore_env):
        """Test hstore each() function to expand hstore to key-value rows."""
        backend, dialect = hstore_env
        opts = ExecutionOptions(stmt_type=StatementType.DQL)

        # Test each() function
        each_func = FunctionCall(
            dialect, "each",
            Column(dialect, "data"),
        ).as_("kv_pair")
        query = QueryExpression(
            dialect=dialect,
            select=[each_func],
            from_=TableExpression(dialect, T_HSTORE_EACH),
            where=ComparisonPredicate(
                dialect, "=",
                Column(dialect, "id"),
                Literal(dialect, 1),
            ),
        )
        sql, params = query.to_sql()
        result = backend.execute(sql, params, options=opts)
        assert result.data is not None
        assert len(result.data) == 3

    def test_hstore_functions_akeys_avals(self, hstore_env):
        """Test hstore akeys() and avals() functions."""
        backend, dialect = hstore_env
        opts = ExecutionOptions(stmt_type=StatementType.DQL)

        # Test akeys(): get all keys as array
        akeys_func = FunctionCall(
            dialect, "akeys",
            Column(dialect, "data"),
        ).as_("keys")
        query = QueryExpression(
            dialect=dialect,
            select=[akeys_func],
            from_=TableExpression(dialect, T_HSTORE_KEYS_VALS),
            where=ComparisonPredicate(
                dialect, "=",
                Column(dialect, "id"),
                Literal(dialect, 1),
            ),
        )
        sql, params = query.to_sql()
        result = backend.execute(sql, params, options=opts)
        assert result.data is not None
        assert result.data[0]['keys'] is not None
        assert len(result.data[0]['keys']) == 2

        # Test avals(): get all values as array
        avals_func = FunctionCall(
            dialect, "avals",
            Column(dialect, "data"),
        ).as_("vals")
        query2 = QueryExpression(
            dialect=dialect,
            select=[avals_func],
            from_=TableExpression(dialect, T_HSTORE_KEYS_VALS),
            where=ComparisonPredicate(
                dialect, "=",
                Column(dialect, "id"),
                Literal(dialect, 1),
            ),
        )
        sql2, params2 = query2.to_sql()
        result2 = backend.execute(sql2, params2, options=opts)
        assert result2.data is not None
        assert result2.data[0]['vals'] is not None
        assert len(result2.data[0]['vals']) == 2

    def test_hstore_update_key_value(self, hstore_env):
        """Test updating individual key in hstore column using || operator."""
        backend, dialect = hstore_env
        dml_opts = ExecutionOptions(stmt_type=StatementType.DML)
        dql_opts = ExecutionOptions(stmt_type=StatementType.DQL)

        # Update a key value using || operator in UpdateExpression
        # SET data = data || 'status=>inactive'::hstore
        update_expr = UpdateExpression(
            dialect=dialect,
            table=T_HSTORE_UPDATE,
            assignments={
                "data": BinaryExpression(
                    dialect, "||",
                    Column(dialect, "data"),
                    Literal(dialect, "status=>inactive").cast("hstore"),
                ),
            },
            where=ComparisonPredicate(
                dialect, "=",
                Column(dialect, "id"),
                Literal(dialect, 1),
            ),
        )
        sql, params = update_expr.to_sql()
        backend.execute(sql, params, options=dml_opts)

        # Verify the update using -> operator
        status_expr = Subquery(dialect, BinaryExpression(
            dialect, "->",
            Column(dialect, "data"),
            Literal(dialect, "status"),
        )).as_("status")
        query = QueryExpression(
            dialect=dialect,
            select=[status_expr],
            from_=TableExpression(dialect, T_HSTORE_UPDATE),
            where=ComparisonPredicate(
                dialect, "=",
                Column(dialect, "id"),
                Literal(dialect, 1),
            ),
        )
        sql, params = query.to_sql()
        result = backend.execute(sql, params, options=dql_opts)
        assert result.data is not None
        assert result.data[0]['status'] == 'inactive'

    def test_hstore_delete_key(self, hstore_env):
        """Test deleting a key from hstore column using delete() function."""
        backend, dialect = hstore_env
        dml_opts = ExecutionOptions(stmt_type=StatementType.DML)
        dql_opts = ExecutionOptions(stmt_type=StatementType.DQL)

        # Delete a key using delete() function in UpdateExpression
        # SET data = delete(data, 'remove')
        update_expr = UpdateExpression(
            dialect=dialect,
            table=T_HSTORE_DELETE,
            assignments={
                "data": FunctionCall(
                    dialect, "delete",
                    Column(dialect, "data"),
                    Literal(dialect, "remove"),
                ),
            },
            where=ComparisonPredicate(
                dialect, "=",
                Column(dialect, "id"),
                Literal(dialect, 1),
            ),
        )
        sql, params = update_expr.to_sql()
        backend.execute(sql, params, options=dml_opts)

        # Verify using ? operator
        has_remove_expr = Subquery(dialect, BinaryExpression(
            dialect, "?",
            Column(dialect, "data"),
            Literal(dialect, "remove"),
        )).as_("has_remove")
        has_keep_expr = Subquery(dialect, BinaryExpression(
            dialect, "?",
            Column(dialect, "data"),
            Literal(dialect, "keep"),
        )).as_("has_keep")
        query = QueryExpression(
            dialect=dialect,
            select=[has_remove_expr, has_keep_expr],
            from_=TableExpression(dialect, T_HSTORE_DELETE),
            where=ComparisonPredicate(
                dialect, "=",
                Column(dialect, "id"),
                Literal(dialect, 1),
            ),
        )
        sql, params = query.to_sql()
        result = backend.execute(sql, params, options=dql_opts)
        assert result.data is not None
        assert result.data[0]['has_remove'] is False
        assert result.data[0]['has_keep'] is True


# --- Async fixture and tests ---


@pytest_asyncio.fixture
async def async_hstore_env(async_postgres_backend_single):
    """Independent async test environment for hstore extension."""
    backend = async_postgres_backend_single
    await async_ensure_extension_installed(backend, "hstore")
    dialect = backend.dialect

    # Clean up residual tables from previous runs
    for table_name in [T_HSTORE_ASYNC, T_HSTORE_OPS_ASYNC, T_HSTORE_KEY_ASYNC,
                       T_HSTORE_CONCAT_ASYNC, T_HSTORE_EACH_ASYNC,
                       T_HSTORE_KEYS_VALS_ASYNC, T_HSTORE_UPDATE_ASYNC,
                       T_HSTORE_DELETE_ASYNC]:
        await _async_teardown_table(backend, dialect, table_name)

    await _async_setup_hstore_table(backend, dialect, T_HSTORE_ASYNC, "a=>1, b=>2")
    await _async_setup_hstore_table(backend, dialect, T_HSTORE_OPS_ASYNC, "color=>red, size=>large")
    await _async_setup_hstore_table(backend, dialect, T_HSTORE_KEY_ASYNC, "name=>Alice, age=>30")
    await _async_setup_hstore_table(backend, dialect, T_HSTORE_CONCAT_ASYNC, "a=>1")
    await _async_setup_hstore_table(backend, dialect, T_HSTORE_EACH_ASYNC, "x=>10, y=>20, z=>30")
    await _async_setup_hstore_table(backend, dialect, T_HSTORE_KEYS_VALS_ASYNC, "first=>a, second=>b")
    await _async_setup_hstore_table(backend, dialect, T_HSTORE_UPDATE_ASYNC, "status=>active")
    await _async_setup_hstore_table(backend, dialect, T_HSTORE_DELETE_ASYNC, "keep=>yes, remove=>me")

    yield backend, dialect

    for table_name in [T_HSTORE_ASYNC, T_HSTORE_OPS_ASYNC, T_HSTORE_KEY_ASYNC,
                       T_HSTORE_CONCAT_ASYNC, T_HSTORE_EACH_ASYNC,
                       T_HSTORE_KEYS_VALS_ASYNC, T_HSTORE_UPDATE_ASYNC,
                       T_HSTORE_DELETE_ASYNC]:
        await _async_teardown_table(backend, dialect, table_name)


class TestAsyncHstoreIntegration:
    """Async integration tests for hstore extension."""

    @pytest.mark.asyncio
    async def test_async_hstore_create_and_query(self, async_hstore_env):
        """Test creating a table with hstore column, inserting and querying data."""
        backend, dialect = async_hstore_env
        opts = ExecutionOptions(stmt_type=StatementType.DQL)

        # Query back the data
        query = QueryExpression(
            dialect=dialect,
            select=[Column(dialect, "data")],
            from_=TableExpression(dialect, T_HSTORE_ASYNC),
            where=ComparisonPredicate(
                dialect, "=",
                Column(dialect, "id"),
                Literal(dialect, 1),
            ),
        )
        sql, params = query.to_sql()
        result = await backend.execute(sql, params, options=opts)
        assert result.data is not None
        assert result.data[0]['data'] is not None

        # Access individual key using -> operator
        val_a_expr = Subquery(dialect, BinaryExpression(
            dialect, "->",
            Column(dialect, "data"),
            Literal(dialect, "a"),
        )).as_("val_a")
        query_a = QueryExpression(
            dialect=dialect,
            select=[val_a_expr],
            from_=TableExpression(dialect, T_HSTORE_ASYNC),
            where=ComparisonPredicate(
                dialect, "=",
                Column(dialect, "id"),
                Literal(dialect, 1),
            ),
        )
        sql, params = query_a.to_sql()
        result = await backend.execute(sql, params, options=opts)
        assert result.data is not None
        assert result.data[0]['val_a'] == '1'

        # Access key 'b'
        val_b_expr = Subquery(dialect, BinaryExpression(
            dialect, "->",
            Column(dialect, "data"),
            Literal(dialect, "b"),
        )).as_("val_b")
        query_b = QueryExpression(
            dialect=dialect,
            select=[val_b_expr],
            from_=TableExpression(dialect, T_HSTORE_ASYNC),
            where=ComparisonPredicate(
                dialect, "=",
                Column(dialect, "id"),
                Literal(dialect, 1),
            ),
        )
        sql, params = query_b.to_sql()
        result = await backend.execute(sql, params, options=opts)
        assert result.data is not None
        assert result.data[0]['val_b'] == '2'

    @pytest.mark.asyncio
    async def test_async_hstore_operators_contains(self, async_hstore_env):
        """Test hstore @> (contains) operator."""
        backend, dialect = async_hstore_env
        opts = ExecutionOptions(stmt_type=StatementType.DQL)

        # Test @> operator: does data contain 'color=>red'?
        contains_expr = Subquery(dialect, BinaryExpression(
            dialect, "@>",
            Column(dialect, "data"),
            Literal(dialect, "color=>red").cast("hstore"),
        )).as_("contains")
        query = QueryExpression(
            dialect=dialect,
            select=[Column(dialect, "id"), contains_expr],
            from_=TableExpression(dialect, T_HSTORE_OPS_ASYNC),
            where=ComparisonPredicate(
                dialect, "=",
                Column(dialect, "id"),
                Literal(dialect, 1),
            ),
        )
        sql, params = query.to_sql()
        result = await backend.execute(sql, params, options=opts)
        assert result.data is not None
        assert result.data[0]['contains'] is True

        # Test @> with non-existent pair
        contains_expr2 = Subquery(dialect, BinaryExpression(
            dialect, "@>",
            Column(dialect, "data"),
            Literal(dialect, "color=>blue").cast("hstore"),
        )).as_("contains")
        query2 = QueryExpression(
            dialect=dialect,
            select=[Column(dialect, "id"), contains_expr2],
            from_=TableExpression(dialect, T_HSTORE_OPS_ASYNC),
            where=ComparisonPredicate(
                dialect, "=",
                Column(dialect, "id"),
                Literal(dialect, 1),
            ),
        )
        sql2, params2 = query2.to_sql()
        result2 = await backend.execute(sql2, params2, options=opts)
        assert result2.data is not None
        assert result2.data[0]['contains'] is False

    @pytest.mark.asyncio
    async def test_async_hstore_operators_key_exists(self, async_hstore_env):
        """Test hstore ? (key exists) operator."""
        backend, dialect = async_hstore_env
        opts = ExecutionOptions(stmt_type=StatementType.DQL)

        # Test ? operator: does key 'name' exist?
        has_name_expr = Subquery(dialect, BinaryExpression(
            dialect, "?",
            Column(dialect, "data"),
            Literal(dialect, "name"),
        )).as_("has_name")
        query = QueryExpression(
            dialect=dialect,
            select=[Column(dialect, "id"), has_name_expr],
            from_=TableExpression(dialect, T_HSTORE_KEY_ASYNC),
            where=ComparisonPredicate(
                dialect, "=",
                Column(dialect, "id"),
                Literal(dialect, 1),
            ),
        )
        sql, params = query.to_sql()
        result = await backend.execute(sql, params, options=opts)
        assert result.data is not None
        assert result.data[0]['has_name'] is True

        # Test ? operator: does key 'email' exist?
        has_email_expr = Subquery(dialect, BinaryExpression(
            dialect, "?",
            Column(dialect, "data"),
            Literal(dialect, "email"),
        )).as_("has_email")
        query2 = QueryExpression(
            dialect=dialect,
            select=[Column(dialect, "id"), has_email_expr],
            from_=TableExpression(dialect, T_HSTORE_KEY_ASYNC),
            where=ComparisonPredicate(
                dialect, "=",
                Column(dialect, "id"),
                Literal(dialect, 1),
            ),
        )
        sql2, params2 = query2.to_sql()
        result2 = await backend.execute(sql2, params2, options=opts)
        assert result2.data is not None
        assert result2.data[0]['has_email'] is False

    @pytest.mark.asyncio
    async def test_async_hstore_operators_concat(self, async_hstore_env):
        """Test hstore || (concat) operator."""
        backend, dialect = async_hstore_env
        opts = ExecutionOptions(stmt_type=StatementType.DQL)

        # Test || operator: merge hstores
        merged_expr = Subquery(dialect, BinaryExpression(
            dialect, "||",
            Column(dialect, "data"),
            Literal(dialect, "b=>2").cast("hstore"),
        )).as_("merged")
        query = QueryExpression(
            dialect=dialect,
            select=[Column(dialect, "id"), merged_expr],
            from_=TableExpression(dialect, T_HSTORE_CONCAT_ASYNC),
            where=ComparisonPredicate(
                dialect, "=",
                Column(dialect, "id"),
                Literal(dialect, 1),
            ),
        )
        sql, params = query.to_sql()
        result = await backend.execute(sql, params, options=opts)
        assert result.data is not None
        assert result.data[0]['merged'] is not None

    @pytest.mark.asyncio
    async def test_async_hstore_functions_each(self, async_hstore_env):
        """Test hstore each() function to expand hstore to key-value rows."""
        backend, dialect = async_hstore_env
        opts = ExecutionOptions(stmt_type=StatementType.DQL)

        # Test each() function
        each_func = FunctionCall(
            dialect, "each",
            Column(dialect, "data"),
        ).as_("kv_pair")
        query = QueryExpression(
            dialect=dialect,
            select=[each_func],
            from_=TableExpression(dialect, T_HSTORE_EACH_ASYNC),
            where=ComparisonPredicate(
                dialect, "=",
                Column(dialect, "id"),
                Literal(dialect, 1),
            ),
        )
        sql, params = query.to_sql()
        result = await backend.execute(sql, params, options=opts)
        assert result.data is not None
        assert len(result.data) == 3

    @pytest.mark.asyncio
    async def test_async_hstore_functions_akeys_avals(self, async_hstore_env):
        """Test hstore akeys() and avals() functions."""
        backend, dialect = async_hstore_env
        opts = ExecutionOptions(stmt_type=StatementType.DQL)

        # Test akeys(): get all keys as array
        akeys_func = FunctionCall(
            dialect, "akeys",
            Column(dialect, "data"),
        ).as_("keys")
        query = QueryExpression(
            dialect=dialect,
            select=[akeys_func],
            from_=TableExpression(dialect, T_HSTORE_KEYS_VALS_ASYNC),
            where=ComparisonPredicate(
                dialect, "=",
                Column(dialect, "id"),
                Literal(dialect, 1),
            ),
        )
        sql, params = query.to_sql()
        result = await backend.execute(sql, params, options=opts)
        assert result.data is not None
        assert result.data[0]['keys'] is not None
        assert len(result.data[0]['keys']) == 2

        # Test avals(): get all values as array
        avals_func = FunctionCall(
            dialect, "avals",
            Column(dialect, "data"),
        ).as_("vals")
        query2 = QueryExpression(
            dialect=dialect,
            select=[avals_func],
            from_=TableExpression(dialect, T_HSTORE_KEYS_VALS_ASYNC),
            where=ComparisonPredicate(
                dialect, "=",
                Column(dialect, "id"),
                Literal(dialect, 1),
            ),
        )
        sql2, params2 = query2.to_sql()
        result2 = await backend.execute(sql2, params2, options=opts)
        assert result2.data is not None
        assert result2.data[0]['vals'] is not None
        assert len(result2.data[0]['vals']) == 2

    @pytest.mark.asyncio
    async def test_async_hstore_update_key_value(self, async_hstore_env):
        """Test updating individual key in hstore column using || operator."""
        backend, dialect = async_hstore_env
        dml_opts = ExecutionOptions(stmt_type=StatementType.DML)
        dql_opts = ExecutionOptions(stmt_type=StatementType.DQL)

        # Update a key value using || operator in UpdateExpression
        # SET data = data || 'status=>inactive'::hstore
        update_expr = UpdateExpression(
            dialect=dialect,
            table=T_HSTORE_UPDATE_ASYNC,
            assignments={
                "data": BinaryExpression(
                    dialect, "||",
                    Column(dialect, "data"),
                    Literal(dialect, "status=>inactive").cast("hstore"),
                ),
            },
            where=ComparisonPredicate(
                dialect, "=",
                Column(dialect, "id"),
                Literal(dialect, 1),
            ),
        )
        sql, params = update_expr.to_sql()
        await backend.execute(sql, params, options=dml_opts)

        # Verify the update using -> operator
        status_expr = Subquery(dialect, BinaryExpression(
            dialect, "->",
            Column(dialect, "data"),
            Literal(dialect, "status"),
        )).as_("status")
        query = QueryExpression(
            dialect=dialect,
            select=[status_expr],
            from_=TableExpression(dialect, T_HSTORE_UPDATE_ASYNC),
            where=ComparisonPredicate(
                dialect, "=",
                Column(dialect, "id"),
                Literal(dialect, 1),
            ),
        )
        sql, params = query.to_sql()
        result = await backend.execute(sql, params, options=dql_opts)
        assert result.data is not None
        assert result.data[0]['status'] == 'inactive'

    @pytest.mark.asyncio
    async def test_async_hstore_delete_key(self, async_hstore_env):
        """Test deleting a key from hstore column using delete() function."""
        backend, dialect = async_hstore_env
        dml_opts = ExecutionOptions(stmt_type=StatementType.DML)
        dql_opts = ExecutionOptions(stmt_type=StatementType.DQL)

        # Delete a key using delete() function in UpdateExpression
        # SET data = delete(data, 'remove')
        update_expr = UpdateExpression(
            dialect=dialect,
            table=T_HSTORE_DELETE_ASYNC,
            assignments={
                "data": FunctionCall(
                    dialect, "delete",
                    Column(dialect, "data"),
                    Literal(dialect, "remove"),
                ),
            },
            where=ComparisonPredicate(
                dialect, "=",
                Column(dialect, "id"),
                Literal(dialect, 1),
            ),
        )
        sql, params = update_expr.to_sql()
        await backend.execute(sql, params, options=dml_opts)

        # Verify using ? operator
        has_remove_expr = Subquery(dialect, BinaryExpression(
            dialect, "?",
            Column(dialect, "data"),
            Literal(dialect, "remove"),
        )).as_("has_remove")
        has_keep_expr = Subquery(dialect, BinaryExpression(
            dialect, "?",
            Column(dialect, "data"),
            Literal(dialect, "keep"),
        )).as_("has_keep")
        query = QueryExpression(
            dialect=dialect,
            select=[has_remove_expr, has_keep_expr],
            from_=TableExpression(dialect, T_HSTORE_DELETE_ASYNC),
            where=ComparisonPredicate(
                dialect, "=",
                Column(dialect, "id"),
                Literal(dialect, 1),
            ),
        )
        sql, params = query.to_sql()
        result = await backend.execute(sql, params, options=dql_opts)
        assert result.data is not None
        assert result.data[0]['has_remove'] is False
        assert result.data[0]['has_keep'] is True
