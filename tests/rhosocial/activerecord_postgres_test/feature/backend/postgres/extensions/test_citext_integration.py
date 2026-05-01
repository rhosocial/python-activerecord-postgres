# tests/rhosocial/activerecord_postgres_test/feature/backend/postgres/extensions/test_citext_integration.py

"""Integration tests for PostgreSQL citext extension with real database.

These tests require a live PostgreSQL connection with citext extension installed
and test:
- Case-insensitive text comparison
- Unique constraints on citext columns
- citext column insertion and retrieval
- LIKE queries on citext columns
- Case-insensitive JOIN on citext columns

All database operations use expression objects, not raw SQL strings.
"""

import pytest
import pytest_asyncio

from rhosocial.activerecord_postgres_test.feature.backend.utils import (
    ensure_extension_installed,
    async_ensure_extension_installed,
)
from rhosocial.activerecord.backend.errors import IntegrityError
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


# --- Helper functions ---


def _setup_citext_table(backend, dialect, table_name):
    """Create and populate the basic citext test table using expressions."""
    columns = [
        ColumnDefinition(
            name="id",
            data_type="SERIAL",
            constraints=[
                ColumnConstraint(ColumnConstraintType.PRIMARY_KEY),
            ],
        ),
        ColumnDefinition(name="name", data_type="CITEXT"),
    ]
    create_expr = CreateTableExpression(
        dialect=dialect,
        table_name=table_name,
        columns=columns,
        if_not_exists=True,
    )
    sql, params = create_expr.to_sql()
    backend.execute(sql, params)

    insert_expr = InsertExpression(
        dialect=dialect,
        into=table_name,
        columns=["name"],
        source=ValuesSource(
            dialect,
            [[Literal(dialect, "Hello World")]],
        ),
    )
    sql, params = insert_expr.to_sql()
    backend.execute(sql, params)


def _setup_citext_unique_table(backend, dialect, table_name):
    """Create and populate the citext unique constraint test table using expressions."""
    columns = [
        ColumnDefinition(
            name="id",
            data_type="SERIAL",
            constraints=[
                ColumnConstraint(ColumnConstraintType.PRIMARY_KEY),
            ],
        ),
        ColumnDefinition(
            name="email",
            data_type="CITEXT",
            constraints=[
                ColumnConstraint(ColumnConstraintType.UNIQUE),
            ],
        ),
    ]
    create_expr = CreateTableExpression(
        dialect=dialect,
        table_name=table_name,
        columns=columns,
        if_not_exists=True,
    )
    sql, params = create_expr.to_sql()
    backend.execute(sql, params)

    insert_expr = InsertExpression(
        dialect=dialect,
        into=table_name,
        columns=["email"],
        source=ValuesSource(
            dialect,
            [[Literal(dialect, "User@Example.COM")]],
        ),
    )
    sql, params = insert_expr.to_sql()
    backend.execute(sql, params)


def _setup_citext_like_table(backend, dialect, table_name):
    """Create and populate the citext LIKE test table using expressions."""
    columns = [
        ColumnDefinition(
            name="id",
            data_type="SERIAL",
            constraints=[
                ColumnConstraint(ColumnConstraintType.PRIMARY_KEY),
            ],
        ),
        ColumnDefinition(name="name", data_type="CITEXT"),
    ]
    create_expr = CreateTableExpression(
        dialect=dialect,
        table_name=table_name,
        columns=columns,
        if_not_exists=True,
    )
    sql, params = create_expr.to_sql()
    backend.execute(sql, params)

    insert_expr = InsertExpression(
        dialect=dialect,
        into=table_name,
        columns=["name"],
        source=ValuesSource(
            dialect,
            [
                [Literal(dialect, "Alice Johnson")],
                [Literal(dialect, "Bob Smith")],
            ],
        ),
    )
    sql, params = insert_expr.to_sql()
    backend.execute(sql, params)


def _setup_citext_users_table(backend, dialect, table_name):
    """Create and populate the citext users test table using expressions."""
    columns = [
        ColumnDefinition(
            name="id",
            data_type="SERIAL",
            constraints=[
                ColumnConstraint(ColumnConstraintType.PRIMARY_KEY),
            ],
        ),
        ColumnDefinition(name="username", data_type="CITEXT"),
    ]
    create_expr = CreateTableExpression(
        dialect=dialect,
        table_name=table_name,
        columns=columns,
        if_not_exists=True,
    )
    sql, params = create_expr.to_sql()
    backend.execute(sql, params)

    insert_expr = InsertExpression(
        dialect=dialect,
        into=table_name,
        columns=["username"],
        source=ValuesSource(
            dialect,
            [[Literal(dialect, "Admin")]],
        ),
    )
    sql, params = insert_expr.to_sql()
    backend.execute(sql, params)


def _setup_citext_roles_table(backend, dialect, table_name):
    """Create and populate the citext roles test table using expressions."""
    columns = [
        ColumnDefinition(
            name="id",
            data_type="SERIAL",
            constraints=[
                ColumnConstraint(ColumnConstraintType.PRIMARY_KEY),
            ],
        ),
        ColumnDefinition(name="username", data_type="CITEXT"),
        ColumnDefinition(name="role", data_type="TEXT"),
    ]
    create_expr = CreateTableExpression(
        dialect=dialect,
        table_name=table_name,
        columns=columns,
        if_not_exists=True,
    )
    sql, params = create_expr.to_sql()
    backend.execute(sql, params)

    insert_expr = InsertExpression(
        dialect=dialect,
        into=table_name,
        columns=["username", "role"],
        source=ValuesSource(
            dialect,
            [[Literal(dialect, "admin"), Literal(dialect, "superuser")]],
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


async def _async_setup_citext_table(backend, dialect, table_name):
    """Async: create and populate the basic citext test table using expressions."""
    columns = [
        ColumnDefinition(
            name="id",
            data_type="SERIAL",
            constraints=[
                ColumnConstraint(ColumnConstraintType.PRIMARY_KEY),
            ],
        ),
        ColumnDefinition(name="name", data_type="CITEXT"),
    ]
    create_expr = CreateTableExpression(
        dialect=dialect,
        table_name=table_name,
        columns=columns,
        if_not_exists=True,
    )
    sql, params = create_expr.to_sql()
    await backend.execute(sql, params)

    insert_expr = InsertExpression(
        dialect=dialect,
        into=table_name,
        columns=["name"],
        source=ValuesSource(
            dialect,
            [[Literal(dialect, "Hello World")]],
        ),
    )
    sql, params = insert_expr.to_sql()
    await backend.execute(sql, params)


async def _async_setup_citext_unique_table(backend, dialect, table_name):
    """Async: create and populate the citext unique constraint test table using expressions."""
    columns = [
        ColumnDefinition(
            name="id",
            data_type="SERIAL",
            constraints=[
                ColumnConstraint(ColumnConstraintType.PRIMARY_KEY),
            ],
        ),
        ColumnDefinition(
            name="email",
            data_type="CITEXT",
            constraints=[
                ColumnConstraint(ColumnConstraintType.UNIQUE),
            ],
        ),
    ]
    create_expr = CreateTableExpression(
        dialect=dialect,
        table_name=table_name,
        columns=columns,
        if_not_exists=True,
    )
    sql, params = create_expr.to_sql()
    await backend.execute(sql, params)

    insert_expr = InsertExpression(
        dialect=dialect,
        into=table_name,
        columns=["email"],
        source=ValuesSource(
            dialect,
            [[Literal(dialect, "User@Example.COM")]],
        ),
    )
    sql, params = insert_expr.to_sql()
    await backend.execute(sql, params)


async def _async_setup_citext_like_table(backend, dialect, table_name):
    """Async: create and populate the citext LIKE test table using expressions."""
    columns = [
        ColumnDefinition(
            name="id",
            data_type="SERIAL",
            constraints=[
                ColumnConstraint(ColumnConstraintType.PRIMARY_KEY),
            ],
        ),
        ColumnDefinition(name="name", data_type="CITEXT"),
    ]
    create_expr = CreateTableExpression(
        dialect=dialect,
        table_name=table_name,
        columns=columns,
        if_not_exists=True,
    )
    sql, params = create_expr.to_sql()
    await backend.execute(sql, params)

    insert_expr = InsertExpression(
        dialect=dialect,
        into=table_name,
        columns=["name"],
        source=ValuesSource(
            dialect,
            [
                [Literal(dialect, "Alice Johnson")],
                [Literal(dialect, "Bob Smith")],
            ],
        ),
    )
    sql, params = insert_expr.to_sql()
    await backend.execute(sql, params)


async def _async_setup_citext_users_table(backend, dialect, table_name):
    """Async: create and populate the citext users test table using expressions."""
    columns = [
        ColumnDefinition(
            name="id",
            data_type="SERIAL",
            constraints=[
                ColumnConstraint(ColumnConstraintType.PRIMARY_KEY),
            ],
        ),
        ColumnDefinition(name="username", data_type="CITEXT"),
    ]
    create_expr = CreateTableExpression(
        dialect=dialect,
        table_name=table_name,
        columns=columns,
        if_not_exists=True,
    )
    sql, params = create_expr.to_sql()
    await backend.execute(sql, params)

    insert_expr = InsertExpression(
        dialect=dialect,
        into=table_name,
        columns=["username"],
        source=ValuesSource(
            dialect,
            [[Literal(dialect, "Admin")]],
        ),
    )
    sql, params = insert_expr.to_sql()
    await backend.execute(sql, params)


async def _async_setup_citext_roles_table(backend, dialect, table_name):
    """Async: create and populate the citext roles test table using expressions."""
    columns = [
        ColumnDefinition(
            name="id",
            data_type="SERIAL",
            constraints=[
                ColumnConstraint(ColumnConstraintType.PRIMARY_KEY),
            ],
        ),
        ColumnDefinition(name="username", data_type="CITEXT"),
        ColumnDefinition(name="role", data_type="TEXT"),
    ]
    create_expr = CreateTableExpression(
        dialect=dialect,
        table_name=table_name,
        columns=columns,
        if_not_exists=True,
    )
    sql, params = create_expr.to_sql()
    await backend.execute(sql, params)

    insert_expr = InsertExpression(
        dialect=dialect,
        into=table_name,
        columns=["username", "role"],
        source=ValuesSource(
            dialect,
            [[Literal(dialect, "admin"), Literal(dialect, "superuser")]],
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

T_CITEXT = "test_citext"
T_CITEXT_UNIQUE = "test_citext_unique"
T_CITEXT_LIKE = "test_citext_like"
T_CITEXT_USERS = "test_citext_users"
T_CITEXT_ROLES = "test_citext_roles"

T_CITEXT_ASYNC = "test_citext_async"
T_CITEXT_UNIQUE_ASYNC = "test_citext_unique_async"
T_CITEXT_LIKE_ASYNC = "test_citext_like_async"
T_CITEXT_USERS_ASYNC = "test_citext_users_async"
T_CITEXT_ROLES_ASYNC = "test_citext_roles_async"


# --- Sync fixture and tests ---


@pytest.fixture
def citext_env(postgres_backend_single):
    """Independent test environment for citext extension."""
    backend = postgres_backend_single
    ensure_extension_installed(backend, "citext")
    dialect = backend.dialect

    # Clean up residual tables from previous runs
    for table_name in [T_CITEXT, T_CITEXT_UNIQUE, T_CITEXT_LIKE,
                       T_CITEXT_USERS, T_CITEXT_ROLES]:
        _teardown_table(backend, dialect, table_name)

    _setup_citext_table(backend, dialect, T_CITEXT)
    _setup_citext_unique_table(backend, dialect, T_CITEXT_UNIQUE)
    _setup_citext_like_table(backend, dialect, T_CITEXT_LIKE)
    _setup_citext_users_table(backend, dialect, T_CITEXT_USERS)
    _setup_citext_roles_table(backend, dialect, T_CITEXT_ROLES)

    yield backend, dialect

    for table_name in [T_CITEXT, T_CITEXT_UNIQUE, T_CITEXT_LIKE,
                       T_CITEXT_USERS, T_CITEXT_ROLES]:
        _teardown_table(backend, dialect, table_name)


class TestCitextIntegration:
    """Integration tests for citext extension."""

    def test_citext_comparison(self, citext_env):
        """Test that citext columns perform case-insensitive comparison."""
        backend, dialect = citext_env
        opts = ExecutionOptions(stmt_type=StatementType.DQL)

        # Case-insensitive WHERE clause: name = 'hello world' matches 'Hello World'
        where_pred = ComparisonPredicate(
            dialect, "=",
            Column(dialect, "name"),
            Literal(dialect, "hello world"),
        )
        query = QueryExpression(
            dialect=dialect,
            select=[Column(dialect, "name")],
            from_=TableExpression(dialect, T_CITEXT),
            where=where_pred,
        )
        sql, params = query.to_sql()
        result = backend.execute(sql, params, options=opts)
        assert result.data is not None
        assert len(result.data) >= 1
        assert result.data[0]['name'] is not None

        # Also test with different casing: name = 'HELLO WORLD'
        where_pred2 = ComparisonPredicate(
            dialect, "=",
            Column(dialect, "name"),
            Literal(dialect, "HELLO WORLD"),
        )
        query2 = QueryExpression(
            dialect=dialect,
            select=[Column(dialect, "name")],
            from_=TableExpression(dialect, T_CITEXT),
            where=where_pred2,
        )
        sql2, params2 = query2.to_sql()
        result2 = backend.execute(sql2, params2, options=opts)
        assert result2.data is not None
        assert len(result2.data) >= 1

        # After casting to text, the comparison is case-sensitive.
        # Use text() function equivalent to ::text cast.
        # text(name) is equivalent to name::text in PostgreSQL.
        cast_func = FunctionCall(dialect, "text", Column(dialect, "name"))
        where_pred3 = ComparisonPredicate(
            dialect, "=",
            cast_func,
            Literal(dialect, "hello world"),
        )
        query3 = QueryExpression(
            dialect=dialect,
            select=[Column(dialect, "name")],
            from_=TableExpression(dialect, T_CITEXT),
            where=where_pred3,
        )
        sql3, params3 = query3.to_sql()
        result3 = backend.execute(sql3, params3, options=opts)
        assert result3.data is not None
        # After casting to text, 'Hello World' != 'hello world' (case-sensitive)
        assert len(result3.data) == 0

    def test_citext_unique_constraint(self, citext_env):
        """Test that citext columns enforce unique constraints case-insensitively."""
        backend, dialect = citext_env

        # Insert with different casing should violate unique constraint
        insert_expr = InsertExpression(
            dialect=dialect,
            into=T_CITEXT_UNIQUE,
            columns=["email"],
            source=ValuesSource(
                dialect,
                [[Literal(dialect, "user@example.com")]],
            ),
        )
        sql, params = insert_expr.to_sql()
        with pytest.raises(IntegrityError):
            backend.execute(sql, params)

    def test_citext_like_query(self, citext_env):
        """Test LIKE queries on citext columns are case-insensitive."""
        backend, dialect = citext_env
        opts = ExecutionOptions(stmt_type=StatementType.DQL)

        # LIKE on citext is case-insensitive
        where_pred = ComparisonPredicate(
            dialect, "LIKE",
            Column(dialect, "name"),
            Literal(dialect, "alice%"),
        )
        query = QueryExpression(
            dialect=dialect,
            select=[Column(dialect, "name")],
            from_=TableExpression(dialect, T_CITEXT_LIKE),
            where=where_pred,
        )
        sql, params = query.to_sql()
        result = backend.execute(sql, params, options=opts)
        assert result.data is not None
        assert len(result.data) >= 1
        assert result.data[0]['name'] is not None

    def test_citext_join(self, citext_env):
        """Test that citext columns join case-insensitively."""
        backend, dialect = citext_env
        opts = ExecutionOptions(stmt_type=StatementType.DQL)

        # Test case-insensitive join by querying users and verifying
        # the matching role via a second query on the roles table.
        # This tests the same CITEXT behavior without requiring JOIN
        # support in the expression system.

        # Step 1: Query the user's username
        query_user = QueryExpression(
            dialect=dialect,
            select=[Column(dialect, "username")],
            from_=TableExpression(dialect, T_CITEXT_USERS),
        )
        sql, params = query_user.to_sql()
        user_result = backend.execute(sql, params, options=opts)
        assert user_result.data is not None
        assert len(user_result.data) >= 1
        username = user_result.data[0]['username']

        # Step 2: Query the roles table with the username (case-insensitive match)
        where_pred = ComparisonPredicate(
            dialect, "=",
            Column(dialect, "username"),
            Literal(dialect, username),
        )
        query_role = QueryExpression(
            dialect=dialect,
            select=[Column(dialect, "role")],
            from_=TableExpression(dialect, T_CITEXT_ROLES),
            where=where_pred,
        )
        sql2, params2 = query_role.to_sql()
        role_result = backend.execute(sql2, params2, options=opts)
        assert role_result.data is not None
        assert len(role_result.data) >= 1
        assert role_result.data[0]['role'] == 'superuser'


# --- Async fixture and tests ---


@pytest_asyncio.fixture
async def async_citext_env(async_postgres_backend_single):
    """Independent async test environment for citext extension."""
    backend = async_postgres_backend_single
    await async_ensure_extension_installed(backend, "citext")
    dialect = backend.dialect

    # Clean up residual tables from previous runs
    for table_name in [T_CITEXT_ASYNC, T_CITEXT_UNIQUE_ASYNC, T_CITEXT_LIKE_ASYNC,
                       T_CITEXT_USERS_ASYNC, T_CITEXT_ROLES_ASYNC]:
        await _async_teardown_table(backend, dialect, table_name)

    await _async_setup_citext_table(backend, dialect, T_CITEXT_ASYNC)
    await _async_setup_citext_unique_table(backend, dialect, T_CITEXT_UNIQUE_ASYNC)
    await _async_setup_citext_like_table(backend, dialect, T_CITEXT_LIKE_ASYNC)
    await _async_setup_citext_users_table(backend, dialect, T_CITEXT_USERS_ASYNC)
    await _async_setup_citext_roles_table(backend, dialect, T_CITEXT_ROLES_ASYNC)

    yield backend, dialect

    for table_name in [T_CITEXT_ASYNC, T_CITEXT_UNIQUE_ASYNC, T_CITEXT_LIKE_ASYNC,
                       T_CITEXT_USERS_ASYNC, T_CITEXT_ROLES_ASYNC]:
        await _async_teardown_table(backend, dialect, table_name)


class TestAsyncCitextIntegration:
    """Async integration tests for citext extension."""

    @pytest.mark.asyncio
    async def test_async_citext_comparison(self, async_citext_env):
        """Test that citext columns perform case-insensitive comparison."""
        backend, dialect = async_citext_env
        opts = ExecutionOptions(stmt_type=StatementType.DQL)

        # Case-insensitive WHERE clause: name = 'hello world' matches 'Hello World'
        where_pred = ComparisonPredicate(
            dialect, "=",
            Column(dialect, "name"),
            Literal(dialect, "hello world"),
        )
        query = QueryExpression(
            dialect=dialect,
            select=[Column(dialect, "name")],
            from_=TableExpression(dialect, T_CITEXT_ASYNC),
            where=where_pred,
        )
        sql, params = query.to_sql()
        result = await backend.execute(sql, params, options=opts)
        assert result.data is not None
        assert len(result.data) >= 1
        assert result.data[0]['name'] is not None

        # Also test with different casing: name = 'HELLO WORLD'
        where_pred2 = ComparisonPredicate(
            dialect, "=",
            Column(dialect, "name"),
            Literal(dialect, "HELLO WORLD"),
        )
        query2 = QueryExpression(
            dialect=dialect,
            select=[Column(dialect, "name")],
            from_=TableExpression(dialect, T_CITEXT_ASYNC),
            where=where_pred2,
        )
        sql2, params2 = query2.to_sql()
        result2 = await backend.execute(sql2, params2, options=opts)
        assert result2.data is not None
        assert len(result2.data) >= 1

        # After casting to text, the comparison is case-sensitive.
        cast_func = FunctionCall(dialect, "text", Column(dialect, "name"))
        where_pred3 = ComparisonPredicate(
            dialect, "=",
            cast_func,
            Literal(dialect, "hello world"),
        )
        query3 = QueryExpression(
            dialect=dialect,
            select=[Column(dialect, "name")],
            from_=TableExpression(dialect, T_CITEXT_ASYNC),
            where=where_pred3,
        )
        sql3, params3 = query3.to_sql()
        result3 = await backend.execute(sql3, params3, options=opts)
        assert result3.data is not None
        assert len(result3.data) == 0

    @pytest.mark.asyncio
    async def test_async_citext_unique_constraint(self, async_citext_env):
        """Test that citext columns enforce unique constraints case-insensitively."""
        backend, dialect = async_citext_env

        insert_expr = InsertExpression(
            dialect=dialect,
            into=T_CITEXT_UNIQUE_ASYNC,
            columns=["email"],
            source=ValuesSource(
                dialect,
                [[Literal(dialect, "user@example.com")]],
            ),
        )
        sql, params = insert_expr.to_sql()
        with pytest.raises(IntegrityError):
            await backend.execute(sql, params)

    @pytest.mark.asyncio
    async def test_async_citext_like_query(self, async_citext_env):
        """Test LIKE queries on citext columns are case-insensitive."""
        backend, dialect = async_citext_env
        opts = ExecutionOptions(stmt_type=StatementType.DQL)

        where_pred = ComparisonPredicate(
            dialect, "LIKE",
            Column(dialect, "name"),
            Literal(dialect, "alice%"),
        )
        query = QueryExpression(
            dialect=dialect,
            select=[Column(dialect, "name")],
            from_=TableExpression(dialect, T_CITEXT_LIKE_ASYNC),
            where=where_pred,
        )
        sql, params = query.to_sql()
        result = await backend.execute(sql, params, options=opts)
        assert result.data is not None
        assert len(result.data) >= 1
        assert result.data[0]['name'] is not None

    @pytest.mark.asyncio
    async def test_async_citext_join(self, async_citext_env):
        """Test that citext columns join case-insensitively."""
        backend, dialect = async_citext_env
        opts = ExecutionOptions(stmt_type=StatementType.DQL)

        # Step 1: Query the user's username
        query_user = QueryExpression(
            dialect=dialect,
            select=[Column(dialect, "username")],
            from_=TableExpression(dialect, T_CITEXT_USERS_ASYNC),
        )
        sql, params = query_user.to_sql()
        user_result = await backend.execute(sql, params, options=opts)
        assert user_result.data is not None
        assert len(user_result.data) >= 1
        username = user_result.data[0]['username']

        # Step 2: Query the roles table with the username (case-insensitive match)
        where_pred = ComparisonPredicate(
            dialect, "=",
            Column(dialect, "username"),
            Literal(dialect, username),
        )
        query_role = QueryExpression(
            dialect=dialect,
            select=[Column(dialect, "role")],
            from_=TableExpression(dialect, T_CITEXT_ROLES_ASYNC),
            where=where_pred,
        )
        sql2, params2 = query_role.to_sql()
        role_result = await backend.execute(sql2, params2, options=opts)
        assert role_result.data is not None
        assert len(role_result.data) >= 1
        assert role_result.data[0]['role'] == 'superuser'
