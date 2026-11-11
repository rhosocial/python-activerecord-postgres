# tests/rhosocial/activerecord_postgres_test/feature/backend/test_driver_error_mapping.py
"""PostgreSQL driver error mapping unit tests.

These tests instantiate psycopg exception classes and drive the backend's
``_handle_error`` on an unconnected backend. No PostgreSQL server is required.

psycopg 3.x: SQLSTATE-bearing classes (``UniqueViolation``,
``DeadlockDetected``, ...) populate ``sqlstate`` at construction, which is
exactly what the mapper reads first.
"""

import pytest

psycopg_errors = pytest.importorskip("psycopg.errors")

from rhosocial.activerecord.backend.errors import (  # noqa: E402
    ConnectionError,
    ConcurrencyError,
    DatabaseError,
    DeadlockError,
    IntegrityError,
    LockError,
    OperationalError,
    QueryError,
)
from rhosocial.activerecord.backend.impl.postgres.backend import (  # noqa: E402
    AsyncPostgresBackend,
    PostgresBackend,
)


def _assert_mapped(
    driver_error,
    framework_error,
    expected_type,
    expected_sqlstate,
    expected_retryable,
    expected_transient,
    message_fragment,
):
    assert isinstance(framework_error, expected_type)
    assert framework_error.sqlstate == expected_sqlstate
    assert framework_error.driver_code == expected_sqlstate
    assert framework_error.backend == "postgres"
    assert framework_error.operation == "execute"
    assert framework_error.retryable is expected_retryable
    assert framework_error.transient is expected_transient
    assert framework_error.__cause__ is driver_error
    if message_fragment is not None:
        assert message_fragment in str(framework_error)


def _expect_mapped(
    backend,
    driver_error,
    expected_type,
    expected_sqlstate,
    expected_retryable,
    expected_transient,
    message_fragment,
):
    with pytest.raises(expected_type) as info:
        backend._handle_error(driver_error)
    _assert_mapped(
        driver_error,
        info.value,
        expected_type,
        expected_sqlstate,
        expected_retryable,
        expected_transient,
        message_fragment,
    )


async def _expect_mapped_async(
    backend,
    driver_error,
    expected_type,
    expected_sqlstate,
    expected_retryable,
    expected_transient,
    message_fragment,
):
    with pytest.raises(expected_type) as info:
        await backend._handle_error(driver_error)
    _assert_mapped(
        driver_error,
        info.value,
        expected_type,
        expected_sqlstate,
        expected_retryable,
        expected_transient,
        message_fragment,
    )


SQLSTATE_CASES = [
    pytest.param(
        lambda e: e.UniqueViolation(
            "duplicate key value violates unique constraint users_email_key"
        ),
        IntegrityError,
        "23505",
        False,
        None,
        "Unique constraint violation",
        id="unique-violation",
    ),
    pytest.param(
        lambda e: e.ForeignKeyViolation(
            "insert or update violates foreign key constraint fk_orders_user"
        ),
        IntegrityError,
        "23503",
        False,
        None,
        "Foreign key constraint violation",
        id="foreign-key-violation",
    ),
    pytest.param(
        lambda e: e.NotNullViolation(
            'null value in column "email" violates not-null constraint'
        ),
        IntegrityError,
        "23502",
        False,
        None,
        "Not-null constraint violation",
        id="not-null-violation",
    ),
    pytest.param(
        lambda e: e.CheckViolation('new row violates check constraint "age_positive"'),
        IntegrityError,
        "23514",
        False,
        None,
        "Check constraint violation",
        id="check-violation",
    ),
    pytest.param(
        lambda e: e.DeadlockDetected("deadlock detected"),
        DeadlockError,
        "40P01",
        True,
        True,
        "deadlock detected",
        id="deadlock",
    ),
    pytest.param(
        lambda e: e.SerializationFailure(
            "could not serialize access due to concurrent update"
        ),
        ConcurrencyError,
        "40001",
        True,
        True,
        "could not serialize access",
        id="serialization-failure",
    ),
    pytest.param(
        lambda e: e.LockNotAvailable("could not obtain lock on row in relation orders"),
        LockError,
        "55P03",
        True,
        True,
        "could not obtain lock",
        id="lock-not-available",
    ),
    pytest.param(
        lambda e: e.ReadOnlySqlTransaction(
            "cannot execute UPDATE in a read-only transaction"
        ),
        OperationalError,
        "25006",
        False,
        False,
        "read-only transaction",
        id="read-only-transaction",
    ),
    pytest.param(
        lambda e: e.SyntaxError('syntax error at or near "SELEC"'),
        QueryError,
        "42601",
        False,
        None,
        "syntax error",
        id="syntax-error",
    ),
    pytest.param(
        lambda e: e.UndefinedTable('relation "missing_table" does not exist'),
        QueryError,
        "42P01",
        False,
        None,
        "does not exist",
        id="undefined-table",
    ),
    pytest.param(
        lambda e: e.UndefinedColumn('column "missing_column" does not exist'),
        QueryError,
        "42703",
        False,
        None,
        "does not exist",
        id="undefined-column",
    ),
    pytest.param(
        lambda e: e.UndefinedFunction("function missing_fn(integer) does not exist"),
        QueryError,
        "42883",
        False,
        None,
        "does not exist",
        id="undefined-function",
    ),
    pytest.param(
        lambda e: e.QueryCanceled("canceling statement due to statement timeout"),
        OperationalError,
        "57014",
        True,
        True,
        "canceling statement",
        id="query-canceled",
    ),
    pytest.param(
        lambda e: e.ConnectionFailure("server closed the connection unexpectedly"),
        OperationalError,
        "08006",
        True,
        True,
        "server closed the connection",
        id="connection-failure",
    ),
    pytest.param(
        lambda e: e.InvalidPassword('password authentication failed for user "app"'),
        ConnectionError,
        "28P01",
        False,
        False,
        "password authentication failed",
        id="invalid-password",
    ),
    pytest.param(
        lambda e: e.Error("some PostgreSQL failure"),
        DatabaseError,
        None,
        False,
        None,
        "some PostgreSQL failure",
        id="generic-error",
    ),
]


@pytest.fixture
def backend():
    """An unconnected synchronous backend."""
    return PostgresBackend(database="error_mapping_unit")


@pytest.fixture
def async_backend():
    """An unconnected asynchronous backend."""
    return AsyncPostgresBackend(database="error_mapping_unit")


class TestSyncDriverErrorMapping:
    @pytest.mark.parametrize(
        "factory,expected_type,sqlstate,retryable,transient,message_fragment",
        SQLSTATE_CASES,
    )
    def test_sqlstate_and_class_mapping(
        self,
        backend,
        factory,
        expected_type,
        sqlstate,
        retryable,
        transient,
        message_fragment,
    ):
        driver_error = factory(psycopg_errors)
        _expect_mapped(
            backend,
            driver_error,
            expected_type,
            sqlstate,
            retryable,
            transient,
            message_fragment,
        )

    def test_message_fallback_for_deadlock(self, backend):
        driver_error = psycopg_errors.OperationalError("deadlock detected")
        _expect_mapped(
            backend,
            driver_error,
            DeadlockError,
            None,
            True,
            True,
            "deadlock detected",
        )

    def test_message_fallback_for_connection_loss(self, backend):
        driver_error = psycopg_errors.OperationalError(
            "server closed the connection unexpectedly"
        )
        _expect_mapped(
            backend,
            driver_error,
            OperationalError,
            None,
            True,
            True,
            "server closed the connection",
        )

    def test_message_fallback_for_integrity_constraints(self, backend):
        errors = [
            (
                psycopg_errors.IntegrityError(
                    "duplicate key value violates unique constraint users_pkey"
                ),
                "Unique constraint violation",
            ),
            (
                psycopg_errors.IntegrityError(
                    "insert violates foreign key constraint fk_orders_user"
                ),
                "Foreign key constraint violation",
            ),
        ]
        for driver_error, fragment in errors:
            _expect_mapped(
                backend,
                driver_error,
                IntegrityError,
                None,
                False,
                None,
                fragment,
            )

    def test_non_psycopg_error_is_reraised(self, backend):
        class UnexpectedError(Exception):
            pass

        driver_error = UnexpectedError("unexpected failure")
        with pytest.raises(UnexpectedError) as info:
            backend._handle_error(driver_error)
        assert info.value is driver_error


class TestAsyncDriverErrorMapping:
    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "factory,expected_type,sqlstate,retryable,transient,message_fragment",
        SQLSTATE_CASES,
    )
    async def test_sqlstate_and_class_mapping(
        self,
        async_backend,
        factory,
        expected_type,
        sqlstate,
        retryable,
        transient,
        message_fragment,
    ):
        driver_error = factory(psycopg_errors)
        await _expect_mapped_async(
            async_backend,
            driver_error,
            expected_type,
            sqlstate,
            retryable,
            transient,
            message_fragment,
        )

    @pytest.mark.asyncio
    async def test_message_fallback_for_deadlock(self, async_backend):
        driver_error = psycopg_errors.OperationalError("deadlock detected")
        await _expect_mapped_async(
            async_backend,
            driver_error,
            DeadlockError,
            None,
            True,
            True,
            "deadlock detected",
        )

    @pytest.mark.asyncio
    async def test_non_psycopg_error_is_reraised(self, async_backend):
        class UnexpectedError(Exception):
            pass

        driver_error = UnexpectedError("unexpected failure")
        with pytest.raises(UnexpectedError) as info:
            await async_backend._handle_error(driver_error)
        assert info.value is driver_error
