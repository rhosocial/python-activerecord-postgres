# tests/rhosocial/activerecord_postgres_test/feature/logging/test_execution_logging_unit.py
"""Database-free coverage for PostgreSQL execution logging.

The PostgreSQL package keeps two identical sync backend modules (``backend.py``
and ``sync.py``) plus the async module. Coverage only reaches one of them through
normal imports, so this module instantiates every class and drives its execution
logging against fake connections. No PostgreSQL server is required.

The same file also exercises the transaction managers' deferred-constraint
logging and imports the CLI modules so their module-level logger bindings count.
"""

import logging
from unittest.mock import AsyncMock, MagicMock

import pytest

from rhosocial.activerecord.backend.errors import (
    DatabaseError,
    DeadlockError,
    IntegrityError,
    OperationalError,
    QueryError,
)
from rhosocial.activerecord.backend.impl.postgres.backend import (
    async_backend as async_backend_module,
)
from rhosocial.activerecord.backend.impl.postgres.backend import backend as backend_module
from rhosocial.activerecord.backend.impl.postgres.backend import base as base_module
from rhosocial.activerecord.backend.impl.postgres.backend import sync as sync_module
from rhosocial.activerecord.backend.impl.postgres.cli import info as cli_info
from rhosocial.activerecord.backend.impl.postgres.cli import query as cli_query
from rhosocial.activerecord.backend.options import (
    ExecutionOptions,
    LogContext,
    StatementType,
)
from rhosocial.activerecord.backend.result import QueryResult
from rhosocial.activerecord.logging.redaction import SensitiveValue

POSTGRES_LOGGER = "rhosocial.activerecord.backend.postgres"

SYNC_BACKEND_MODULES = (backend_module, sync_module)


class FakeCursor:
    """Minimal synchronous cursor that records executed parameters."""

    def __init__(self, rowcount: int = 1) -> None:
        self.rowcount = rowcount
        self.executed = []
        self.close_calls = 0

    def execute(self, sql, params=None):
        self.executed.append((sql, params))

    def close(self) -> None:
        self.close_calls += 1


class FakeAsyncCursor(FakeCursor):
    """Minimal asynchronous cursor that records executed parameters."""

    async def execute(self, sql, params=None):
        self.executed.append((sql, params))

    async def close(self) -> None:
        self.close_calls += 1


class FakeConnection:
    """Minimal synchronous connection, open and healthy enough for ``_get_cursor``."""

    closed = False
    broken = False

    def __init__(self, cursor) -> None:
        self._cursor = cursor
        self.rollback_calls = 0

    def cursor(self):
        return self._cursor

    def rollback(self) -> None:
        self.rollback_calls += 1


class FakeAsyncConnection(FakeConnection):
    """Minimal asynchronous connection, open and healthy enough for ``_get_cursor``."""

    async def rollback(self) -> None:
        self.rollback_calls += 1


def _messages(caplog) -> str:
    return "\n".join(record.getMessage() for record in caplog.records)


def _driver_error_cases(psycopg):
    """Return (error, framework error, log fragment) triples for mapped branches."""
    errors = psycopg.errors
    return {
        "unique_violation": (
            errors.IntegrityError(
                "duplicate key value violates unique constraint users_email_key"
            ),
            IntegrityError,
            "Unique constraint violation",
        ),
        "foreign_key_violation": (
            errors.IntegrityError("insert violates foreign key constraint fk_orders_user"),
            IntegrityError,
            "Foreign key constraint violation",
        ),
        "integrity_other": (
            errors.IntegrityError("integrity constraint failed"),
            IntegrityError,
            "Integrity error",
        ),
        "deadlock": (
            errors.OperationalError("deadlock detected"),
            DeadlockError,
            "Deadlock error",
        ),
        "operational_other": (
            errors.OperationalError("server closed the connection unexpectedly"),
            OperationalError,
            "Operational error",
        ),
        "programming": (
            errors.ProgrammingError("syntax error at or near SELECT"),
            QueryError,
            "Programming error",
        ),
        "postgres_other": (
            errors.Error("some PostgreSQL failure"),
            DatabaseError,
            "PostgreSQL error",
        ),
    }


@pytest.fixture(params=SYNC_BACKEND_MODULES, ids=["backend_module", "sync_module"])
def sync_backend(request):
    """Instantiate the sync backend from each duplicate module (no connect)."""
    return request.param.PostgresBackend(database="logging_unit_db")


@pytest.fixture
def async_backend():
    """Instantiate the async backend (no connect)."""
    return async_backend_module.AsyncPostgresBackend(database="logging_unit_db")


@pytest.fixture(
    params=[
        "unique_violation",
        "foreign_key_violation",
        "integrity_other",
        "deadlock",
        "operational_other",
        "programming",
        "postgres_other",
    ]
)
def driver_error(request):
    """Build one mapped psycopg exception per test invocation."""
    psycopg = pytest.importorskip("psycopg")
    return _driver_error_cases(psycopg)[request.param]


class TestSyncExecutionLogging:
    """Synchronous execution logging, run once per duplicate backend module."""

    def test_backend_instantiates_without_connecting(self, sync_backend):
        """Instantiation alone must not open a connection."""
        assert sync_backend._connection is None
        assert sync_backend._get_logger_name() == POSTGRES_LOGGER
        assert sync_backend.logger.name == POSTGRES_LOGGER
        assert base_module.PostgresBackendMixin._logger_name == POSTGRES_LOGGER

    def test_execute_many_logs_counts_not_values(self, sync_backend, caplog):
        """The batch record carries counts, never parameter values."""
        cursor = FakeCursor(rowcount=1)
        sync_backend._connection = FakeConnection(cursor)
        caplog.set_level(logging.DEBUG, logger=POSTGRES_LOGGER)

        params_list = [
            ("alice", SensitiveValue("hunter2")),
            ("bob", SensitiveValue("hunter3")),
        ]
        options = ExecutionOptions(
            stmt_type=StatementType.DML,
            log_context=LogContext(operation="insert_many", object_name="users"),
        )

        result = sync_backend.execute_many(
            "INSERT INTO users (name, password) VALUES (%s, %s)",
            params_list,
            options=options,
        )

        assert result.affected_rows == 2
        assert cursor.close_calls == 1
        # SensitiveValue is unwrapped before binding, so the driver sees values.
        assert cursor.executed[0][1] == ("alice", "hunter2")
        assert cursor.executed[1][1] == ("bob", "hunter3")

        messages = _messages(caplog)
        assert "parameter_sets: 2" in messages
        assert "hunter2" not in messages
        assert "hunter3" not in messages

        # The default-options path builds its own DML ExecutionOptions.
        default_result = sync_backend.execute_many(
            "UPDATE users SET name = %s",
            [("carol",)],
        )
        assert default_result.affected_rows == 1

    def test_handle_error_maps_driver_errors(self, sync_backend, driver_error, monkeypatch, caplog):
        """Each mapped psycopg branch logs at DEBUG and raises the framework error."""
        error, framework_error, fragment = driver_error

        manager = sync_backend._transaction_manager
        manager._transaction_level = 1
        monkeypatch.setattr(manager, "rollback", lambda: None)
        connection = FakeConnection(FakeCursor())
        sync_backend._connection = connection
        caplog.set_level(logging.DEBUG, logger=POSTGRES_LOGGER)

        with pytest.raises(framework_error):
            sync_backend._handle_error(error)

        assert fragment in _messages(caplog)
        # Active transaction rolls back through the manager, then the connection.
        assert connection.rollback_calls == 1

    def test_defer_constraint_logs_deferred_name(self, sync_backend, monkeypatch, caplog):
        """defer_constraint logs the accepted constraint once it is registered."""
        monkeypatch.setattr(
            sync_backend,
            "execute",
            MagicMock(return_value=QueryResult(affected_rows=1)),
        )
        caplog.set_level(logging.DEBUG, logger=POSTGRES_LOGGER)

        sync_backend.transaction_manager.defer_constraint("users_email_key")

        assert sync_backend.transaction_manager.get_deferred_constraints() == [
            "users_email_key"
        ]
        assert "Deferred constraint: users_email_key" in _messages(caplog)


class TestAsyncExecutionLogging:
    """Asynchronous counterparts of the synchronous logging tests."""

    @pytest.mark.asyncio
    async def test_backend_instantiates_without_connecting(self, async_backend):
        """Async instantiation must not open a connection."""
        assert async_backend._connection is None
        assert async_backend._get_logger_name() == POSTGRES_LOGGER
        assert async_backend.logger.name == POSTGRES_LOGGER

    @pytest.mark.asyncio
    async def test_execute_many_logs_counts_not_values(self, async_backend, caplog):
        """The async batch record carries counts, never parameter values."""
        cursor = FakeAsyncCursor(rowcount=1)
        async_backend._connection = FakeAsyncConnection(cursor)
        caplog.set_level(logging.DEBUG, logger=POSTGRES_LOGGER)

        params_list = [
            ("alice", SensitiveValue("hunter2")),
            ("bob", SensitiveValue("hunter3")),
        ]
        options = ExecutionOptions(
            stmt_type=StatementType.DML,
            log_context=LogContext(operation="insert_many", object_name="users"),
        )

        result = await async_backend.execute_many(
            "INSERT INTO users (name, password) VALUES (%s, %s)",
            params_list,
            options=options,
        )

        assert result.affected_rows == 2
        assert cursor.close_calls == 1
        assert cursor.executed[0][1] == ("alice", "hunter2")
        assert cursor.executed[1][1] == ("bob", "hunter3")

        messages = _messages(caplog)
        assert "parameter_sets: 2" in messages
        assert "hunter2" not in messages
        assert "hunter3" not in messages

        # The default-options path builds its own DML ExecutionOptions.
        default_result = await async_backend.execute_many(
            "UPDATE users SET name = %s",
            [("carol",)],
        )
        assert default_result.affected_rows == 1

    @pytest.mark.asyncio
    async def test_handle_error_maps_driver_errors(
        self, async_backend, driver_error, monkeypatch, caplog
    ):
        """Each mapped psycopg branch logs at DEBUG and raises the framework error."""
        error, framework_error, fragment = driver_error

        async def _noop() -> None:
            return None

        manager = async_backend._transaction_manager
        manager._transaction_level = 1
        monkeypatch.setattr(manager, "rollback", _noop)
        connection = FakeAsyncConnection(FakeAsyncCursor())
        async_backend._connection = connection
        caplog.set_level(logging.DEBUG, logger=POSTGRES_LOGGER)

        with pytest.raises(framework_error):
            await async_backend._handle_error(error)

        assert fragment in _messages(caplog)
        assert connection.rollback_calls == 1

    @pytest.mark.asyncio
    async def test_defer_constraint_logs_deferred_name(self, async_backend, monkeypatch, caplog):
        """Async defer_constraint logs the accepted constraint once registered."""
        monkeypatch.setattr(
            async_backend,
            "execute",
            AsyncMock(return_value=QueryResult(affected_rows=1)),
        )
        caplog.set_level(logging.DEBUG, logger=POSTGRES_LOGGER)

        await async_backend.transaction_manager.defer_constraint("users_email_key")

        assert async_backend.transaction_manager.get_deferred_constraints() == [
            "users_email_key"
        ]
        assert "Deferred constraint: users_email_key" in _messages(caplog)


class TestCLILogging:
    """CLI modules bind their module-level logger to the shared backend logger."""

    def test_cli_modules_use_postgres_logger(self):
        assert cli_info.logger.name == POSTGRES_LOGGER
        assert cli_query.logger.name == POSTGRES_LOGGER
