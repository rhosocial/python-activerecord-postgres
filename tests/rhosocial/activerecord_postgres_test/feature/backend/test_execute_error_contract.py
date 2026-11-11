# tests/rhosocial/activerecord_postgres_test/feature/backend/test_execute_error_contract.py
"""PostgreSQL execute-phase error contract tests.

No PostgreSQL server is required: the tests instantiate unconnected backends
and drive them with fake connections, cursors and transaction managers. The
psycopg test cases are skipped when the driver is not installed.
"""

import logging

import pytest

psycopg_errors = pytest.importorskip("psycopg.errors")

from rhosocial.activerecord.backend.base import (  # noqa: E402
    AsyncStorageBackend,
    StorageBackend,
)
from rhosocial.activerecord.backend.errors import (  # noqa: E402
    OperationalError,
    TransactionError,
    TypeConversionError,
)
from rhosocial.activerecord.backend.impl.postgres.backend import (  # noqa: E402
    AsyncPostgresBackend,
    PostgresBackend,
)
from rhosocial.activerecord.backend.result import QueryResult  # noqa: E402


class _FakeCursor:
    """Minimal synchronous cursor that records nothing."""

    def __init__(self):
        self.closed = False
        self.rowcount = 0

    def execute(self, *args, **kwargs):
        return None

    def close(self):
        self.closed = True


class _FakeConnection:
    """Synchronous connection double with a controllable commit/rollback."""

    closed = False
    broken = False

    def __init__(self, commit_error=None):
        self.commit_error = commit_error
        self.rollback_calls = 0

    def cursor(self):
        return _FakeCursor()

    def commit(self):
        if self.commit_error is not None:
            raise self.commit_error

    def rollback(self):
        self.rollback_calls += 1


class _FakeTransactionManager:
    """Transaction manager double exposing only what the backend reads."""

    def __init__(self, active=True, rollback_error=None):
        self.is_active = active
        self.rollback_error = rollback_error
        self.rollback_calls = 0

    def rollback(self):
        self.rollback_calls += 1
        if self.rollback_error is not None:
            raise self.rollback_error


class _AsyncFakeCursor:
    def __init__(self):
        self.closed = False
        self.rowcount = 0

    async def execute(self, *args, **kwargs):
        return None

    async def close(self):
        self.closed = True


class _AsyncFakeConnection:
    closed = False
    broken = False

    def __init__(self, commit_error=None):
        self.commit_error = commit_error
        self.rollback_calls = 0

    def cursor(self):
        return _AsyncFakeCursor()

    async def commit(self):
        if self.commit_error is not None:
            raise self.commit_error

    async def rollback(self):
        self.rollback_calls += 1


class _AsyncFakeTransactionManager:
    def __init__(self, active=True, rollback_error=None):
        self.is_active = active
        self.rollback_error = rollback_error
        self.rollback_calls = 0

    async def rollback(self):
        self.rollback_calls += 1
        if self.rollback_error is not None:
            raise self.rollback_error


def _operational_error(message, transient):
    error = OperationalError(message)
    error.transient = transient
    return error


@pytest.fixture
def backend():
    """An unconnected synchronous backend."""
    return PostgresBackend(database="execute_contract_unit")


@pytest.fixture
def async_backend():
    """An unconnected asynchronous backend."""
    return AsyncPostgresBackend(database="execute_contract_unit")


class TestSyncAutoCommitContract:
    @pytest.mark.parametrize(
        "method_name",
        ["_handle_auto_commit", "_handle_auto_commit_if_needed"],
    )
    def test_commit_failure_raises_transaction_error(self, backend, method_name):
        commit_error = RuntimeError("connection lost during commit")
        backend._connection = _FakeConnection(commit_error=commit_error)
        backend._transaction_manager = _FakeTransactionManager(active=False)

        with pytest.raises(TransactionError) as info:
            getattr(backend, method_name)()

        assert info.value.__cause__ is commit_error
        assert "auto-commit" in str(info.value).lower()

    def test_autocommit_config_still_skips_commit(self, backend):
        backend._connection = _FakeConnection(commit_error=RuntimeError("must not commit"))
        backend._transaction_manager = _FakeTransactionManager(active=False)
        backend.config.autocommit = True

        backend._handle_auto_commit()

    def test_active_transaction_is_not_auto_committed(self, backend):
        backend._connection = _FakeConnection(commit_error=RuntimeError("must not commit"))
        backend._transaction_manager = _FakeTransactionManager(active=True)

        backend._handle_auto_commit()


class TestSyncErrorPhaseContract:
    def test_framework_error_is_not_remapped_and_does_not_roll_back(self, backend):
        manager = _FakeTransactionManager(active=True)
        backend._transaction_manager = manager
        error = TypeConversionError("cannot convert value")

        with pytest.raises(TypeConversionError) as info:
            backend._handle_error(error)

        assert info.value is error
        assert manager.rollback_calls == 0

    def test_prepare_parameters_failure_does_not_roll_back(self, backend, monkeypatch):
        manager = _FakeTransactionManager(active=True)
        backend._connection = _FakeConnection()
        backend._transaction_manager = manager
        conversion_error = TypeConversionError("cannot convert value")

        def failing_prepare(params, param_adapters):
            raise conversion_error

        monkeypatch.setattr(backend, "prepare_parameters", failing_prepare)

        with pytest.raises(TypeConversionError) as info:
            backend.execute("SELECT 1", (1,))

        assert info.value is conversion_error
        assert manager.rollback_calls == 0

    def test_driver_error_in_transaction_rolls_back_once(self, backend):
        manager = _FakeTransactionManager(active=True)
        connection = _FakeConnection()
        backend._transaction_manager = manager
        backend._connection = connection
        driver_error = psycopg_errors.OperationalError(
            "server closed the connection unexpectedly"
        )

        with pytest.raises(OperationalError) as info:
            backend._handle_error(driver_error)

        assert isinstance(info.value, OperationalError)
        assert info.value.__cause__ is driver_error
        # The transaction manager rollback is the authoritative recovery path.
        assert manager.rollback_calls == 1

    def test_driver_error_outside_transaction_does_not_roll_back(self, backend):
        manager = _FakeTransactionManager(active=False)
        connection = _FakeConnection()
        backend._transaction_manager = manager
        backend._connection = connection
        driver_error = psycopg_errors.OperationalError(
            "server closed the connection unexpectedly"
        )

        with pytest.raises(OperationalError):
            backend._handle_error(driver_error)

        assert manager.rollback_calls == 0
        assert connection.rollback_calls == 0

    def test_rollback_failure_is_logged_at_error_and_original_propagates(
        self, backend, caplog
    ):
        rollback_error = RuntimeError("rollback exploded")
        manager = _FakeTransactionManager(active=True, rollback_error=rollback_error)
        backend._transaction_manager = manager
        driver_error = psycopg_errors.OperationalError(
            "server closed the connection unexpectedly"
        )

        with caplog.at_level(
            logging.ERROR, logger="rhosocial.activerecord.backend.postgres"
        ):
            with pytest.raises(OperationalError) as info:
                backend._handle_error(driver_error)

        assert manager.rollback_calls == 1
        assert info.value.__cause__ is driver_error
        error_records = [record for record in caplog.records if record.levelno == logging.ERROR]
        assert any("rollback" in record.getMessage().lower() for record in error_records)


class TestSyncRetryContract:
    def test_transient_operational_error_is_retried(self, backend, monkeypatch):
        result = QueryResult(affected_rows=1)
        calls = {"execute": 0, "reconnect": 0}

        def fake_execute(self, sql, params=None, *, options=None):
            calls["execute"] += 1
            if calls["execute"] == 1:
                raise _operational_error(
                    "server closed the connection unexpectedly", transient=True
                )
            return result

        def fake_reconnect():
            calls["reconnect"] += 1
            return True

        monkeypatch.setattr(StorageBackend, "execute", fake_execute)
        monkeypatch.setattr(backend, "_reconnect", fake_reconnect)

        assert backend.execute("SELECT 1") is result
        assert calls["execute"] == 2
        assert calls["reconnect"] == 1

    def test_non_transient_operational_error_is_not_retried(self, backend, monkeypatch):
        calls = {"execute": 0, "reconnect": 0}
        failure = _operational_error("password authentication failed", transient=False)

        def fake_execute(self, sql, params=None, *, options=None):
            calls["execute"] += 1
            raise failure

        def fake_reconnect():
            calls["reconnect"] += 1
            return True

        monkeypatch.setattr(StorageBackend, "execute", fake_execute)
        monkeypatch.setattr(backend, "_reconnect", fake_reconnect)

        with pytest.raises(OperationalError) as info:
            backend.execute("SELECT 1")

        assert info.value is failure
        assert calls["execute"] == 1
        assert calls["reconnect"] == 0

    def test_untagged_connection_message_still_retries(self, backend, monkeypatch):
        result = QueryResult(affected_rows=1)
        calls = {"execute": 0, "reconnect": 0}

        def fake_execute(self, sql, params=None, *, options=None):
            calls["execute"] += 1
            if calls["execute"] == 1:
                raise _operational_error(
                    "server closed the connection unexpectedly", transient=None
                )
            return result

        def fake_reconnect():
            calls["reconnect"] += 1
            return True

        monkeypatch.setattr(StorageBackend, "execute", fake_execute)
        monkeypatch.setattr(backend, "_reconnect", fake_reconnect)

        assert backend.execute("SELECT 1") is result
        assert calls["execute"] == 2
        assert calls["reconnect"] == 1


class TestAsyncAutoCommitContract:
    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "method_name",
        ["_handle_auto_commit", "_handle_auto_commit_if_needed"],
    )
    async def test_commit_failure_raises_transaction_error(self, async_backend, method_name):
        commit_error = RuntimeError("connection lost during commit")
        async_backend._connection = _AsyncFakeConnection(commit_error=commit_error)
        async_backend._transaction_manager = _AsyncFakeTransactionManager(active=False)

        with pytest.raises(TransactionError) as info:
            await getattr(async_backend, method_name)()

        assert info.value.__cause__ is commit_error
        assert "auto-commit" in str(info.value).lower()

    @pytest.mark.asyncio
    async def test_autocommit_config_still_skips_commit(self, async_backend):
        async_backend._connection = _AsyncFakeConnection(
            commit_error=RuntimeError("must not commit")
        )
        async_backend._transaction_manager = _AsyncFakeTransactionManager(active=False)
        async_backend.config.autocommit = True

        await async_backend._handle_auto_commit()


class TestAsyncErrorPhaseContract:
    @pytest.mark.asyncio
    async def test_framework_error_is_not_remapped_and_does_not_roll_back(
        self, async_backend
    ):
        manager = _AsyncFakeTransactionManager(active=True)
        async_backend._transaction_manager = manager
        error = TypeConversionError("cannot convert value")

        with pytest.raises(TypeConversionError) as info:
            await async_backend._handle_error(error)

        assert info.value is error
        assert manager.rollback_calls == 0

    @pytest.mark.asyncio
    async def test_prepare_parameters_failure_does_not_roll_back(
        self, async_backend, monkeypatch
    ):
        manager = _AsyncFakeTransactionManager(active=True)
        async_backend._connection = _AsyncFakeConnection()
        async_backend._transaction_manager = manager
        conversion_error = TypeConversionError("cannot convert value")

        def failing_prepare(params, param_adapters):
            raise conversion_error

        monkeypatch.setattr(async_backend, "prepare_parameters", failing_prepare)

        with pytest.raises(TypeConversionError) as info:
            await async_backend.execute("SELECT 1", (1,))

        assert info.value is conversion_error
        assert manager.rollback_calls == 0

    @pytest.mark.asyncio
    async def test_driver_error_in_transaction_rolls_back_once(self, async_backend):
        manager = _AsyncFakeTransactionManager(active=True)
        connection = _AsyncFakeConnection()
        async_backend._transaction_manager = manager
        async_backend._connection = connection
        driver_error = psycopg_errors.OperationalError(
            "server closed the connection unexpectedly"
        )

        with pytest.raises(OperationalError) as info:
            await async_backend._handle_error(driver_error)

        assert isinstance(info.value, OperationalError)
        assert info.value.__cause__ is driver_error
        # The transaction manager rollback is the authoritative recovery path.
        assert manager.rollback_calls == 1

    @pytest.mark.asyncio
    async def test_driver_error_outside_transaction_does_not_roll_back(self, async_backend):
        manager = _AsyncFakeTransactionManager(active=False)
        connection = _AsyncFakeConnection()
        async_backend._transaction_manager = manager
        async_backend._connection = connection
        driver_error = psycopg_errors.OperationalError(
            "server closed the connection unexpectedly"
        )

        with pytest.raises(OperationalError):
            await async_backend._handle_error(driver_error)

        assert manager.rollback_calls == 0
        assert connection.rollback_calls == 0

    @pytest.mark.asyncio
    async def test_rollback_failure_is_logged_at_error_and_original_propagates(
        self, async_backend, caplog
    ):
        rollback_error = RuntimeError("rollback exploded")
        manager = _AsyncFakeTransactionManager(active=True, rollback_error=rollback_error)
        async_backend._transaction_manager = manager
        driver_error = psycopg_errors.OperationalError(
            "server closed the connection unexpectedly"
        )

        with caplog.at_level(
            logging.ERROR, logger="rhosocial.activerecord.backend.postgres"
        ):
            with pytest.raises(OperationalError) as info:
                await async_backend._handle_error(driver_error)

        assert manager.rollback_calls == 1
        assert info.value.__cause__ is driver_error
        error_records = [record for record in caplog.records if record.levelno == logging.ERROR]
        assert any("rollback" in record.getMessage().lower() for record in error_records)


class TestAsyncRetryContract:
    @pytest.mark.asyncio
    async def test_transient_operational_error_is_retried(self, async_backend, monkeypatch):
        result = QueryResult(affected_rows=1)
        calls = {"execute": 0, "reconnect": 0}

        async def fake_execute(self, sql, params=None, *, options=None):
            calls["execute"] += 1
            if calls["execute"] == 1:
                raise _operational_error(
                    "server closed the connection unexpectedly", transient=True
                )
            return result

        async def fake_reconnect():
            calls["reconnect"] += 1
            return True

        monkeypatch.setattr(AsyncStorageBackend, "execute", fake_execute)
        monkeypatch.setattr(async_backend, "_reconnect", fake_reconnect)

        assert await async_backend.execute("SELECT 1") is result
        assert calls["execute"] == 2
        assert calls["reconnect"] == 1

    @pytest.mark.asyncio
    async def test_non_transient_operational_error_is_not_retried(
        self, async_backend, monkeypatch
    ):
        calls = {"execute": 0, "reconnect": 0}
        failure = _operational_error("password authentication failed", transient=False)

        async def fake_execute(self, sql, params=None, *, options=None):
            calls["execute"] += 1
            raise failure

        async def fake_reconnect():
            calls["reconnect"] += 1
            return True

        monkeypatch.setattr(AsyncStorageBackend, "execute", fake_execute)
        monkeypatch.setattr(async_backend, "_reconnect", fake_reconnect)

        with pytest.raises(OperationalError) as info:
            await async_backend.execute("SELECT 1")

        assert info.value is failure
        assert calls["execute"] == 1
        assert calls["reconnect"] == 0
