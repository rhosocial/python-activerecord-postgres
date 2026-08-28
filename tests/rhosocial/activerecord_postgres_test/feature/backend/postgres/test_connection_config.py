# tests/rhosocial/activerecord_postgres_test/feature/backend/postgres/test_connection_config.py
"""
Unit tests for PostgresConnectionConfig autocommit option.

The backend reads ``config.autocommit`` to decide whether an explicit COMMIT
should be issued after statements executed outside a managed transaction.
The field must therefore exist on the config (previously it was only read
via ``getattr`` with a fallback, and passing it raised a TypeError).

Covers:
  - Default value and explicit assignment of ``autocommit``
  - Sync/Async backend ``requires_manual_commit()`` reflecting the config
"""

import pytest

from rhosocial.activerecord.backend.impl.postgres import (
    AsyncPostgresBackend,
    PostgresBackend,
    PostgresConnectionConfig,
)


class FakeConnection:
    def __init__(self):
        self.commit_count = 0

    def commit(self):
        self.commit_count += 1

    def rollback(self):
        pass


class FakeAsyncConnection:
    def __init__(self):
        self.commit_count = 0

    async def commit(self):
        self.commit_count += 1

    async def rollback(self):
        pass


def make_config(**overrides) -> PostgresConnectionConfig:
    params = dict(
        host="localhost",
        port=5432,
        database="test",
        username="postgres",
        password="",
    )
    params.update(overrides)
    return PostgresConnectionConfig(**params)


class TestAutocommitConfig:
    """autocommit must be a first-class config field."""

    def test_default_autocommit_is_false(self):
        config = make_config()
        assert config.autocommit is False

    def test_autocommit_can_be_enabled(self):
        config = make_config(autocommit=True)
        assert config.autocommit is True

    def test_autocommit_not_in_connection_string(self):
        """autocommit is not a libpq conninfo keyword, so the URI must not include it."""
        conn_str = make_config(autocommit=True).to_connection_string()
        assert "autocommit" not in conn_str


class TestRequiresManualCommit:
    """Backends expose requires_manual_commit() based on config.autocommit."""

    def test_sync_backend_requires_commit_by_default(self):
        backend = PostgresBackend(connection_config=make_config())
        assert backend.requires_manual_commit() is True

    def test_sync_backend_skips_commit_when_autocommit(self):
        backend = PostgresBackend(connection_config=make_config(autocommit=True))
        assert backend.requires_manual_commit() is False

    @pytest.mark.asyncio
    async def test_async_backend_requires_commit_by_default(self):
        backend = AsyncPostgresBackend(connection_config=make_config())
        assert backend.requires_manual_commit() is True

    @pytest.mark.asyncio
    async def test_async_backend_skips_commit_when_autocommit(self):
        backend = AsyncPostgresBackend(connection_config=make_config(autocommit=True))
        assert backend.requires_manual_commit() is False


class TestAutoCommitHandlers:
    """The backend auto-commit handlers honor config.autocommit."""

    def test_handle_auto_commit_commits_when_disabled(self):
        backend = PostgresBackend(connection_config=make_config())
        backend._connection = FakeConnection()
        backend._handle_auto_commit()
        assert backend._connection.commit_count == 1

    def test_handle_auto_commit_skips_when_enabled(self):
        backend = PostgresBackend(connection_config=make_config(autocommit=True))
        backend._connection = FakeConnection()
        backend._handle_auto_commit()
        assert backend._connection.commit_count == 0

    def test_handle_auto_commit_noop_without_connection(self):
        backend = PostgresBackend(connection_config=make_config())
        backend._handle_auto_commit()

    def test_handle_auto_commit_if_needed_commits_when_disabled(self):
        backend = PostgresBackend(connection_config=make_config())
        backend._connection = FakeConnection()
        backend._handle_auto_commit_if_needed()
        assert backend._connection.commit_count == 1

    def test_handle_auto_commit_if_needed_skips_when_enabled(self):
        backend = PostgresBackend(connection_config=make_config(autocommit=True))
        backend._connection = FakeConnection()
        backend._handle_auto_commit_if_needed()
        assert backend._connection.commit_count == 0

    @pytest.mark.asyncio
    async def test_async_handle_auto_commit_commits_when_disabled(self):
        backend = AsyncPostgresBackend(connection_config=make_config())
        backend._connection = FakeAsyncConnection()
        await backend._handle_auto_commit()
        assert backend._connection.commit_count == 1

    @pytest.mark.asyncio
    async def test_async_handle_auto_commit_skips_when_enabled(self):
        backend = AsyncPostgresBackend(connection_config=make_config(autocommit=True))
        backend._connection = FakeAsyncConnection()
        await backend._handle_auto_commit()
        assert backend._connection.commit_count == 0

    @pytest.mark.asyncio
    async def test_async_handle_auto_commit_if_needed_commits_when_disabled(self):
        backend = AsyncPostgresBackend(connection_config=make_config())
        backend._connection = FakeAsyncConnection()
        await backend._handle_auto_commit_if_needed()
        assert backend._connection.commit_count == 1

    @pytest.mark.asyncio
    async def test_async_handle_auto_commit_if_needed_skips_when_enabled(self):
        backend = AsyncPostgresBackend(connection_config=make_config(autocommit=True))
        backend._connection = FakeAsyncConnection()
        await backend._handle_auto_commit_if_needed()
        assert backend._connection.commit_count == 0
