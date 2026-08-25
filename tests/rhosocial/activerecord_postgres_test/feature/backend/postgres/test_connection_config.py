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
