# tests/rhosocial/activerecord_postgres_test/feature/backend/test_ssl_connection_params.py
"""Regression tests: SSL/TLS config must actually reach ``psycopg.connect()``.

The config exposes libpq keyword names (``sslmode``/``sslcert``/``sslkey``/
``sslrootcert``/``sslcrl``/``sslcompression``). These tests pin that the driver
call receives exactly those names, so the generic ``ssl_ca``/``ssl_cert``/
``ssl_key``/``ssl_mode`` naming can never creep back in and silently drop the
settings.
"""

from unittest.mock import patch

import pytest

from rhosocial.activerecord.backend.impl.postgres.backend.async_backend import AsyncPostgresBackend
from rhosocial.activerecord.backend.impl.postgres.backend.backend import PostgresBackend
from rhosocial.activerecord.backend.impl.postgres.config import PostgresConnectionConfig


@pytest.fixture
def ssl_config():
    return PostgresConnectionConfig(
        host="db.example.com",
        port=5432,
        database="test_db",
        username="tester",
        password="secret",
        sslmode="verify-full",
        sslcert="/certs/client.crt",
        sslkey="/certs/client.key",
        sslrootcert="/certs/ca.crt",
        sslcrl="/certs/ca.crl",
        sslcompression=True,
    )


def test_config_exposes_libpq_ssl_field_names(ssl_config):
    """The generic SSL mixin names must not be present on the postgres config.

    ``ConnectionConfig`` does not inherit ``SSLMixin``, so a config carrying
    ``ssl_ca``/``ssl_mode`` would make the driver look for attributes that do
    not exist and quietly forward nothing.
    """
    for missing in ("ssl_ca", "ssl_cert", "ssl_key", "ssl_mode"):
        assert not hasattr(ssl_config, missing), (
            f"PostgresConnectionConfig must not expose generic SSL field {missing!r}"
        )


def test_get_ssl_connection_params_returns_all_set_values(ssl_config):
    assert ssl_config.get_ssl_connection_params() == {
        "sslmode": "verify-full",
        "sslcert": "/certs/client.crt",
        "sslkey": "/certs/client.key",
        "sslrootcert": "/certs/ca.crt",
        "sslcrl": "/certs/ca.crl",
        "sslcompression": True,
    }


def test_get_ssl_connection_params_omits_unset_values():
    config = PostgresConnectionConfig(host="localhost", database="test_db", sslmode="require")
    assert config.get_ssl_connection_params() == {"sslmode": "require"}


def test_get_ssl_connection_params_empty_by_default():
    config = PostgresConnectionConfig(host="localhost", database="test_db")
    assert config.get_ssl_connection_params() == {}


def test_sync_connect_forwards_ssl_params(ssl_config):
    """Sync connect() must hand the libpq SSL keywords to psycopg verbatim."""
    backend = PostgresBackend(connection_config=ssl_config)
    with patch("rhosocial.activerecord.backend.impl.postgres.backend.backend.psycopg.connect") as connect:
        backend.connect()

    params = connect.call_args.kwargs
    for key, value in ssl_config.get_ssl_connection_params().items():
        assert params[key] == value
    assert params["host"] == "db.example.com"
    assert params["dbname"] == "test_db"


@pytest.mark.asyncio
async def test_async_connect_forwards_ssl_params(ssl_config):
    """Async connect() must hand the same libpq SSL keywords to psycopg."""
    backend = AsyncPostgresBackend(connection_config=ssl_config)
    connect = patch(
        "rhosocial.activerecord.backend.impl.postgres.backend.async_backend.AsyncConnection.connect"
    )
    with connect as mock_connect:
        mock_connect.return_value.set_autocommit = _noop_coroutine
        await backend.connect()

    params = mock_connect.call_args.kwargs
    for key, value in ssl_config.get_ssl_connection_params().items():
        assert params[key] == value
    assert params["host"] == "db.example.com"
    assert params["dbname"] == "test_db"


async def _noop_coroutine(*args, **kwargs):
    return None


def test_sync_connect_sends_no_ssl_keys_when_unset():
    """No stray ``ssl*`` key may be emitted when the user configured nothing.

    psycopg drops ``None`` kwargs, but an explicit ``sslmode=None`` would hide
    the difference between "unset" and "dropped by accident"; keeping the key
    out entirely is what lets libpq/PGSERVICE defaults apply.
    """
    config = PostgresConnectionConfig(host="localhost", port=5432, database="test_db")
    backend = PostgresBackend(connection_config=config)
    with patch("rhosocial.activerecord.backend.impl.postgres.backend.backend.psycopg.connect") as connect:
        backend.connect()

    assert not [key for key in connect.call_args.kwargs if key.startswith("ssl")]


def test_sslmode_disable_is_distinguishable_from_require():
    """A stored sslmode must not be normalised away.

    ``disable`` and ``require`` are the two values that visibly change the
    resulting session, so a config that collapses them is a broken config.
    """
    def build(mode):
        return PostgresConnectionConfig(
            host="localhost", database="test_db", sslmode=mode
        ).get_ssl_connection_params()

    assert build("disable") == {"sslmode": "disable"}
    assert build("require") == {"sslmode": "require"}
