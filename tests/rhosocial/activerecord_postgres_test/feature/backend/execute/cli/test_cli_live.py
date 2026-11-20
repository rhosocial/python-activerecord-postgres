# tests/rhosocial/activerecord_postgres_test/feature/backend/execute/cli/test_cli_live.py
"""End-to-end PostgreSQL CLI tests against a real server.

The offline suites under ``feature/backend/construct/cli`` and the sibling files
in this directory prove argument wiring and dispatch with fakes. These drive
``main()`` with the connection parameters of the live ``postgres_backend_single``
fixture, so a passing test means the command connected, ran, and rendered a
machine-readable answer -- not merely that the parser accepted the argv.

When no PostgreSQL scenario is configured the fixture skips, so these cases are
inert on a machine without a server and run for real in CI.
"""

import json

import pytest

from rhosocial.activerecord.backend.impl.postgres.__main__ import main

#: ``PostgresConnectionConfig.sslmode`` (libpq spelling) -> the CLI's --ssl choice.
_SSL_TO_CLI = {
    "disable": "disabled",
    "allow": "auto",
    "prefer": "auto",
    "require": "require",
    "verify-ca": "verify-ca",
    "verify-full": "verify-full",
}


def _connection_argv(config):
    """Build connection flags for the scenario the fixture connected with."""
    argv = [
        "--host",
        config.host,
        "--port",
        str(config.port),
        "--database",
        config.database,
        "--user",
        config.username,
        "--password",
        config.password or "",
    ]
    sslmode = getattr(config, "sslmode", None)
    if sslmode:
        argv += ["--ssl", _SSL_TO_CLI.get(sslmode, "auto")]
    return argv


@pytest.fixture
def probed_backend(postgres_backend_single):
    """Give the catalogue listings one known table to find."""
    backend = postgres_backend_single
    backend.execute('DROP TABLE IF EXISTS "cli_live_probe"')
    backend.execute('CREATE TABLE "cli_live_probe" (id INTEGER PRIMARY KEY)')
    yield backend
    backend.execute('DROP TABLE IF EXISTS "cli_live_probe"')


class TestLiveCli:
    """Each subcommand returns non-empty, expected JSON from the live server."""

    def test_info(self, postgres_backend_single, capsys):
        main(["info", "-o", "json", *_connection_argv(postgres_backend_single.config)])
        payload = json.loads(capsys.readouterr().out)
        assert isinstance(payload, dict) and payload
        assert {"database", "features", "protocols"} <= payload.keys()
        assert payload["database"]["type"] == "postgresql"
        assert payload["database"]["connected"] is True
        assert payload["database"]["version"]

    def test_query(self, postgres_backend_single, capsys):
        main(
            [
                "query",
                "-o",
                "json",
                *_connection_argv(postgres_backend_single.config),
                "SELECT 1 AS n",
            ]
        )
        payload = json.loads(capsys.readouterr().out)
        assert isinstance(payload, list) and len(payload) == 1
        assert int(payload[0]["n"]) == 1

    def test_introspect_tables(self, probed_backend, capsys):
        main(
            [
                "introspect",
                "tables",
                "-o",
                "json",
                *_connection_argv(probed_backend.config),
            ]
        )
        payload = json.loads(capsys.readouterr().out)
        assert isinstance(payload, list) and payload
        assert "cli_live_probe" in [row["name"] for row in payload]

    def test_introspect_database(self, probed_backend, capsys):
        main(
            [
                "introspect",
                "database",
                "-o",
                "json",
                *_connection_argv(probed_backend.config),
            ]
        )
        payload = json.loads(capsys.readouterr().out)
        assert isinstance(payload, list) and payload
        assert {"name", "version", "vendor"} <= payload[0].keys()
        assert payload[0]["name"]

    def test_status_all(self, postgres_backend_single, capsys):
        main(
            [
                "status",
                "all",
                "-o",
                "json",
                *_connection_argv(postgres_backend_single.config),
            ]
        )
        payload = json.loads(capsys.readouterr().out)
        assert isinstance(payload, dict) and payload
        assert {"server_version", "server_vendor", "configuration"} <= payload.keys()
        assert payload["server_version"]

    def test_status_config(self, postgres_backend_single, capsys):
        main(
            [
                "status",
                "config",
                "-o",
                "json",
                *_connection_argv(postgres_backend_single.config),
            ]
        )
        payload = json.loads(capsys.readouterr().out)
        assert isinstance(payload, list) and payload
        assert all(row.get("name") for row in payload)
