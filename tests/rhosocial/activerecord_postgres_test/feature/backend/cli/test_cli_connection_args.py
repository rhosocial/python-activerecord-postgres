# tests/rhosocial/activerecord_postgres_test/feature/backend/cli/test_cli_connection_args.py
"""Offline tests for ``resolve_connection_config_from_args`` (SSL mapping, etc.)."""
import argparse

from rhosocial.activerecord.backend.impl.postgres.cli.connection import (
    add_connection_args,
    resolve_connection_config_from_args,
)


def _parse(extra=None):
    parser = argparse.ArgumentParser()
    add_connection_args(parser)
    return parser.parse_args(extra or [])


class TestCLISSLMapping:
    """SSL parameter mapping in resolve_connection_config_from_args."""

    def test_default_ssl_maps_to_prefer(self):
        config = resolve_connection_config_from_args(_parse())
        assert config.sslmode == "prefer"

    def test_ssl_auto(self):
        config = resolve_connection_config_from_args(_parse(["--ssl", "auto"]))
        assert config.sslmode == "prefer"

    def test_ssl_require(self):
        config = resolve_connection_config_from_args(_parse(["--ssl", "require"]))
        assert config.sslmode == "require"

    def test_ssl_verify_ca(self):
        config = resolve_connection_config_from_args(_parse(["--ssl", "verify-ca"]))
        assert config.sslmode == "verify-ca"

    def test_ssl_verify_full(self):
        config = resolve_connection_config_from_args(_parse(["--ssl", "verify-full"]))
        assert config.sslmode == "verify-full"

    def test_ssl_disabled(self):
        config = resolve_connection_config_from_args(_parse(["--ssl", "disabled"]))
        assert config.sslmode == "disable"

    def test_is_async_flag(self):
        assert _parse(["--async"]).is_async is True
        assert _parse([]).is_async is False