# tests/rhosocial/activerecord_postgres_test/feature/backend/postgres/extensions/test_pg_repack.py
"""
Unit tests for PostgreSQL pg_repack extension functions.

Tests for:
- repack_version
"""

from rhosocial.activerecord.backend.impl.postgres.dialect import PostgresDialect
from rhosocial.activerecord.backend.expression import core
from rhosocial.activerecord.backend.impl.postgres.functions.pg_repack import (
    repack_version,
)


class TestPgRepackMixin:
    """Test pg_repack extension functions.

    Note: pg_repack is primarily a CLI tool. The only public SQL-callable
    function is repack.repack_version(). The internal functions (repack_apply,
    repack_swap, etc.) are not intended for direct use.
    """

    def test_repack_version(self):
        """repack_version should return FunctionCall with repack.repack_version."""
        dialect = PostgresDialect((14, 0, 0))
        result = repack_version(dialect)
        assert isinstance(result, core.FunctionCall)
        sql, params = result.to_sql()
        assert "repack_version" in sql.lower()
        assert "repack" in sql.lower()
