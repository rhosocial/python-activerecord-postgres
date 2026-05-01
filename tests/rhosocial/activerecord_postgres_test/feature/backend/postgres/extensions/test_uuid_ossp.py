# tests/rhosocial/activerecord_postgres_test/feature/backend/postgres/extensions/test_uuid_ossp.py
"""
Unit tests for PostgreSQL uuid-ossp extension functions.

Tests for:
- uuid_generate_v1
- uuid_generate_v1mc
- uuid_generate_v4
- uuid_generate_v5
"""

from rhosocial.activerecord.backend.impl.postgres.dialect import PostgresDialect
from rhosocial.activerecord.backend.expression import core
from rhosocial.activerecord.backend.impl.postgres.functions.uuid import (
    uuid_generate_v1,
    uuid_generate_v1mc,
    uuid_generate_v4,
    uuid_generate_v5,
)


class TestPostgresUuidOsspFunctions:
    """Tests for uuid-ossp function factories."""

    def test_uuid_generate_v1(self):
        """uuid_generate_v1 should return FunctionCall with uuid_generate_v1."""
        dialect = PostgresDialect((14, 0, 0))
        result = uuid_generate_v1(dialect)
        assert isinstance(result, core.FunctionCall)
        sql, params = result.to_sql()
        assert "uuid_generate_v1" in sql.lower()

    def test_uuid_generate_v1mc(self):
        """uuid_generate_v1mc should return FunctionCall with uuid_generate_v1mc."""
        dialect = PostgresDialect((14, 0, 0))
        result = uuid_generate_v1mc(dialect)
        assert isinstance(result, core.FunctionCall)
        sql, params = result.to_sql()
        assert "uuid_generate_v1mc" in sql.lower()

    def test_uuid_generate_v4(self):
        """uuid_generate_v4 should return FunctionCall with uuid_generate_v4."""
        dialect = PostgresDialect((14, 0, 0))
        result = uuid_generate_v4(dialect)
        assert isinstance(result, core.FunctionCall)
        sql, params = result.to_sql()
        assert "uuid_generate_v4" in sql.lower()

    def test_uuid_generate_v5(self):
        """uuid_generate_v5 should return FunctionCall with uuid_generate_v5."""
        dialect = PostgresDialect((14, 0, 0))
        result = uuid_generate_v5(dialect, "uuid_ns_dns()", "example.com")
        assert isinstance(result, core.FunctionCall)
        sql, params = result.to_sql()
        assert "uuid_generate_v5" in sql.lower()
