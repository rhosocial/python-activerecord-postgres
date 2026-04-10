# tests/rhosocial/activerecord_postgres_test/feature/backend/postgres/test_postgres_uuid_functions.py
"""
Tests for PostgreSQL UUID functions.

Functions: uuid_generate_v1, uuid_generate_v1mc, uuid_generate_v3,
           uuid_generate_v4, uuid_generate_v5, uuid_nil, uuid_max
"""

from rhosocial.activerecord.backend.impl.postgres.dialect import PostgresDialect
from rhosocial.activerecord.backend.impl.postgres.functions.uuid import (
    uuid_generate_v1,
    uuid_generate_v1mc,
    uuid_generate_v3,
    uuid_generate_v4,
    uuid_generate_v5,
    uuid_nil,
    uuid_max,
)


class TestPostgresUUIDFunctions:
    """Tests for PostgreSQL UUID functions."""

    def test_uuid_generate_v1(self, postgres_dialect: PostgresDialect):
        """Test uuid_generate_v1() function."""
        result = uuid_generate_v1(postgres_dialect)
        assert result == "uuid_generate_v1()"

    def test_uuid_generate_v1mc(self, postgres_dialect: PostgresDialect):
        """Test uuid_generate_v1mc() function."""
        result = uuid_generate_v1mc(postgres_dialect)
        assert result == "uuid_generate_v1mc()"

    def test_uuid_generate_v3(self, postgres_dialect: PostgresDialect):
        """Test uuid_generate_v3() function."""
        result = uuid_generate_v3(postgres_dialect, "'dns'", "'example.com'")
        assert result == "uuid_generate_v3('dns', 'example.com')"

    def test_uuid_generate_v4(self, postgres_dialect: PostgresDialect):
        """Test uuid_generate_v4() function."""
        result = uuid_generate_v4(postgres_dialect)
        assert result == "uuid_generate_v4()"

    def test_uuid_generate_v5(self, postgres_dialect: PostgresDialect):
        """Test uuid_generate_v5() function."""
        result = uuid_generate_v5(postgres_dialect, "'dns'", "'example.com'")
        assert result == "uuid_generate_v5('dns', 'example.com')"

    def test_uuid_nil(self, postgres_dialect: PostgresDialect):
        """Test uuid_nil() function."""
        result = uuid_nil(postgres_dialect)
        assert result == "uuid_nil()"

    def test_uuid_max(self, postgres_dialect: PostgresDialect):
        """Test uuid_max() function."""
        result = uuid_max(postgres_dialect)
        assert result == "uuid_max()"