# tests/rhosocial/activerecord_postgres_test/feature/backend/postgres/extensions/test_uuid_ossp.py
"""
Unit tests for PostgreSQL uuid-ossp extension mixin.

Tests for PostgresUuidOssMixin format methods:
- format_uuid_generate_v1
- format_uuid_generate_v4
- format_uuid_generate_v5
"""

from rhosocial.activerecord.backend.impl.postgres.dialect import PostgresDialect


class TestPostgresUuidOssMixin:
    """Tests for PostgresUuidOssMixin format methods."""

    def setup_method(self):
        """Set up test fixtures."""
        self.dialect = PostgresDialect(version=(14, 0, 0))

    def test_format_uuid_generate_v1(self):
        """format_uuid_generate_v1 should return SQL containing uuid_generate_v1."""
        result = self.dialect.format_uuid_generate_v1()
        assert "uuid_generate_v1" in result

    def test_format_uuid_generate_v4(self):
        """format_uuid_generate_v4 should return SQL containing uuid_generate_v4."""
        result = self.dialect.format_uuid_generate_v4()
        assert "uuid_generate_v4" in result

    def test_format_uuid_generate_v5(self):
        """format_uuid_generate_v5 should return SQL containing uuid_generate_v5."""
        result = self.dialect.format_uuid_generate_v5("namespace", "name")
        assert "uuid_generate_v5" in result
