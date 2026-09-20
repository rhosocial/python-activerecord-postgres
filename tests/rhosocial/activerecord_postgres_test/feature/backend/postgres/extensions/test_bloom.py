# tests/rhosocial/activerecord_postgres_test/feature/backend/postgres/extensions/test_bloom.py
"""
Unit tests for PostgreSQL bloom extension mixin.

Tests for PostgresBloomMixin format methods:
- format_bloom_index
- format_bloom_access_method
"""

from rhosocial.activerecord.backend.expression import CreateIndexExpression
from rhosocial.activerecord.backend.impl.postgres.dialect import PostgresDialect
from rhosocial.activerecord.backend.impl.postgres.expression.ddl import (
    PostgresCreateIndexExpression,
)


class TestPostgresBloomMixin:
    """Tests for PostgresBloomMixin format methods."""

    def setup_method(self):
        """Set up test fixtures."""
        self.dialect = PostgresDialect(version=(14, 0, 0))

    def test_format_bloom_index(self):
        """CreateIndexExpression with index_type bloom should render a bloom index."""
        expr = CreateIndexExpression(
            self.dialect,
            index_name="idx_name",
            table_name="table_name",
            columns=["col1", "col2"],
            index_type="bloom",
        )
        sql, params = expr.to_sql()
        assert "bloom" in sql
        assert "idx_name" in sql
        assert "table_name" in sql
        assert params == ()

    def test_format_bloom_index_with_fill_factor(self):
        """CreateIndexExpression with fillfactor should include WITH (fillfactor = ...)."""
        expr = PostgresCreateIndexExpression(
            self.dialect,
            index_name="idx_name",
            table_name="table_name",
            columns=["col1", "col2"],
            index_type="bloom",
            with_options={"fillfactor": 90},
        )
        sql, _ = expr.to_sql()
        assert "bloom" in sql
        assert "fillfactor" in sql

    def test_format_bloom_access_method(self):
        """format_bloom_access_method should return SQL containing bloom."""
        result = self.dialect.format_bloom_access_method()
        assert "bloom" in result
