# tests/rhosocial/activerecord_postgres_test/feature/backend/postgres/extensions/test_btree_gin.py
"""
Unit tests for PostgreSQL btree_gin extension mixin.

Tests for PostgresBtreeGinMixin format methods:
- format_gin_index
- format_btree_gin_operator_class
"""

from rhosocial.activerecord.backend.expression import CreateIndexExpression
from rhosocial.activerecord.backend.impl.postgres.dialect import PostgresDialect


class TestPostgresBtreeGinMixin:
    """Tests for PostgresBtreeGinMixin format methods."""

    def setup_method(self):
        """Set up test fixtures."""
        self.dialect = PostgresDialect(version=(14, 0, 0))

    def test_format_gin_index(self):
        """CreateIndexExpression with index_type gin should render a gin index."""
        expr = CreateIndexExpression(
            self.dialect,
            index_name="idx_name",
            table_name="table_name",
            columns=["column"],
            index_type="gin",
        )
        sql, params = expr.to_sql()
        assert "gin" in sql
        assert "idx_name" in sql
        assert "table_name" in sql
        assert params == ()

    def test_format_btree_gin_operator_class(self):
        """format_btree_gin_operator_class should return operator class name."""
        result = self.dialect.format_btree_gin_operator_class("int4")
        assert result == "int4_ops"
