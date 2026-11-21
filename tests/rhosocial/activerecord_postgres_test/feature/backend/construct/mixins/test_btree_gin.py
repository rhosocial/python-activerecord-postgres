# tests/rhosocial/activerecord_postgres_test/feature/backend/construct/mixins/test_btree_gin.py
"""
Unit tests for PostgreSQL btree_gin extension mixin.

Tests for btree_gin index expression rendering (no scalar formatter methods).
"""

from rhosocial.activerecord.backend.expression import CreateIndexStatement
from rhosocial.activerecord.backend.impl.postgres.dialect import PostgresDialect
from rhosocial.activerecord.backend.expression.objects import Index, Table


class TestPostgresBtreeGinMixin:
    """Tests for PostgresBtreeGinMixin format methods."""

    def setup_method(self):
        """Set up test fixtures."""
        self.dialect = PostgresDialect(version=(14, 0, 0))

    def test_gin_index_expression(self):
        """CreateIndexStatement with index_type gin should render a gin index."""
        expr = CreateIndexStatement(
            self.dialect,
            index=Index(self.dialect, 'idx_name'),
            table=Table(self.dialect, 'table_name'),
            columns=["column"],
            index_type="gin",
        )
        sql, params = expr.to_sql()
        assert "gin" in sql
        assert "idx_name" in sql
        assert "table_name" in sql
        assert params == ()

    def test_btree_gin_index_factory(self):
        """The factory returns an expression node, not a raw SQL string."""
        from rhosocial.activerecord.backend.impl.postgres.functions import (
            btree_gin_index,
        )

        expr = btree_gin_index(self.dialect, "table_name", ["column"], "idx_name")
        sql, params = expr.to_sql()
        assert "USING gin" in sql
        assert "idx_name" in sql
        assert params == ()
