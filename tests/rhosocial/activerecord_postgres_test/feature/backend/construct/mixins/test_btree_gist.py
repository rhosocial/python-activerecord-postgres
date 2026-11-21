# tests/rhosocial/activerecord_postgres_test/feature/backend/construct/mixins/test_btree_gist.py
"""
Unit tests for PostgreSQL btree_gist extension mixin.

Tests for btree_gist index expression rendering (no scalar formatter methods).
"""

from rhosocial.activerecord.backend.expression import CreateIndexStatement
from rhosocial.activerecord.backend.impl.postgres.dialect import PostgresDialect
from rhosocial.activerecord.backend.expression.objects import Index, Table


class TestPostgresBtreeGistMixin:
    """Tests for PostgresBtreeGistMixin format methods."""

    def setup_method(self):
        """Set up test fixtures."""
        self.dialect = PostgresDialect(version=(14, 0, 0))

    def test_gist_index_expression(self):
        """CreateIndexStatement with index_type gist should render a gist index."""
        expr = CreateIndexStatement(
            self.dialect,
            index=Index(self.dialect, 'idx_name'),
            table=Table(self.dialect, 'table_name'),
            columns=["column"],
            index_type="gist",
        )
        sql, params = expr.to_sql()
        assert "gist" in sql
        assert "idx_name" in sql
        assert "table_name" in sql
        assert params == ()

    def test_gist_index_with_include_expression(self):
        """CreateIndexStatement with include should include additional columns."""
        expr = CreateIndexStatement(
            self.dialect,
            index=Index(self.dialect, 'idx_name'),
            table=Table(self.dialect, 'table_name'),
            columns=["column"],
            index_type="gist",
            include=["extra_col"],
        )
        sql, _ = expr.to_sql()
        assert "gist" in sql
        assert "INCLUDE" in sql

    def test_btree_gist_index_factory(self):
        """The factory returns an expression node, not a raw SQL string."""
        from rhosocial.activerecord.backend.impl.postgres.functions import (
            btree_gist_index,
        )

        expr = btree_gist_index(self.dialect, "table_name", ["column"], "idx_name")
        sql, params = expr.to_sql()
        assert "USING gist" in sql
        assert "idx_name" in sql
        assert params == ()
