# tests/rhosocial/activerecord_postgres_test/feature/backend/postgres/extensions/test_btree_gist.py
"""
Unit tests for PostgreSQL btree_gist extension mixin.

Tests for PostgresBtreeGistMixin format methods:
- format_gist_index
- format_btree_gist_operator_class
"""

from rhosocial.activerecord.backend.expression import CreateIndexStatement
from rhosocial.activerecord.backend.impl.postgres.dialect import PostgresDialect
from rhosocial.activerecord.backend.expression.objects import Index, Table


class TestPostgresBtreeGistMixin:
    """Tests for PostgresBtreeGistMixin format methods."""

    def setup_method(self):
        """Set up test fixtures."""
        self.dialect = PostgresDialect(version=(14, 0, 0))

    def test_format_gist_index(self):
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

    def test_format_gist_index_with_include(self):
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

    def test_format_btree_gist_operator_class(self):
        """format_btree_gist_operator_class should return operator class name."""
        result = self.dialect.format_btree_gist_operator_class("int4")
        assert result == "int4_ops"
