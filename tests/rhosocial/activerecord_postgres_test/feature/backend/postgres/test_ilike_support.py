# tests/rhosocial/activerecord_postgres_test/feature/backend/postgres/test_ilike_support.py
"""Tests for ILIKE protocol support in PostgreSQL dialect."""
from rhosocial.activerecord.backend.expression.core import Column
from rhosocial.activerecord.backend.impl.postgres.dialect import PostgresDialect
from rhosocial.activerecord.backend.impl.postgres.expression.ilike import (
    ILIKEExpression,
)
from rhosocial.activerecord.backend.dialect.protocols import ILIKESupport


class TestILIKESupport:
    """Test ILIKE protocol support."""

    def test_dialect_implements_ilike_support(self):
        """Verify PostgresDialect implements ILIKESupport protocol."""
        dialect = PostgresDialect()
        assert isinstance(dialect, ILIKESupport)

    def test_supports_ilike(self):
        """Test ILIKE support."""
        dialect = PostgresDialect()
        assert dialect.supports_ilike() is True

    def test_format_ilike_expression_basic(self):
        """Test basic ILIKE expression formatting."""
        dialect = PostgresDialect()
        expr = ILIKEExpression(
            dialect, column=Column(dialect, "username"), pattern="%smith%"
        )
        sql, params = expr.to_sql()

        assert sql == '"username" ILIKE %s'
        assert params == ('%smith%',)

    def test_format_ilike_expression_with_negate(self):
        """Test NOT ILIKE expression formatting."""
        dialect = PostgresDialect()
        expr = ILIKEExpression(
            dialect,
            column=Column(dialect, "username"),
            pattern="%smith%",
            negate=True,
        )
        sql, params = expr.to_sql()

        assert sql == '"username" NOT ILIKE %s'
        assert params == ('%smith%',)

    def test_format_ilike_expression_case_insensitive(self):
        """Test ILIKE is truly case-insensitive."""
        dialect = PostgresDialect()

        # ILIKE should handle case differences automatically
        sql, params = ILIKEExpression(
            dialect, column=Column(dialect, "username"), pattern="%SMITH%"
        ).to_sql()
        assert sql == '"username" ILIKE %s'
        assert params == ('%SMITH%',)

        # Pattern is passed as-is to ILIKE, which handles case-insensitivity
        _, params = ILIKEExpression(
            dialect, column=Column(dialect, "username"), pattern="%Smith%"
        ).to_sql()
        assert params == ('%Smith%',)

    def test_format_ilike_expression_with_wildcards(self):
        """Test ILIKE with various wildcard patterns."""
        dialect = PostgresDialect()

        # Test with % wildcard
        sql, params = ILIKEExpression(
            dialect, column=Column(dialect, "email"), pattern="%@example.com"
        ).to_sql()
        assert sql == '"email" ILIKE %s'
        assert params == ('%@example.com',)

        # Test with _ wildcard
        sql, params = ILIKEExpression(
            dialect, column=Column(dialect, "username"), pattern="user_"
        ).to_sql()
        assert sql == '"username" ILIKE %s'
        assert params == ('user_',)

        # Test with mixed wildcards
        sql, params = ILIKEExpression(
            dialect, column=Column(dialect, "username"), pattern="%user_%"
        ).to_sql()
        assert sql == '"username" ILIKE %s'
        assert params == ('%user_%',)

    def test_format_ilike_expression_with_special_characters(self):
        """Test ILIKE with special characters in pattern."""
        dialect = PostgresDialect()

        # Test with hyphen
        _, params = ILIKEExpression(
            dialect, column=Column(dialect, "username"), pattern="user-name%"
        ).to_sql()
        assert params == ('user-name%',)

        # Test with dot
        _, params = ILIKEExpression(
            dialect, column=Column(dialect, "email"), pattern="%.com"
        ).to_sql()
        assert params == ('%.com',)

        # Test with spaces
        _, params = ILIKEExpression(
            dialect, column=Column(dialect, "name"), pattern="%John Doe%"
        ).to_sql()
        assert params == ('%John Doe%',)
