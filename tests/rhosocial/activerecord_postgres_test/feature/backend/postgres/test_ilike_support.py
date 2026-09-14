# tests/rhosocial/activerecord_postgres_test/feature/backend/postgres/test_ilike_support.py
"""Tests for ILIKE protocol support in PostgreSQL dialect."""
import pytest  # noqa: F401
from rhosocial.activerecord.backend.impl.postgres.dialect import PostgresDialect
from rhosocial.activerecord.backend.impl.postgres.expression.ilike import (
    ILIKEExpression,
)
from rhosocial.activerecord.backend.dialect.protocols import ILIKESupport


def format_ilike(dialect, column, pattern, negate=False):
    """Dispatch an ILIKE expression through the dialect formatter."""
    expr = ILIKEExpression(dialect, column=column, pattern=pattern, negate=negate)
    return dialect.format_ilike_expression(expr)


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
        sql, params = format_ilike(dialect, "username", "%smith%")

        assert sql == '"username" ILIKE %s'
        assert params == ('%smith%',)

    def test_format_ilike_expression_with_negate(self):
        """Test NOT ILIKE expression formatting."""
        dialect = PostgresDialect()
        sql, params = format_ilike(dialect, "username", "%smith%", negate=True)

        assert sql == '"username" NOT ILIKE %s'
        assert params == ('%smith%',)

    def test_format_ilike_expression_case_insensitive(self):
        """Test ILIKE is truly case-insensitive."""
        dialect = PostgresDialect()

        # ILIKE should handle case differences automatically
        sql, params = format_ilike(dialect, "username", "%SMITH%")
        assert sql == '"username" ILIKE %s'
        assert params == ('%SMITH%',)

        # Pattern is passed as-is to ILIKE, which handles case-insensitivity
        sql, params = format_ilike(dialect, "username", "%Smith%")
        assert params == ('%Smith%',)

    def test_format_ilike_expression_with_wildcards(self):
        """Test ILIKE with various wildcard patterns."""
        dialect = PostgresDialect()

        # Test with % wildcard
        sql, params = format_ilike(dialect, "email", "%@example.com")
        assert sql == '"email" ILIKE %s'
        assert params == ('%@example.com',)

        # Test with _ wildcard
        sql, params = format_ilike(dialect, "username", "user_")
        assert sql == '"username" ILIKE %s'
        assert params == ('user_',)

        # Test with mixed wildcards
        sql, params = format_ilike(dialect, "username", "%user_%")
        assert sql == '"username" ILIKE %s'
        assert params == ('%user_%',)

    def test_format_ilike_expression_with_special_characters(self):
        """Test ILIKE with special characters in pattern."""
        dialect = PostgresDialect()

        # Test with hyphen
        sql, params = format_ilike(dialect, "username", "user-name%")
        assert params == ('user-name%',)

        # Test with dot
        sql, params = format_ilike(dialect, "email", "%.com")
        assert params == ('%.com',)

        # Test with spaces
        sql, params = format_ilike(dialect, "name", "%John Doe%")
        assert params == ('%John Doe%',)
