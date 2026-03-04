# tests/rhosocial/activerecord_postgres_test/feature/backend/postgres/test_ilike_support.py
"""Tests for ILIKE protocol support in PostgreSQL dialect."""
import pytest
from rhosocial.activerecord.backend.impl.postgres.dialect import PostgresDialect
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
        sql, params = dialect.format_ilike_expression("username", "%smith%")
        
        assert sql == '"username" ILIKE %s'
        assert params == ('%smith%',)

    def test_format_ilike_expression_with_negate(self):
        """Test NOT ILIKE expression formatting."""
        dialect = PostgresDialect()
        sql, params = dialect.format_ilike_expression("username", "%smith%", negate=True)
        
        assert sql == '"username" NOT ILIKE %s'
        assert params == ('%smith%',)

    def test_format_ilike_expression_case_insensitive(self):
        """Test ILIKE is truly case-insensitive."""
        dialect = PostgresDialect()
        
        # ILIKE should handle case differences automatically
        sql, params = dialect.format_ilike_expression("username", "%SMITH%")
        assert sql == '"username" ILIKE %s'
        assert params == ('%SMITH%',)
        
        # Pattern is passed as-is to ILIKE, which handles case-insensitivity
        sql, params = dialect.format_ilike_expression("username", "%Smith%")
        assert params == ('%Smith%',)

    def test_format_ilike_expression_with_wildcards(self):
        """Test ILIKE with various wildcard patterns."""
        dialect = PostgresDialect()
        
        # Test with % wildcard
        sql, params = dialect.format_ilike_expression("email", "%@example.com")
        assert sql == '"email" ILIKE %s'
        assert params == ('%@example.com',)
        
        # Test with _ wildcard
        sql, params = dialect.format_ilike_expression("username", "user_")
        assert sql == '"username" ILIKE %s'
        assert params == ('user_',)
        
        # Test with mixed wildcards
        sql, params = dialect.format_ilike_expression("username", "%user_%")
        assert sql == '"username" ILIKE %s'
        assert params == ('%user_%',)

    def test_format_ilike_expression_with_special_characters(self):
        """Test ILIKE with special characters in pattern."""
        dialect = PostgresDialect()
        
        # Test with hyphen
        sql, params = dialect.format_ilike_expression("username", "user-name%")
        assert params == ('user-name%',)
        
        # Test with dot
        sql, params = dialect.format_ilike_expression("email", "%.com")
        assert params == ('%.com',)
        
        # Test with spaces
        sql, params = dialect.format_ilike_expression("name", "%John Doe%")
        assert params == ('%John Doe%',)
