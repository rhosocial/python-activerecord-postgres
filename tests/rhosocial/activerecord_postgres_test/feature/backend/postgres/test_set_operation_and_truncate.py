# tests/rhosocial/activerecord_postgres_test/feature/backend/postgres/test_set_operation_and_truncate.py
"""Tests for SetOperation and Truncate protocol support in PostgreSQL dialect."""
import pytest
from rhosocial.activerecord.backend.impl.postgres.dialect import PostgresDialect
from rhosocial.activerecord.backend.dialect.protocols import (
    SetOperationSupport, TruncateSupport
)


class TestSetOperationSupport:
    """Test SetOperation protocol support."""

    def test_dialect_implements_set_operation_support(self):
        """Verify PostgresDialect implements SetOperationSupport protocol."""
        dialect = PostgresDialect()
        assert isinstance(dialect, SetOperationSupport)

    def test_supports_union(self):
        """Test UNION support."""
        dialect = PostgresDialect()
        assert dialect.supports_union() is True

    def test_supports_union_all(self):
        """Test UNION ALL support."""
        dialect = PostgresDialect()
        assert dialect.supports_union_all() is True

    def test_supports_intersect(self):
        """Test INTERSECT support."""
        dialect = PostgresDialect()
        assert dialect.supports_intersect() is True

    def test_supports_except(self):
        """Test EXCEPT support."""
        dialect = PostgresDialect()
        assert dialect.supports_except() is True

    def test_supports_set_operation_order_by(self):
        """Test ORDER BY support for set operations."""
        dialect = PostgresDialect()
        assert dialect.supports_set_operation_order_by() is True

    def test_supports_set_operation_limit_offset(self):
        """Test LIMIT/OFFSET support for set operations."""
        dialect = PostgresDialect()
        assert dialect.supports_set_operation_limit_offset() is True

    def test_supports_set_operation_for_update(self):
        """Test FOR UPDATE support for set operations."""
        dialect = PostgresDialect()
        assert dialect.supports_set_operation_for_update() is True


class TestTruncateSupport:
    """Test Truncate protocol support."""

    def test_dialect_implements_truncate_support(self):
        """Verify PostgresDialect implements TruncateSupport protocol."""
        dialect = PostgresDialect()
        assert isinstance(dialect, TruncateSupport)

    def test_supports_truncate(self):
        """Test TRUNCATE support."""
        dialect = PostgresDialect()
        assert dialect.supports_truncate() is True

    def test_supports_truncate_table_keyword(self):
        """Test TABLE keyword support in TRUNCATE."""
        dialect = PostgresDialect()
        assert dialect.supports_truncate_table_keyword() is True

    def test_supports_truncate_restart_identity_postgres84(self):
        """Test RESTART IDENTITY support in PostgreSQL 8.4+."""
        dialect_83 = PostgresDialect(version=(8, 3, 0))
        dialect_84 = PostgresDialect(version=(8, 4, 0))
        dialect_90 = PostgresDialect(version=(9, 0, 0))
        dialect_13 = PostgresDialect(version=(13, 0, 0))

        # PostgreSQL 8.3 does not support RESTART IDENTITY
        assert dialect_83.supports_truncate_restart_identity() is False
        # PostgreSQL 8.4+ supports RESTART IDENTITY
        assert dialect_84.supports_truncate_restart_identity() is True
        assert dialect_90.supports_truncate_restart_identity() is True
        assert dialect_13.supports_truncate_restart_identity() is True

    def test_supports_truncate_cascade(self):
        """Test CASCADE support in TRUNCATE."""
        dialect = PostgresDialect()
        assert dialect.supports_truncate_cascade() is True

    def test_format_truncate_statement_basic(self):
        """Test basic TRUNCATE statement formatting."""
        from rhosocial.activerecord.backend.expression.statements import TruncateExpression

        dialect = PostgresDialect()
        expr = TruncateExpression(dialect, table_name="users")
        sql, params = dialect.format_truncate_statement(expr)

        assert sql == 'TRUNCATE TABLE "users"'
        assert params == ()

    def test_format_truncate_statement_with_restart_identity(self):
        """Test TRUNCATE statement with RESTART IDENTITY."""
        from rhosocial.activerecord.backend.expression.statements import TruncateExpression

        dialect = PostgresDialect(version=(9, 0, 0))
        expr = TruncateExpression(dialect, table_name="users", restart_identity=True)
        sql, params = dialect.format_truncate_statement(expr)

        assert sql == 'TRUNCATE TABLE "users" RESTART IDENTITY'
        assert params == ()

    def test_format_truncate_statement_with_cascade(self):
        """Test TRUNCATE statement with CASCADE."""
        from rhosocial.activerecord.backend.expression.statements import TruncateExpression

        dialect = PostgresDialect()
        expr = TruncateExpression(dialect, table_name="orders", cascade=True)
        sql, params = dialect.format_truncate_statement(expr)

        assert sql == 'TRUNCATE TABLE "orders" CASCADE'
        assert params == ()

    def test_format_truncate_statement_with_restart_identity_and_cascade(self):
        """Test TRUNCATE statement with both RESTART IDENTITY and CASCADE."""
        from rhosocial.activerecord.backend.expression.statements import TruncateExpression

        dialect = PostgresDialect(version=(9, 0, 0))
        expr = TruncateExpression(
            dialect,
            table_name="orders",
            restart_identity=True,
            cascade=True
        )
        sql, params = dialect.format_truncate_statement(expr)

        assert sql == 'TRUNCATE TABLE "orders" RESTART IDENTITY CASCADE'
        assert params == ()

    def test_format_truncate_statement_restart_identity_unsupported_version(self):
        """Test TRUNCATE with RESTART IDENTITY on unsupported version (should be ignored)."""
        from rhosocial.activerecord.backend.expression.statements import TruncateExpression

        # PostgreSQL 8.3 does not support RESTART IDENTITY
        dialect = PostgresDialect(version=(8, 3, 0))
        expr = TruncateExpression(dialect, table_name="users", restart_identity=True)
        sql, params = dialect.format_truncate_statement(expr)

        # RESTART IDENTITY should be ignored on unsupported version
        assert sql == 'TRUNCATE TABLE "users"'
        assert params == ()
