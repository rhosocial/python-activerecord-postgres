# tests/rhosocial/activerecord_postgres_test/feature/backend/postgres/dialect/test_qualify_not_supported.py
"""Tests verifying PostgreSQL dialect does not support the QUALIFY clause.

PostgreSQL does not implement the QUALIFY clause (it is only a proposed
extension). Filtering on window function results must be expressed through a
subquery or CTE. These tests lock in the unsupported behavior so that future
changes do not silently emit invalid SQL.
"""
import pytest  # noqa: F401
from rhosocial.activerecord.backend.impl.postgres.dialect import PostgresDialect
from rhosocial.activerecord.backend.dialect.exceptions import UnsupportedFeatureError
from rhosocial.activerecord.backend.dialect.protocols import QualifyClauseSupport
from rhosocial.activerecord.backend.expression import (
    QualifyClause,
    Column,
    Literal,
)


class TestQualifyClauseNotSupported:
    """Test that QUALIFY clause support is disabled in PostgreSQL."""

    def test_dialect_implements_qualify_support_protocol(self):
        """PostgresDialect should still implement the protocol interface."""
        dialect = PostgresDialect()
        assert isinstance(dialect, QualifyClauseSupport)

    def test_supports_qualify_clause_is_false(self):
        """PostgreSQL does not support the QUALIFY clause."""
        dialect = PostgresDialect()
        assert dialect.supports_qualify_clause() is False

    def test_format_qualify_clause_raises(self):
        """Formatting a QUALIFY clause should raise UnsupportedFeatureError."""
        dialect = PostgresDialect()
        qualify = QualifyClause(dialect, Column(dialect, "x") == Literal(dialect, 1))
        with pytest.raises(UnsupportedFeatureError) as exc_info:
            qualify.to_sql()
        assert "QUALIFY clause" in str(exc_info.value)

    def test_qualify_clause_to_sql_raises(self):
        """QualifyClause.to_sql() should surface the unsupported error."""
        dialect = PostgresDialect()
        qualify = QualifyClause(dialect, Column(dialect, "x") == Literal(dialect, 1))
        with pytest.raises(UnsupportedFeatureError):
            qualify.to_sql()
