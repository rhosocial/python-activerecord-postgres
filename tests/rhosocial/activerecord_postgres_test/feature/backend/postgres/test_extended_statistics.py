# tests/rhosocial/activerecord_postgres_test/feature/backend/postgres/test_extended_statistics.py
"""Unit tests for PostgreSQL extended statistics mixin.

Tests for:
- PostgresExtendedStatisticsMixin feature detection
- Format CREATE STATISTICS statement
- Format DROP STATISTICS statement
"""
import pytest

from rhosocial.activerecord.backend.impl.postgres.dialect import PostgresDialect
from rhosocial.activerecord.backend.impl.postgres.expression.ddl import (
    PostgresCreateStatisticsExpression,
    PostgresDropStatisticsExpression,
)


class TestExtendedStatisticsFeatureDetection:
    """Test extended statistics feature detection methods."""

    def test_supports_create_statistics_pg9(self):
        """PostgreSQL 9.5 does not support CREATE STATISTICS."""
        dialect = PostgresDialect((9, 5, 0))
        assert dialect.supports_create_statistics() is False

    def test_supports_create_statistics_pg10(self):
        """PostgreSQL 10 supports CREATE STATISTICS."""
        dialect = PostgresDialect((10, 0, 0))
        assert dialect.supports_create_statistics() is True

    def test_supports_statistics_dependencies_pg9(self):
        """PostgreSQL 9.5 does not support dependencies statistics."""
        dialect = PostgresDialect((9, 5, 0))
        assert dialect.supports_statistics_dependencies() is False

    def test_supports_statistics_dependencies_pg10(self):
        """PostgreSQL 10 supports dependencies statistics."""
        dialect = PostgresDialect((10, 0, 0))
        assert dialect.supports_statistics_dependencies() is True

    def test_supports_statistics_ndistinct_pg9(self):
        """PostgreSQL 9.5 does not support ndistinct statistics."""
        dialect = PostgresDialect((9, 5, 0))
        assert dialect.supports_statistics_ndistinct() is False

    def test_supports_statistics_ndistinct_pg10(self):
        """PostgreSQL 10 supports ndistinct statistics."""
        dialect = PostgresDialect((10, 0, 0))
        assert dialect.supports_statistics_ndistinct() is True

    def test_supports_statistics_mcv_pg11(self):
        """PostgreSQL 11 does not support MCV statistics."""
        dialect = PostgresDialect((11, 0, 0))
        assert dialect.supports_statistics_mcv() is False

    def test_supports_statistics_mcv_pg12(self):
        """PostgreSQL 12 supports MCV statistics."""
        dialect = PostgresDialect((12, 0, 0))
        assert dialect.supports_statistics_mcv() is True


class TestFormatCreateStatisticsStatement:
    """Test CREATE STATISTICS statement formatting."""

    def test_create_statistics_pg9_raises_error(self):
        """CREATE STATISTICS should raise error on PostgreSQL 9.5."""
        dialect = PostgresDialect((9, 5, 0))
        expr = PostgresCreateStatisticsExpression(
            dialect,
            name="test_stats",
            columns=["col1", "col2"],
            table_name="test_table"
        )
        with pytest.raises(ValueError, match="requires PostgreSQL 10"):
            dialect.format_create_statistics_statement(expr)

    def test_create_statistics_basic(self):
        """Test basic CREATE STATISTICS statement."""
        dialect = PostgresDialect((14, 0, 0))
        expr = PostgresCreateStatisticsExpression(
            dialect,
            name="test_stats",
            columns=["col1", "col2"],
            table_name="test_table"
        )
        sql, params = dialect.format_create_statistics_statement(expr)
        assert "CREATE STATISTICS test_stats" in sql
        assert "ON col1, col2" in sql
        assert "FROM test_table" in sql
        assert params == ()

    def test_create_statistics_with_schema(self):
        """Test CREATE STATISTICS with schema."""
        dialect = PostgresDialect((14, 0, 0))
        expr = PostgresCreateStatisticsExpression(
            dialect,
            name="test_stats",
            columns=["col1", "col2"],
            table_name="test_table",
            schema="public"
        )
        sql, params = dialect.format_create_statistics_statement(expr)
        assert "public.test_stats" in sql
        assert "FROM public.test_table" in sql

    def test_create_statistics_if_not_exists(self):
        """Test CREATE STATISTICS IF NOT EXISTS."""
        dialect = PostgresDialect((14, 0, 0))
        expr = PostgresCreateStatisticsExpression(
            dialect,
            name="test_stats",
            columns=["col1", "col2"],
            table_name="test_table",
            if_not_exists=True
        )
        sql, params = dialect.format_create_statistics_statement(expr)
        assert "CREATE STATISTICS IF NOT EXISTS test_stats" in sql

    def test_create_statistics_with_ndistinct(self):
        """Test CREATE STATISTICS with ndistinct type."""
        dialect = PostgresDialect((14, 0, 0))
        expr = PostgresCreateStatisticsExpression(
            dialect,
            name="test_stats",
            columns=["col1", "col2"],
            table_name="test_table",
            statistics_type="ndistinct"
        )
        sql, params = dialect.format_create_statistics_statement(expr)
        assert "test_stats(ndistinct)" in sql

    def test_create_statistics_with_dependencies(self):
        """Test CREATE STATISTICS with dependencies type."""
        dialect = PostgresDialect((14, 0, 0))
        expr = PostgresCreateStatisticsExpression(
            dialect,
            name="test_stats",
            columns=["col1", "col2"],
            table_name="test_table",
            statistics_type="dependencies"
        )
        sql, params = dialect.format_create_statistics_statement(expr)
        assert "test_stats(dependencies)" in sql

    def test_create_statistics_with_mcv_pg12(self):
        """Test CREATE STATISTICS with MCV type on PostgreSQL 12."""
        dialect = PostgresDialect((12, 0, 0))
        expr = PostgresCreateStatisticsExpression(
            dialect,
            name="test_stats",
            columns=["col1", "col2"],
            table_name="test_table",
            statistics_type="mcv"
        )
        sql, params = dialect.format_create_statistics_statement(expr)
        assert "test_stats(mcv)" in sql

    def test_create_statistics_with_mcv_pg11_raises_error(self):
        """Test CREATE STATISTICS with MCV type on PostgreSQL 11 raises error."""
        dialect = PostgresDialect((11, 0, 0))
        expr = PostgresCreateStatisticsExpression(
            dialect,
            name="test_stats",
            columns=["col1", "col2"],
            table_name="test_table",
            statistics_type="mcv"
        )
        with pytest.raises(ValueError, match="MCV statistics require PostgreSQL 12"):
            dialect.format_create_statistics_statement(expr)

    def test_create_statistics_invalid_type_raises_error(self):
        """Test CREATE STATISTICS with invalid type raises error."""
        dialect = PostgresDialect((14, 0, 0))
        expr = PostgresCreateStatisticsExpression(
            dialect,
            name="test_stats",
            columns=["col1", "col2"],
            table_name="test_table",
            statistics_type="invalid"
        )
        with pytest.raises(ValueError, match="Invalid statistics type"):
            dialect.format_create_statistics_statement(expr)


class TestFormatDropStatisticsStatement:
    """Test DROP STATISTICS statement formatting."""

    def test_drop_statistics_basic(self):
        """Test basic DROP STATISTICS statement."""
        dialect = PostgresDialect((14, 0, 0))
        expr = PostgresDropStatisticsExpression(
            dialect,
            name="test_stats"
        )
        sql, params = dialect.format_drop_statistics_statement(expr)
        assert sql == "DROP STATISTICS test_stats"
        assert params == ()

    def test_drop_statistics_with_schema(self):
        """Test DROP STATISTICS with schema."""
        dialect = PostgresDialect((14, 0, 0))
        expr = PostgresDropStatisticsExpression(
            dialect,
            name="test_stats",
            schema="public"
        )
        sql, params = dialect.format_drop_statistics_statement(expr)
        assert "DROP STATISTICS public.test_stats" in sql

    def test_drop_statistics_if_exists(self):
        """Test DROP STATISTICS IF EXISTS."""
        dialect = PostgresDialect((14, 0, 0))
        expr = PostgresDropStatisticsExpression(
            dialect,
            name="test_stats",
            if_exists=True
        )
        sql, params = dialect.format_drop_statistics_statement(expr)
        assert "DROP STATISTICS IF EXISTS test_stats" in sql
