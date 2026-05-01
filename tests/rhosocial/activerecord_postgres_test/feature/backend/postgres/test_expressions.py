# tests/rhosocial/activerecord_postgres_test/feature/backend/postgres/test_expressions.py
"""Tests for PostgreSQL-specific expression classes and format methods.

This module tests the expression-based format methods for DDL/DML operations,
including proper quote handling for partition values.
"""
import pytest

from rhosocial.activerecord.backend.impl.postgres.dialect import PostgresDialect
from rhosocial.activerecord.backend.impl.postgres.expression.ddl import (
    PostgresVacuumExpression,
    PostgresAnalyzeExpression,
    PostgresCreatePartitionExpression,
    PostgresDetachPartitionExpression,
    PostgresAttachPartitionExpression,
    PostgresReindexExpression,
    PostgresCreateStatisticsExpression,
    PostgresDropStatisticsExpression,
)


class TestPostgresVacuumExpression:
    """Test PostgresVacuumExpression and format_vacuum_statement."""

    @pytest.fixture
    def dialect(self):
        return PostgresDialect(version=(14, 0, 0))

    def test_basic_vacuum(self, dialect):
        """Test basic VACUUM statement."""
        expr = PostgresVacuumExpression(dialect)
        sql, params = expr.to_sql()
        assert sql == "VACUUM"
        assert params == ()

    def test_vacuum_with_table(self, dialect):
        """Test VACUUM with table name."""
        expr = PostgresVacuumExpression(dialect, table_name="users")
        sql, params = expr.to_sql()
        assert '"users"' in sql
        assert params == ()

    def test_vacuum_with_schema(self, dialect):
        """Test VACUUM with schema.table."""
        expr = PostgresVacuumExpression(dialect, table_name="users", schema="public")
        sql, params = expr.to_sql()
        assert '"public"."users"' in sql

    def test_vacuum_with_options(self, dialect):
        """Test VACUUM with multiple options."""
        expr = PostgresVacuumExpression(
            dialect,
            table_name="users",
            verbose=True,
            analyze=True,
            full=True
        )
        sql, params = expr.to_sql()
        assert "FULL" in sql
        assert "VERBOSE" in sql
        assert "ANALYZE" in sql

    def test_vacuum_parallel_pg13(self):
        """Test VACUUM PARALLEL requires PG 13+."""
        dialect_pg12 = PostgresDialect(version=(12, 0, 0))
        expr = PostgresVacuumExpression(dialect_pg12, parallel=4)

        with pytest.raises(ValueError, match="Parallel VACUUM requires PostgreSQL 13"):
            expr.to_sql()

        dialect_pg13 = PostgresDialect(version=(13, 0, 0))
        expr = PostgresVacuumExpression(dialect_pg13, parallel=4)
        sql, _ = expr.to_sql()
        assert "PARALLEL 4" in sql

    def test_vacuum_index_cleanup_pg14(self):
        """Test VACUUM INDEX_CLEANUP requires PG 14+."""
        dialect_pg13 = PostgresDialect(version=(13, 0, 0))
        expr = PostgresVacuumExpression(dialect_pg13, index_cleanup="AUTO")

        with pytest.raises(ValueError, match="INDEX_CLEANUP requires PostgreSQL 14"):
            expr.to_sql()

        dialect_pg14 = PostgresDialect(version=(14, 0, 0))
        expr = PostgresVacuumExpression(dialect_pg14, index_cleanup="AUTO")
        sql, _ = expr.to_sql()
        assert "INDEX_CLEANUP AUTO" in sql

    def test_vacuum_truncate_excludes_full(self, dialect):
        """Test TRUNCATE and FULL are mutually exclusive."""
        expr = PostgresVacuumExpression(dialect, full=True, truncate=True)

        with pytest.raises(ValueError, match="TRUNCATE cannot be used with FULL"):
            expr.to_sql()


class TestPostgresAnalyzeExpression:
    """Test PostgresAnalyzeExpression and format_analyze_statement."""

    @pytest.fixture
    def dialect(self):
        return PostgresDialect(version=(14, 0, 0))

    def test_basic_analyze(self, dialect):
        """Test basic ANALYZE statement."""
        expr = PostgresAnalyzeExpression(dialect)
        sql, params = expr.to_sql()
        assert sql == "ANALYZE"
        assert params == ()

    def test_analyze_with_columns(self, dialect):
        """Test ANALYZE with specific columns."""
        expr = PostgresAnalyzeExpression(
            dialect,
            table_name="users",
            columns=["id", "name", "email"]
        )
        sql, params = expr.to_sql()
        assert '"users"' in sql
        assert '("id", "name", "email")' in sql


class TestPostgresCreatePartitionExpression:
    """Test PostgresCreatePartitionExpression and format_create_partition_statement."""

    @pytest.fixture
    def dialect(self):
        return PostgresDialect(version=(14, 0, 0))

    def test_range_partition_with_dates(self, dialect):
        """Test RANGE partition with date values - verify proper quoting."""
        expr = PostgresCreatePartitionExpression(
            dialect,
            partition_name="users_2024",
            parent_table="users",
            partition_type="RANGE",
            partition_values={"from": "2024-01-01", "to": "2025-01-01"}
        )
        sql, params = expr.to_sql()

        # Verify single quotes around date values (not double quotes)
        assert "'2024-01-01'" in sql
        assert "'2025-01-01'" in sql
        # Ensure no double single quotes
        assert "''2024" not in sql

    def test_range_partition_with_maxvalue(self, dialect):
        """Test RANGE partition with MAXVALUE - no quotes."""
        expr = PostgresCreatePartitionExpression(
            dialect,
            partition_name="users_max",
            parent_table="users",
            partition_type="RANGE",
            partition_values={"from": "2024-01-01", "to": "MAXVALUE"}
        )
        sql, _ = expr.to_sql()

        # MAXVALUE should not be quoted
        assert "MAXVALUE" in sql
        assert "'MAXVALUE'" not in sql

    def test_range_partition_with_minvalue(self, dialect):
        """Test RANGE partition with MINVALUE - no quotes."""
        expr = PostgresCreatePartitionExpression(
            dialect,
            partition_name="users_min",
            parent_table="users",
            partition_type="RANGE",
            partition_values={"from": "MINVALUE", "to": "2024-01-01"}
        )
        sql, _ = expr.to_sql()

        # MINVALUE should not be quoted
        assert "MINVALUE" in sql
        assert "'MINVALUE'" not in sql

    def test_list_partition_with_values(self, dialect):
        """Test LIST partition with values - verify proper quoting."""
        expr = PostgresCreatePartitionExpression(
            dialect,
            partition_name="users_active",
            parent_table="users",
            partition_type="LIST",
            partition_values={"values": ["active", "pending", "verified"]}
        )
        sql, _ = expr.to_sql()

        # Verify single quotes around string values
        assert "'active'" in sql
        assert "'pending'" in sql
        assert "'verified'" in sql
        # Ensure no double single quotes
        assert "''active''" not in sql

    def test_list_partition_default(self, dialect):
        """Test LIST partition with DEFAULT."""
        expr = PostgresCreatePartitionExpression(
            dialect,
            partition_name="users_default",
            parent_table="users",
            partition_type="LIST",
            partition_values={"default": True}
        )
        sql, _ = expr.to_sql()
        assert "DEFAULT" in sql

    def test_hash_partition(self, dialect):
        """Test HASH partition."""
        expr = PostgresCreatePartitionExpression(
            dialect,
            partition_name="users_hash_0",
            parent_table="users",
            partition_type="HASH",
            partition_values={"modulus": 4, "remainder": 0}
        )
        sql, _ = expr.to_sql()

        assert "MODULUS 4" in sql
        assert "REMAINDER 0" in sql

    def test_hash_partition_requires_pg11(self):
        """Test HASH partition requires PostgreSQL 11+."""
        dialect_pg10 = PostgresDialect(version=(10, 0, 0))
        expr = PostgresCreatePartitionExpression(
            dialect_pg10,
            partition_name="users_hash",
            parent_table="users",
            partition_type="HASH",
            partition_values={"modulus": 4, "remainder": 0}
        )

        with pytest.raises(ValueError, match="HASH partitioning requires PostgreSQL 11"):
            expr.to_sql()

    def test_partition_with_tablespace(self, dialect):
        """Test partition with tablespace."""
        expr = PostgresCreatePartitionExpression(
            dialect,
            partition_name="users_2024",
            parent_table="users",
            partition_type="RANGE",
            partition_values={"from": "2024-01-01", "to": "2025-01-01"},
            tablespace="fast_storage"
        )
        sql, _ = expr.to_sql()
        assert 'TABLESPACE "fast_storage"' in sql


class TestPostgresDetachPartitionExpression:
    """Test PostgresDetachPartitionExpression and format_detach_partition_statement."""

    @pytest.fixture
    def dialect(self):
        return PostgresDialect(version=(14, 0, 0))

    def test_basic_detach(self, dialect):
        """Test basic DETACH PARTITION."""
        expr = PostgresDetachPartitionExpression(
            dialect,
            partition_name="users_2024",
            parent_table="users"
        )
        sql, _ = expr.to_sql()
        assert "DETACH PARTITION" in sql
        assert '"users_2024"' in sql

    def test_detach_concurrently_pg14(self, dialect):
        """Test DETACH CONCURRENTLY requires PostgreSQL 14+."""
        dialect_pg13 = PostgresDialect(version=(13, 0, 0))
        expr = PostgresDetachPartitionExpression(
            dialect_pg13,
            partition_name="users_2024",
            parent_table="users",
            concurrently=True
        )

        with pytest.raises(ValueError, match="DETACH CONCURRENTLY requires PostgreSQL 14"):
            expr.to_sql()

        # PG 14 should work
        expr = PostgresDetachPartitionExpression(
            dialect,
            partition_name="users_2024",
            parent_table="users",
            concurrently=True
        )
        sql, _ = expr.to_sql()
        assert "DETACH CONCURRENTLY" in sql

    def test_finalize_requires_concurrently(self, dialect):
        """Test FINALIZE only valid with CONCURRENTLY."""
        expr = PostgresDetachPartitionExpression(
            dialect,
            partition_name="users_2024",
            parent_table="users",
            concurrently=False,
            finalize=True
        )

        with pytest.raises(ValueError, match="FINALIZE only valid with CONCURRENTLY"):
            expr.to_sql()


class TestPostgresAttachPartitionExpression:
    """Test PostgresAttachPartitionExpression and format_attach_partition_statement."""

    @pytest.fixture
    def dialect(self):
        return PostgresDialect(version=(14, 0, 0))

    def test_attach_range_partition(self, dialect):
        """Test ATTACH PARTITION for RANGE."""
        expr = PostgresAttachPartitionExpression(
            dialect,
            partition_name="users_2024",
            parent_table="users",
            partition_type="RANGE",
            partition_values={"from": "2024-01-01", "to": "2025-01-01"}
        )
        sql, _ = expr.to_sql()
        assert "ATTACH PARTITION" in sql
        assert "'2024-01-01'" in sql


class TestQuoteHandling:
    """Test quote handling in partition value formatting."""

    @pytest.fixture
    def dialect(self):
        return PostgresDialect(version=(14, 0, 0))

    def test_string_values_are_quoted(self, dialect):
        """Test that string values are properly quoted."""
        expr = PostgresCreatePartitionExpression(
            dialect,
            partition_name="test",
            parent_table="parent",
            partition_type="LIST",
            partition_values={"values": ["value1", "value2"]}
        )
        sql, _ = expr.to_sql()

        # Single quotes should wrap values
        assert "'value1'" in sql
        assert "'value2'" in sql

        # No double single quotes
        assert "''value" not in sql

    def test_numeric_values_not_quoted(self, dialect):
        """Test that numeric values are not quoted."""
        expr = PostgresCreatePartitionExpression(
            dialect,
            partition_name="test",
            parent_table="parent",
            partition_type="RANGE",
            partition_values={"from": 1, "to": 100}
        )
        sql, _ = expr.to_sql()

        # Numbers should not have quotes
        assert "FROM (1)" in sql
        assert "TO (100)" in sql

    def test_null_value(self, dialect):
        """Test NULL value handling."""
        expr = PostgresCreatePartitionExpression(
            dialect,
            partition_name="test",
            parent_table="parent",
            partition_type="LIST",
            partition_values={"values": [None, "value"]}
        )
        sql, _ = expr.to_sql()

        # NULL should not be quoted
        assert "NULL" in sql
        assert "'value'" in sql
