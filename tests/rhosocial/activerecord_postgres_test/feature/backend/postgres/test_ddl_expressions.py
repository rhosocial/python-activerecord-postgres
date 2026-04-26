# tests/rhosocial/activerecord_postgres_test/feature/backend/postgres/test_ddl_expressions.py
"""Tests for PostgreSQL DDL expression classes.

This module tests the expression-based format methods for DDL operations,
including materialized view refresh, comment, and partition expressions.
"""
import pytest

import pytest
from rhosocial.activerecord.backend.impl.postgres.dialect import PostgresDialect
from rhosocial.activerecord.backend.impl.postgres.expression.ddl import (
    PostgresRefreshMaterializedViewExpression,
    PostgresCommentExpression,
    PostgresCreatePartitionExpression,
    PostgresDetachPartitionExpression,
    PostgresAttachPartitionExpression,
    PostgresVacuumExpression,
    PostgresAnalyzeExpression,
)
from rhosocial.activerecord.backend.impl.postgres.mixins.dml.extended_statistics import (
    PostgresExtendedStatisticsMixin,
)


class TestPostgresRefreshMaterializedViewExpression:
    """Test PostgresRefreshMaterializedViewExpression."""

    @pytest.fixture
    def dialect(self):
        return PostgresDialect(version=(14, 0, 0))

    def test_basic_refresh(self, dialect):
        """Test basic REFRESH MATERIALIZED VIEW statement."""
        expr = PostgresRefreshMaterializedViewExpression(
            dialect=dialect,
            name="monthly_sales_summary",
        )
        sql, params = expr.to_sql()
        assert sql == "REFRESH MATERIALIZED VIEW monthly_sales_summary"
        assert params == ()

    def test_refresh_with_schema(self, dialect):
        """Test REFRESH MATERIALIZED VIEW with schema."""
        expr = PostgresRefreshMaterializedViewExpression(
            dialect=dialect,
            name="monthly_sales_summary",
            schema="analytics",
        )
        sql, params = expr.to_sql()
        assert "analytics.monthly_sales_summary" in sql
        assert params == ()

    def test_refresh_concurrently_pg13(self):
        """Test CONCURRENTLY refresh requires PG 9.4+."""
        dialect_pg93 = PostgresDialect(version=(9, 3, 0))
        expr = PostgresRefreshMaterializedViewExpression(
            dialect=dialect_pg93,
            name="monthly_sales_summary",
            concurrently=True,
        )

        with pytest.raises(ValueError, match="CONCURRENTLY requires PostgreSQL 9"):
            expr.to_sql()

    def test_refresh_concurrently_pg94(self, dialect):
        """Test CONCURRENTLY refresh with PG 9.4+."""
        expr = PostgresRefreshMaterializedViewExpression(
            dialect=dialect,
            name="monthly_sales_summary",
            concurrently=True,
        )
        sql, params = expr.to_sql()
        assert "CONCURRENTLY" in sql
        assert params == ()

    def test_refresh_with_data_false_pg93(self):
        """Test WITH NO DATA requires PG 9.4+."""
        dialect_pg93 = PostgresDialect(version=(9, 3, 0))
        expr = PostgresRefreshMaterializedViewExpression(
            dialect=dialect_pg93,
            name="monthly_sales_summary",
            with_data=False,
        )

        sql, params = expr.to_sql()
        assert "WITH NO DATA" in sql

    def test_refresh_with_data_false_pg94(self, dialect):
        """Test WITH NO DATA with PG 9.4+."""
        expr = PostgresRefreshMaterializedViewExpression(
            dialect=dialect,
            name="monthly_sales_summary",
            with_data=False,
        )
        sql, params = expr.to_sql()
        assert "WITH NO DATA" in sql
        assert params == ()

    def test_refresh_concurrently_and_with_data(self, dialect):
        """Test CONCURRENTLY with WITH NO DATA."""
        expr = PostgresRefreshMaterializedViewExpression(
            dialect=dialect,
            name="monthly_sales_summary",
            concurrently=True,
            with_data=False,
        )
        sql, params = expr.to_sql()
        assert "CONCURRENTLY" in sql
        assert "WITH NO DATA" in sql
        assert params == ()


class TestPostgresCommentExpression:
    """Test PostgresCommentExpression."""

    @pytest.fixture
    def dialect(self):
        return PostgresDialect(version=(14, 0, 0))

    def test_comment_on_table(self, dialect):
        """Test COMMENT ON TABLE."""
        expr = PostgresCommentExpression(
            dialect=dialect,
            object_type="TABLE",
            object_name="users",
            comment="User accounts table",
        )
        sql, params = expr.to_sql()
        assert "COMMENT ON TABLE" in sql
        assert "users" in sql
        assert params == ("User accounts table",)

    def test_comment_on_column(self, dialect):
        """Test COMMENT ON COLUMN."""
        expr = PostgresCommentExpression(
            dialect=dialect,
            object_type="COLUMN",
            object_name="users.email",
            comment="User email address",
        )
        sql, params = expr.to_sql()
        assert "COMMENT ON COLUMN" in sql
        assert "users.email" in sql
        assert params == ("User email address",)

    def test_comment_on_index(self, dialect):
        """Test COMMENT ON INDEX."""
        expr = PostgresCommentExpression(
            dialect=dialect,
            object_type="INDEX",
            object_name="users_email_idx",
            comment="Email index for users table",
        )
        sql, params = expr.to_sql()
        assert "COMMENT ON INDEX" in sql

    def test_comment_on_view(self, dialect):
        """Test COMMENT ON VIEW."""
        expr = PostgresCommentExpression(
            dialect=dialect,
            object_type="VIEW",
            object_name="user_stats",
            comment="User statistics view",
        )
        sql, params = expr.to_sql()
        assert "COMMENT ON VIEW" in sql

    def test_comment_on_schema(self, dialect):
        """Test COMMENT ON SCHEMA."""
        expr = PostgresCommentExpression(
            dialect=dialect,
            object_type="SCHEMA",
            object_name="analytics",
            comment="Analytics schema",
        )
        sql, params = expr.to_sql()
        assert "COMMENT ON SCHEMA" in sql

    def test_comment_on_function(self, dialect):
        """Test COMMENT ON FUNCTION."""
        expr = PostgresCommentExpression(
            dialect=dialect,
            object_type="FUNCTION",
            object_name="calculate_total",
            comment="Calculate total amount",
        )
        sql, params = expr.to_sql()
        assert "COMMENT ON FUNCTION" in sql

    def test_remove_comment(self, dialect):
        """Test removing comment by setting comment to None."""
        expr = PostgresCommentExpression(
            dialect=dialect,
            object_type="TABLE",
            object_name="users",
            comment=None,
        )
        sql, params = expr.to_sql()
        assert "COMMENT ON TABLE" in sql
        assert "NULL" in sql
        assert params == ()

    def test_comment_with_schema(self, dialect):
        """Test comment on object with schema."""
        expr = PostgresCommentExpression(
            dialect=dialect,
            object_type="TABLE",
            object_name="public.users",
            comment="Public users table",
            schema="public",
        )
        sql, params = expr.to_sql()
        assert "public.users" in sql
        assert params == ("Public users table",)


class TestPostgresCreatePartitionExpression:
    """Test PostgresCreatePartitionExpression."""

    @pytest.fixture
    def dialect(self):
        return PostgresDialect(version=(14, 0, 0))

    def test_create_range_partition(self, dialect):
        """Test CREATE TABLE ... PARTITION OF for RANGE."""
        expr = PostgresCreatePartitionExpression(
            dialect=dialect,
            partition_name="orders_2024_q1",
            parent_table="orders",
            partition_type="RANGE",
            partition_values={"from": "2024-01-01", "to": "2024-04-01"},
        )
        sql, params = expr.to_sql()
        assert "CREATE TABLE" in sql
        assert "PARTITION OF" in sql
        assert "FOR VALUES" in sql
        assert "FROM" in sql
        assert "TO" in sql
        assert params == ()

    def test_create_list_partition(self, dialect):
        """Test CREATE TABLE ... PARTITION OF for LIST."""
        expr = PostgresCreatePartitionExpression(
            dialect=dialect,
            partition_name="orders_active",
            parent_table="orders",
            partition_type="LIST",
            partition_values={"values": ["active", "pending"]},
        )
        sql, params = expr.to_sql()
        assert "FOR VALUES" in sql
        assert "IN" in sql
        assert params == ()

    def test_create_hash_partition_pg10(self):
        """Test HASH partitioning requires PG 11+."""
        dialect_pg10 = PostgresDialect(version=(10, 0, 0))
        expr = PostgresCreatePartitionExpression(
            dialect=dialect_pg10,
            partition_name="orders shard0",
            parent_table="orders",
            partition_type="HASH",
            partition_values={"modulus": 4, "remainder": 0},
        )
        with pytest.raises(ValueError, match="HASH partitioning requires PostgreSQL 11"):
            expr.to_sql()

    def test_create_hash_partition_pg11(self, dialect):
        """Test HASH partitioning with PG 11+."""
        expr = PostgresCreatePartitionExpression(
            dialect=dialect,
            partition_name="orders_shard0",
            parent_table="orders",
            partition_type="HASH",
            partition_values={"modulus": 4, "remainder": 0},
        )
        sql, params = expr.to_sql()
        assert "FOR VALUES" in sql
        assert "MODULUS 4" in sql
        assert "REMAINDER 0" in sql

    def test_create_partition_with_schema(self, dialect):
        """Test partition with schema."""
        expr = PostgresCreatePartitionExpression(
            dialect=dialect,
            partition_name="2024_q1",
            parent_table="orders",
            partition_type="RANGE",
            partition_values={"from": "2024-01-01", "to": "2024-04-01"},
            schema="sales",
        )
        sql, params = expr.to_sql()
        assert '"sales".' in sql

    def test_create_partition_if_not_exists(self, dialect):
        """Test partition with IF NOT EXISTS."""
        expr = PostgresCreatePartitionExpression(
            dialect=dialect,
            partition_name="orders_2024_q1",
            parent_table="orders",
            partition_type="RANGE",
            partition_values={"from": "2024-01-01", "to": "2024-04-01"},
            if_not_exists=True,
        )
        sql, params = expr.to_sql()
        assert "IF NOT EXISTS" in sql

    def test_create_partition_with_tablespace(self, dialect):
        """Test partition with TABLESPACE."""
        expr = PostgresCreatePartitionExpression(
            dialect=dialect,
            partition_name="orders_2024_q1",
            parent_table="orders",
            partition_type="RANGE",
            partition_values={"from": "2024-01-01", "to": "2024-04-01"},
            tablespace="faststorage",
        )
        sql, params = expr.to_sql()
        assert "TABLESPACE" in sql
        assert "faststorage" in sql


class TestPostgresDetachPartitionExpression:
    """Test PostgresDetachPartitionExpression."""

    @pytest.fixture
    def dialect(self):
        return PostgresDialect(version=(14, 0, 0))

    def test_basic_detach(self, dialect):
        """Test basic DETACH PARTITION."""
        expr = PostgresDetachPartitionExpression(
            dialect=dialect,
            partition_name="orders_2023",
            parent_table="orders",
        )
        sql, params = expr.to_sql()
        assert "ALTER TABLE" in sql
        assert "DETACH PARTITION" in sql
        assert "orders_2023" in sql
        assert params == ()

    def test_detach_concurrently_pg13(self):
        """Test CONCURRENTLY requires PG 14+."""
        dialect_pg13 = PostgresDialect(version=(13, 0, 0))
        expr = PostgresDetachPartitionExpression(
            dialect=dialect_pg13,
            partition_name="orders_2023",
            parent_table="orders",
            concurrently=True,
        )
        with pytest.raises(ValueError, match="DETACH CONCURRENTLY requires PostgreSQL 14"):
            expr.to_sql()

    def test_detach_concurrently_pg14(self, dialect):
        """Test DETACH CONCURRENTLY with PG 14+."""
        expr = PostgresDetachPartitionExpression(
            dialect=dialect,
            partition_name="orders_2023",
            parent_table="orders",
            concurrently=True,
        )
        sql, params = expr.to_sql()
        assert "DETACH CONCURRENTLY" in sql

    def test_detach_finalize_requires_concurrently(self, dialect):
        """Test FINALIZE requires CONCURRENTLY."""
        expr = PostgresDetachPartitionExpression(
            dialect=dialect,
            partition_name="orders_2023",
            parent_table="orders",
            finalize=True,
        )
        with pytest.raises(ValueError, match="FINALIZE only valid with CONCURRENTLY"):
            expr.to_sql()

    def test_detach_with_schema(self, dialect):
        """Test partition detach with schema."""
        expr = PostgresDetachPartitionExpression(
            dialect=dialect,
            partition_name="orders_2023",
            parent_table="orders",
            schema="sales",
        )
        sql, params = expr.to_sql()
        assert '"sales".' in sql


class TestPostgresAttachPartitionExpression:
    """Test PostgresAttachPartitionExpression."""

    @pytest.fixture
    def dialect(self):
        return PostgresDialect(version=(14, 0, 0))

    def test_attach_range_partition(self, dialect):
        """Test ATTACH PARTITION for RANGE."""
        expr = PostgresAttachPartitionExpression(
            dialect=dialect,
            partition_name="orders_2024_q1",
            parent_table="orders",
            partition_type="RANGE",
            partition_values={"from": "2024-01-01", "to": "2024-04-01"},
        )
        sql, params = expr.to_sql()
        assert "ALTER TABLE" in sql
        assert "ATTACH PARTITION" in sql
        assert "FOR VALUES" in sql
        assert "FROM" in sql
        assert "TO" in sql
        assert params == ()

    def test_attach_list_partition(self, dialect):
        """Test ATTACH PARTITION for LIST."""
        expr = PostgresAttachPartitionExpression(
            dialect=dialect,
            partition_name="orders_active",
            parent_table="orders",
            partition_type="LIST",
            partition_values={"values": ["active"]},
        )
        sql, params = expr.to_sql()
        assert "FOR VALUES" in sql
        assert "IN" in sql

    def test_attach_hash_partition(self, dialect):
        """Test ATTACH PARTITION for HASH."""
        expr = PostgresAttachPartitionExpression(
            dialect=dialect,
            partition_name="orders_shard0",
            parent_table="orders",
            partition_type="HASH",
            partition_values={"modulus": 4, "remainder": 0},
        )
        sql, params = expr.to_sql()
        assert "FOR VALUES" in sql
        assert "MODULUS 4" in sql