# tests/rhosocial/activerecord_postgres_test/feature/backend/postgres/test_ddl_expressions.py
"""Tests for PostgreSQL DDL expression classes.

This module tests the expression-based format methods for DDL operations,
including materialized view refresh, comment, and partition expressions.
"""
from datetime import date, datetime
from decimal import Decimal

import pytest

from rhosocial.activerecord.backend.expression import Column, Literal
from rhosocial.activerecord.backend.expression.statements import (
    AlterColumn,
    ColumnDefinition,
    CreateTableExpression,
    PartitionClause,
    PartitionStrategy,
)
from rhosocial.activerecord.backend.impl.postgres.dialect import PostgresDialect
from rhosocial.activerecord.backend.dialect.exceptions import (
    UnsupportedFeatureError,
)
from rhosocial.activerecord.backend.impl.postgres.expression.ddl import (
    PartitionValue,
    PostgresRefreshMaterializedViewExpression,
    PostgresCommentExpression,
    PostgresCreatePartitionExpression,
    PostgresDetachPartitionExpression,
    PostgresAttachPartitionExpression,
    PostgresPartitionMetadataExpression,
    PostgresPgPartmanCreateParentExpression,
    PostgresPgPartmanDeleteConfigExpression,
    PostgresPgPartmanRunMaintenanceExpression,
    PostgresPgPartmanUpdateConfigExpression,
    PostgresVacuumExpression,  # noqa: F401
    PostgresAnalyzeExpression,  # noqa: F401
    LoggingMode,
    RlsConfigurationMode,
    AlterDomainActionType,
    PostgresAlterTableRlsExpression,
    PostgresForceRlsExpression,
    PostgresAlterTableSettingsExpression,
    PostgresClusterExpression,
    PostgresCreateDomainExpression,
    PostgresAlterDomainExpression,
    PostgresDropDomainExpression,
    PostgresCreateCollationExpression,
    PostgresDropCollationExpression,
    PostgresCreateForeignTableExpression,
    PostgresDropForeignTableExpression,
    PostgresCreateFunctionExpression,
    PostgresDropFunctionExpression,
    PostgresCreateAggregateExpression,
    PostgresDropAggregateExpression,
    PostgresCreatePublicationExpression,
    PostgresDropPublicationExpression,
    PostgresCreateSubscriptionExpression,
    PostgresDropSubscriptionExpression,
)
from rhosocial.activerecord.backend.expression.types import (
    BigIntType, TextType, TimestampType,
)
from rhosocial.activerecord.backend.expression.statements import (
    ColumnConstraint, ColumnConstraintType,
)
from rhosocial.activerecord.backend.impl.postgres.mixins.dml.extended_statistics import (
    PostgresExtendedStatisticsMixin,  # noqa: F401
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


class TestPostgresPartitionedTableCreation:
    """Test PostgreSQL CREATE TABLE ... PARTITION BY support."""

    @pytest.fixture
    def dialect(self):
        return PostgresDialect(version=(14, 0, 0))

    def test_create_range_partitioned_parent_table(self, dialect):
        """Test creating a RANGE-partitioned parent table."""
        expr = CreateTableExpression(
            dialect=dialect,
            table="events",
            columns=[
                ColumnDefinition("id", BigIntType()),
                ColumnDefinition("created_at", TimestampType(), constraints=[ColumnConstraint(ColumnConstraintType.NOT_NULL)]),
            ],
            partition=PartitionClause(
                dialect=dialect,
                method=PartitionStrategy.RANGE,
                keys=[Column(dialect, "created_at")],
            ),
        )
        sql, params = expr.to_sql()

        assert sql.startswith('CREATE TABLE "events"')
        assert 'PARTITION BY RANGE ("created_at")' in sql
        assert params == ()

    def test_create_list_partitioned_parent_table(self, dialect):
        """Test creating a LIST-partitioned parent table."""
        expr = CreateTableExpression(
            dialect=dialect,
            table="events",
            columns=[
                ColumnDefinition("id", BigIntType()),
                ColumnDefinition("status", TextType(), constraints=[ColumnConstraint(ColumnConstraintType.NOT_NULL)]),
            ],
            partition=PartitionClause(
                dialect=dialect,
                method=PartitionStrategy.LIST,
                keys=[Column(dialect, "status")],
            ),
        )
        sql, params = expr.to_sql()

        assert 'PARTITION BY LIST ("status")' in sql
        assert params == ()

    def test_create_hash_partitioned_parent_table_pg10(self):
        """HASH parent table partitioning requires PostgreSQL 11+."""
        dialect = PostgresDialect(version=(10, 0, 0))
        expr = CreateTableExpression(
            dialect=dialect,
            table="events",
            columns=[ColumnDefinition("tenant_id", BigIntType(), constraints=[ColumnConstraint(ColumnConstraintType.NOT_NULL)])],
            partition=PartitionClause(
                dialect=dialect,
                method=PartitionStrategy.HASH,
                keys=[Column(dialect, "tenant_id")],
            ),
        )

        with pytest.raises(Exception, match="HASH partitioning requires PostgreSQL 11"):
            expr.to_sql()

    def test_create_hash_partitioned_parent_table_pg11(self):
        """Test HASH parent table partitioning on PostgreSQL 11+."""
        dialect = PostgresDialect(version=(11, 0, 0))
        expr = CreateTableExpression(
            dialect=dialect,
            table="events",
            columns=[ColumnDefinition("tenant_id", BigIntType(), constraints=[ColumnConstraint(ColumnConstraintType.NOT_NULL)])],
            partition=PartitionClause(
                dialect=dialect,
                method=PartitionStrategy.HASH,
                keys=[Column(dialect, "tenant_id")],
            ),
        )
        sql, params = expr.to_sql()

        assert 'PARTITION BY HASH ("tenant_id")' in sql
        assert params == ()

    def test_create_partitioned_parent_table_pg9(self):
        """Declarative parent table partitioning requires PostgreSQL 10+."""
        dialect = PostgresDialect(version=(9, 6, 0))
        expr = CreateTableExpression(
            dialect=dialect,
            table="events",
            columns=[ColumnDefinition("created_at", TimestampType(), constraints=[ColumnConstraint(ColumnConstraintType.NOT_NULL)])],
            partition=PartitionClause(
                dialect=dialect,
                method=PartitionStrategy.RANGE,
                keys=[Column(dialect, "created_at")],
            ),
        )

        with pytest.raises(Exception, match="Declarative table partitioning requires PostgreSQL 10"):
            expr.to_sql()

    def test_key_partitioning_is_rejected(self, dialect):
        """Generic PartitionClause rejects non-core KEY partitioning."""
        with pytest.raises(TypeError, match="PartitionStrategy"):
            PartitionClause(
                dialect=dialect,
                method="KEY",
                keys=[Column(dialect, "id")],
            )

    def test_multi_column_range_partitioned_parent_table(self, dialect):
        """Test creating a RANGE-partitioned parent table with multiple partition keys."""
        expr = CreateTableExpression(
            dialect=dialect,
            table="tenanted_events",
            columns=[
                ColumnDefinition("tenant_id", BigIntType(), constraints=[ColumnConstraint(ColumnConstraintType.NOT_NULL)]),
                ColumnDefinition("created_at", TimestampType(), constraints=[ColumnConstraint(ColumnConstraintType.NOT_NULL)]),
                ColumnDefinition("payload", TextType()),
            ],
            partition=PartitionClause(
                dialect=dialect,
                method=PartitionStrategy.RANGE,
                keys=[Column(dialect, "tenant_id"), Column(dialect, "created_at")],
            ),
        )
        sql, params = expr.to_sql()

        assert sql.startswith('CREATE TABLE "tenanted_events"')
        assert 'PARTITION BY RANGE ("tenant_id", "created_at")' in sql
        assert params == ()


class TestPostgresPartitionValue:
    """Test safe PostgreSQL partition bound value formatting."""

    @pytest.fixture
    def dialect(self):
        return PostgresDialect(version=(14, 0, 0))

    @pytest.mark.parametrize(
        "value,expected",
        [
            (None, "NULL"),
            ("MAXVALUE", "MAXVALUE"),
            ("minvalue", "MINVALUE"),
            ("default", "DEFAULT"),
            ("2024-01-01", "'2024-01-01'"),
            ("O'Reilly", "'O''Reilly'"),
            (42, "42"),
            (3.5, "3.5"),
            (Decimal("12.30"), "12.30"),
            (date(2024, 1, 1), "'2024-01-01'"),
            (datetime(2024, 1, 1, 12, 30, 45), "'2024-01-01 12:30:45'"),
        ],
    )
    def test_partition_value_formats_whitelisted_values(self, dialect, value, expected):
        expr = PartitionValue(dialect=dialect, value=value)
        assert expr.to_sql() == (expected, ())

    @pytest.mark.parametrize("value", [True, object(), ["x"]])
    def test_partition_value_rejects_invalid_types(self, dialect, value):
        with pytest.raises(TypeError):
            PartitionValue(dialect=dialect, value=value)

    @pytest.mark.parametrize("value", [float("inf"), float("nan"), Decimal("Infinity"), Decimal("NaN")])
    def test_partition_value_rejects_nonfinite_numbers(self, dialect, value):
        with pytest.raises(ValueError):
            PartitionValue(dialect=dialect, value=value)


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

    @pytest.mark.parametrize("partition_type", ["RANGE", "LIST"])
    def test_create_default_partition(self, dialect, partition_type):
        """Test CREATE TABLE ... PARTITION OF for DEFAULT catch-all partitions."""
        expr = PostgresCreatePartitionExpression(
            dialect=dialect,
            partition_name="orders_default",
            parent_table="orders",
            partition_type=partition_type,
            partition_values={"default": True},
        )
        sql, params = expr.to_sql()
        assert "PARTITION OF" in sql
        assert sql.endswith(" DEFAULT")
        assert "FOR VALUES" not in sql
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
        with pytest.raises(Exception, match="HASH partitioning requires PostgreSQL 11"):
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

    @pytest.mark.parametrize(
        "partition_type,partition_values,expected_clause",
        [
            ("RANGE", {"from": date(2024, 1, 1), "to": date(2025, 1, 1)},
             "FROM ('2024-01-01') TO ('2025-01-01')"),
            ("RANGE", {"from": datetime(2024, 1, 1, 12, 30), "to": datetime(2025, 1, 1, 0, 0)},
             "FROM ('2024-01-01 12:30:00') TO ('2025-01-01 00:00:00')"),
            ("RANGE", {"from": Decimal("1.5"), "to": Decimal("10.5")},
             "FROM (1.5) TO (10.5)"),
            ("LIST", {"values": ["active", "pending"]},
             "IN ('active', 'pending')"),
            ("HASH", {"modulus": 6, "remainder": 3},
             "WITH (MODULUS 6, REMAINDER 3)"),
        ],
    )
    def test_create_partition_value_types(self, dialect, partition_type, partition_values, expected_clause):
        """Test PARTITION OF with various value types and expressions (PG 12+)."""
        expr = PostgresCreatePartitionExpression(
            dialect=dialect,
            partition_name="test_partition",
            parent_table="test_parent",
            partition_type=partition_type,
            partition_values=partition_values,
        )
        sql, params = expr.to_sql()
        assert expected_clause in sql
        assert params == ()


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
        assert "DETACH PARTITION" in sql
        assert "CONCURRENTLY" in sql

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

    def test_detach_concurrently_finalize_pg14(self, dialect):
        """Test DETACH CONCURRENTLY FINALIZE on PG 14+."""
        expr = PostgresDetachPartitionExpression(
            dialect=dialect,
            partition_name="orders_2023",
            parent_table="orders",
            concurrently=True,
            finalize=True,
        )
        sql, params = expr.to_sql()
        assert "DETACH PARTITION" in sql
        assert "CONCURRENTLY" in sql
        assert "FINALIZE" in sql
        assert sql.index("CONCURRENTLY") < sql.index("FINALIZE")

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

    def test_attach_range_partition_requires_bounds(self, dialect):
        """RANGE attach requires explicit from/to bounds."""
        expr = PostgresAttachPartitionExpression(
            dialect=dialect,
            partition_name="orders_2024_q1",
            parent_table="orders",
            partition_type="RANGE",
            partition_values={"from": "2024-01-01"},
        )
        with pytest.raises(ValueError, match="RANGE partition requires"):
            expr.to_sql()

    def test_attach_list_partition_requires_values(self, dialect):
        """LIST attach requires non-empty values."""
        expr = PostgresAttachPartitionExpression(
            dialect=dialect,
            partition_name="orders_empty",
            parent_table="orders",
            partition_type="LIST",
            partition_values={"values": []},
        )
        with pytest.raises(ValueError, match="LIST partition requires"):
            expr.to_sql()

    def test_attach_hash_partition_requires_pg11(self):
        """HASH attach requires PostgreSQL 11+."""
        dialect = PostgresDialect(version=(10, 0, 0))
        expr = PostgresAttachPartitionExpression(
            dialect=dialect,
            partition_name="orders_shard0",
            parent_table="orders",
            partition_type="HASH",
            partition_values={"modulus": 4, "remainder": 0},
        )
        with pytest.raises(ValueError, match="HASH partitioning requires PostgreSQL 11"):
            expr.to_sql()

    def test_attach_concurrently_pg13(self):
        """ATTACH CONCURRENTLY requires PG 14+."""
        dialect_pg13 = PostgresDialect(version=(13, 0, 0))
        expr = PostgresAttachPartitionExpression(
            dialect=dialect_pg13,
            partition_name="orders_2024_q1",
            parent_table="orders",
            partition_type="RANGE",
            partition_values={"from": "2024-01-01", "to": "2024-04-01"},
            concurrently=True,
        )
        with pytest.raises(ValueError, match="ATTACH CONCURRENTLY requires PostgreSQL 14"):
            expr.to_sql()

    def test_attach_concurrently_pg14(self, dialect):
        """ATTACH CONCURRENTLY with PG 14+ includes CONCURRENTLY keyword."""
        expr = PostgresAttachPartitionExpression(
            dialect=dialect,
            partition_name="orders_2024_q1",
            parent_table="orders",
            partition_type="RANGE",
            partition_values={"from": "2024-01-01", "to": "2024-04-01"},
            concurrently=True,
        )
        sql, params = expr.to_sql()
        assert "CONCURRENTLY" in sql
        assert "FOR VALUES" in sql
        assert params == ()

    def test_attach_default_range_partition(self, dialect):
        """ATTACH DEFAULT partition for RANGE should emit DEFAULT keyword."""
        expr = PostgresAttachPartitionExpression(
            dialect=dialect,
            partition_name="orders_default",
            parent_table="orders",
            partition_type="RANGE",
            partition_values={"default": True},
        )
        sql, params = expr.to_sql()
        assert "DEFAULT" in sql
        assert "FOR VALUES" not in sql
        assert params == ()

    def test_attach_default_list_partition(self, dialect):
        """ATTACH DEFAULT partition for LIST should emit DEFAULT keyword."""
        expr = PostgresAttachPartitionExpression(
            dialect=dialect,
            partition_name="orders_default",
            parent_table="orders",
            partition_type="LIST",
            partition_values={"default": True},
        )
        sql, params = expr.to_sql()
        assert "DEFAULT" in sql
        assert "FOR VALUES" not in sql
        assert params == ()


class TestPostgresPartitionMetadataExpression:
    """Test PostgreSQL partition metadata query expression."""

    def test_metadata_query_for_parent(self):
        """Metadata query uses pg_catalog and parameter binding."""
        dialect = PostgresDialect(version=(14, 0, 0))
        expr = PostgresPartitionMetadataExpression(
            dialect=dialect,
            parent_table="orders",
        )
        sql, params = expr.to_sql()
        assert "pg_get_partkeydef" in sql
        assert "pg_inherits" in sql
        assert params == ("orders",)

    def test_metadata_query_with_schema(self):
        """Metadata query includes schema filter when schema is specified."""
        dialect = PostgresDialect(version=(14, 0, 0))
        expr = PostgresPartitionMetadataExpression(
            dialect=dialect,
            parent_table="orders",
            schema="public",
        )
        sql, params = expr.to_sql()
        assert "pg_get_partkeydef" in sql
        assert "parent_ns.nspname = %s" in sql
        assert params == ("orders", "public")

    def test_metadata_query_without_partitions(self):
        """Metadata query omits partition details when include_partitions=False."""
        dialect = PostgresDialect(version=(14, 0, 0))
        expr = PostgresPartitionMetadataExpression(
            dialect=dialect,
            parent_table="orders",
            include_partitions=False,
        )
        sql, params = expr.to_sql()
        assert "pg_inherits" not in sql
        assert "NULL::text AS name" in sql
        assert params == ("orders",)

    def test_metadata_query_requires_pg10(self):
        """Metadata introspection follows PostgreSQL declarative partition support."""
        dialect = PostgresDialect(version=(9, 6, 0))
        expr = PostgresPartitionMetadataExpression(
            dialect=dialect,
            parent_table="orders",
        )
        with pytest.raises(Exception, match="partition metadata introspection"):
            expr.to_sql()


class TestPostgresPgPartmanExpressions:
    """Test pg_partman maintenance expression SQL generation."""

    @pytest.fixture
    def dialect(self):
        return PostgresDialect(version=(14, 0, 0))

    def test_create_parent_expression(self, dialect):
        """pg_partman create_parent uses named arguments and bound parameters."""
        expr = PostgresPgPartmanCreateParentExpression(
            dialect=dialect,
            parent_table="public.events",
            control="created_at",
            interval="1 month",
            partition_type="range",
            premake=2,
            schema="partman",
        )
        sql, params = expr.to_sql()
        assert '"partman"."create_parent"' in sql
        assert "p_parent_table" in sql
        assert "p_premake" in sql
        assert params == ("public.events", "created_at", "1 month", "range", 2)

    def test_create_parent_with_optional_params(self, dialect):
        """pg_partman create_parent with all optional parameters."""
        expr = PostgresPgPartmanCreateParentExpression(
            dialect=dialect,
            parent_table="public.events",
            control="created_at",
            interval="1 month",
            partition_type="native",
            premake=6,
            start_partition="2026-01-01",
            primary_key="id",
            default_table=True,
            constraint_cols=["tenant_id"],
            template_table="public.events_template",
            epoch="seconds",
            jobmon=False,
            schema="partman",
        )
        sql, params = expr.to_sql()
        assert "p_premake" in sql
        assert "p_start_partition" in sql
        assert "p_primary_key" in sql
        assert "p_default_table" in sql
        assert "p_constraint_cols" in sql
        assert "p_template_table" in sql
        assert "p_epoch" in sql
        assert "p_jobmon" in sql
        assert params == (
            "public.events", "created_at", "1 month", "native", 6,
            "2026-01-01", "id", True, ["tenant_id"], "public.events_template",
            "seconds", False,
        )

    def test_update_config_expression(self, dialect):
        """pg_partman update_config updates only requested options."""
        expr = PostgresPgPartmanUpdateConfigExpression(
            dialect=dialect,
            parent_table="public.events",
            automatic_maintenance="on",
            infinite_time_partitions=True,
            retention="3 months",
            retention_keep_table=False,
            retention_keep_index=True,
            schema="partman",
        )
        sql, params = expr.to_sql()
        assert 'UPDATE "partman"."part_config"' in sql
        assert "automatic_maintenance" in sql
        assert "infinite_time_partitions" in sql
        assert "retention_keep_index" in sql
        assert params == ("on", True, "3 months", False, True, "public.events")

    def test_update_config_requires_at_least_one_option(self, dialect):
        """pg_partman update_config rejects empty updates."""
        expr = PostgresPgPartmanUpdateConfigExpression(
            dialect=dialect,
            parent_table="public.events",
            schema="partman",
        )
        with pytest.raises(ValueError, match="At least one pg_partman config option"):
            expr.to_sql()

    def test_delete_config_expression(self, dialect):
        """pg_partman delete_config targets one parent table."""
        expr = PostgresPgPartmanDeleteConfigExpression(
            dialect=dialect,
            parent_table="public.events",
            schema="partman",
        )
        sql, params = expr.to_sql()
        assert sql == 'DELETE FROM "partman"."part_config" WHERE parent_table = %s'
        assert params == ("public.events",)

    def test_run_maintenance_scoped_expression(self, dialect):
        """pg_partman scoped maintenance targets one parent table."""
        expr = PostgresPgPartmanRunMaintenanceExpression(
            dialect=dialect,
            parent_table="public.events",
            schema="partman",
        )
        sql, params = expr.to_sql()
        assert sql == 'SELECT "partman"."run_maintenance"(%s::text)'
        assert params == ("public.events",)

    def test_run_maintenance_global_expression(self, dialect):
        """pg_partman global maintenance omits the parent table argument."""
        expr = PostgresPgPartmanRunMaintenanceExpression(
            dialect=dialect,
            schema="partman",
        )
        sql, params = expr.to_sql()
        assert sql == 'SELECT "partman"."run_maintenance"()'
        assert params == ()


class TestPostgresRlsConfigExpression:
    """Test RLS enable/disable/always/force DDL expressions."""

    @pytest.fixture
    def dialect(self):
        return PostgresDialect(version=(14, 0, 0))

    def test_enable(self, dialect):
        sql, _ = PostgresAlterTableRlsExpression(
            dialect, "orders", RlsConfigurationMode.ENABLE
        ).to_sql()
        assert sql == 'ALTER TABLE "orders" ENABLE ROW LEVEL SECURITY'

    def test_disable(self, dialect):
        sql, _ = PostgresAlterTableRlsExpression(
            dialect, "orders", RlsConfigurationMode.DISABLE
        ).to_sql()
        assert sql == 'ALTER TABLE "orders" DISABLE ROW LEVEL SECURITY'

    def test_enable_always(self, dialect):
        sql, _ = PostgresAlterTableRlsExpression(
            dialect, "orders", RlsConfigurationMode.ENABLE, always=True
        ).to_sql()
        assert sql == 'ALTER TABLE "orders" ENABLE ALWAYS ROW LEVEL SECURITY'

    def test_schema_qualify(self, dialect):
        sql, _ = PostgresAlterTableRlsExpression(
            dialect, "orders", RlsConfigurationMode.ENABLE, schema="app"
        ).to_sql()
        assert sql == 'ALTER TABLE "app"."orders" ENABLE ROW LEVEL SECURITY'

    def test_force(self, dialect):
        sql, _ = PostgresForceRlsExpression(dialect, "orders").to_sql()
        assert sql == 'ALTER TABLE "orders" FORCE ROW LEVEL SECURITY'

    def test_no_force(self, dialect):
        sql, _ = PostgresForceRlsExpression(dialect, "orders", force=False).to_sql()
        assert sql == 'ALTER TABLE "orders" NO FORCE ROW LEVEL SECURITY'

    def test_version_gate_94(self):
        d = PostgresDialect(version=(9, 4, 0))
        expr = PostgresAlterTableRlsExpression(d, "orders", RlsConfigurationMode.ENABLE)
        with pytest.raises(UnsupportedFeatureError):
            expr.to_sql()


class TestPostgresAlterTableSettingsExpression:
    """SET LOGGED / UNLOGGED / ACCESS METHOD DDL."""

    @pytest.fixture
    def dialect(self):
        return PostgresDialect(version=(15, 0, 0))

    def test_unlogged(self, dialect):
        sql, _ = PostgresAlterTableSettingsExpression(
            dialect, "t", mode=LoggingMode.UNLOGGED
        ).to_sql()
        assert sql == 'ALTER TABLE "t" SET UNLOGGED'

    def test_logged_schema(self, dialect):
        sql, _ = PostgresAlterTableSettingsExpression(
            dialect, "t", schema="s", mode=LoggingMode.LOGGED
        ).to_sql()
        assert sql == 'ALTER TABLE "s"."t" SET LOGGED'

    def test_access_method(self, dialect):
        sql, _ = PostgresAlterTableSettingsExpression(
            dialect, "t", access_method="heap"
        ).to_sql()
        assert sql == 'ALTER TABLE "t" SET ACCESS METHOD "heap"'

    def test_access_method_requires_15(self):
        d = PostgresDialect(version=(14, 0, 0))
        expr = PostgresAlterTableSettingsExpression(d, "t", access_method="heap")
        with pytest.raises(UnsupportedFeatureError):
            expr.to_sql()

    def test_no_clause_raises(self, dialect):
        expr = PostgresAlterTableSettingsExpression(dialect, "t")
        with pytest.raises(ValueError):
            expr.to_sql()


class TestPostgresClusterExpression:
    """CLUSTER DDL."""

    @pytest.fixture
    def dialect(self):
        return PostgresDialect(version=(14, 0, 0))

    def test_full(self, dialect):
        sql, _ = PostgresClusterExpression(
            dialect, "orders", schema="public",
            using_index="orders_pkey", verbose=True,
        ).to_sql()
        assert sql == 'CLUSTER VERBOSE "public"."orders" USING "orders_pkey"'

    def test_bare(self, dialect):
        sql, _ = PostgresClusterExpression(dialect, verbose=True).to_sql()
        assert sql == "CLUSTER VERBOSE"

    def test_version_gate_95(self):
        d = PostgresDialect(version=(9, 5, 0))
        with pytest.raises(UnsupportedFeatureError):
            PostgresClusterExpression(d, "t").to_sql()


class TestPostgresDomainExpression:
    """CREATE / ALTER / DROP DOMAIN."""

    @pytest.fixture
    def dialect(self):
        return PostgresDialect(version=(14, 0, 0))

    def test_create(self, dialect):
        sql, _ = PostgresCreateDomainExpression(
            dialect, "posint", "NUMERIC",
            default="0", constraints=["CHECK (VALUE > 0)"],
        ).to_sql()
        assert sql == "CREATE DOMAIN \"posint\" AS NUMERIC DEFAULT 0 CHECK (VALUE > 0)"

    def test_alter_set_default(self, dialect):
        sql, _ = PostgresAlterDomainExpression(
            dialect, "posint", AlterDomainActionType.SET_DEFAULT, new_value="0"
        ).to_sql()
        assert sql == 'ALTER DOMAIN "posint" SET DEFAULT 0'

    def test_alter_rename(self, dialect):
        sql, _ = PostgresAlterDomainExpression(
            dialect, "posint", AlterDomainActionType.RENAME_TO, new_name="posint2"
        ).to_sql()
        assert sql == 'ALTER DOMAIN "posint" RENAME TO "posint2"'

    def test_drop(self, dialect):
        sql, _ = PostgresDropDomainExpression(
            dialect, "posint", if_exists=True, cascade=True
        ).to_sql()
        assert sql == 'DROP DOMAIN IF EXISTS "posint" CASCADE'


class TestPostgresCollationDDLExpression:
    """CREATE / DROP COLLATION DDL."""

    @pytest.fixture
    def dialect(self):
        return PostgresDialect(version=(14, 0, 0))

    def test_create(self, dialect):
        sql, _ = PostgresCreateCollationExpression(
            dialect, "enloc", locale="en_US.UTF-8", provider="libc"
        ).to_sql()
        assert sql == 'CREATE COLLATION "enloc" (LOCALE = en_US.UTF-8, PROVIDER = libc)'

    def test_create_if_not_exists(self, dialect):
        sql, _ = PostgresCreateCollationExpression(
            dialect, "myloc", if_not_exists=True, lc_collate="en_US",
        ).to_sql()
        assert sql == 'CREATE COLLATION IF NOT EXISTS "myloc" (LC_COLLATE = en_US)'

    def test_drop(self, dialect):
        sql, _ = PostgresDropCollationExpression(
            dialect, "enloc", if_exists=True
        ).to_sql()
        assert sql == 'DROP COLLATION IF EXISTS "enloc"'


class TestPostgresForeignTableDDLExpression:
    """CREATE / DROP FOREIGN TABLE."""

    @pytest.fixture
    def dialect(self):
        return PostgresDialect(version=(14, 0, 0))

    def test_create(self, dialect):
        sql, _ = PostgresCreateForeignTableExpression(
            dialect, "ft", "srv", columns=["a integer", "b text"],
            options=["host 'h'"],
        ).to_sql()
        assert sql == (
            'CREATE FOREIGN TABLE "ft" (a integer, b text) '
            'SERVER "srv" OPTIONS (host \'h\')'
        )

    def test_drop(self, dialect):
        sql, _ = PostgresDropForeignTableExpression(
            dialect, "ft", if_exists=True, cascade=True
        ).to_sql()
        assert sql == 'DROP FOREIGN TABLE IF EXISTS "ft" CASCADE'


class TestPostgresRoutineDDLExpression:
    """CREATE / DROP FUNCTION and AGGREGATE."""

    @pytest.fixture
    def dialect(self):
        return PostgresDialect(version=(14, 0, 0))

    def test_create_function(self, dialect):
        sql, _ = PostgresCreateFunctionExpression(
            dialect, "add", "integer", "int 1;",
            args=["a integer"], strict=True, security="DEFINER",
        ).to_sql()
        assert sql == (
            'CREATE FUNCTION "add"(a integer) RETURNS integer '
            'STRICT SECURITY DEFINER LANGUAGE plpgsql AS $$ int 1; $$'
        )

    def test_drop_function(self, dialect):
        sql, _ = PostgresDropFunctionExpression(
            dialect, "add", args=["integer"], cascade=True
        ).to_sql()
        assert sql == 'DROP FUNCTION "add" (integer) CASCADE'

    def test_create_aggregate(self, dialect):
        sql, _ = PostgresCreateAggregateExpression(
            dialect, "mysum", "sum", "integer", initcond="0"
        ).to_sql()
        assert sql == 'CREATE AGGREGATE "mysum" (SFUNC=sum, STYPE=integer, INITCOND=0)'

    def test_drop_aggregate(self, dialect):
        sql, _ = PostgresDropAggregateExpression(
            dialect, "mysum", "integer", if_exists=True
        ).to_sql()
        assert sql == 'DROP AGGREGATE IF EXISTS "mysum" (integer)'


class TestPostgresPublicationExpression:
    """CREATE / DROP PUBLICATION and SUBSCRIPTION."""

    @pytest.fixture
    def dialect(self):
        return PostgresDialect(version=(14, 0, 0))

    def test_create_publication_tables(self, dialect):
        sql, _ = PostgresCreatePublicationExpression(
            dialect, "pub1", tables=["orders", "users"]
        ).to_sql()
        assert sql == 'CREATE PUBLICATION "pub1" FOR TABLE "orders", "users"'

    def test_create_publication_all_tables(self, dialect):
        sql, _ = PostgresCreatePublicationExpression(
            dialect, "pub_all", all_tables=True,
            options=["publish='insert'"],
        ).to_sql()
        assert sql == (
            'CREATE PUBLICATION "pub_all" FOR ALL TABLES WITH (publish=\'insert\')'
        )

    def test_drop_publication(self, dialect):
        sql, _ = PostgresDropPublicationExpression(
            dialect, "pub1", if_exists=True
        ).to_sql()
        assert sql == 'DROP PUBLICATION IF EXISTS "pub1"'

    def test_create_subscription(self, dialect):
        sql, _ = PostgresCreateSubscriptionExpression(
            dialect, "sub1", "host=db port=5432", ["pub1"]
        ).to_sql()
        assert sql == (
            'CREATE SUBSCRIPTION "sub1" CONNECTION \'host=db port=5432\' '
            'PUBLICATION "pub1"'
        )

    def test_drop_subscription(self, dialect):
        sql, _ = PostgresDropSubscriptionExpression(dialect, "sub1", cascade=True).to_sql()
        assert sql == 'DROP SUBSCRIPTION "sub1" CASCADE'


class TestPostgresAlterColumnUsingExpression:
    """ALTER COLUMN ... SET DATA TYPE ... USING."""

    @pytest.fixture
    def dialect(self):
        return PostgresDialect(version=(14, 0, 0))

    def test_using_clause(self, dialect):
        action = AlterColumn(
            dialect,
            "price",
            "SET DATA TYPE",
            new_value="NUMERIC(10,2)",
            dialect_options={"using": Column(dialect, "price") + Literal(dialect, 1)},
        )
        sql, serialized = dialect.format_alter_column_action(action)
        assert 'ALTER COLUMN "price" SET DATA TYPE NUMERIC(10,2)' in sql
        assert 'USING ("price" + %s)' in sql
        assert serialized == (1,)

    def test_using_rejected_for_non_set_data_type(self, dialect):
        """USING is only valid on SET DATA TYPE."""
        action = AlterColumn(
            dialect,
            "price",
            "SET DEFAULT",
            new_value="0",
            dialect_options={"using": Column(dialect, "price")},
        )
        with pytest.raises(ValueError, match="USING"):
            dialect.format_alter_column_action(action)

    def test_without_using_unaffected(self, dialect):
        """No USING -> output matches the standard form."""
        action = AlterColumn(
            dialect,
            "price",
            "SET DATA TYPE",
            new_value="NUMERIC(10,2)",
        )
        sql, _ = dialect.format_alter_column_action(action)
        assert sql == 'ALTER COLUMN "price" SET DATA TYPE NUMERIC(10,2)'
