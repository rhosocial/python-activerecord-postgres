# tests/rhosocial/activerecord_postgres_test/feature/backend/postgres/test_ddl_expressions.py
"""Tests for PostgreSQL DDL expression classes.

This module tests the expression-based format methods for DDL operations,
including materialized view refresh, comment, and partition expressions.
"""
from datetime import date, datetime
from decimal import Decimal

import pytest

from rhosocial.activerecord.backend.expression import Column
from rhosocial.activerecord.backend.expression.statements import (
    ColumnDefinition,
    CreateTableExpression,
    PartitionClause,
    PartitionStrategy,
)
from rhosocial.activerecord.backend.impl.postgres.dialect import PostgresDialect
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
                ColumnDefinition("id", "BIGINT"),
                ColumnDefinition("created_at", "TIMESTAMP NOT NULL"),
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
                ColumnDefinition("id", "BIGINT"),
                ColumnDefinition("status", "TEXT NOT NULL"),
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
            columns=[ColumnDefinition("tenant_id", "BIGINT NOT NULL")],
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
            columns=[ColumnDefinition("tenant_id", "BIGINT NOT NULL")],
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
            columns=[ColumnDefinition("created_at", "TIMESTAMP NOT NULL")],
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
        assert params == ("orders", None, None)

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
        assert params == ("public.events", "created_at", "1 month", "range", 2)

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
