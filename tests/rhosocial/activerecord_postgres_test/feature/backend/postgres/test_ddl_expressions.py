# tests/rhosocial/activerecord_postgres_test/feature/backend/postgres/test_ddl_expressions.py
"""Tests for PostgreSQL DDL expression classes.

This module tests the expression-based format methods for DDL operations,
including materialized view refresh, comment, and partition expressions.
"""
from datetime import date, datetime
from decimal import Decimal

import pytest

from rhosocial.activerecord.backend.dialect.exceptions import UnsupportedFeatureError
from rhosocial.activerecord.backend.expression import (
    Column,
    FunctionCall,
    Literal,
    RawSQLExpression,
)
from rhosocial.activerecord.backend.expression.statements import (
    ColumnDefinition,
    CreateTableExpression,
    PartitionClause,
    PartitionStrategy,
)
from rhosocial.activerecord.backend.impl.dummy import DummyDialect
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
        assert sql == 'REFRESH MATERIALIZED VIEW "monthly_sales_summary"'
        assert params == ()

    def test_refresh_with_schema(self, dialect):
        """Test REFRESH MATERIALIZED VIEW with schema."""
        expr = PostgresRefreshMaterializedViewExpression(
            dialect=dialect,
            name="monthly_sales_summary",
            schema="analytics",
        )
        sql, params = expr.to_sql()
        assert sql == 'REFRESH MATERIALIZED VIEW "analytics"."monthly_sales_summary"'
        assert params == ()

    def test_refresh_concurrently_pg13(self):
        """Test CONCURRENTLY refresh requires PG 9.4+."""
        dialect_pg93 = PostgresDialect(version=(9, 3, 0))
        expr = PostgresRefreshMaterializedViewExpression(
            dialect=dialect_pg93,
            name="monthly_sales_summary",
            concurrently=True,
        )

        with pytest.raises(UnsupportedFeatureError) as exc:
            expr.to_sql()
        assert "CONCURRENTLY" in str(exc.value)
        assert "9.4" in str(exc.value)

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
        """Test COMMENT ON COLUMN (dotted target quoted segment-by-segment)."""
        expr = PostgresCommentExpression(
            dialect=dialect,
            object_type="COLUMN",
            object_name="users.email",
            comment="User email address",
        )
        sql, params = expr.to_sql()
        assert "COMMENT ON COLUMN" in sql
        assert '"users"."email"' in sql
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
        """Test comment on object with schema (dotted target quoted per segment)."""
        expr = PostgresCommentExpression(
            dialect=dialect,
            object_type="TABLE",
            object_name="public.users",
            comment="Public users table",
            schema="public",
        )
        sql, params = expr.to_sql()
        assert '"public"."users"' in sql
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
                ColumnDefinition(dialect, "id", BigIntType(dialect=dialect)),
                ColumnDefinition(
                    dialect,
                    "created_at", TimestampType(dialect=dialect),
                    constraints=[ColumnConstraint(dialect, ColumnConstraintType.NOT_NULL)],
                ),
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
                ColumnDefinition(dialect, "id", BigIntType(dialect=dialect)),
                ColumnDefinition(dialect, "status", TextType(dialect=dialect), constraints=[ColumnConstraint(dialect, ColumnConstraintType.NOT_NULL)]),
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
            columns=[
                ColumnDefinition(
                    dialect,
                    "tenant_id", BigIntType(dialect=dialect),
                    constraints=[ColumnConstraint(dialect, ColumnConstraintType.NOT_NULL)],
                )
            ],
            partition=PartitionClause(
                dialect=dialect,
                method=PartitionStrategy.HASH,
                keys=[Column(dialect, "tenant_id")],
            ),
        )

        with pytest.raises(UnsupportedFeatureError, match="HASH partitioning requires PostgreSQL 11"):
            expr.to_sql()

    def test_create_hash_partitioned_parent_table_pg11(self):
        """Test HASH parent table partitioning on PostgreSQL 11+."""
        dialect = PostgresDialect(version=(11, 0, 0))
        expr = CreateTableExpression(
            dialect=dialect,
            table="events",
            columns=[
                ColumnDefinition(
                    dialect,
                    "tenant_id", BigIntType(dialect=dialect),
                    constraints=[ColumnConstraint(dialect, ColumnConstraintType.NOT_NULL)],
                )
            ],
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
            columns=[
                ColumnDefinition(
                    dialect,
                    "created_at", TimestampType(dialect=dialect),
                    constraints=[ColumnConstraint(dialect, ColumnConstraintType.NOT_NULL)],
                )
            ],
            partition=PartitionClause(
                dialect=dialect,
                method=PartitionStrategy.RANGE,
                keys=[Column(dialect, "created_at")],
            ),
        )

        with pytest.raises(UnsupportedFeatureError, match="Declarative table partitioning requires PostgreSQL 10"):
            expr.to_sql()

    def test_key_partitioning_is_rejected(self, dialect):
        """Generic PartitionClause rejects non-core KEY partitioning."""
        with pytest.raises(TypeError, match="PartitionStrategy"):
            PartitionClause(
                dialect=dialect,
                method="KEY",
                keys=[Column(dialect, "id")],
            )

    def test_postgres_partition_clause_is_generic_subclass(self, dialect):
        """PostgresPartitionClause derives from the generic PartitionClause."""
        from rhosocial.activerecord.backend.impl.postgres.expression.ddl import (
            PostgresPartitionClause,
        )

        clause = PostgresPartitionClause(
            dialect=dialect,
            method=PartitionStrategy.RANGE,
            keys=[Column(dialect, "created_at")],
        )
        assert isinstance(clause, PartitionClause)
        sql, params = clause.to_sql()
        assert sql == ' PARTITION BY RANGE ("created_at")'
        assert params == ()

    def test_postgres_partition_clause_in_create_table(self, dialect):
        """CreateTableExpression accepts the PostgreSQL-owned clause subclass."""
        from rhosocial.activerecord.backend.impl.postgres.expression.ddl import (
            PostgresPartitionClause,
        )

        expr = CreateTableExpression(
            dialect=dialect,
            table="events",
            columns=[
                ColumnDefinition(
                    dialect,
                    "created_at", TimestampType(dialect=dialect),
                    constraints=[ColumnConstraint(dialect, ColumnConstraintType.NOT_NULL)],
                )
            ],
            partition=PostgresPartitionClause(
                dialect=dialect,
                method=PartitionStrategy.RANGE,
                keys=[Column(dialect, "created_at")],
            ),
        )
        sql, _ = expr.to_sql()
        assert "PARTITION BY RANGE" in sql

    def test_partition_clause_rejects_cross_dialect_key(self, dialect):
        clause = PartitionClause(
            dialect,
            PartitionStrategy.RANGE,
            [Column(DummyDialect(), "created_at")],
        )
        with pytest.raises(ValueError, match="same dialect|PostgreSQL dialect"):
            clause.to_sql()

    def test_create_partition_rejects_cross_dialect_clause(self, dialect):
        clause = PartitionClause(
            DummyDialect(),
            PartitionStrategy.HASH,
            [Column(DummyDialect(), "bucket")],
        )
        with pytest.raises(ValueError, match="same dialect"):
            PostgresCreatePartitionExpression(
                dialect,
                "events_p1",
                "events",
                "RANGE",
                {"from": "2026-01-01", "to": "2027-01-01"},
                partition_clause=clause,
            )

    def test_multi_column_range_partitioned_parent_table(self, dialect):
        """Test creating a RANGE-partitioned parent table with multiple partition keys."""
        expr = CreateTableExpression(
            dialect=dialect,
            table="tenanted_events",
            columns=[
                ColumnDefinition(
                    dialect,
                    "tenant_id", BigIntType(dialect=dialect),
                    constraints=[ColumnConstraint(dialect, ColumnConstraintType.NOT_NULL)],
                ),
                ColumnDefinition(
                    dialect,
                    "created_at", TimestampType(dialect=dialect),
                    constraints=[ColumnConstraint(dialect, ColumnConstraintType.NOT_NULL)],
                ),
                ColumnDefinition(dialect, "payload", TextType(dialect=dialect)),
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

    def test_multi_column_list_partitioned_parent_table_rejected(self, dialect):
        clause = PartitionClause(
            dialect=dialect,
            method=PartitionStrategy.LIST,
            keys=[Column(dialect, "region"), Column(dialect, "status")],
        )

        with pytest.raises(ValueError, match="exactly one key expression"):
            clause.to_sql()

    def test_child_multi_column_list_partition_clause_rejected(self, dialect):
        clause = PartitionClause(
            dialect=dialect,
            method=PartitionStrategy.LIST,
            keys=[Column(dialect, "region"), Column(dialect, "status")],
        )
        expr = PostgresCreatePartitionExpression(
            dialect,
            "events_p1",
            "events",
            "RANGE",
            {"from": "2026-01-01", "to": "2027-01-01"},
            partition_clause=clause,
        )

        with pytest.raises(ValueError, match="exactly one key expression"):
            expr.to_sql()


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

    @pytest.mark.parametrize("value", ["MINVALUE", "MAXVALUE", "DEFAULT"])
    def test_list_partition_value_quotes_special_range_strings(self, dialect, value):
        expr = PartitionValue(dialect, value, partition_type="LIST")
        assert expr.to_sql() == (f"'{value}'", ())

    def test_partition_value_renders_function_call_with_inline_literals(self, dialect):
        expr = PartitionValue(
            dialect,
            FunctionCall(dialect, "DATE_TRUNC", Literal(dialect, "month")),
        )
        assert expr.to_sql() == ("DATE_TRUNC('month')", ())

    def test_partition_value_rejects_parameterized_raw_sql(self, dialect):
        expr = PartitionValue(
            dialect,
            RawSQLExpression(dialect, "DATE %s", ("2024-01-01",)),
        )
        with pytest.raises(ValueError, match="must not contain bind parameters"):
            expr.to_sql()

    def test_partition_value_rejects_cross_dialect_expression(self, dialect):
        expression = FunctionCall(DummyDialect(), "DATE", niladic=True)
        with pytest.raises(ValueError, match="same dialect"):
            PartitionValue(dialect, expression)

    @pytest.mark.parametrize("expression_factory", [
        lambda dialect: Column(dialect, "created_at"),
        lambda dialect: FunctionCall(dialect, "DATE", Column(dialect, "created_at")),
    ])
    def test_partition_value_rejects_unsafe_expression(self, dialect, expression_factory):
        with pytest.raises(TypeError, match="FunctionCall, Literal, or RawSQLExpression"):
            PartitionValue(dialect, expression_factory(dialect))

    def test_default_partition_value_requires_pg11(self):
        dialect = PostgresDialect(version=(10, 0, 0))
        with pytest.raises(UnsupportedFeatureError, match="DEFAULT partitions require PostgreSQL 11"):
            PartitionValue(dialect, "DEFAULT").to_sql()

    def test_partition_value_requires_pg10(self):
        dialect = PostgresDialect(version=(9, 6, 0))
        with pytest.raises(UnsupportedFeatureError, match="requires PostgreSQL 10"):
            PartitionValue(dialect, 1).to_sql()


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
        with pytest.raises(UnsupportedFeatureError, match="HASH partitioning requires PostgreSQL 11"):
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

    def test_create_multi_column_range_partition(self, dialect):
        expr = PostgresCreatePartitionExpression(
            dialect,
            "events_p1",
            "events",
            "RANGE",
            {
                "from": [1, date(2026, 1, 1)],
                "to": [2, date(2027, 1, 1)],
            },
        )
        sql, params = expr.to_sql()
        assert "FROM (1, '2026-01-01') TO (2, '2027-01-01')" in sql
        assert params == ()

    def test_create_multi_column_list_partition_rejected(self, dialect):
        expr = PostgresCreatePartitionExpression(
            dialect,
            "events_p1",
            "events",
            "LIST",
            {"values": [["us", 1], ["eu", 2]]},
        )

        with pytest.raises(ValueError, match="nested rows"):
            expr.to_sql()

    def test_list_special_range_strings_remain_quoted(self, dialect):
        expr = PostgresCreatePartitionExpression(
            dialect,
            "events_special",
            "events",
            "LIST",
            {"values": ["MINVALUE", "MAXVALUE", "DEFAULT"]},
        )
        sql, _ = expr.to_sql()
        assert "IN ('MINVALUE', 'MAXVALUE', 'DEFAULT')" in sql

    def test_child_partition_clause_order_and_schemas(self, dialect):
        clause = PartitionClause(
            dialect,
            PartitionStrategy.HASH,
            [Column(dialect, "bucket")],
        )
        expr = PostgresCreatePartitionExpression(
            dialect,
            "events_p1",
            "events",
            "RANGE",
            {"from": "2026-01-01", "to": "2027-01-01"},
            schema="child_schema",
            parent_schema="parent_schema",
            tablespace="fastspace",
            partition_clause=clause,
        )
        sql, params = expr.to_sql()
        assert sql == (
            'CREATE TABLE "child_schema"."events_p1" '
            'PARTITION OF "parent_schema"."events" '
            "FOR VALUES FROM ('2026-01-01') TO ('2027-01-01') "
            'PARTITION BY HASH ("bucket") TABLESPACE "fastspace"'
        )
        assert sql.index("FOR VALUES") < sql.index("PARTITION BY") < sql.index("TABLESPACE")
        assert params == ()

    def test_create_partition_rejects_nested_parameters(self, dialect):
        clause = PartitionClause(
            dialect,
            PartitionStrategy.HASH,
            [RawSQLExpression(dialect, "bucket %s", (1,))],
        )
        expr = PostgresCreatePartitionExpression(
            dialect,
            "events_p1",
            "events",
            "RANGE",
            {
                "from": RawSQLExpression(dialect, "DATE %s", ("2026-01-01",)),
                "to": "MAXVALUE",
            },
            partition_clause=clause,
        )
        with pytest.raises(ValueError, match="must not contain bind parameters"):
            expr.to_sql()

    def test_create_partition_literals_are_inlined(self, dialect):
        clause = PartitionClause(
            dialect,
            PartitionStrategy.HASH,
            [FunctionCall(dialect, "LOWER", Literal(dialect, "bucket"))],
        )
        expr = PostgresCreatePartitionExpression(
            dialect,
            "events_p1",
            "events",
            "RANGE",
            {
                "from": Literal(dialect, "2026-01-01"),
                "to": Literal(dialect, "2027-01-01"),
            },
            partition_clause=clause,
        )

        sql, params = expr.to_sql()

        assert params == ()
        assert "%s" not in sql
        assert "LOWER('bucket')" in sql
        assert "FROM ('2026-01-01')" in sql

    def test_create_list_partition_literals_are_inlined(self, dialect):
        expr = PostgresCreatePartitionExpression(
            dialect,
            "events_p1",
            "events",
            "LIST",
            {"values": [FunctionCall(dialect, "LOWER", Literal(dialect, "ACTIVE"))]},
        )

        sql, params = expr.to_sql()

        assert params == ()
        assert "%s" not in sql
        assert "IN (LOWER('ACTIVE'))" in sql

    def test_create_default_partition_requires_pg11(self):
        dialect = PostgresDialect(version=(10, 0, 0))
        expr = PostgresCreatePartitionExpression(
            dialect,
            "events_default",
            "events",
            "RANGE",
            {"default": True},
        )
        with pytest.raises(UnsupportedFeatureError, match="DEFAULT partitions require PostgreSQL 11"):
            expr.to_sql()

    @pytest.mark.parametrize("modulus", [True, "4", 4.0])
    def test_create_hash_rejects_non_integer_modulus(self, dialect, modulus):
        expr = PostgresCreatePartitionExpression(
            dialect,
            "events_p1",
            "events",
            "HASH",
            {"modulus": modulus, "remainder": 0},
        )
        with pytest.raises(TypeError, match="modulus must be an int"):
            expr.to_sql()

    @pytest.mark.parametrize("modulus", [0, -1])
    def test_create_hash_rejects_non_positive_modulus(self, dialect, modulus):
        expr = PostgresCreatePartitionExpression(
            dialect,
            "events_p1",
            "events",
            "HASH",
            {"modulus": modulus, "remainder": 0},
        )
        with pytest.raises(ValueError, match="modulus must be a positive integer"):
            expr.to_sql()

    @pytest.mark.parametrize("remainder", [True, "0", 0.0])
    def test_create_hash_rejects_non_integer_remainder(self, dialect, remainder):
        expr = PostgresCreatePartitionExpression(
            dialect,
            "events_p1",
            "events",
            "HASH",
            {"modulus": 4, "remainder": remainder},
        )
        with pytest.raises(TypeError, match="remainder must be an int"):
            expr.to_sql()

    @pytest.mark.parametrize("remainder", [-1, 4])
    def test_create_hash_rejects_out_of_range_remainder(self, dialect, remainder):
        expr = PostgresCreatePartitionExpression(
            dialect,
            "events_p1",
            "events",
            "HASH",
            {"modulus": 4, "remainder": remainder},
        )
        with pytest.raises(ValueError, match=r"0 <= remainder < modulus"):
            expr.to_sql()

    def test_create_partition_requires_pg10(self):
        dialect = PostgresDialect(version=(9, 6, 0))
        expr = PostgresCreatePartitionExpression(
            dialect,
            "events_p1",
            "events",
            "RANGE",
            {"from": "2026-01-01", "to": "2027-01-01"},
        )
        with pytest.raises(UnsupportedFeatureError, match="requires PostgreSQL 10"):
            expr.to_sql()


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

    def test_detach_partition_requires_pg10(self):
        dialect = PostgresDialect(version=(9, 6, 0))
        expr = PostgresDetachPartitionExpression(
            dialect,
            "events_p1",
            "events",
        )
        with pytest.raises(UnsupportedFeatureError, match="requires PostgreSQL 10"):
            expr.to_sql()


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

    def test_attach_multi_column_list_partition_rejected(self, dialect):
        expr = PostgresAttachPartitionExpression(
            dialect,
            "events_p1",
            "events",
            "LIST",
            {"values": [["us", 1], ["eu", 2]]},
        )

        with pytest.raises(ValueError, match="nested rows"):
            expr.to_sql()

    def test_attach_partition_literals_are_inlined(self, dialect):
        expr = PostgresAttachPartitionExpression(
            dialect,
            "events_p1",
            "events",
            "RANGE",
            {
                "from": Literal(dialect, "2026-01-01"),
                "to": FunctionCall(dialect, "DATE_TRUNC", Literal(dialect, "year")),
            },
        )

        sql, params = expr.to_sql()

        assert params == ()
        assert "%s" not in sql
        assert "FROM ('2026-01-01')" in sql
        assert "TO (DATE_TRUNC('year'))" in sql

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
        with pytest.raises(UnsupportedFeatureError, match="HASH partitioning requires PostgreSQL 11"):
            expr.to_sql()

    def test_attach_concurrently_pg13(self):
        """ATTACH CONCURRENTLY is rejected on PG 13."""
        dialect_pg13 = PostgresDialect(version=(13, 0, 0))
        expr = PostgresAttachPartitionExpression(
            dialect=dialect_pg13,
            partition_name="orders_2024_q1",
            parent_table="orders",
            partition_type="RANGE",
            partition_values={"from": "2024-01-01", "to": "2024-04-01"},
            concurrently=True,
        )
        with pytest.raises(UnsupportedFeatureError, match="ATTACH PARTITION CONCURRENTLY"):
            expr.to_sql()

    def test_attach_concurrently_pg14(self, dialect):
        """ATTACH CONCURRENTLY is rejected on PG 14+."""
        expr = PostgresAttachPartitionExpression(
            dialect=dialect,
            partition_name="orders_2024_q1",
            parent_table="orders",
            partition_type="RANGE",
            partition_values={"from": "2024-01-01", "to": "2024-04-01"},
            concurrently=True,
        )
        with pytest.raises(UnsupportedFeatureError, match="ATTACH PARTITION CONCURRENTLY"):
            expr.to_sql()

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

    def test_attach_multi_column_range_partition(self, dialect):
        expr = PostgresAttachPartitionExpression(
            dialect,
            "events_p1",
            "events",
            "RANGE",
            {"from": [1, "2026-01-01"], "to": [2, "2027-01-01"]},
        )
        sql, params = expr.to_sql()
        assert "FROM (1, '2026-01-01') TO (2, '2027-01-01')" in sql
        assert params == ()

    def test_attach_default_partition_requires_pg11(self):
        dialect = PostgresDialect(version=(10, 0, 0))
        expr = PostgresAttachPartitionExpression(
            dialect,
            "events_default",
            "events",
            "LIST",
            {"default": True},
        )
        with pytest.raises(UnsupportedFeatureError, match="DEFAULT partitions require PostgreSQL 11"):
            expr.to_sql()

    @pytest.mark.parametrize("modulus,remainder", [
        ("4); DROP TABLE events; --", 0),
        (4, "0); DROP TABLE events; --"),
        (4, 4),
    ])
    def test_attach_hash_rejects_invalid_integers(self, dialect, modulus, remainder):
        expr = PostgresAttachPartitionExpression(
            dialect,
            "events_p1",
            "events",
            "HASH",
            {"modulus": modulus, "remainder": remainder},
        )
        with pytest.raises((TypeError, ValueError)):
            expr.to_sql()

    def test_attach_partition_requires_pg10(self):
        dialect = PostgresDialect(version=(9, 6, 0))
        expr = PostgresAttachPartitionExpression(
            dialect,
            "events_p1",
            "events",
            "RANGE",
            {"from": "2026-01-01", "to": "2027-01-01"},
        )
        with pytest.raises(UnsupportedFeatureError, match="requires PostgreSQL 10"):
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
        assert "pg_partitioned_table" in sql
        assert "parent.relkind = 'p'" in sql
        assert "partitioned_parent.partrelid IS NOT NULL" in sql
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
        assert "pg_partitioned_table" in sql
        assert "parent.relkind = 'p'" in sql
        assert "NULL::text AS name" in sql
        assert params == ("orders",)

    def test_metadata_query_requires_pg10(self):
        """Metadata introspection follows PostgreSQL declarative partition support."""
        dialect = PostgresDialect(version=(9, 6, 0))
        expr = PostgresPartitionMetadataExpression(
            dialect=dialect,
            parent_table="orders",
        )
        with pytest.raises(UnsupportedFeatureError, match="partition metadata introspection"):
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

