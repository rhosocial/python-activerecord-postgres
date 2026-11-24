# tests/rhosocial/activerecord_postgres_test/feature/backend/construct/dialect/test_ddl_improvements.py
"""Tests for PostgreSQL DDL improvements: capability gating, UnsupportedFeatureError."""
import pytest

from rhosocial.activerecord.backend.expression import (
    Column,
    QueryStatement,
    CreateViewStatement,
    DropViewStatement,
)
from rhosocial.activerecord.backend.expression.objects import Table, View
from rhosocial.activerecord.backend.impl.postgres.dialect import PostgresDialect
from rhosocial.activerecord.backend.dialect.exceptions import UnsupportedFeatureError


class TestViewCapabilityGating:
    """Tests for PostgreSQL VIEW DDL capability gating."""

    def test_create_or_replace_view_supported(self):
        """PostgreSQL supports CREATE OR REPLACE VIEW."""
        dialect = PostgresDialect()
        assert dialect.supports_create_or_replace_view() is True

    def test_drop_view_if_exists_supported(self):
        """PostgreSQL supports DROP VIEW IF EXISTS."""
        dialect = PostgresDialect()
        assert dialect.supports_if_exists_view() is True

    def test_drop_view_if_exists_renders(self):
        """The probe and the formatter agree: the clause is rendered."""
        dialect = PostgresDialect((18, 0, 0))
        expr = DropViewStatement(dialect, view=View(dialect, "v"), if_exists=True)
        sql, params = expr.to_sql()
        assert sql == 'DROP VIEW IF EXISTS "v"'
        assert params == ()

    def test_create_view_if_not_exists_refused(self):
        """PostgreSQL has no CREATE VIEW IF NOT EXISTS; the clause is refused."""
        dialect = PostgresDialect((18, 0, 0))
        query = QueryStatement(
            dialect,
            select=[Column(dialect, "id")],
            from_=Table(dialect, "t"),
        )
        expr = CreateViewStatement(
            dialect,
            view=View(dialect, "v"),
            query=query,
            if_not_exists=True,
        )
        with pytest.raises(UnsupportedFeatureError, match="CREATE VIEW IF NOT EXISTS"):
            expr.to_sql()

    def test_drop_view_cascade_supported(self):
        """PostgreSQL supports DROP VIEW CASCADE."""
        dialect = PostgresDialect()
        assert dialect.supports_cascade_view() is True

    def test_materialized_view_supported(self):
        """PostgreSQL supports materialized views (PG 9.3+).

        The probe is version aware, so the dialect must carry a version —
        same contract as supports_table_partitioning().
        """
        assert PostgresDialect(version=(9, 3, 0)).supports_materialized_view() is True
        assert PostgresDialect(version=(15, 0, 0)).supports_materialized_view() is True
        assert PostgresDialect(version=(9, 2, 0)).supports_materialized_view() is False


class TestColumnCapabilityGating:
    """Tests for PostgreSQL COLUMN DDL capability gating."""

    def test_foreign_key_on_delete_supported(self):
        """PostgreSQL supports FK ON DELETE."""
        dialect = PostgresDialect()
        assert dialect.supports_foreign_key_on_delete() is True

    def test_foreign_key_on_update_supported(self):
        """PostgreSQL supports FK ON UPDATE."""
        dialect = PostgresDialect()
        assert dialect.supports_foreign_key_on_update() is True


class TestSchemaCapabilityGating:
    """Tests for PostgreSQL SCHEMA DDL capability gating."""

    def test_create_schema_supported(self):
        """PostgreSQL supports CREATE SCHEMA."""
        dialect = PostgresDialect()
        assert dialect.supports_create_schema() is True

    def test_drop_schema_supported(self):
        """PostgreSQL supports DROP SCHEMA."""
        dialect = PostgresDialect()
        assert dialect.supports_drop_schema() is True

    def test_schema_if_not_exists_supported(self):
        """PostgreSQL supports CREATE SCHEMA IF NOT EXISTS."""
        dialect = PostgresDialect()
        assert dialect.supports_schema_if_not_exists() is True

    def test_schema_if_exists_supported(self):
        """PostgreSQL supports DROP SCHEMA IF EXISTS."""
        dialect = PostgresDialect()
        assert dialect.supports_schema_if_exists() is True

    def test_schema_cascade_supported(self):
        """PostgreSQL supports DROP SCHEMA CASCADE."""
        dialect = PostgresDialect()
        assert dialect.supports_schema_cascade() is True
