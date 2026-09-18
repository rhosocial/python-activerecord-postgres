# tests/rhosocial/activerecord_postgres_test/feature/backend/test_postgres_ddl_improvements.py
"""Tests for PostgreSQL DDL improvements: capability gating, UnsupportedFeatureError."""
import pytest
from unittest.mock import patch, PropertyMock

from rhosocial.activerecord.backend.expression import (
    Column,
    TableExpression,
    QueryExpression,
    CreateViewExpression,
    DropViewExpression,
)
from rhosocial.activerecord.backend.impl.postgres.dialect import PostgresDialect
from rhosocial.activerecord.backend.dialect.exceptions import UnsupportedFeatureError


class TestPostgresViewCapabilityGating:
    """Tests for PostgreSQL VIEW DDL capability gating."""

    def test_create_or_replace_view_supported(self):
        """PostgreSQL supports CREATE OR REPLACE VIEW."""
        dialect = PostgresDialect()
        assert dialect.supports_create_or_replace_view() is True

    def test_drop_view_if_exists_supported(self):
        """PostgreSQL supports DROP VIEW IF EXISTS."""
        dialect = PostgresDialect()
        assert dialect.supports_if_exists_view() is True

    def test_drop_view_cascade_supported(self):
        """PostgreSQL supports DROP VIEW CASCADE."""
        dialect = PostgresDialect()
        assert dialect.supports_cascade_view() is True

    def test_materialized_view_supported(self):
        """PostgreSQL supports materialized views."""
        dialect = PostgresDialect()
        assert dialect.supports_materialized_view() is True


class TestPostgresColumnCapabilityGating:
    """Tests for PostgreSQL COLUMN DDL capability gating."""

    def test_foreign_key_on_delete_supported(self):
        """PostgreSQL supports FK ON DELETE."""
        dialect = PostgresDialect()
        assert dialect.supports_foreign_key_on_delete() is True

    def test_foreign_key_on_update_supported(self):
        """PostgreSQL supports FK ON UPDATE."""
        dialect = PostgresDialect()
        assert dialect.supports_foreign_key_on_update() is True


class TestPostgresSchemaCapabilityGating:
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
