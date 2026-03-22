# tests/rhosocial/activerecord_postgres_test/feature/backend/postgres/test_trigger.py
"""Unit tests for PostgreSQL trigger mixin.

Tests for:
- PostgresTriggerMixin feature detection
- Format CREATE TRIGGER statement
- Format DROP TRIGGER statement
"""
import pytest

from rhosocial.activerecord.backend.impl.postgres.dialect import PostgresDialect
from rhosocial.activerecord.backend.expression.statements import (
    CreateTriggerExpression,
    DropTriggerExpression,
    TriggerTiming,
    TriggerEvent,
    TriggerLevel,
)


class TestTriggerFeatureDetection:
    """Test trigger feature detection methods."""

    def test_supports_trigger_referencing_pg9(self):
        """PostgreSQL 9.5 does not support REFERENCING clause."""
        dialect = PostgresDialect((9, 5, 0))
        assert dialect.supports_trigger_referencing() is False

    def test_supports_trigger_referencing_pg10(self):
        """PostgreSQL 10 supports REFERENCING clause."""
        dialect = PostgresDialect((10, 0, 0))
        assert dialect.supports_trigger_referencing() is True

    def test_supports_trigger_when(self):
        """WHEN condition is supported in all versions."""
        dialect = PostgresDialect((9, 5, 0))
        assert dialect.supports_trigger_when() is True

    def test_supports_statement_trigger(self):
        """FOR EACH STATEMENT triggers are supported."""
        dialect = PostgresDialect((9, 5, 0))
        assert dialect.supports_statement_trigger() is True

    def test_supports_instead_of_trigger(self):
        """INSTEAD OF triggers are supported."""
        dialect = PostgresDialect((9, 5, 0))
        assert dialect.supports_instead_of_trigger() is True

    def test_supports_trigger_if_not_exists_pg9(self):
        """PostgreSQL 9.4 does not support IF NOT EXISTS."""
        dialect = PostgresDialect((9, 4, 0))
        assert dialect.supports_trigger_if_not_exists() is False

    def test_supports_trigger_if_not_exists_pg95(self):
        """PostgreSQL 9.5 supports IF NOT EXISTS."""
        dialect = PostgresDialect((9, 5, 0))
        assert dialect.supports_trigger_if_not_exists() is True


class TestFormatCreateTriggerStatement:
    """Test CREATE TRIGGER statement formatting."""

    def test_create_trigger_basic_before(self):
        """Test basic CREATE TRIGGER BEFORE statement."""
        dialect = PostgresDialect((14, 0, 0))
        expr = CreateTriggerExpression(
            dialect,
            trigger_name="update_timestamp",
            table_name="users",
            timing=TriggerTiming.BEFORE,
            events=[TriggerEvent.UPDATE],
            function_name="update_updated_at_column"
        )
        sql, params = dialect.format_create_trigger_statement(expr)
        assert 'CREATE TRIGGER "update_timestamp"' in sql
        assert "BEFORE UPDATE" in sql
        assert 'ON "users"' in sql
        assert "EXECUTE FUNCTION update_updated_at_column()" in sql

    def test_create_trigger_basic_after(self):
        """Test basic CREATE TRIGGER AFTER statement."""
        dialect = PostgresDialect((14, 0, 0))
        expr = CreateTriggerExpression(
            dialect,
            trigger_name="log_insert",
            table_name="users",
            timing=TriggerTiming.AFTER,
            events=[TriggerEvent.INSERT],
            function_name="log_user_insert"
        )
        sql, params = dialect.format_create_trigger_statement(expr)
        assert "AFTER INSERT" in sql

    def test_create_trigger_multiple_events(self):
        """Test CREATE TRIGGER with multiple events."""
        dialect = PostgresDialect((14, 0, 0))
        expr = CreateTriggerExpression(
            dialect,
            trigger_name="audit_trigger",
            table_name="users",
            timing=TriggerTiming.AFTER,
            events=[TriggerEvent.INSERT, TriggerEvent.UPDATE, TriggerEvent.DELETE],
            function_name="audit_function"
        )
        sql, params = dialect.format_create_trigger_statement(expr)
        assert "INSERT OR UPDATE OR DELETE" in sql

    def test_create_trigger_update_of(self):
        """Test CREATE TRIGGER with UPDATE OF columns."""
        dialect = PostgresDialect((14, 0, 0))
        expr = CreateTriggerExpression(
            dialect,
            trigger_name="check_status",
            table_name="orders",
            timing=TriggerTiming.BEFORE,
            events=[TriggerEvent.UPDATE],
            update_columns=["status", "updated_at"],
            function_name="validate_status_change"
        )
        sql, params = dialect.format_create_trigger_statement(expr)
        assert 'UPDATE OF "status", "updated_at"' in sql

    def test_create_trigger_with_level_row(self):
        """Test CREATE TRIGGER with FOR EACH ROW."""
        dialect = PostgresDialect((14, 0, 0))
        expr = CreateTriggerExpression(
            dialect,
            trigger_name="test_trigger",
            table_name="users",
            timing=TriggerTiming.BEFORE,
            events=[TriggerEvent.UPDATE],
            function_name="test_func",
            level=TriggerLevel.ROW
        )
        sql, params = dialect.format_create_trigger_statement(expr)
        assert "FOR EACH ROW" in sql

    def test_create_trigger_with_level_statement(self):
        """Test CREATE TRIGGER with FOR EACH STATEMENT."""
        dialect = PostgresDialect((14, 0, 0))
        expr = CreateTriggerExpression(
            dialect,
            trigger_name="test_trigger",
            table_name="users",
            timing=TriggerTiming.AFTER,
            events=[TriggerEvent.INSERT],
            function_name="test_func",
            level=TriggerLevel.STATEMENT
        )
        sql, params = dialect.format_create_trigger_statement(expr)
        assert "FOR EACH STATEMENT" in sql

    def test_create_trigger_if_not_exists(self):
        """Test CREATE TRIGGER IF NOT EXISTS."""
        dialect = PostgresDialect((14, 0, 0))
        expr = CreateTriggerExpression(
            dialect,
            trigger_name="test_trigger",
            table_name="users",
            timing=TriggerTiming.BEFORE,
            events=[TriggerEvent.UPDATE],
            function_name="test_func",
            if_not_exists=True
        )
        sql, params = dialect.format_create_trigger_statement(expr)
        assert 'CREATE TRIGGER IF NOT EXISTS "test_trigger"' in sql

    def test_create_trigger_with_referencing_pg10(self):
        """Test CREATE TRIGGER with REFERENCING clause on PG 10."""
        dialect = PostgresDialect((10, 0, 0))
        expr = CreateTriggerExpression(
            dialect,
            trigger_name="test_trigger",
            table_name="users",
            timing=TriggerTiming.AFTER,
            events=[TriggerEvent.UPDATE],
            function_name="test_func",
            referencing="OLD TABLE AS old NEW TABLE AS new"
        )
        sql, params = dialect.format_create_trigger_statement(expr)
        assert "OLD TABLE AS old NEW TABLE AS new" in sql

    def test_create_trigger_instead_of(self):
        """Test CREATE TRIGGER INSTEAD OF for views."""
        dialect = PostgresDialect((14, 0, 0))
        expr = CreateTriggerExpression(
            dialect,
            trigger_name="view_trigger",
            table_name="user_view",
            timing=TriggerTiming.INSTEAD_OF,
            events=[TriggerEvent.INSERT],
            function_name="handle_view_insert"
        )
        sql, params = dialect.format_create_trigger_statement(expr)
        assert "INSTEAD OF INSERT" in sql


class TestFormatDropTriggerStatement:
    """Test DROP TRIGGER statement formatting."""

    def test_drop_trigger_basic(self):
        """Test basic DROP TRIGGER statement."""
        dialect = PostgresDialect((14, 0, 0))
        expr = DropTriggerExpression(
            dialect,
            trigger_name="test_trigger"
        )
        sql, params = dialect.format_drop_trigger_statement(expr)
        assert 'DROP TRIGGER "test_trigger"' in sql

    def test_drop_trigger_with_table(self):
        """Test DROP TRIGGER with table name."""
        dialect = PostgresDialect((14, 0, 0))
        expr = DropTriggerExpression(
            dialect,
            trigger_name="test_trigger",
            table_name="users"
        )
        sql, params = dialect.format_drop_trigger_statement(expr)
        assert 'ON "users"' in sql

    def test_drop_trigger_if_exists(self):
        """Test DROP TRIGGER IF EXISTS."""
        dialect = PostgresDialect((14, 0, 0))
        expr = DropTriggerExpression(
            dialect,
            trigger_name="test_trigger",
            if_exists=True
        )
        sql, params = dialect.format_drop_trigger_statement(expr)
        assert "DROP TRIGGER IF EXISTS" in sql
