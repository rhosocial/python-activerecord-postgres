# tests/rhosocial/activerecord_postgres_test/feature/backend/postgres/extensions/test_pglogical.py
"""
Unit tests for PostgreSQL pglogical extension functions.

Tests for:
- pglogical_create_node
- pglogical_create_publication
- pglogical_create_subscription
- pglogical_show_subscription_status
- pglogical_alter_subscription_synchronize
"""

from rhosocial.activerecord.backend.impl.postgres.dialect import PostgresDialect
from rhosocial.activerecord.backend.expression import core
from rhosocial.activerecord.backend.impl.postgres.functions.pglogical import (
    pglogical_create_node,
    pglogical_create_publication,
    pglogical_create_subscription,
    pglogical_show_subscription_status,
    pglogical_alter_subscription_synchronize,
)


class TestPostgresPgLogicalFunctions:
    """Tests for pglogical extension function factories."""

    def test_pglogical_create_node(self):
        """pglogical_create_node should return FunctionCall with pglogical.create_node."""
        dialect = PostgresDialect((14, 0, 0))
        result = pglogical_create_node(dialect, "node1", "host=db")
        assert isinstance(result, core.FunctionCall)
        sql, params = result.to_sql()
        assert "pglogical.create_node" in sql.lower()

    def test_pglogical_create_publication(self):
        """pglogical_create_publication should return FunctionCall with pglogical.create_publication."""
        dialect = PostgresDialect((14, 0, 0))
        result = pglogical_create_publication(dialect, "pub1", "{users, orders}")
        assert isinstance(result, core.FunctionCall)
        sql, params = result.to_sql()
        assert "pglogical.create_publication" in sql.lower()

    def test_pglogical_create_subscription(self):
        """pglogical_create_subscription should return FunctionCall with pglogical.create_subscription."""
        dialect = PostgresDialect((14, 0, 0))
        result = pglogical_create_subscription(dialect, "sub1", "host=pub", "{pub1}")
        assert isinstance(result, core.FunctionCall)
        sql, params = result.to_sql()
        assert "pglogical.create_subscription" in sql.lower()

    def test_pglogical_show_subscription_status(self):
        """pglogical_show_subscription_status should return FunctionCall with pglogical.show_subscription_status."""
        dialect = PostgresDialect((14, 0, 0))
        result = pglogical_show_subscription_status(dialect)
        assert isinstance(result, core.FunctionCall)
        sql, params = result.to_sql()
        assert "pglogical.show_subscription_status" in sql.lower()

    def test_pglogical_alter_subscription_synchronize(self):
        """pglogical_alter_subscription_synchronize should return FunctionCall with pglogical.alter_subscription_synchronize."""
        dialect = PostgresDialect((14, 0, 0))
        result = pglogical_alter_subscription_synchronize(dialect, "sub1")
        assert isinstance(result, core.FunctionCall)
        sql, params = result.to_sql()
        assert "pglogical.alter_subscription_synchronize" in sql.lower()
