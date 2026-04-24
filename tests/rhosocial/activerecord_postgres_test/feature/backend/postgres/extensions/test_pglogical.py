# tests/rhosocial/activerecord_postgres_test/feature/backend/postgres/extensions/test_pglogical.py
"""Unit tests for PostgreSQL pglogical extension mixin."""

from rhosocial.activerecord.backend.impl.postgres.mixins.extensions.pglogical import PostgresPgLogicalMixin


class TestPgLogicalMixin:
    """Test pglogical extension mixin."""

    def setup_method(self):
        """Set up test fixture."""
        self.mixin = PostgresPgLogicalMixin()

    def test_format_pglogical_create_node(self):
        """Test pglogical node creation formatting."""
        result = self.mixin.format_pglogical_create_node('node1', 'host=db')
        assert "pglogical.create_node" in result

    def test_format_pglogical_create_publication(self):
        """Test pglogical publication creation formatting."""
        result = self.mixin.format_pglogical_create_publication('pub1')
        assert "pglogical.create_publication" in result

    def test_format_pglogical_create_subscription(self):
        """Test pglogical subscription creation formatting."""
        result = self.mixin.format_pglogical_create_subscription('sub1', 'host=pub')
        assert "pglogical.create_subscription" in result

    def test_format_pglogical_show_subscription_status(self):
        """Test pglogical subscription status formatting."""
        result = self.mixin.format_pglogical_show_subscription_status()
        assert "pglogical.show_subscription_status" in result

    def test_format_pglogical_alter_subscription_synchronize(self):
        """Test pglogical subscription synchronization formatting."""
        result = self.mixin.format_pglogical_alter_subscription_synchronize('sub1')
        assert "pglogical.alter_subscription_synchronize" in result
