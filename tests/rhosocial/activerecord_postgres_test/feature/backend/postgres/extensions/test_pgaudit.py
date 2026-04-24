# tests/rhosocial/activerecord_postgres_test/feature/backend/postgres/extensions/test_pgaudit.py
"""Unit tests for PostgreSQL pgaudit extension mixin."""

from rhosocial.activerecord.backend.impl.postgres.mixins.extensions.pgaudit import PostgresPgauditMixin


class TestPgauditMixin:
    """Test pgaudit extension mixin."""

    def setup_method(self):
        """Set up test fixture."""
        self.mixin = PostgresPgauditMixin()

    def test_format_pgaudit_set_role(self):
        """Test audit role assignment formatting."""
        result = self.mixin.format_pgaudit_set_role('audit_role')
        assert "pgaudit" in result

    def test_format_pgaudit_log_level(self):
        """Test audit log level configuration formatting."""
        result = self.mixin.format_pgaudit_log_level('log')
        assert "pgaudit.log" in result
