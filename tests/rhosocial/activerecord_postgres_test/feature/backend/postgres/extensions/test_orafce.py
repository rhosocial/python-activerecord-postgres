# tests/rhosocial/activerecord_postgres_test/feature/backend/postgres/extensions/test_orafce.py
"""Unit tests for PostgreSQL orafce extension mixin."""

from rhosocial.activerecord.backend.impl.postgres.mixins.extensions.orafce import PostgresOrafceMixin


class TestOrafceMixin:
    """Test orafce extension mixin."""

    def setup_method(self):
        """Set up test fixture."""
        self.mixin = PostgresOrafceMixin()

    def test_format_nvl(self):
        """Test NVL function formatting."""
        result = self.mixin.format_nvl("'expr1'", "'expr2'")
        assert "NVL" in result

    def test_format_add_months(self):
        """Test ADD_MONTHS function formatting."""
        result = self.mixin.format_add_months("'date'", 3)
        assert "ADD_MONTHS" in result

    def test_format_last_day(self):
        """Test LAST_DAY function formatting."""
        result = self.mixin.format_last_day("'date'")
        assert "LAST_DAY" in result

    def test_format_instr(self):
        """Test INSTR function formatting."""
        result = self.mixin.format_instr("'string'", "'sub'")
        assert "INSTR" in result

    def test_format_substr(self):
        """Test SUBSTR function formatting."""
        result = self.mixin.format_substr("'string'", 1, 5)
        assert "SUBSTR" in result
