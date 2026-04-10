# tests/rhosocial/activerecord_postgres_test/feature/backend/postgres/test_postgres_range_constructors.py
"""
Tests for PostgreSQL Range Type Constructors.

Functions: int4range, int8range, numrange, tsrange, tstzrange, daterange
"""

from rhosocial.activerecord.backend.impl.postgres.dialect import PostgresDialect
from rhosocial.activerecord.backend.impl.postgres.functions.range import (
    int4range,
    int8range,
    numrange,
    tsrange,
    tstzrange,
    daterange,
)


class TestRangeConstructors:
    """Tests for PostgreSQL range type constructors."""

    def test_int4range_with_bounds(self, postgres_dialect: PostgresDialect):
        """Test int4range() with lower and upper bounds."""
        result = int4range(postgres_dialect, 1, 10)
        assert result == "int4range(1, 10)"

    def test_int4range_with_bounds_and_inclusive(self, postgres_dialect: PostgresDialect):
        """Test int4range() with inclusive bounds."""
        result = int4range(postgres_dialect, 1, 10, "[)")
        assert result == "int4range(1, 10, '[)')"

    def test_int4range_empty(self, postgres_dialect: PostgresDialect):
        """Test int4range() with no arguments."""
        result = int4range(postgres_dialect)
        assert result == "int4range()"

    def test_int8range_with_bounds(self, postgres_dialect: PostgresDialect):
        """Test int8range() with lower and upper bounds."""
        result = int8range(postgres_dialect, 1, 1000000)
        assert result == "int8range(1, 1000000)"

    def test_int8range_with_bounds_and_exclusive(self, postgres_dialect: PostgresDialect):
        """Test int8range() with exclusive bounds."""
        result = int8range(postgres_dialect, 1, 100, "()")
        assert result == "int8range(1, 100, '()')"

    def test_numrange_with_bounds(self, postgres_dialect: PostgresDialect):
        """Test numrange() with lower and upper bounds."""
        result = numrange(postgres_dialect, 1.5, 10.5)
        assert result == "numrange(1.5, 10.5)"

    def test_numrange_with_bounds_and_inclusive(self, postgres_dialect: PostgresDialect):
        """Test numrange() with inclusive bounds."""
        result = numrange(postgres_dialect, 1.5, 10.5, "[]")
        assert result == "numrange(1.5, 10.5, '[]')"

    def test_tsrange_with_bounds(self, postgres_dialect: PostgresDialect):
        """Test tsrange() with lower and upper bounds."""
        result = tsrange(postgres_dialect, "'2024-01-01 00:00:00'", "'2024-12-31 23:59:59'")
        assert result == "tsrange('2024-01-01 00:00:00', '2024-12-31 23:59:59')"

    def test_tstzrange_with_bounds(self, postgres_dialect: PostgresDialect):
        """Test tstzrange() with lower and upper bounds."""
        result = tstzrange(postgres_dialect, "'2024-01-01 00:00:00+00'", "'2024-12-31 23:59:59+00'")
        assert result == "tstzrange('2024-01-01 00:00:00+00', '2024-12-31 23:59:59+00')"

    def test_daterange_with_bounds(self, postgres_dialect: PostgresDialect):
        """Test daterange() with lower and upper bounds."""
        result = daterange(postgres_dialect, "'2024-01-01'", "'2024-12-31'")
        assert result == "daterange('2024-01-01', '2024-12-31')"