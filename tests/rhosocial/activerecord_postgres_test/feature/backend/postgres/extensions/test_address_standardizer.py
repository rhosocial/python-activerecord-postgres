# tests/rhosocial/activerecord_postgres_test/feature/backend/postgres/extensions/test_address_standardizer.py
"""Unit tests for PostgreSQL address_standardizer extension functions.

Tests for:
- parse_address
- standardize_address
"""

from rhosocial.activerecord.backend.impl.postgres.dialect import PostgresDialect
from rhosocial.activerecord.backend.expression import core
from rhosocial.activerecord.backend.impl.postgres.functions.address_standardizer import (
    standardize_address,
    parse_address,
)


class TestAddressStandardizerFunctions:
    """Tests for address_standardizer function factories."""

    def test_standardize_address(self):
        """standardize_address should return FunctionCall with 4 parameters."""
        dialect = PostgresDialect((14, 0, 0))
        result = standardize_address(
            dialect, 'pagc_lex', 'pagc_gaz', 'pagc_rules', '123 Main St'
        )
        assert isinstance(result, core.FunctionCall)
        sql, params = result.to_sql()
        assert "standardize_address" in sql.lower()
        # Should have 4 parameters (lextab, gaztab, rultab, address)
        assert sql.count("%s") == 4

    def test_parse_address(self):
        """parse_address should return FunctionCall with 1 parameter."""
        dialect = PostgresDialect((14, 0, 0))
        result = parse_address(dialect, '123 Main St')
        assert isinstance(result, core.FunctionCall)
        sql, params = result.to_sql()
        assert "parse_address" in sql.lower()
        # Should have 1 parameter (address)
        assert sql.count("%s") == 1
