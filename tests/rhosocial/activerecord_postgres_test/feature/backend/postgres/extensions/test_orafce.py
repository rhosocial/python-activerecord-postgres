# tests/rhosocial/activerecord_postgres_test/feature/backend/postgres/extensions/test_orafce.py
"""
Unit tests for PostgreSQL orafce extension functions.

Tests for:
- nvl
- add_months
- last_day
- instr
- substr
"""

from rhosocial.activerecord.backend.impl.postgres.dialect import PostgresDialect
from rhosocial.activerecord.backend.expression import core
from rhosocial.activerecord.backend.impl.postgres.functions.orafce import (
    nvl,
    add_months,
    last_day,
    instr,
    substr,
)


class TestPostgresOrafceFunctions:
    """Tests for orafce extension function factories."""

    def test_nvl(self):
        """nvl should return FunctionCall with NVL."""
        dialect = PostgresDialect((14, 0, 0))
        result = nvl(dialect, "expr1", "expr2")
        assert isinstance(result, core.FunctionCall)
        sql, params = result.to_sql()
        assert "nvl" in sql.lower()

    def test_add_months(self):
        """add_months should return FunctionCall with ADD_MONTHS."""
        dialect = PostgresDialect((14, 0, 0))
        result = add_months(dialect, "date", 3)
        assert isinstance(result, core.FunctionCall)
        sql, params = result.to_sql()
        assert "add_months" in sql.lower()

    def test_last_day(self):
        """last_day should return FunctionCall with LAST_DAY."""
        dialect = PostgresDialect((14, 0, 0))
        result = last_day(dialect, "date")
        assert isinstance(result, core.FunctionCall)
        sql, params = result.to_sql()
        assert "last_day" in sql.lower()

    def test_instr(self):
        """instr should return FunctionCall with INSTR."""
        dialect = PostgresDialect((14, 0, 0))
        result = instr(dialect, "string", "sub")
        assert isinstance(result, core.FunctionCall)
        sql, params = result.to_sql()
        assert "instr" in sql.lower()

    def test_substr(self):
        """substr should return FunctionCall with SUBSTR."""
        dialect = PostgresDialect((14, 0, 0))
        result = substr(dialect, "string", 1, 5)
        assert isinstance(result, core.FunctionCall)
        sql, params = result.to_sql()
        assert "substr" in sql.lower()
