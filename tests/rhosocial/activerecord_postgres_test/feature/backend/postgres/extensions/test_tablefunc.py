# tests/rhosocial/activerecord_postgres_test/feature/backend/postgres/extensions/test_tablefunc.py
"""
Unit tests for PostgreSQL tablefunc extension functions.

Tests for:
- crosstab
- connectby
- normal_rand
"""

from rhosocial.activerecord.backend.impl.postgres.dialect import PostgresDialect
from rhosocial.activerecord.backend.expression import core
from rhosocial.activerecord.backend.impl.postgres.functions.tablefunc import (
    crosstab,
    connectby,
    normal_rand,
)


class TestTablefuncMixin:
    """Test tablefunc extension functions."""

    def test_crosstab(self):
        """crosstab should return FunctionCall with crosstab."""
        dialect = PostgresDialect((14, 0, 0))
        result = crosstab(dialect, "SELECT row_name, cat, value FROM table")
        assert isinstance(result, core.FunctionCall)
        sql, params = result.to_sql()
        assert "crosstab" in sql.lower()

    def test_crosstab_with_categories(self):
        """crosstab with categories_sql should return FunctionCall with crosstab."""
        dialect = PostgresDialect((14, 0, 0))
        result = crosstab(
            dialect,
            "SELECT row_name, cat, value FROM table",
            "SELECT DISTINCT cat FROM table ORDER BY 1"
        )
        assert isinstance(result, core.FunctionCall)
        sql, params = result.to_sql()
        assert "crosstab" in sql.lower()

    def test_connectby(self):
        """connectby should return FunctionCall with connectby."""
        dialect = PostgresDialect((14, 0, 0))
        result = connectby(dialect, 'tree', 'id', 'parent_id', '1')
        assert isinstance(result, core.FunctionCall)
        sql, params = result.to_sql()
        assert "connectby" in sql.lower()

    def test_connectby_with_max_depth(self):
        """connectby with max_depth should return FunctionCall with connectby."""
        dialect = PostgresDialect((14, 0, 0))
        result = connectby(dialect, 'tree', 'id', 'parent_id', '1', max_depth=3)
        assert isinstance(result, core.FunctionCall)
        sql, params = result.to_sql()
        assert "connectby" in sql.lower()

    def test_normal_rand(self):
        """normal_rand should return FunctionCall with normal_rand."""
        dialect = PostgresDialect((14, 0, 0))
        result = normal_rand(dialect, 100, 0, 1)
        assert isinstance(result, core.FunctionCall)
        sql, params = result.to_sql()
        assert "normal_rand" in sql.lower()
