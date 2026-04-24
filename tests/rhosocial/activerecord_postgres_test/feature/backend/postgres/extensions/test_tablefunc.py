# tests/rhosocial/activerecord_postgres_test/feature/backend/postgres/extensions/test_tablefunc.py
"""Unit tests for PostgreSQL tablefunc extension mixin."""

from rhosocial.activerecord.backend.impl.postgres.dialect import PostgresDialect


class TestTablefuncMixin:
    """Test tablefunc extension mixin."""

    def test_format_crosstab_function(self):
        """Test crosstab function formatting."""
        dialect = PostgresDialect((14, 0, 0))
        result = dialect.format_crosstab_function("SELECT row_name, cat, value FROM table")
        assert "crosstab" in result

    def test_format_crosstab_function_with_categories(self):
        """Test crosstab function formatting with categories."""
        dialect = PostgresDialect((14, 0, 0))
        result = dialect.format_crosstab_function(
            "SELECT row_name, cat, value FROM table",
            "SELECT DISTINCT cat FROM table ORDER BY 1"
        )
        assert "crosstab" in result

    def test_format_connectby_function(self):
        """Test connectby function formatting."""
        dialect = PostgresDialect((14, 0, 0))
        result = dialect.format_connectby_function('tree', 'id', 'parent_id', '1')
        assert "connectby" in result

    def test_format_connectby_function_with_max_depth(self):
        """Test connectby function formatting with max depth."""
        dialect = PostgresDialect((14, 0, 0))
        result = dialect.format_connectby_function('tree', 'id', 'parent_id', '1', max_depth=3)
        assert "connectby" in result

    def test_format_normal_rand_function(self):
        """Test normal_rand function formatting."""
        dialect = PostgresDialect((14, 0, 0))
        result = dialect.format_normal_rand_function(100, 0, 1)
        assert "normal_rand" in result
