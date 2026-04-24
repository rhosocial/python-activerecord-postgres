# tests/rhosocial/activerecord_postgres_test/feature/backend/postgres/extensions/test_pg_stat_statements.py
"""Unit tests for PostgreSQL pg_stat_statements extension mixin."""

from rhosocial.activerecord.backend.impl.postgres.dialect import PostgresDialect


class TestPgStatStatementsMixin:
    """Test pg_stat_statements extension mixin."""

    def test_format_query_stats_statement(self):
        """Test query stats statement formatting."""
        dialect = PostgresDialect((14, 0, 0))
        sql, params = dialect.format_query_stats_statement()
        assert "pg_stat_statements" in sql
        assert params == ()

    def test_format_query_stats_statement_with_limit(self):
        """Test query stats statement formatting with limit."""
        dialect = PostgresDialect((14, 0, 0))
        sql, params = dialect.format_query_stats_statement(limit=10)
        assert "pg_stat_statements" in sql
        assert "LIMIT 10" in sql

    def test_format_reset_stats_statement(self):
        """Test reset stats statement formatting."""
        dialect = PostgresDialect((14, 0, 0))
        sql, params = dialect.format_reset_stats_statement()
        assert "pg_stat_statements_reset" in sql
        assert params == ()

    def test_format_query_by_id_statement(self):
        """Test query by ID statement formatting."""
        dialect = PostgresDialect((14, 0, 0))
        sql, params = dialect.format_query_by_id_statement(12345)
        assert "queryid" in sql
        assert params == (12345,)

    def test_format_top_queries_by_time(self):
        """Test top queries by time formatting."""
        dialect = PostgresDialect((14, 0, 0))
        sql, params = dialect.format_top_queries_by_time(limit=5)
        assert "total_exec_time" in sql
        assert params == ()

    def test_format_top_queries_by_calls(self):
        """Test top queries by calls formatting."""
        dialect = PostgresDialect((14, 0, 0))
        sql, params = dialect.format_top_queries_by_calls(limit=5)
        assert "calls" in sql
        assert params == ()

    def test_format_io_stats_statement(self):
        """Test I/O stats statement formatting."""
        dialect = PostgresDialect((14, 0, 0))
        sql, params = dialect.format_io_stats_statement(limit=5)
        assert "shared_blks" in sql
        assert params == ()
