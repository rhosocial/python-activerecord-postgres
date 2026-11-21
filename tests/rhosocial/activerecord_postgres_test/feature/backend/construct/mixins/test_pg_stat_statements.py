# tests/rhosocial/activerecord_postgres_test/feature/backend/construct/mixins/test_pg_stat_statements.py
"""pg_stat_statements queries are expression nodes from factories."""

from rhosocial.activerecord.backend.impl.postgres.dialect import PostgresDialect
from rhosocial.activerecord.backend.expression import core
from rhosocial.activerecord.backend.impl.postgres.functions import (
    pg_stat_statements_reset,
    query_stats_statement,
    query_by_id_statement,
    top_queries_by_time,
    top_queries_by_calls,
    top_queries_by_rows,
    io_stats_statement,
)


class TestPgStatStatementsMixin:
    def test_query_stats_statement(self):
        d = PostgresDialect((14, 0, 0))
        sql, params = query_stats_statement(d).to_sql()
        assert "pg_stat_statements" in sql
        assert params == ()

    def test_query_stats_statement_with_limit(self):
        d = PostgresDialect((14, 0, 0))
        sql, _ = query_stats_statement(d, limit=10).to_sql()
        assert "LIMIT 10" in sql

    def test_reset_stats_statement(self):
        d = PostgresDialect((14, 0, 0))
        result = pg_stat_statements_reset(d)
        assert isinstance(result, core.FunctionCall)
        sql, _ = result.to_sql()
        assert "pg_stat_statements_reset" in sql.lower()

    def test_query_by_id_statement(self):
        d = PostgresDialect((14, 0, 0))
        sql, params = query_by_id_statement(d, 12345).to_sql()
        assert "queryid" in sql
        assert params == (12345,)

    def test_top_queries_by_time(self):
        d = PostgresDialect((14, 0, 0))
        sql, params = top_queries_by_time(d, limit=5).to_sql()
        assert "total_exec_time" in sql
        assert params == ()

    def test_top_queries_by_calls(self):
        d = PostgresDialect((14, 0, 0))
        sql, params = top_queries_by_calls(d, limit=5).to_sql()
        assert "calls" in sql
        assert params == ()

    def test_top_queries_by_rows(self):
        d = PostgresDialect((14, 0, 0))
        sql, params = top_queries_by_rows(d, limit=5).to_sql()
        assert "rows" in sql
        assert params == ()

    def test_io_stats_statement(self):
        d = PostgresDialect((14, 0, 0))
        sql, params = io_stats_statement(d, limit=5).to_sql()
        assert "shared_blks" in sql
        assert params == ()
