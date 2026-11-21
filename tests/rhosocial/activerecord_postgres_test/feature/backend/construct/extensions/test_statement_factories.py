# tests/rhosocial/activerecord_postgres_test/feature/backend/construct/extensions/test_statement_factories.py
"""Extension statements are built by factories and rendered by (self, expr)."""

import pytest

from rhosocial.activerecord.backend.impl.postgres.dialect import PostgresDialect
from rhosocial.activerecord.backend.impl.postgres.functions import (
    bloom_index,
    citext_column,
    connectby_full,
    crosstab_with_definition,
    io_stats_statement,
    ltree_index,
    query_by_id_statement,
    query_stats_statement,
    top_queries_by_calls,
    top_queries_by_rows,
    top_queries_by_time,
)


class TestStatementFactories:
    def setup_method(self):
        self.dialect = PostgresDialect((15, 0, 0))

    def test_bloom_index(self):
        sql, _ = bloom_index(
            self.dialect, "t", ["a", "b"], "ix", fill_factor=70
        ).to_sql()
        assert "USING BLOOM" in sql
        assert "fillfactor = 70" in sql

    def test_citext_column(self):
        sql, _ = citext_column(self.dialect, "name").to_sql()
        assert "CITEXT" in sql and "name" in sql

    def test_ltree_index(self):
        sql, _ = ltree_index(self.dialect, "t", ["path"], "ix").to_sql()
        assert "USING gist" in sql
        with pytest.raises(ValueError, match="gist.*btree"):
            ltree_index(self.dialect, "t", ["path"], "ix", index_type="gin")

    def test_pg_stat_queries(self):
        for factory in (
            query_stats_statement,
            top_queries_by_time,
            top_queries_by_calls,
            top_queries_by_rows,
            io_stats_statement,
        ):
            sql, params = factory(self.dialect).to_sql()
            assert "pg_stat_statements" in sql
            assert params == ()
        sql, params = query_by_id_statement(self.dialect, 42).to_sql()
        assert "queryid" in sql
        assert params == (42,)

    def test_tablefunc_queries(self):
        sql, _ = crosstab_with_definition(
            self.dialect, "SELECT a,b,c FROM t", "row text, v int"
        ).to_sql()
        assert "crosstab(" in sql
        assert "AS ct(row text, v int)" in sql
        sql, _ = connectby_full(
            self.dialect, "categories", "id", "parent_id", "1", 5
        ).to_sql()
        assert "connectby(" in sql
        assert "AS t(keyid text" in sql
