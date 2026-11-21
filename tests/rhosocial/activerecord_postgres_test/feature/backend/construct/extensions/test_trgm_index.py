# tests/rhosocial/activerecord_postgres_test/feature/backend/construct/extensions/test_trgm_index.py
"""Construction tests for pg_trgm index operator classes."""
import pytest

from rhosocial.activerecord.backend.impl.postgres.dialect import PostgresDialect


class TestTrgmIndexOpclass:
    def test_gin_uses_gin_trgm_ops(self):
        d = PostgresDialect((15, 0, 0))
        sql, params = d.format_trgm_index_statement("ix", "t", "name")
        assert "USING gin" in sql
        assert "gin_trgm_ops" in sql
        assert "gist_trgm_ops" not in sql
        assert params == ()

    def test_gist_uses_gist_trgm_ops(self):
        d = PostgresDialect((15, 0, 0))
        sql, params = d.format_trgm_index_statement(
            "ix", "t", "name", index_type="gist"
        )
        assert "USING gist" in sql
        assert "gist_trgm_ops" in sql
        assert "gin_trgm_ops" not in sql
        assert params == ()

    def test_unknown_access_method_refused(self):
        d = PostgresDialect((15, 0, 0))
        with pytest.raises(ValueError, match="gin.*gist"):
            d.format_trgm_index_statement("ix", "t", "name", index_type="brin")
