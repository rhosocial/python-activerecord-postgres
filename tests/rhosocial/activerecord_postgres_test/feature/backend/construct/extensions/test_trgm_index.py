# tests/rhosocial/activerecord_postgres_test/feature/backend/construct/extensions/test_trgm_index.py
"""Trigram index factories return expression nodes, not raw SQL."""
import pytest

from rhosocial.activerecord.backend.impl.postgres.dialect import PostgresDialect
from rhosocial.activerecord.backend.impl.postgres.functions import trgm_index


class TestTrgmIndexExpression:
    def test_gin_uses_gin_trgm_ops(self):
        d = PostgresDialect((15, 0, 0))
        sql, params = trgm_index(d, "t", ["name"], "ix").to_sql()
        assert "USING gin" in sql
        assert "gin_trgm_ops" in sql
        assert "gist_trgm_ops" not in sql
        assert params == ()

    def test_gist_uses_gist_trgm_ops(self):
        d = PostgresDialect((15, 0, 0))
        sql, params = trgm_index(
            d, "t", ["name"], "ix", index_type="gist"
        ).to_sql()
        assert "USING gist" in sql
        assert "gist_trgm_ops" in sql
        assert "gin_trgm_ops" not in sql
        assert params == ()

    def test_unknown_access_method_refused(self):
        d = PostgresDialect((15, 0, 0))
        with pytest.raises(ValueError, match="gin.*gist"):
            trgm_index(d, "t", ["name"], "ix", index_type="brin")
