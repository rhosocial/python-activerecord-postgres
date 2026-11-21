# tests/rhosocial/activerecord_postgres_test/feature/backend/construct/extensions/test_extension_indexes.py
"""Extension index helpers render through the expression system."""
import pytest

from rhosocial.activerecord.backend.expression import Column, Literal
from rhosocial.activerecord.backend.expression.functions.aggregate import count
from rhosocial.activerecord.backend.expression.predicates import ComparisonPredicate
from rhosocial.activerecord.backend.impl.postgres.dialect import PostgresDialect
from rhosocial.activerecord.backend.impl.postgres.functions.vector_search import vector_search


class TestSpatialIndex:
    def test_default_gist(self):
        d = PostgresDialect((15, 0, 0))
        sql, params = d.format_spatial_index_statement("ix", "t", "geom")
        assert sql == 'CREATE INDEX "ix" ON "t" USING GIST ("geom")'
        assert params == ()

    @pytest.mark.parametrize("method", ["spgist", "brin"])
    def test_other_access_methods(self, method):
        d = PostgresDialect((15, 0, 0))
        sql, _ = d.format_spatial_index_statement(
            "ix", "t", "geom", index_type=method
        )
        assert f"USING {method.upper()}" in sql

    def test_invalid_method(self):
        d = PostgresDialect((15, 0, 0))
        with pytest.raises(ValueError, match="spgist"):
            d.format_spatial_index_statement("ix", "t", "geom", index_type="gin")


class TestOtherExtensionIndexes:
    def test_intarray(self):
        d = PostgresDialect((15, 0, 0))
        sql = d.format_intarray_index_statement("t", "tags")
        assert sql == 'CREATE INDEX "idx_t_tags" ON "t" USING GIN ("tags" gin__int_ops)'

    def test_btree_gin(self):
        d = PostgresDialect((15, 0, 0))
        sql = d.format_gin_index("ix", "t", ["a", "b"])
        assert sql == 'CREATE INDEX "ix" ON "t" USING GIN ("a", "b")'

    def test_btree_gist_with_include(self):
        d = PostgresDialect((15, 0, 0))
        sql = d.format_gist_index("ix", "t", ["a"], include=["b"])
        assert sql == 'CREATE INDEX "ix" ON "t" USING GIST ("a") INCLUDE ("b")'

    def test_hnsw(self):
        d = PostgresDialect((15, 0, 0))
        sql = d.format_create_hnsw_index_statement("t", "v", m=16)
        assert 'USING HNSW ("v" vector_cosine_ops)' in sql
        assert "WITH (m = 16)" in sql


class TestPreviouslyClaimedGaps:
    """Capabilities the test suite had recorded as missing but which exist."""

    def test_predicate_alias(self):
        d = PostgresDialect((15, 0, 0))
        expr = ComparisonPredicate(
            d, "=", Column(d, "a"), Literal(d, 1, inline=True)
        ).as_("cmp")
        assert expr.to_sql() == ('"a" = 1 AS "cmp"', ())

    def test_count_star(self):
        d = PostgresDialect((15, 0, 0))
        assert count(d, "*").as_("cnt").to_sql() == ('COUNT(*) AS "cnt"', ())

    def test_explicit_inner_join(self):
        d = PostgresDialect((15, 0, 0))
        assert d.supports_explicit_inner_join() is True

    def test_ip_similarity_does_not_silently_drop_column(self):
        d = PostgresDialect((15, 0, 0))
        with pytest.warns(UserWarning, match="include_similarity"):
            expr = vector_search(
                d, "t", [1.0, 2.0, 3.0], metric="ip", include_similarity=True
            )
        sql, _ = expr.to_sql()
        assert "similarity" not in sql
