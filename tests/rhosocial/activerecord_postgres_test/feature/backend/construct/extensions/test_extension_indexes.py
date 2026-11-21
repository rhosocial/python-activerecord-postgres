# tests/rhosocial/activerecord_postgres_test/feature/backend/construct/extensions/test_extension_indexes.py
"""Extension index factories return expression nodes with one-to-one formatters."""
import pytest

from rhosocial.activerecord.backend.expression import Column, Literal
from rhosocial.activerecord.backend.expression.functions.aggregate import count
from rhosocial.activerecord.backend.expression.predicates import ComparisonPredicate
from rhosocial.activerecord.backend.impl.postgres.dialect import PostgresDialect
from rhosocial.activerecord.backend.impl.postgres.functions import (
    btree_gin_index,
    btree_gist_index,
    intarray_index,
    spatial_index,
)
from rhosocial.activerecord.backend.impl.postgres.functions.vector_search import (
    create_vector_index,
    vector_search,
)


class TestSpatialIndexExpression:
    def test_default_gist(self):
        d = PostgresDialect((15, 0, 0))
        sql, params = spatial_index(d, "t", ["geom"], "ix").to_sql()
        assert sql == 'CREATE INDEX "ix" ON "t" USING gist ("geom")'
        assert params == ()

    @pytest.mark.parametrize("method", ["spgist", "brin"])
    def test_other_access_methods(self, method):
        d = PostgresDialect((15, 0, 0))
        sql, _ = spatial_index(
            d, "t", ["geom"], "ix", index_type=method
        ).to_sql()
        assert f"USING {method}" in sql

    def test_invalid_method(self):
        d = PostgresDialect((15, 0, 0))
        with pytest.raises(ValueError, match="spgist"):
            spatial_index(d, "t", ["geom"], "ix", index_type="gin").to_sql()


class TestOtherExtensionIndexes:
    def test_intarray(self):
        d = PostgresDialect((15, 0, 0))
        sql = intarray_index(d, "t", ["tags"]).to_sql()[0]
        assert sql == 'CREATE INDEX "idx_t_tags" ON "t" USING gin ("tags" gin__int_ops)'

    def test_btree_gin(self):
        d = PostgresDialect((15, 0, 0))
        sql = btree_gin_index(d, "t", ["a", "b"], "ix").to_sql()[0]
        assert sql == 'CREATE INDEX "ix" ON "t" USING gin ("a", "b")'

    def test_btree_gist_with_include(self):
        d = PostgresDialect((15, 0, 0))
        sql = btree_gist_index(
            d, "t", ["a"], "ix", include=["b"]
        ).to_sql()[0]
        assert sql == 'CREATE INDEX "ix" ON "t" USING gist ("a") INCLUDE ("b")'

    def test_hnsw(self):
        d = PostgresDialect((15, 0, 0))
        sql = create_vector_index(
            d, "t", "v", index_type="hnsw", m=16
        ).to_sql()[0]
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
            sql, _ = vector_search(
                d, "t", [1.0, 2.0, 3.0], metric="ip", include_similarity=True
            ).to_sql()
        assert "similarity" not in sql
