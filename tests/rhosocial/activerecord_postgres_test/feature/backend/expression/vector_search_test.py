# tests/rhosocial/activerecord_postgres_test/feature/backend/expression/vector_search_test.py
"""SQL-snapshot tests for the pgvector similarity-search helpers.

These tests build expressions with a bare ``PostgresDialect`` (no DB
connection) and assert on the generated SQL, so they run without a server.
"""
import pytest

from rhosocial.activerecord.backend.expression import Column, Literal, predicates
from rhosocial.activerecord.backend.impl.postgres.functions import (
    create_vector_index,
    vector_distance,
    vector_search,
)


class TestVectorSearch:
    """Tests for the vector_search helper."""

    def test_default_select_star(self, postgres_dialect):
        expr = vector_search(postgres_dialect, "documents", [1.0, 0.5, 0.2])
        sql, params = expr.to_sql()
        assert "SELECT *" in sql
        assert 'AS "cosine_distance"' in sql
        assert "ORDER BY" in sql and "<=>" in sql
        assert "LIMIT %s" in sql
        assert params[-1] == 10

    def test_columns_and_similarity(self, postgres_dialect):
        expr = vector_search(
            postgres_dialect,
            "documents",
            [1.0, 0.55, 0.18],
            columns=["content", "tenant"],
            include_similarity=True,
            top_k=3,
        )
        sql, params = expr.to_sql()
        assert "SELECT \"content\", \"tenant\"" in sql
        assert 'AS "cosine_distance"' in sql
        assert 'AS "cosine_similarity"' in sql
        assert "LIMIT %s" in sql and params[-1] == 3

    def test_where_filter(self, postgres_dialect):
        tenant_pred = predicates.ComparisonPredicate(
            postgres_dialect, "=", Column(postgres_dialect, "tenant"),
            Literal(postgres_dialect, "acme"),
        )
        expr = vector_search(
            postgres_dialect, "documents", [1.0, 0.5, 0.2],
            where=tenant_pred,
        )
        sql, params = expr.to_sql()
        assert 'WHERE "tenant" = %s' in sql
        assert "acme" in params

    def test_l2_metric(self, postgres_dialect):
        expr = vector_search(
            postgres_dialect, "documents", [0.1, 0.2, 0.9],
            metric="l2", columns=["content"],
        )
        sql, params = expr.to_sql()
        assert 'AS "l2_distance"' in sql
        assert "<->" in sql

    def test_ip_metric(self, postgres_dialect):
        expr = vector_search(
            postgres_dialect, "documents", [0.1, 0.2, 0.9],
            metric="ip", columns=["content"],
        )
        sql, params = expr.to_sql()
        assert 'AS "inner_product"' in sql
        assert "<#>" in sql

    def test_invalid_metric(self, postgres_dialect):
        with pytest.raises(ValueError, match="Unsupported vector metric"):
            vector_search(postgres_dialect, "documents", [1.0], metric="hamming")

    def test_non_positive_top_k(self, postgres_dialect):
        with pytest.raises(ValueError, match="top_k must be positive"):
            vector_search(postgres_dialect, "documents", [1.0], top_k=0)


class TestVectorDistance:
    """Tests for the vector_distance column-name convenience."""

    def test_column_name_wrapped_as_column(self, postgres_dialect):
        expr = vector_distance(postgres_dialect, "embedding", [1.0, 0.5, 0.2])
        sql, params = expr.to_sql()
        # Column name must render as identifier, not a string literal parameter.
        assert 'AS' not in sql
        assert '"embedding" <=> %s::vector(3)' in sql
        assert "embedding" not in params

    def test_l2_column(self, postgres_dialect):
        expr = vector_distance(postgres_dialect, "embedding", [1.0], metric="l2")
        sql, params = expr.to_sql()
        assert '"embedding" <-> %s::vector(1)' in sql

    def test_invalid_metric(self, postgres_dialect):
        with pytest.raises(ValueError, match="Unsupported vector metric"):
            vector_distance(postgres_dialect, "embedding", [1.0], metric="bad")


class TestCreateVectorIndex:
    """Tests for the create_vector_index helper."""

    def test_hnsw_default_cosine(self, postgres_dialect):
        expr = create_vector_index(postgres_dialect, "documents", "embedding")
        sql, params = expr.to_sql()
        assert "CREATE INDEX IF NOT EXISTS" in sql
        assert 'USING HNSW ("embedding" vector_cosine_ops)' in sql

    def test_hnsw_with_options(self, postgres_dialect):
        expr = create_vector_index(
            postgres_dialect, "documents", "embedding",
            index_type="hnsw", m=16, ef_construction=64,
        )
        sql, params = expr.to_sql()
        assert "WITH (m = 16, ef_construction = 64)" in sql

    def test_ivfflat_l2(self, postgres_dialect):
        expr = create_vector_index(
            postgres_dialect, "documents", "embedding",
            metric="l2", index_type="ivfflat", lists=4,
        )
        sql, params = expr.to_sql()
        assert 'USING IVFFLAT ("embedding" vector_l2_ops)' in sql
        assert "WITH (lists = 4)" in sql

    def test_ip_opclass(self, postgres_dialect):
        expr = create_vector_index(
            postgres_dialect, "documents", "embedding",
            metric="ip", index_type="hnsw",
        )
        sql, params = expr.to_sql()
        assert 'vector_ip_ops' in sql

    def test_invalid_index_type(self, postgres_dialect):
        with pytest.raises(ValueError, match="Unsupported vector index type"):
            create_vector_index(postgres_dialect, "documents", "embedding",
                                index_type="btree")
