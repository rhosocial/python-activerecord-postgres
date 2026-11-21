# tests/rhosocial/activerecord_postgres_test/feature/backend/construct/functions/test_index_type.py
"""Index access-method vocabulary covers native and extension methods."""
import pytest

from rhosocial.activerecord.backend.expression.objects import Index, Table
from rhosocial.activerecord.backend.impl.postgres.dialect import PostgresDialect
from rhosocial.activerecord.backend.impl.postgres.expression.ddl import (
    PostgresCreateIndexExpression,
    PostgresIndexType,
)


class TestPostgresIndexType:
    def test_coerce_preserves_caller_spelling(self):
        assert PostgresIndexType.coerce("gin") == "gin"
        assert PostgresIndexType.coerce("GIN") == "GIN"

    def test_normalize_accepts_aliases(self):
        assert PostgresIndexType.normalize("SP-GiST") is PostgresIndexType.SPGIST
        assert PostgresIndexType.normalize("sp_gist") is PostgresIndexType.SPGIST
        assert PostgresIndexType.normalize("ivf_flat") is PostgresIndexType.IVFFLAT

    def test_alias_is_rendered_canonically(self):
        assert PostgresIndexType.coerce("sp-gist") == "spgist"

    def test_member_input(self):
        assert PostgresIndexType.coerce(PostgresIndexType.HNSW) == "hnsw"

    def test_unknown_refused(self):
        with pytest.raises(ValueError, match="Unsupported index type"):
            PostgresIndexType.coerce("bogus")

    def test_native_and_extension_groups(self):
        assert PostgresIndexType.GIN.is_native is True
        assert PostgresIndexType.GIN.is_extension is False
        assert PostgresIndexType.IVFFLAT.is_extension is True
        assert PostgresIndexType.IVFFLAT.is_native is False


class TestIndexTypeRendering:
    def test_native_and_extension_methods_render(self):
        d = PostgresDialect((15, 0, 0))
        for method in ("gin", "hnsw", "ivfflat", "bloom"):
            expr = PostgresCreateIndexExpression(
                d, Index(d, "ix"), Table(d, "t"), ["a"], index_type=method
            )
            sql, _ = expr.to_sql()
            assert f"USING {method}" in sql

    def test_generic_statement_validates_too(self):
        from rhosocial.activerecord.backend.expression.statements.ddl_index import (
            CreateIndexStatement,
        )

        d = PostgresDialect((15, 0, 0))
        expr = CreateIndexStatement(
            d, Index(d, "ix"), Table(d, "t"), ["a"], index_type="bogus"
        )
        with pytest.raises(ValueError, match="Unsupported index type"):
            expr.to_sql()

    def test_extension_type_listing(self):
        d = PostgresDialect((15, 0, 0))
        assert d.get_supported_index_types() == [
            "BTREE", "HASH", "GIST", "GIN", "SPGIST", "BRIN"
        ]
        assert d.get_supported_extension_index_types() == ["HNSW", "IVFFLAT", "BLOOM"]
