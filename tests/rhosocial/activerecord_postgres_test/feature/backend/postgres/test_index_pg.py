# tests/rhosocial/activerecord_postgres_test/feature/backend/postgres/test_index_pg.py
"""Unit tests for PostgreSQL index mixin.

Tests for:
- PostgresIndexMixin feature detection
- Format REINDEX statement
- Format CREATE INDEX with PostgreSQL-specific options
"""
import pytest

from rhosocial.activerecord.backend.impl.postgres.dialect import PostgresDialect
from rhosocial.activerecord.backend.impl.postgres.expression.ddl import PostgresReindexExpression


class TestIndexFeatureDetection:
    """Test index feature detection methods."""

    def test_supports_safe_hash_index_pg9(self):
        """PostgreSQL 9.5 does not support safe hash index."""
        dialect = PostgresDialect((9, 5, 0))
        assert dialect.supports_safe_hash_index() is False

    def test_supports_safe_hash_index_pg10(self):
        """PostgreSQL 10 supports safe hash index."""
        dialect = PostgresDialect((10, 0, 0))
        assert dialect.supports_safe_hash_index() is True

    def test_supports_parallel_create_index_pg10(self):
        """PostgreSQL 10 does not support parallel CREATE INDEX."""
        dialect = PostgresDialect((10, 0, 0))
        assert dialect.supports_parallel_create_index() is False

    def test_supports_parallel_create_index_pg11(self):
        """PostgreSQL 11 supports parallel CREATE INDEX."""
        dialect = PostgresDialect((11, 0, 0))
        assert dialect.supports_parallel_create_index() is True

    def test_supports_gist_include_pg11(self):
        """PostgreSQL 11 does not support GiST INCLUDE."""
        dialect = PostgresDialect((11, 0, 0))
        assert dialect.supports_gist_include() is False

    def test_supports_gist_include_pg12(self):
        """PostgreSQL 12 supports GiST INCLUDE."""
        dialect = PostgresDialect((12, 0, 0))
        assert dialect.supports_gist_include() is True

    def test_supports_reindex_concurrently_pg11(self):
        """PostgreSQL 11 does not support REINDEX CONCURRENTLY."""
        dialect = PostgresDialect((11, 0, 0))
        assert dialect.supports_reindex_concurrently() is False

    def test_supports_reindex_concurrently_pg12(self):
        """PostgreSQL 12 supports REINDEX CONCURRENTLY."""
        dialect = PostgresDialect((12, 0, 0))
        assert dialect.supports_reindex_concurrently() is True

    def test_supports_btree_deduplication_pg12(self):
        """PostgreSQL 12 does not support B-tree deduplication."""
        dialect = PostgresDialect((12, 0, 0))
        assert dialect.supports_btree_deduplication() is False

    def test_supports_btree_deduplication_pg13(self):
        """PostgreSQL 13 supports B-tree deduplication."""
        dialect = PostgresDialect((13, 0, 0))
        assert dialect.supports_btree_deduplication() is True

    def test_supports_brin_multivalue_pg13(self):
        """PostgreSQL 13 does not support BRIN multivalue."""
        dialect = PostgresDialect((13, 0, 0))
        assert dialect.supports_brin_multivalue() is False

    def test_supports_brin_multivalue_pg14(self):
        """PostgreSQL 14 supports BRIN multivalue."""
        dialect = PostgresDialect((14, 0, 0))
        assert dialect.supports_brin_multivalue() is True

    def test_supports_brin_bloom_pg13(self):
        """PostgreSQL 13 does not support BRIN bloom."""
        dialect = PostgresDialect((13, 0, 0))
        assert dialect.supports_brin_bloom() is False

    def test_supports_brin_bloom_pg14(self):
        """PostgreSQL 14 supports BRIN bloom."""
        dialect = PostgresDialect((14, 0, 0))
        assert dialect.supports_brin_bloom() is True

    def test_supports_spgist_include_pg13(self):
        """PostgreSQL 13 does not support SP-GiST INCLUDE."""
        dialect = PostgresDialect((13, 0, 0))
        assert dialect.supports_spgist_include() is False

    def test_supports_spgist_include_pg14(self):
        """PostgreSQL 14 supports SP-GiST INCLUDE."""
        dialect = PostgresDialect((14, 0, 0))
        assert dialect.supports_spgist_include() is True


class TestFormatReindexStatement:
    """Test REINDEX statement formatting."""

    def test_reindex_index_basic(self):
        """Test basic REINDEX INDEX."""
        dialect = PostgresDialect((14, 0, 0))
        expr = PostgresReindexExpression(
            dialect,
            target_type="INDEX",
            name="idx_users_email"
        )
        sql, params = dialect.format_reindex_statement(expr)
        assert 'REINDEX INDEX "idx_users_email"' in sql
        assert params == ()

    def test_reindex_table(self):
        """Test REINDEX TABLE."""
        dialect = PostgresDialect((14, 0, 0))
        expr = PostgresReindexExpression(
            dialect,
            target_type="TABLE",
            name="users"
        )
        sql, params = dialect.format_reindex_statement(expr)
        assert 'REINDEX TABLE "users"' in sql

    def test_reindex_schema(self):
        """Test REINDEX SCHEMA."""
        dialect = PostgresDialect((14, 0, 0))
        expr = PostgresReindexExpression(
            dialect,
            target_type="SCHEMA",
            name="public"
        )
        sql, params = dialect.format_reindex_statement(expr)
        assert 'REINDEX SCHEMA "public"' in sql

    def test_reindex_database(self):
        """Test REINDEX DATABASE."""
        dialect = PostgresDialect((14, 0, 0))
        expr = PostgresReindexExpression(
            dialect,
            target_type="DATABASE",
            name="mydb"
        )
        sql, params = dialect.format_reindex_statement(expr)
        assert 'REINDEX DATABASE "mydb"' in sql

    def test_reindex_system(self):
        """Test REINDEX SYSTEM."""
        dialect = PostgresDialect((14, 0, 0))
        expr = PostgresReindexExpression(
            dialect,
            target_type="SYSTEM",
            name="mydb"
        )
        sql, params = dialect.format_reindex_statement(expr)
        assert 'REINDEX SYSTEM "mydb"' in sql

    def test_reindex_invalid_target_type(self):
        """Test REINDEX with invalid target type."""
        dialect = PostgresDialect((14, 0, 0))
        expr = PostgresReindexExpression(
            dialect,
            target_type="INVALID",
            name="test"
        )
        with pytest.raises(ValueError, match="Invalid target_type"):
            dialect.format_reindex_statement(expr)

    def test_reindex_concurrently_pg11_raises_error(self):
        """REINDEX CONCURRENTLY should raise error on PostgreSQL 11."""
        dialect = PostgresDialect((11, 0, 0))
        expr = PostgresReindexExpression(
            dialect,
            target_type="INDEX",
            name="idx_test",
            concurrently=True
        )
        with pytest.raises(ValueError, match="requires PostgreSQL 12"):
            dialect.format_reindex_statement(expr)

    def test_reindex_concurrently_pg12(self):
        """Test REINDEX CONCURRENTLY on PostgreSQL 12."""
        dialect = PostgresDialect((12, 0, 0))
        expr = PostgresReindexExpression(
            dialect,
            target_type="INDEX",
            name="idx_test",
            concurrently=True
        )
        sql, params = dialect.format_reindex_statement(expr)
        assert 'REINDEX CONCURRENTLY INDEX "idx_test"' in sql

    def test_reindex_with_tablespace(self):
        """Test REINDEX with TABLESPACE."""
        dialect = PostgresDialect((14, 0, 0))
        expr = PostgresReindexExpression(
            dialect,
            target_type="INDEX",
            name="idx_test",
            tablespace="pg_fast"
        )
        sql, params = dialect.format_reindex_statement(expr)
        assert 'TABLESPACE "pg_fast"' in sql

    def test_reindex_verbose(self):
        """Test REINDEX VERBOSE."""
        dialect = PostgresDialect((14, 0, 0))
        expr = PostgresReindexExpression(
            dialect,
            target_type="INDEX",
            name="idx_test",
            verbose=True
        )
        sql, params = dialect.format_reindex_statement(expr)
        assert "VERBOSE" in sql

    def test_reindex_with_schema(self):
        """Test REINDEX with schema."""
        dialect = PostgresDialect((14, 0, 0))
        expr = PostgresReindexExpression(
            dialect,
            target_type="INDEX",
            name="idx_test",
            schema="public"
        )
        sql, params = dialect.format_reindex_statement(expr)
        assert '"public"' in sql


class TestFormatCreateIndexPgStatement:
    """Test CREATE INDEX with PostgreSQL-specific options."""

    def test_create_index_basic(self):
        """Test basic CREATE INDEX."""
        dialect = PostgresDialect((14, 0, 0))
        sql, params = dialect.format_create_index_pg_statement(
            index_name="idx_users_email",
            table_name="users",
            columns=["email"]
        )
        assert 'CREATE INDEX "idx_users_email"' in sql
        assert 'ON "users" USING btree ("email")' in sql

    def test_create_unique_index(self):
        """Test CREATE UNIQUE INDEX."""
        dialect = PostgresDialect((14, 0, 0))
        sql, params = dialect.format_create_index_pg_statement(
            index_name="idx_users_email",
            table_name="users",
            columns=["email"],
            unique=True
        )
        assert "CREATE UNIQUE INDEX" in sql

    def test_create_index_with_type(self):
        """Test CREATE INDEX with specific type."""
        dialect = PostgresDialect((14, 0, 0))
        sql, params = dialect.format_create_index_pg_statement(
            index_name="idx_users_email",
            table_name="users",
            columns=["email"],
            index_type="hash"
        )
        assert "USING hash" in sql

    def test_create_index_invalid_type(self):
        """Test CREATE INDEX with invalid type."""
        dialect = PostgresDialect((14, 0, 0))
        with pytest.raises(ValueError, match="Invalid index_type"):
            dialect.format_create_index_pg_statement(
                index_name="idx_test",
                table_name="users",
                columns=["email"],
                index_type="invalid"
            )

    def test_create_index_concurrently_pg10_raises_error(self):
        """CREATE INDEX CONCURRENTLY should raise error on PostgreSQL 10."""
        dialect = PostgresDialect((10, 0, 0))
        with pytest.raises(ValueError, match="requires PostgreSQL 11"):
            dialect.format_create_index_pg_statement(
                index_name="idx_test",
                table_name="users",
                columns=["email"],
                concurrently=True
            )

    def test_create_index_concurrently_pg11(self):
        """Test CREATE INDEX CONCURRENTLY on PostgreSQL 11."""
        dialect = PostgresDialect((11, 0, 0))
        sql, params = dialect.format_create_index_pg_statement(
            index_name="idx_test",
            table_name="users",
            columns=["email"],
            concurrently=True
        )
        assert "CREATE INDEX CONCURRENTLY" in sql

    def test_create_index_if_not_exists(self):
        """Test CREATE INDEX IF NOT EXISTS."""
        dialect = PostgresDialect((14, 0, 0))
        sql, params = dialect.format_create_index_pg_statement(
            index_name="idx_test",
            table_name="users",
            columns=["email"],
            if_not_exists=True
        )
        assert "CREATE INDEX IF NOT EXISTS" in sql

    def test_create_index_with_include(self):
        """Test CREATE INDEX with INCLUDE clause."""
        dialect = PostgresDialect((14, 0, 0))
        sql, params = dialect.format_create_index_pg_statement(
            index_name="idx_test",
            table_name="users",
            columns=["email"],
            include_columns=["name", "created_at"]
        )
        assert 'INCLUDE ("name", "created_at")' in sql

    def test_create_index_with_include_gist_pg11_raises_error(self):
        """CREATE INDEX GiST with INCLUDE should raise error on PostgreSQL 11."""
        dialect = PostgresDialect((11, 0, 0))
        with pytest.raises(ValueError, match="INCLUDE for GiST indexes requires PostgreSQL 12"):
            dialect.format_create_index_pg_statement(
                index_name="idx_test",
                table_name="users",
                columns=["location"],
                index_type="gist",
                include_columns=["name"]
            )

    def test_create_index_with_include_spgist_pg13_raises_error(self):
        """CREATE INDEX SP-GiST with INCLUDE should raise error on PostgreSQL 13."""
        dialect = PostgresDialect((13, 0, 0))
        with pytest.raises(ValueError, match="INCLUDE for SP-GiST indexes requires PostgreSQL 14"):
            dialect.format_create_index_pg_statement(
                index_name="idx_test",
                table_name="users",
                columns=["location"],
                index_type="spgist",
                include_columns=["name"]
            )

    def test_create_index_with_options(self):
        """Test CREATE INDEX with WITH options."""
        dialect = PostgresDialect((14, 0, 0))
        sql, params = dialect.format_create_index_pg_statement(
            index_name="idx_test",
            table_name="users",
            columns=["email"],
            with_options={"fillfactor": "80", "deduplicate_items": "on"}
        )
        assert "WITH (fillfactor = 80, deduplicate_items = on)" in sql

    def test_create_index_with_tablespace(self):
        """Test CREATE INDEX with TABLESPACE."""
        dialect = PostgresDialect((14, 0, 0))
        sql, params = dialect.format_create_index_pg_statement(
            index_name="idx_test",
            table_name="users",
            columns=["email"],
            tablespace="pg_fast"
        )
        assert 'TABLESPACE "pg_fast"' in sql

    def test_create_index_with_where(self):
        """Test CREATE INDEX with WHERE clause (partial index)."""
        dialect = PostgresDialect((14, 0, 0))
        sql, params = dialect.format_create_index_pg_statement(
            index_name="idx_active_users",
            table_name="users",
            columns=["email"],
            where_clause="active = true"
        )
        assert "WHERE active = true" in sql

    def test_create_index_with_schema(self):
        """Test CREATE INDEX with schema."""
        dialect = PostgresDialect((14, 0, 0))
        sql, params = dialect.format_create_index_pg_statement(
            index_name="idx_test",
            table_name="users",
            columns=["email"],
            schema="public"
        )
        assert '"public"."idx_test"' in sql
        assert '"public"."users"' in sql

    def test_create_index_multiple_columns(self):
        """Test CREATE INDEX with multiple columns."""
        dialect = PostgresDialect((14, 0, 0))
        sql, params = dialect.format_create_index_pg_statement(
            index_name="idx_test",
            table_name="users",
            columns=["email", "name"]
        )
        assert '("email", "name")' in sql
