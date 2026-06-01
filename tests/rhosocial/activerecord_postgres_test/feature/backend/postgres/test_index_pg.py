# tests/rhosocial/activerecord_postgres_test/feature/backend/postgres/test_index_pg.py
"""Unit tests for PostgreSQL index mixin.

Tests for:
- PostgresIndexMixin feature detection
- Format REINDEX statement
- Format CREATE INDEX with PostgreSQL-specific options
- Format DROP INDEX with CONCURRENTLY
- Format ALTER INDEX
- PostgreSQL-specific IndexSupport protocol methods
- PostgresCreateIndexExpression / PostgresDropIndexExpression / PostgresAlterIndexExpression
"""
import pytest

from rhosocial.activerecord.backend.dialect.exceptions import UnsupportedFeatureError
from rhosocial.activerecord.backend.expression.statements.ddl_alter import (
    AddIndex,
    DropIndex as DropIndexAction,
)
from rhosocial.activerecord.backend.expression.statements.ddl_table import IndexDefinition

from rhosocial.activerecord.backend.impl.postgres.dialect import PostgresDialect
from rhosocial.activerecord.backend.impl.postgres.expression.ddl import (
    PostgresCreateIndexExpression,
    PostgresDropIndexExpression,
    PostgresAlterIndexExpression,
    PostgresAlterIndexActionType,
    PostgresReindexExpression,
)


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


class TestIndexSupportProtocol:
    """Test the 7 IndexSupport protocol methods that were previously missing."""

    def test_supports_index_type(self):
        d = PostgresDialect()
        assert d.supports_index_type() is True

    def test_supports_partial_index(self):
        d = PostgresDialect()
        assert d.supports_partial_index() is True

    def test_supports_functional_index(self):
        d = PostgresDialect()
        assert d.supports_functional_index() is True

    def test_supports_index_tablespace(self):
        d = PostgresDialect()
        assert d.supports_index_tablespace() is True

    def test_supports_index_include_pg10(self):
        assert PostgresDialect((10, 0, 0)).supports_index_include() is False

    def test_supports_index_include_pg11(self):
        assert PostgresDialect((11, 0, 0)).supports_index_include() is True

    def test_supports_concurrent_index_pg10(self):
        assert PostgresDialect((10, 0, 0)).supports_concurrent_index() is False

    def test_supports_concurrent_index_pg11(self):
        assert PostgresDialect((11, 0, 0)).supports_concurrent_index() is True

    def test_get_supported_index_types(self):
        types = PostgresDialect().get_supported_index_types()
        assert types == ["BTREE", "HASH", "GIST", "GIN", "SPGIST", "BRIN"]


class TestPostgresCreateIndexExpression:
    """Test PostgresCreateIndexExpression with NULLS NOT DISTINCT."""

    def test_basic_create(self):
        d = PostgresDialect((15, 0, 0))
        expr = PostgresCreateIndexExpression(d, "idx_test", "t", ["a"])
        sql, _ = expr.to_sql()
        assert sql == 'CREATE INDEX "idx_test" ON "t" ("a")'

    def test_unique(self):
        d = PostgresDialect((15, 0, 0))
        expr = PostgresCreateIndexExpression(d, "idx_u", "t", ["a"], unique=True)
        sql, _ = expr.to_sql()
        assert sql == 'CREATE UNIQUE INDEX "idx_u" ON "t" ("a")'

    def test_nulls_not_distinct(self):
        d = PostgresDialect((15, 0, 0))
        expr = PostgresCreateIndexExpression(
            d, "idx_u", "t", ["a"], unique=True, nulls_not_distinct_unique=True
        )
        sql, _ = expr.to_sql()
        assert sql == 'CREATE UNIQUE INDEX "idx_u" ON "t" ("a") NULLS NOT DISTINCT'

    def test_nulls_not_distinct_pg14_raises(self):
        d = PostgresDialect((14, 0, 0))
        expr = PostgresCreateIndexExpression(
            d, "idx_u", "t", ["a"], unique=True, nulls_not_distinct_unique=True
        )
        with pytest.raises(ValueError, match="NULLS NOT DISTINCT requires PostgreSQL 15"):
            expr.to_sql()

    def test_nulls_not_distinct_non_unique_raises(self):
        d = PostgresDialect((15, 0, 0))
        expr = PostgresCreateIndexExpression(
            d, "idx_u", "t", ["a"], unique=False, nulls_not_distinct_unique=True
        )
        with pytest.raises(ValueError, match="NULLS NOT DISTINCT is only valid for UNIQUE"):
            expr.to_sql()

    def test_concurrent_and_nulls_not_distinct_pg15_raises(self):
        d = PostgresDialect((15, 0, 0))
        expr = PostgresCreateIndexExpression(
            d, "idx_u", "t", ["a"],
            unique=True, concurrent=True, nulls_not_distinct_unique=True,
        )
        with pytest.raises(ValueError, match="CONCURRENTLY.*NULLS NOT DISTINCT.*PostgreSQL 16"):
            expr.to_sql()

    def test_concurrent_and_nulls_not_distinct_pg16(self):
        d = PostgresDialect((16, 0, 0))
        expr = PostgresCreateIndexExpression(
            d, "idx_u", "t", ["a"],
            unique=True, concurrent=True, nulls_not_distinct_unique=True,
        )
        sql, _ = expr.to_sql()
        assert "NULLS NOT DISTINCT" in sql
        assert "CONCURRENTLY" in sql


class TestPostgresDropIndexExpression:
    """Test PostgresDropIndexExpression with CONCURRENTLY."""

    def test_basic_drop(self):
        d = PostgresDialect((18, 0, 0))
        expr = PostgresDropIndexExpression(d, "idx_test")
        sql, _ = expr.to_sql()
        assert sql == 'DROP INDEX "idx_test"'

    def test_drop_if_exists(self):
        d = PostgresDialect((18, 0, 0))
        expr = PostgresDropIndexExpression(d, "idx_test", if_exists=True)
        sql, _ = expr.to_sql()
        assert sql == 'DROP INDEX IF EXISTS "idx_test"'

    def test_drop_concurrently(self):
        d = PostgresDialect((18, 0, 0))
        expr = PostgresDropIndexExpression(d, "idx_test", concurrent=True)
        sql, _ = expr.to_sql()
        assert sql == 'DROP INDEX CONCURRENTLY "idx_test"'

    def test_drop_concurrently_pg17_raises(self):
        d = PostgresDialect((17, 0, 0))
        expr = PostgresDropIndexExpression(d, "idx_test", concurrent=True)
        with pytest.raises(ValueError, match="DROP INDEX CONCURRENTLY requires PostgreSQL 18"):
            expr.to_sql()

    def test_drop_concurrently_if_exists(self):
        d = PostgresDialect((18, 0, 0))
        expr = PostgresDropIndexExpression(d, "idx_test", if_exists=True, concurrent=True)
        sql, _ = expr.to_sql()
        assert sql == 'DROP INDEX CONCURRENTLY IF EXISTS "idx_test"'


class TestPostgresAlterIndexExpression:
    """Test PostgresAlterIndexExpression for all action types."""

    def test_rename_to(self):
        d = PostgresDialect()
        expr = PostgresAlterIndexExpression(
            d, "idx_old", PostgresAlterIndexActionType.RENAME_TO, new_name="idx_new"
        )
        sql, _ = expr.to_sql()
        assert sql == 'ALTER INDEX "idx_old" RENAME TO "idx_new"'

    def test_rename_to_if_exists(self):
        d = PostgresDialect()
        expr = PostgresAlterIndexExpression(
            d, "idx_old", PostgresAlterIndexActionType.RENAME_TO,
            new_name="idx_new", if_exists=True,
        )
        sql, _ = expr.to_sql()
        assert sql == 'ALTER INDEX IF EXISTS "idx_old" RENAME TO "idx_new"'

    def test_rename_to_missing_name_raises(self):
        d = PostgresDialect()
        expr = PostgresAlterIndexExpression(
            d, "idx_old", PostgresAlterIndexActionType.RENAME_TO
        )
        with pytest.raises(ValueError, match="new_name is required"):
            expr.to_sql()

    def test_set_tablespace(self):
        d = PostgresDialect()
        expr = PostgresAlterIndexExpression(
            d, "idx_old", PostgresAlterIndexActionType.SET_TABLESPACE,
            tablespace="fast_ts",
        )
        sql, _ = expr.to_sql()
        assert sql == 'ALTER INDEX "idx_old" SET TABLESPACE "fast_ts"'

    def test_set_storage_parameters(self):
        d = PostgresDialect()
        expr = PostgresAlterIndexExpression(
            d, "idx_old", PostgresAlterIndexActionType.SET_STORAGE_PARAMETERS,
            storage_parameters={"fillfactor": 70},
        )
        sql, _ = expr.to_sql()
        assert sql == 'ALTER INDEX "idx_old" SET (fillfactor = 70)'

    def test_set_storage_parameters_multiple(self):
        d = PostgresDialect()
        expr = PostgresAlterIndexExpression(
            d, "idx_old", PostgresAlterIndexActionType.SET_STORAGE_PARAMETERS,
            storage_parameters={"fillfactor": 70, "deduplicate_items": "off"},
        )
        sql, _ = expr.to_sql()
        assert "fillfactor = 70" in sql
        assert "deduplicate_items = off" in sql

    def test_reset_storage_parameters(self):
        d = PostgresDialect()
        expr = PostgresAlterIndexExpression(
            d, "idx_old", PostgresAlterIndexActionType.RESET_STORAGE_PARAMETERS,
            storage_parameters={"fillfactor": 70},
        )
        sql, _ = expr.to_sql()
        assert sql == 'ALTER INDEX "idx_old" RESET (fillfactor)'

    def test_alter_column_statistics(self):
        d = PostgresDialect()
        expr = PostgresAlterIndexExpression(
            d, "idx_old", PostgresAlterIndexActionType.ALTER_COLUMN_STATISTICS,
            column_number=1, statistics_target=100,
        )
        sql, _ = expr.to_sql()
        assert sql == 'ALTER INDEX "idx_old" ALTER COLUMN 1 SET STATISTICS 100'

    def test_all_in_tablespace(self):
        d = PostgresDialect()
        expr = PostgresAlterIndexExpression(
            d, "", PostgresAlterIndexActionType.ALL_IN_TABLESPACE,
            source_tablespace="old_ts", target_tablespace="new_ts",
        )
        sql, _ = expr.to_sql()
        assert sql == 'ALTER INDEX ALL IN TABLESPACE "old_ts" SET TABLESPACE "new_ts"'

    def test_all_in_tablespace_nowait(self):
        d = PostgresDialect()
        expr = PostgresAlterIndexExpression(
            d, "", PostgresAlterIndexActionType.ALL_IN_TABLESPACE,
            source_tablespace="old_ts", target_tablespace="new_ts", nowait=True,
        )
        sql, _ = expr.to_sql()
        assert sql == 'ALTER INDEX ALL IN TABLESPACE "old_ts" SET TABLESPACE "new_ts" NOWAIT'


class TestFormatAddDropIndexAction:
    """Test format_add/drop_index_action raise UnsupportedFeatureError for PostgreSQL."""

    def test_format_add_index_action_raises(self):
        d = PostgresDialect()
        add = AddIndex(index=IndexDefinition(name="idx_test", columns=["a"]))
        with pytest.raises(UnsupportedFeatureError, match="ALTER TABLE ADD INDEX"):
            d.format_add_index_action(add)

    def test_format_drop_index_action_raises(self):
        d = PostgresDialect()
        drop = DropIndexAction(index_name="idx_test")
        with pytest.raises(UnsupportedFeatureError, match="ALTER TABLE DROP INDEX"):
            d.format_drop_index_action(drop)


class TestDialectIndexSupport:
    """Test the 5 PostgresDialect-level index support methods (B1–B5)."""

    def test_supports_create_index(self):
        assert PostgresDialect().supports_create_index() is True

    def test_supports_drop_index(self):
        assert PostgresDialect().supports_drop_index() is True

    def test_supports_unique_index(self):
        assert PostgresDialect().supports_unique_index() is True

    def test_supports_index_if_not_exists(self):
        assert PostgresDialect().supports_index_if_not_exists() is True

    def test_supports_index_if_exists(self):
        assert PostgresDialect().supports_index_if_exists() is True


class TestPostgresCreateIndexExpressionAllOptions:
    """Expand CREATE INDEX expression coverage — all remaining options."""

    def test_concurrent(self):
        d = PostgresDialect((11, 0, 0))
        expr = PostgresCreateIndexExpression(d, "idx_c", "t", ["a"], concurrent=True)
        sql, _ = expr.to_sql()
        assert "CONCURRENTLY" in sql

    def test_concurrent_pg10_raises(self):
        d = PostgresDialect((10, 0, 0))
        expr = PostgresCreateIndexExpression(d, "idx_c", "t", ["a"], concurrent=True)
        with pytest.raises(ValueError, match="CONCURRENTLY requires PostgreSQL 11"):
            expr.to_sql()

    def test_if_not_exists(self):
        d = PostgresDialect()
        expr = PostgresCreateIndexExpression(d, "idx_t", "t", ["a"], if_not_exists=True)
        sql, _ = expr.to_sql()
        assert "IF NOT EXISTS" in sql

    def test_index_type(self):
        d = PostgresDialect()
        expr = PostgresCreateIndexExpression(d, "idx_h", "t", ["a"], index_type="hash")
        sql, _ = expr.to_sql()
        assert "USING hash" in sql

    def test_include(self):
        d = PostgresDialect((12, 0, 0))
        expr = PostgresCreateIndexExpression(
            d, "idx_i", "t", ["a"], include=["b", "c"]
        )
        sql, _ = expr.to_sql()
        assert 'INCLUDE ("b", "c")' in sql

    def test_tablespace(self):
        d = PostgresDialect()
        expr = PostgresCreateIndexExpression(
            d, "idx_t", "t", ["a"], tablespace="fast_ts"
        )
        sql, _ = expr.to_sql()
        assert 'TABLESPACE "fast_ts"' in sql

    def test_where(self):
        d = PostgresDialect()
        from rhosocial.activerecord.backend.expression import Literal
        from rhosocial.activerecord.backend.expression import Column
        expr = PostgresCreateIndexExpression(
            d, "idx_w", "t", ["a"],
            where=Column(d, "status") == Literal(d, 1),
        )
        sql, params = expr.to_sql()
        assert "WHERE" in sql
        assert "%s" in sql
        assert params == (1,)

    def test_with_options(self):
        d = PostgresDialect()
        expr = PostgresCreateIndexExpression(
            d, "idx_w", "t", ["a"],
            dialect_options={"with": {"fillfactor": 70}},
        )
        sql, _ = expr.to_sql()
        assert "WITH (fillfactor = 70)" in sql

    def test_opclasses(self):
        d = PostgresDialect()
        expr = PostgresCreateIndexExpression(
            d, "idx_o", "t", ["a", "b"],
            dialect_options={"opclasses": {"a": "text_pattern_ops"}},
        )
        sql, _ = expr.to_sql()
        assert '"a" text_pattern_ops' in sql
        assert '"b"' in sql


class TestFormatCreateIndexPgStatementWhereExpression:
    """Test format_create_index_pg_statement where_clause as ToSQLProtocol."""

    def test_where_expression(self):
        d = PostgresDialect()
        from rhosocial.activerecord.backend.expression import Column, Literal
        sql, params = d.format_create_index_pg_statement(
            index_name="idx_active",
            table_name="users",
            columns=["email"],
            where_clause=Column(d, "status") == Literal(d, 1),
        )
        assert "WHERE" in sql
        assert params == (1,)


class TestPostgresAlterIndexExpressionErrorPaths:
    """Test error paths in ALTER INDEX operations."""

    def test_all_in_tablespace_missing_source_raises(self):
        d = PostgresDialect()
        expr = PostgresAlterIndexExpression(
            d, "", PostgresAlterIndexActionType.ALL_IN_TABLESPACE,
            target_tablespace="new_ts",
        )
        with pytest.raises(ValueError, match="source_tablespace"):
            expr.to_sql()

    def test_all_in_tablespace_missing_target_raises(self):
        d = PostgresDialect()
        expr = PostgresAlterIndexExpression(
            d, "", PostgresAlterIndexActionType.ALL_IN_TABLESPACE,
            source_tablespace="old_ts",
        )
        with pytest.raises(ValueError, match="target_tablespace"):
            expr.to_sql()

    def test_set_tablespace_missing_raises(self):
        d = PostgresDialect()
        expr = PostgresAlterIndexExpression(
            d, "idx", PostgresAlterIndexActionType.SET_TABLESPACE,
        )
        with pytest.raises(ValueError, match="tablespace is required"):
            expr.to_sql()

    def test_set_storage_params_missing_raises(self):
        d = PostgresDialect()
        expr = PostgresAlterIndexExpression(
            d, "idx", PostgresAlterIndexActionType.SET_STORAGE_PARAMETERS,
        )
        with pytest.raises(ValueError, match="storage_parameters"):
            expr.to_sql()

    def test_reset_storage_params_missing_raises(self):
        d = PostgresDialect()
        expr = PostgresAlterIndexExpression(
            d, "idx", PostgresAlterIndexActionType.RESET_STORAGE_PARAMETERS,
        )
        with pytest.raises(ValueError, match="storage_parameters"):
            expr.to_sql()

    def test_alter_statistics_missing_params_raises(self):
        d = PostgresDialect()
        expr = PostgresAlterIndexExpression(
            d, "idx", PostgresAlterIndexActionType.ALTER_COLUMN_STATISTICS,
        )
        with pytest.raises(ValueError, match="column_number"):
            expr.to_sql()
