# tests/rhosocial/activerecord_postgres_test/feature/backend/test_version_features.py
"""PostgreSQL version-specific feature detection tests.

This module tests the version-specific feature detection methods
added to the PostgreSQL dialect mixins.
"""
import pytest

from rhosocial.activerecord.backend.impl.postgres.dialect import PostgresDialect


class TestParallelQueryFeatures:
    """Test parallel query feature detection."""

    def test_parallel_query_pg95(self):
        """PostgreSQL 9.5 does not support parallel query."""
        dialect = PostgresDialect((9, 5, 0))
        assert dialect.supports_parallel_query() is False
        assert dialect.supports_parallel_append() is False
        assert dialect.supports_parallel_index_scan() is False
        assert dialect.supports_parallel_index_only_scan() is False
        assert dialect.supports_parallel_hash_join() is False
        assert dialect.supports_parallel_gather_merge() is False

    def test_parallel_query_pg96(self):
        """PostgreSQL 9.6 supports basic parallel query."""
        dialect = PostgresDialect((9, 6, 0))
        assert dialect.supports_parallel_query() is True
        assert dialect.supports_parallel_append() is False
        assert dialect.supports_parallel_index_scan() is False
        assert dialect.supports_parallel_index_only_scan() is False
        assert dialect.supports_parallel_hash_join() is False
        assert dialect.supports_parallel_gather_merge() is False

    def test_parallel_query_pg10(self):
        """PostgreSQL 10 adds more parallel features."""
        dialect = PostgresDialect((10, 0, 0))
        assert dialect.supports_parallel_query() is True
        assert dialect.supports_parallel_append() is True
        assert dialect.supports_parallel_index_scan() is True
        assert dialect.supports_parallel_index_only_scan() is False
        assert dialect.supports_parallel_hash_join() is False
        assert dialect.supports_parallel_gather_merge() is True

    def test_parallel_query_pg11(self):
        """PostgreSQL 11 adds parallel index-only scan and hash join."""
        dialect = PostgresDialect((11, 0, 0))
        assert dialect.supports_parallel_query() is True
        assert dialect.supports_parallel_append() is True
        assert dialect.supports_parallel_index_scan() is True
        assert dialect.supports_parallel_index_only_scan() is True
        assert dialect.supports_parallel_hash_join() is True
        assert dialect.supports_parallel_gather_merge() is True

    def test_parallel_query_pg18(self):
        """PostgreSQL 18 supports all parallel features."""
        dialect = PostgresDialect((18, 0, 0))
        assert dialect.supports_parallel_query() is True
        assert dialect.supports_parallel_append() is True
        assert dialect.supports_parallel_index_scan() is True
        assert dialect.supports_parallel_index_only_scan() is True
        assert dialect.supports_parallel_hash_join() is True
        assert dialect.supports_parallel_gather_merge() is True


class TestStoredProcedureFeatures:
    """Test stored procedure feature detection."""

    def test_stored_procedure_pg10(self):
        """PostgreSQL 10 does not support stored procedures."""
        dialect = PostgresDialect((10, 0, 0))
        assert dialect.supports_call_statement() is False
        assert dialect.supports_stored_procedure_transaction_control() is False
        assert dialect.supports_sql_body_functions() is False

    def test_stored_procedure_pg11(self):
        """PostgreSQL 11 supports CALL statement and transaction control."""
        dialect = PostgresDialect((11, 0, 0))
        assert dialect.supports_call_statement() is True
        assert dialect.supports_stored_procedure_transaction_control() is True
        assert dialect.supports_sql_body_functions() is False

    def test_stored_procedure_pg14(self):
        """PostgreSQL 14 supports SQL-body functions."""
        dialect = PostgresDialect((14, 0, 0))
        assert dialect.supports_call_statement() is True
        assert dialect.supports_stored_procedure_transaction_control() is True
        assert dialect.supports_sql_body_functions() is True


class TestExtendedStatisticsFeatures:
    """Test extended statistics feature detection."""

    def test_extended_statistics_pg95(self):
        """PostgreSQL 9.5 does not support extended statistics."""
        dialect = PostgresDialect((9, 5, 0))
        assert dialect.supports_create_statistics() is False
        assert dialect.supports_statistics_dependencies() is False
        assert dialect.supports_statistics_ndistinct() is False
        assert dialect.supports_statistics_mcv() is False

    def test_extended_statistics_pg10(self):
        """PostgreSQL 10 supports basic extended statistics."""
        dialect = PostgresDialect((10, 0, 0))
        assert dialect.supports_create_statistics() is True
        assert dialect.supports_statistics_dependencies() is True
        assert dialect.supports_statistics_ndistinct() is True
        assert dialect.supports_statistics_mcv() is False

    def test_extended_statistics_pg12(self):
        """PostgreSQL 12 adds MCV statistics."""
        dialect = PostgresDialect((12, 0, 0))
        assert dialect.supports_create_statistics() is True
        assert dialect.supports_statistics_dependencies() is True
        assert dialect.supports_statistics_ndistinct() is True
        assert dialect.supports_statistics_mcv() is True


class TestPostgreSQL15PlusFeatures:
    """Test PostgreSQL 15+ feature detection."""

    def test_features_pg14(self):
        """PostgreSQL 14 does not support PG15+ features."""
        dialect = PostgresDialect((14, 0, 0))
        assert dialect.supports_nulls_not_distinct_unique() is False
        assert dialect.supports_regexp_like() is False
        assert dialect.supports_random_normal() is False
        assert dialect.supports_json_table_nested_path() is False
        assert dialect.supports_merge_with_cte() is False
        assert dialect.supports_update_returning_old() is False

    def test_features_pg15(self):
        """PostgreSQL 15 adds MERGE and NULLS NOT DISTINCT."""
        dialect = PostgresDialect((15, 0, 0))
        assert dialect.supports_nulls_not_distinct_unique() is True
        assert dialect.supports_regexp_like() is False
        assert dialect.supports_random_normal() is False
        assert dialect.supports_json_table_nested_path() is False
        assert dialect.supports_merge_with_cte() is False
        assert dialect.supports_update_returning_old() is False

    def test_features_pg16(self):
        """PostgreSQL 16 adds REGEXP_LIKE and random_normal."""
        dialect = PostgresDialect((16, 0, 0))
        assert dialect.supports_nulls_not_distinct_unique() is True
        assert dialect.supports_regexp_like() is True
        assert dialect.supports_random_normal() is True
        assert dialect.supports_json_table_nested_path() is False
        assert dialect.supports_merge_with_cte() is False
        assert dialect.supports_update_returning_old() is False

    def test_features_pg17(self):
        """PostgreSQL 17 adds enhanced JSON_TABLE, MERGE with CTE."""
        dialect = PostgresDialect((17, 0, 0))
        assert dialect.supports_nulls_not_distinct_unique() is True
        assert dialect.supports_regexp_like() is True
        assert dialect.supports_random_normal() is True
        assert dialect.supports_json_table_nested_path() is True
        assert dialect.supports_merge_with_cte() is True
        assert dialect.supports_update_returning_old() is True


class TestIndexFeatures:
    """Test index feature detection."""

    def test_index_features_pg15(self):
        """PostgreSQL 15 index features."""
        dialect = PostgresDialect((15, 0, 0))
        assert dialect.supports_concurrent_unique_nulls_not_distinct() is False
        assert dialect.supports_streaming_btree_index_build() is False

    def test_index_features_pg16(self):
        """PostgreSQL 16 adds concurrent unique nulls not distinct."""
        dialect = PostgresDialect((16, 0, 0))
        assert dialect.supports_concurrent_unique_nulls_not_distinct() is True
        assert dialect.supports_streaming_btree_index_build() is False

    def test_index_features_pg18(self):
        """PostgreSQL 18 adds streaming btree index build."""
        dialect = PostgresDialect((18, 0, 0))
        assert dialect.supports_concurrent_unique_nulls_not_distinct() is True
        assert dialect.supports_streaming_btree_index_build() is True


class TestDialectProtocols:
    """Test that dialect implements all required protocols."""

    def test_dialect_implements_parallel_query_protocol(self):
        """Dialect should implement PostgresParallelQuerySupport protocol."""
        from rhosocial.activerecord.backend.impl.postgres.protocols import (
            PostgresParallelQuerySupport
        )
        dialect = PostgresDialect((14, 0, 0))
        assert isinstance(dialect, PostgresParallelQuerySupport)

    def test_dialect_implements_stored_procedure_protocol(self):
        """Dialect should implement PostgresStoredProcedureSupport protocol."""
        from rhosocial.activerecord.backend.impl.postgres.protocols import (
            PostgresStoredProcedureSupport
        )
        dialect = PostgresDialect((14, 0, 0))
        assert isinstance(dialect, PostgresStoredProcedureSupport)

    def test_dialect_implements_extended_statistics_protocol(self):
        """Dialect should implement PostgresExtendedStatisticsSupport protocol."""
        from rhosocial.activerecord.backend.impl.postgres.protocols import (
            PostgresExtendedStatisticsSupport
        )
        dialect = PostgresDialect((14, 0, 0))
        assert isinstance(dialect, PostgresExtendedStatisticsSupport)
