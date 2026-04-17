# tests/rhosocial/activerecord_postgres_test/feature/backend/postgres/test_ddl_protocols.py
"""Tests for PostgreSQL DDL protocol feature detection.

This module tests the protocol-based feature detection methods:
- PostgresPartitionSupport
- PostgresCommentSupport
- PostgresIndexSupport
- PostgresMaterializedViewSupport
"""
import pytest

from rhosocial.activerecord.backend.impl.postgres.dialect import PostgresDialect


class TestPostgresPartitionSupportFeatureDetection:
    """Test PostgresPartitionSupport feature detection methods."""

    def test_supports_hash_partitioning_pg10(self):
        """HASH partitioning requires PG 11+."""
        dialect = PostgresDialect(version=(10, 0, 0))
        assert dialect.supports_hash_partitioning() is False

    def test_supports_hash_partitioning_pg11(self):
        """PostgreSQL 11 supports HASH partitioning."""
        dialect = PostgresDialect(version=(11, 0, 0))
        assert dialect.supports_hash_partitioning() is True

    def test_supports_default_partition_pg10(self):
        """DEFAULT partition requires PG 11+."""
        dialect = PostgresDialect(version=(10, 0, 0))
        assert dialect.supports_default_partition() is False

    def test_supports_default_partition_pg11(self):
        """PostgreSQL 11 supports DEFAULT partition."""
        dialect = PostgresDialect(version=(11, 0, 0))
        assert dialect.supports_default_partition() is True

    def test_supports_partition_key_update_pg10(self):
        """Partition key update requires PG 11+."""
        dialect = PostgresDialect(version=(10, 0, 0))
        assert dialect.supports_partition_key_update() is False

    def test_supports_partition_key_update_pg11(self):
        """PostgreSQL 11 supports partition key update."""
        dialect = PostgresDialect(version=(11, 0, 0))
        assert dialect.supports_partition_key_update() is True

    def test_supports_concurrent_detach_pg13(self):
        """CONCURRENTLY DETACH requires PG 14+."""
        dialect = PostgresDialect(version=(13, 0, 0))
        assert dialect.supports_concurrent_detach() is False

    def test_supports_concurrent_detach_pg14(self):
        """PostgreSQL 14 supports CONCURRENTLY DETACH."""
        dialect = PostgresDialect(version=(14, 0, 0))
        assert dialect.supports_concurrent_detach() is True

    def test_supports_partition_bounds_expression_pg11(self):
        """Partition bounds expression requires PG 12+."""
        dialect = PostgresDialect(version=(11, 0, 0))
        assert dialect.supports_partition_bounds_expression() is False

    def test_supports_partition_bounds_expression_pg12(self):
        """PostgreSQL 12 supports partition bounds expression."""
        dialect = PostgresDialect(version=(12, 0, 0))
        assert dialect.supports_partition_bounds_expression() is True

    def test_supports_partitionwise_join_pg10(self):
        """Partitionwise join requires PG 11+."""
        dialect = PostgresDialect(version=(10, 0, 0))
        assert dialect.supports_partitionwise_join() is False

    def test_supports_partitionwise_join_pg11(self):
        """PostgreSQL 11 supports partitionwise join."""
        dialect = PostgresDialect(version=(11, 0, 0))
        assert dialect.supports_partitionwise_join() is True

    def test_supports_partitionwise_aggregate_pg10(self):
        """Partitionwise aggregate requires PG 11+."""
        dialect = PostgresDialect(version=(10, 0, 0))
        assert dialect.supports_partitionwise_aggregate() is False

    def test_supports_partitionwise_aggregate_pg11(self):
        """PostgreSQL 11 supports partitionwise aggregate."""
        dialect = PostgresDialect(version=(11, 0, 0))
        assert dialect.supports_partitionwise_aggregate() is True


class TestPostgresIndexSupportFeatureDetection:
    """Test PostgresIndexSupport feature detection methods."""

    def test_supports_safe_hash_index_pg9(self):
        """Hash index WAL requires PG 10+."""
        dialect = PostgresDialect(version=(9, 0, 0))
        assert dialect.supports_safe_hash_index() is False

    def test_supports_safe_hash_index_pg10(self):
        """PostgreSQL 10 supports safe hash indexes."""
        dialect = PostgresDialect(version=(10, 0, 0))
        assert dialect.supports_safe_hash_index() is True

    def test_supports_parallel_create_index_pg10(self):
        """Parallel CREATE INDEX requires PG 11+."""
        dialect = PostgresDialect(version=(10, 0, 0))
        assert dialect.supports_parallel_create_index() is False

    def test_supports_parallel_create_index_pg11(self):
        """PostgreSQL 11 supports parallel CREATE INDEX."""
        dialect = PostgresDialect(version=(11, 0, 0))
        assert dialect.supports_parallel_create_index() is True

    def test_supports_gist_include_pg11(self):
        """GiST INCLUDE requires PG 12+."""
        dialect = PostgresDialect(version=(11, 0, 0))
        assert dialect.supports_gist_include() is False

    def test_supports_gist_include_pg12(self):
        """PostgreSQL 12 supports GiST INCLUDE."""
        dialect = PostgresDialect(version=(12, 0, 0))
        assert dialect.supports_gist_include() is True

    def test_supports_reindex_concurrently_pg11(self):
        """REINDEX CONCURRENTLY requires PG 12+."""
        dialect = PostgresDialect(version=(11, 0, 0))
        assert dialect.supports_reindex_concurrently() is False

    def test_supports_reindex_concurrently_pg12(self):
        """PostgreSQL 12 supports REINDEX CONCURRENTLY."""
        dialect = PostgresDialect(version=(12, 0, 0))
        assert dialect.supports_reindex_concurrently() is True

    def test_supports_btree_deduplication_pg12(self):
        """B-tree deduplication requires PG 13+."""
        dialect = PostgresDialect(version=(12, 0, 0))
        assert dialect.supports_btree_deduplication() is False

    def test_supports_btree_deduplication_pg13(self):
        """PostgreSQL 13 supports B-tree deduplication."""
        dialect = PostgresDialect(version=(13, 0, 0))
        assert dialect.supports_btree_deduplication() is True

    def test_supports_brin_multivalue_pg13(self):
        """BRIN multivalue requires PG 14+."""
        dialect = PostgresDialect(version=(13, 0, 0))
        assert dialect.supports_brin_multivalue() is False

    def test_supports_brin_multivalue_pg14(self):
        """PostgreSQL 14 supports BRIN multivalue."""
        dialect = PostgresDialect(version=(14, 0, 0))
        assert dialect.supports_brin_multivalue() is True

    def test_supports_brin_bloom_pg13(self):
        """BRIN bloom requires PG 14+."""
        dialect = PostgresDialect(version=(13, 0, 0))
        assert dialect.supports_brin_bloom() is False

    def test_supports_brin_bloom_pg14(self):
        """PostgreSQL 14 supports BRIN bloom."""
        dialect = PostgresDialect(version=(14, 0, 0))
        assert dialect.supports_brin_bloom() is True

    def test_supports_spgist_include_pg13(self):
        """SP-GiST INCLUDE requires PG 14+."""
        dialect = PostgresDialect(version=(13, 0, 0))
        assert dialect.supports_spgist_include() is False

    def test_supports_spgist_include_pg14(self):
        """PostgreSQL 14 supports SP-GiST INCLUDE."""
        dialect = PostgresDialect(version=(14, 0, 0))
        assert dialect.supports_spgist_include() is True


class TestPostgresMaterializedViewSupportFeatureDetection:
    """Test PostgresMaterializedViewSupport feature detection methods."""

    def test_supports_materialized_view_concurrent_refresh_pg93(self):
        """CONCURRENTLY refresh requires PG 9.4+."""
        dialect = PostgresDialect(version=(9, 3, 0))
        assert dialect.supports_materialized_view_concurrent_refresh() is False

    def test_supports_materialized_view_concurrent_refresh_pg94(self):
        """PostgreSQL 9.4 supports CONCURRENTLY refresh."""
        dialect = PostgresDialect(version=(9, 4, 0))
        assert dialect.supports_materialized_view_concurrent_refresh() is True


class TestPostgresVacuumFeatureDetection:
    """Test PostgresVacuumMixin feature detection methods."""

    def test_supports_parallel_vacuum_pg12(self):
        """Parallel VACUUM requires PG 13+."""
        dialect = PostgresDialect(version=(12, 0, 0))
        assert dialect.supports_parallel_vacuum() is False

    def test_supports_parallel_vacuum_pg13(self):
        """PostgreSQL 13 supports parallel VACUUM."""
        dialect = PostgresDialect(version=(13, 0, 0))
        assert dialect.supports_parallel_vacuum() is True

    def test_supports_index_cleanup_auto_pg13(self):
        """INDEX_CLEANUP requires PG 14+."""
        dialect = PostgresDialect(version=(13, 0, 0))
        assert dialect.supports_index_cleanup_auto() is False

    def test_supports_index_cleanup_auto_pg14(self):
        """PostgreSQL 14 supports INDEX_CLEANUP."""
        dialect = PostgresDialect(version=(14, 0, 0))
        assert dialect.supports_index_cleanup_auto() is True

    def test_supports_vacuum_process_toast_pg13(self):
        """PROCESS_TOAST requires PG 14+."""
        dialect = PostgresDialect(version=(13, 0, 0))
        assert dialect.supports_vacuum_process_toast() is False

    def test_supports_vacuum_process_toast_pg14(self):
        """PostgreSQL 14 supports PROCESS_TOAST."""
        dialect = PostgresDialect(version=(14, 0, 0))
        assert dialect.supports_vacuum_process_toast() is True


class TestProtocolRuntimeCheckable:
    """Test that protocols are runtime checkable."""

    def test_postgres_partition_support_is_runtime_checkable(self):
        """PostgresPartitionSupport should be runtime checkable."""
        from rhosocial.activerecord.backend.impl.postgres.protocols.ddl import (
            PostgresPartitionSupport,
        )
        dialect = PostgresDialect(version=(14, 0, 0))
        assert isinstance(dialect, PostgresPartitionSupport)

    def test_postgres_comment_support_is_runtime_checkable(self):
        """PostgresCommentSupport should be runtime checkable."""
        from rhosocial.activerecord.backend.impl.postgres.protocols.ddl import (
            PostgresCommentSupport,
        )
        dialect = PostgresDialect(version=(14, 0, 0))
        assert isinstance(dialect, PostgresCommentSupport)

    def test_postgres_index_support_is_runtime_checkable(self):
        """PostgresIndexSupport should be runtime checkable."""
        from rhosocial.activerecord.backend.impl.postgres.protocols.ddl import (
            PostgresIndexSupport,
        )
        dialect = PostgresDialect(version=(14, 0, 0))
        assert isinstance(dialect, PostgresIndexSupport)

    def test_postgres_materialized_view_support_is_runtime_checkable(self):
        """PostgresMaterializedViewSupport should be runtime checkable."""
        from rhosocial.activerecord.backend.impl.postgres.protocols import (
            PostgresMaterializedViewSupport,
        )
        dialect = PostgresDialect(version=(14, 0, 0))
        assert isinstance(dialect, PostgresMaterializedViewSupport)