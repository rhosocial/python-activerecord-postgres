# tests/rhosocial/activerecord_postgres_test/feature/backend/postgres/test_ddl_protocols.py
"""Tests for PostgreSQL DDL protocol feature detection.

This module tests the protocol-based feature detection methods:
- PostgresPartitionSupport
- PostgresCommentSupport
- PostgresIndexSupport
- PostgresMaterializedViewSupport
"""
import pytest  # noqa: F401

from rhosocial.activerecord.backend.impl.postgres.dialect import PostgresDialect
from rhosocial.activerecord.backend.impl.postgres.mixins.ddl.partition import PostgresPartitionMixin


class TestPostgresPartitionSupportFeatureDetection:
    """Test PostgresPartitionSupport feature detection methods."""

    def test_supports_table_partitioning_pg9(self):
        """Declarative table partitioning requires PG 10+."""
        dialect = PostgresDialect(version=(9, 6, 0))
        assert dialect.supports_table_partitioning() is False

    def test_supports_table_partitioning_pg10(self):
        """PostgreSQL 10 supports declarative table partitioning."""
        dialect = PostgresDialect(version=(10, 0, 0))
        assert dialect.supports_table_partitioning() is True

    def test_supports_hash_table_partitioning_pg10(self):
        """HASH partitioning requires PG 11+."""
        dialect = PostgresDialect(version=(10, 0, 0))
        assert dialect.supports_hash_table_partitioning() is False

    def test_supports_hash_table_partitioning_pg11(self):
        """PostgreSQL 11 supports HASH partitioning."""
        dialect = PostgresDialect(version=(11, 0, 0))
        assert dialect.supports_hash_table_partitioning() is True

    def test_supports_partitioned_table_creation_pg9(self):
        """Declarative partitioned table creation requires PG 10+."""
        dialect = PostgresDialect(version=(9, 6, 0))
        assert dialect.supports_partitioned_table_creation() is False

    def test_supports_partitioned_table_creation_pg10(self):
        """PostgreSQL 10 supports partitioned parent table creation."""
        dialect = PostgresDialect(version=(10, 0, 0))
        assert dialect.supports_partitioned_table_creation() is True

    def test_supports_range_and_list_partitioning_pg10(self):
        """PostgreSQL 10 supports RANGE and LIST partitioning."""
        dialect = PostgresDialect(version=(10, 0, 0))
        assert dialect.supports_range_table_partitioning() is True
        assert dialect.supports_list_table_partitioning() is True

    def test_subpartitioning_not_exposed_yet(self):
        """Nested partitioning is not exposed by this API yet."""
        dialect = PostgresDialect(version=(14, 0, 0))
        assert dialect.supports_subpartitioning() is False

    def test_partition_introspection_pg9(self):
        """Partition metadata introspection follows declarative partitioning support."""
        dialect = PostgresDialect(version=(9, 6, 0))
        assert dialect.supports_partition_metadata_introspection() is False

    def test_partition_introspection_pg10(self):
        """PostgreSQL 10 supports partition metadata through pg_catalog."""
        dialect = PostgresDialect(version=(10, 0, 0))
        assert dialect.supports_partition_metadata_introspection() is True

    def test_partition_maintenance_capabilities_pg9(self):
        """Generic partition maintenance capabilities require PG 10+."""
        dialect = PostgresDialect(version=(9, 6, 0))
        assert dialect.supports_add_partition() is False
        assert dialect.supports_drop_partition() is False
        assert dialect.supports_truncate_partition() is False
        assert dialect.supports_attach_partition() is False
        assert dialect.supports_detach_partition() is False

    def test_partition_maintenance_capabilities_pg10(self):
        """PostgreSQL maps generic maintenance capabilities to PG operations."""
        dialect = PostgresDialect(version=(10, 0, 0))
        assert dialect.supports_add_partition() is True
        assert dialect.supports_drop_partition() is True
        assert dialect.supports_truncate_partition() is True
        assert dialect.supports_attach_partition() is True
        assert dialect.supports_detach_partition() is True
        assert dialect.supports_reorganize_partition() is False

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

    def test_supports_concurrent_attach_pg13(self):
        """CONCURRENTLY ATTACH requires PG 14+."""
        dialect = PostgresDialect(version=(13, 0, 0))
        assert dialect.supports_concurrent_attach() is False

    def test_supports_concurrent_attach_pg14(self):
        """PostgreSQL 14 supports CONCURRENTLY ATTACH."""
        dialect = PostgresDialect(version=(14, 0, 0))
        assert dialect.supports_concurrent_attach() is True

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


class TestPostgresPartitionMixinDirect:
    """Test PostgresPartitionMixin partition capability methods directly."""

    class _Host:
        version = (15, 0, 0)

    class _Pg10Host:
        version = (10, 0, 0)

    class _LowHost:
        version = (9, 6, 0)

    class _PartitionMixin(_Host, PostgresPartitionMixin):
        pass

    class _PartitionMixinPg10(_Pg10Host, PostgresPartitionMixin):
        pass

    class _PartitionMixinLow(_LowHost, PostgresPartitionMixin):
        pass

    def test_supports_table_partitioning_direct(self):
        """PostgreSQL 15 supports declarative table partitioning."""
        assert self._PartitionMixin().supports_table_partitioning() is True

    def test_supports_table_partitioning_low(self):
        """PostgreSQL 9.6 does not support declarative table partitioning."""
        assert self._PartitionMixinLow().supports_table_partitioning() is False

    def test_supports_partitioned_table_creation_direct(self):
        """Partitioned parent table creation follows table partitioning."""
        assert self._PartitionMixin().supports_partitioned_table_creation() is True
        assert self._PartitionMixinLow().supports_partitioned_table_creation() is False

    def test_supports_range_and_list_partitioning_direct(self):
        """PostgreSQL 10 supports RANGE and LIST partitioning."""
        mixin = self._PartitionMixinPg10()
        assert mixin.supports_range_table_partitioning() is True
        assert mixin.supports_list_table_partitioning() is True

    def test_supports_hash_table_partitioning_direct(self):
        """HASH table partitioning follows PostgreSQL 11+ support."""
        assert self._PartitionMixin().supports_hash_table_partitioning() is True
        assert self._PartitionMixinPg10().supports_hash_table_partitioning() is False

    def test_subpartitioning_not_exposed_direct(self):
        """Nested partitioning is not exposed by this API yet."""
        assert self._PartitionMixin().supports_subpartitioning() is False

    def test_partition_introspection_direct(self):
        """Partition metadata introspection follows table partitioning support."""
        assert self._PartitionMixin().supports_partition_metadata_introspection() is True
        assert self._PartitionMixinLow().supports_partition_metadata_introspection() is False

    def test_partition_maintenance_capabilities_direct(self):
        """Generic partition maintenance capabilities map to PostgreSQL operations."""
        mixin = self._PartitionMixin()
        assert mixin.supports_add_partition() is True
        assert mixin.supports_drop_partition() is True
        assert mixin.supports_truncate_partition() is True
        assert mixin.supports_attach_partition() is True
        assert mixin.supports_detach_partition() is True
        assert mixin.supports_reorganize_partition() is False

    def test_supports_concurrent_attach_direct(self):
        """CONCURRENTLY ATTACH follows PG version."""
        assert self._PartitionMixin().supports_concurrent_attach() is True
        assert self._PartitionMixinPg10().supports_concurrent_attach() is False


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