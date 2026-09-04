# ddl tests

PostgreSQL DDL execution coverage: auto-increment / defaults DDL regressions, partition advanced operations, EXPLAIN on partitioned tables, and partition operations including pg_partman and production-style time partitioning.

## Key files

- `test_auto_increment_ddl.py` — auto-increment / boolean default / timestamp regressions
- `test_partition_advanced_operations.py` — advanced partition capabilities
- `test_partition_explain.py` — EXPLAIN on partitioned tables
- `test_partition_operations.py` — partition operations incl. pg_partman
