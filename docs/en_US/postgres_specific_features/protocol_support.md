# PostgreSQL Protocol Support Matrix

This document provides a comprehensive matrix of SQL dialect protocols supported by the PostgreSQL backend, along with version-specific feature support.

## Overview

The PostgreSQL dialect implements numerous protocols from the `rhosocial.activerecord.backend.dialect.protocols` module, providing fine-grained feature detection and graceful error handling for database-specific functionality.

## Protocol Support Matrix

### Query-Related Protocols

| Protocol | Support Status | Version Requirements | Notes |
|----------|---------------|---------------------|-------|
| **WindowFunctionSupport** | ✅ Full | ≥ 8.4 | Window functions and frame clauses supported |
| **CTESupport** | ✅ Full | ≥ 8.4 | Basic and recursive CTEs supported; MATERIALIZED hint ≥ 12 |
| **SetOperationSupport** | ✅ Full | All versions | UNION, UNION ALL, INTERSECT, EXCEPT fully supported |
| **WildcardSupport** | ✅ Full | All versions | SELECT * syntax supported |
| **JoinSupport** | ✅ Full | All versions | All join types supported (INNER, LEFT, RIGHT, FULL, CROSS, NATURAL) |
| **FilterClauseSupport** | ✅ Full | ≥ 9.4 | FILTER clause for aggregate functions |
| **ReturningSupport** | ✅ Full | ≥ 8.2 | RETURNING clause for INSERT/UPDATE/DELETE |
| **UpsertSupport** | ✅ Full | ≥ 9.5 | ON CONFLICT DO UPDATE/NOTHING syntax |
| **LateralJoinSupport** | ✅ Full | ≥ 9.3 | LATERAL joins |
| **ArraySupport** | ✅ Full | All versions | Native array types and operations |
| **JSONSupport** | ✅ Full | ≥ 9.2 | JSON type; JSONB ≥ 9.4; JSON_TABLE ≥ 12 |
| **ExplainSupport** | ✅ Full | All versions | EXPLAIN and EXPLAIN ANALYZE with multiple formats |
| **GraphSupport** | ❌ None | N/A | No native graph query MATCH clause |
| **LockingSupport** | ✅ Partial | ≥ 9.5 | FOR UPDATE; SKIP LOCKED ≥ 9.5 |
| **MergeSupport** | ✅ Full | ≥ 15.0 | MERGE statement |
| **OrderedSetAggregationSupport** | ✅ Full | ≥ 9.4 | Ordered-set aggregate functions |
| **TemporalTableSupport** | ❌ None | N/A | No built-in temporal tables |
| **QualifyClauseSupport** | ❌ None | N/A | No QUALIFY clause |

### DDL-Related Protocols

| Protocol | Support Status | Version Requirements | Notes |
|----------|---------------|---------------------|-------|
| **TableSupport** | ✅ Full | All versions | CREATE TABLE, DROP TABLE, ALTER TABLE with all features |
| **ViewSupport** | ✅ Full | All versions | CREATE VIEW, DROP VIEW with all options |
| **TruncateSupport** | ✅ Full | All versions | TRUNCATE TABLE; RESTART IDENTITY ≥ 8.4; CASCADE |
| **SchemaSupport** | ✅ Full | All versions | CREATE SCHEMA, DROP SCHEMA with all options |
| **IndexSupport** | ✅ Full | All versions | CREATE INDEX, DROP INDEX with all features |
| **SequenceSupport** | ✅ Full | All versions | CREATE SEQUENCE, DROP SEQUENCE |

## Detailed Feature Support by Protocol

### SetOperationSupport

PostgreSQL provides comprehensive support for SQL set operations:

| Feature | Support | Version | Description |
|---------|---------|---------|-------------|
| `supports_union()` | ✅ | All | UNION operation |
| `supports_union_all()` | ✅ | All | UNION ALL operation |
| `supports_intersect()` | ✅ | All | INTERSECT operation |
| `supports_except()` | ✅ | All | EXCEPT operation |
| `supports_set_operation_order_by()` | ✅ | All | ORDER BY clause in set operations |
| `supports_set_operation_limit_offset()` | ✅ | All | LIMIT/OFFSET in set operations |
| `supports_set_operation_for_update()` | ✅ | All | FOR UPDATE clause in set operations |

**Example:**
```python
from rhosocial.activerecord.backend.impl.postgres import PostgresDialect

dialect = PostgresDialect()
assert dialect.supports_union()  # True
assert dialect.supports_intersect()  # True
assert dialect.supports_except()  # True
```

### TruncateSupport

PostgreSQL supports TRUNCATE TABLE with several database-specific features:

| Feature | Support | Version | Description |
|---------|---------|---------|-------------|
| `supports_truncate()` | ✅ | All | TRUNCATE statement |
| `supports_truncate_table_keyword()` | ✅ | All | TABLE keyword (optional but supported) |
| `supports_truncate_restart_identity()` | ✅ | ≥ 8.4 | RESTART IDENTITY to reset sequences |
| `supports_truncate_cascade()` | ✅ | All | CASCADE to truncate dependent tables |

**Version-Specific Behavior:**

- **PostgreSQL < 8.4**: Basic TRUNCATE TABLE only
- **PostgreSQL ≥ 8.4**: Adds RESTART IDENTITY support

**Example:**
```python
from rhosocial.activerecord.backend.impl.postgres import PostgresDialect
from rhosocial.activerecord.backend.expression.statements import TruncateExpression

# Basic truncate
dialect = PostgresDialect()
expr = TruncateExpression(dialect, table_name="users")
sql, params = dialect.format_truncate_statement(expr)
# Result: TRUNCATE TABLE "users"

# With RESTART IDENTITY (PostgreSQL 8.4+)
expr = TruncateExpression(dialect, table_name="users", restart_identity=True)
sql, params = dialect.format_truncate_statement(expr)
# Result: TRUNCATE TABLE "users" RESTART IDENTITY

# With CASCADE
expr = TruncateExpression(dialect, table_name="orders", cascade=True)
sql, params = dialect.format_truncate_statement(expr)
# Result: TRUNCATE TABLE "orders" CASCADE
```

### CTESupport

Common Table Expressions with version-dependent features:

| Feature | Support | Version | Description |
|---------|---------|---------|-------------|
| `supports_basic_cte()` | ✅ | ≥ 8.4 | Basic CTEs (WITH clause) |
| `supports_recursive_cte()` | ✅ | ≥ 8.4 | Recursive CTEs (WITH RECURSIVE) |
| `supports_materialized_cte()` | ✅ | ≥ 12.0 | MATERIALIZED/NOT MATERIALIZED hints |

### WindowFunctionSupport

Full window function support since PostgreSQL 8.4:

| Feature | Support | Version | Description |
|---------|---------|---------|-------------|
| `supports_window_functions()` | ✅ | ≥ 8.4 | Window functions (OVER clause) |
| `supports_window_frame_clause()` | ✅ | ≥ 8.4 | ROWS/RANGE frame clauses |

### JSONSupport

JSON functionality with progressive version support:

| Feature | Support | Version | Description |
|---------|---------|---------|-------------|
| `supports_json_type()` | ✅ | ≥ 9.2 | JSON data type |
| `supports_jsonb()` | ✅ | ≥ 9.4 | JSONB data type (binary format) |
| `supports_json_table()` | ✅ | ≥ 12.0 | JSON_TABLE function |

### AdvancedGroupingSupport

Advanced grouping constructs:

| Feature | Support | Version | Description |
|---------|---------|---------|-------------|
| `supports_rollup()` | ✅ | ≥ 9.5 | ROLLUP grouping |
| `supports_cube()` | ✅ | ≥ 9.5 | CUBE grouping |
| `supports_grouping_sets()` | ✅ | ≥ 9.5 | GROUPING SETS |

### UpsertSupport

INSERT ... ON CONFLICT support:

| Feature | Support | Version | Description |
|---------|---------|---------|-------------|
| `supports_upsert()` | ✅ | ≥ 9.5 | UPSERT via ON CONFLICT |
| `get_upsert_syntax_type()` | ✅ | ≥ 9.5 | Returns "ON CONFLICT" |

### LateralJoinSupport

LATERAL join support:

| Feature | Support | Version | Description |
|---------|---------|---------|-------------|
| `supports_lateral_join()` | ✅ | ≥ 9.3 | LATERAL joins |

### LockingSupport

Row-level locking features:

| Feature | Support | Version | Description |
|---------|---------|---------|-------------|
| `supports_for_update_skip_locked()` | ✅ | ≥ 9.5 | FOR UPDATE SKIP LOCKED |

### MergeSupport

MERGE statement support:

| Feature | Support | Version | Description |
|---------|---------|---------|-------------|
| `supports_merge_statement()` | ✅ | ≥ 15.0 | MERGE statement |

### OrderedSetAggregationSupport

Ordered-set aggregate functions:

| Feature | Support | Version | Description |
|---------|---------|---------|-------------|
| `supports_ordered_set_aggregation()` | ✅ | ≥ 9.4 | PERCENTILE_CONT, PERCENTILE_DISC, etc. |

## PostgreSQL-Specific Protocols

In addition to standard protocols, PostgreSQL provides database-specific protocols for native features and extension support.

### Extension Protocols

| Protocol | Extension | Description | Documentation |
|----------|-----------|-------------|---------------|
| **PostgresExtensionSupport** | - | Extension detection and management | [PostgreSQL Extensions](https://www.postgresql.org/docs/current/extend.html) |
| **PostgresPgvectorSupport** | pgvector | Vector similarity search | [pgvector GitHub](https://github.com/pgvector/pgvector) |
| **PostgresPostGISSupport** | postgis | Spatial database functionality | [PostGIS Docs](https://postgis.net/docs/) |
| **PostgresPgTrgmSupport** | pg_trgm | Trigram similarity search | [pg_trgm Docs](https://www.postgresql.org/docs/current/pgtrgm.html) |
| **PostgresHstoreSupport** | hstore | Key-value pair storage | [hstore Docs](https://www.postgresql.org/docs/current/hstore.html) |
| **PostgresLtreeSupport** | ltree | Label tree for hierarchical data | [ltree Docs](https://www.postgresql.org/docs/current/ltree.html) |
| **PostgresIntarraySupport** | intarray | Integer array operators | [intarray Docs](https://www.postgresql.org/docs/current/intarray.html) |
| **PostgresEarthdistanceSupport** | earthdistance | Great-circle distance | [earthdistance Docs](https://www.postgresql.org/docs/current/earthdistance.html) |
| **PostgresTablefuncSupport** | tablefunc | Crosstab and connectby | [tablefunc Docs](https://www.postgresql.org/docs/current/tablefunc.html) |
| **PostgresPgStatStatementsSupport** | pg_stat_statements | Query execution statistics | [pg_stat_statements Docs](https://www.postgresql.org/docs/current/pgstatstatements.html) |

### Native Feature Protocols

| Protocol | Description | Min Version | Documentation |
|----------|-------------|-------------|---------------|
| **PostgresPartitionSupport** | Advanced partitioning features | PG 11+ | [Partitioning Docs](https://www.postgresql.org/docs/current/ddl-partitioning.html) |
| **PostgresIndexSupport** | Index enhancements | PG 10+ | [Index Docs](https://www.postgresql.org/docs/current/indexes.html) |
| **PostgresVacuumSupport** | VACUUM improvements | PG 13+ | [VACUUM Docs](https://www.postgresql.org/docs/current/sql-vacuum.html) |
| **PostgresQueryOptimizationSupport** | Query optimization features | PG 11+ | [Query Docs](https://www.postgresql.org/docs/current/runtime-config-query.html) |
| **PostgresDataTypeSupport** | Data type enhancements | PG 11+ | [Data Types Docs](https://www.postgresql.org/docs/current/datatype.html) |
| **PostgresSQLSyntaxSupport** | SQL syntax enhancements | PG 12+ | [SQL Syntax Docs](https://www.postgresql.org/docs/current/sql-syntax.html) |
| **PostgresLogicalReplicationSupport** | Logical replication features | PG 10+ | [Replication Docs](https://www.postgresql.org/docs/current/logical-replication.html) |
| **PostgresMaterializedViewSupport** | Materialized views | PG 9.3+ | [MV Docs](https://www.postgresql.org/docs/current/rules-materializedviews.html) |
| **PostgresTableSupport** | Table-specific features | All | [Table Docs](https://www.postgresql.org/docs/current/ddl.html) |

## Detailed Native Feature Support

### PostgresPartitionSupport

**Feature Source**: Native support (no extension required)

PostgreSQL partitioning features beyond SQL standard:

| Feature | Support | Version | Description |
|---------|---------|---------|-------------|
| `supports_hash_partitioning()` | ✅ | ≥ 11.0 | HASH partitioning method |
| `supports_default_partition()` | ✅ | ≥ 11.0 | DEFAULT partition for non-matching rows |
| `supports_partition_key_update()` | ✅ | ≥ 11.0 | Automatic row movement on key update |
| `supports_concurrent_detach()` | ✅ | ≥ 14.0 | Non-blocking partition detachment |
| `supports_partition_bounds_expression()` | ✅ | ≥ 12.0 | Expressions in partition bounds |
| `supports_partitionwise_join()` | ✅ | ≥ 11.0 | Partitionwise join optimization |
| `supports_partitionwise_aggregate()` | ✅ | ≥ 11.0 | Partitionwise aggregate optimization |

**Official Documentation**: https://www.postgresql.org/docs/current/ddl-partitioning.html

### PostgresIndexSupport

**Feature Source**: Native support (no extension required)

PostgreSQL index features beyond standard SQL:

| Feature | Support | Version | Description |
|---------|---------|---------|-------------|
| `supports_safe_hash_index()` | ✅ | ≥ 10.0 | Hash indexes are WAL-logged (crash-safe) |
| `supports_parallel_create_index()` | ✅ | ≥ 11.0 | Parallel B-tree index build |
| `supports_gist_include()` | ✅ | ≥ 12.0 | INCLUDE clause for GiST indexes |
| `supports_reindex_concurrently()` | ✅ | ≥ 12.0 | Non-blocking index rebuild |
| `supports_btree_deduplication()` | ✅ | ≥ 13.0 | B-tree duplicate compression |
| `supports_brin_multivalue()` | ✅ | ≥ 14.0 | BRIN multi-value min/max |
| `supports_brin_bloom()` | ✅ | ≥ 14.0 | BRIN bloom filter indexes |
| `supports_spgist_include()` | ✅ | ≥ 14.0 | INCLUDE clause for SP-GiST indexes |

**Official Documentation**: https://www.postgresql.org/docs/current/indexes.html

### PostgresVacuumSupport

**Feature Source**: Native support (no extension required)

| Feature | Support | Version | Description |
|---------|---------|---------|-------------|
| `supports_parallel_vacuum()` | ✅ | ≥ 13.0 | Parallel VACUUM for indexes |
| `supports_index_cleanup_auto()` | ✅ | ≥ 14.0 | INDEX_CLEANUP AUTO option |
| `supports_vacuum_process_toast()` | ✅ | ≥ 14.0 | PROCESS_TOAST control |

**Official Documentation**: https://www.postgresql.org/docs/current/sql-vacuum.html

### PostgresQueryOptimizationSupport

**Feature Source**: Native support (no extension required)

| Feature | Support | Version | Description |
|---------|---------|---------|-------------|
| `supports_jit()` | ✅ | ≥ 11.0 | JIT compilation (requires LLVM) |
| `supports_incremental_sort()` | ✅ | ≥ 13.0 | Incremental sort optimization |
| `supports_memoize()` | ✅ | ≥ 14.0 | Memoize execution node |
| `supports_async_foreign_scan()` | ✅ | ≥ 14.0 | Asynchronous foreign table scan |

**Official Documentation**: https://www.postgresql.org/docs/current/runtime-config-query.html

### PostgresDataTypeSupport

**Feature Source**: Native support (no extension required)

| Feature | Support | Version | Description |
|---------|---------|---------|-------------|
| `supports_multirange_type()` | ✅ | ≥ 14.0 | Multirange data types |
| `supports_domain_arrays()` | ✅ | ≥ 11.0 | Arrays of domain types |
| `supports_composite_domains()` | ✅ | ≥ 11.0 | Domains over composite types |
| `supports_jsonb_subscript()` | ✅ | ≥ 14.0 | JSONB subscript notation |
| `supports_numeric_infinity()` | ✅ | ≥ 14.0 | Infinity values in NUMERIC |
| `supports_nondeterministic_collation()` | ✅ | ≥ 12.0 | Nondeterministic ICU collations |
| `supports_xid8_type()` | ✅ | ≥ 13.0 | 64-bit transaction ID type |

**Official Documentation**: https://www.postgresql.org/docs/current/datatype.html

### PostgresSQLSyntaxSupport

**Feature Source**: Native support (no extension required)

| Feature | Support | Version | Description |
|---------|---------|---------|-------------|
| `supports_generated_columns()` | ✅ | ≥ 12.0 | GENERATED ALWAYS AS columns |
| `supports_cte_search_cycle()` | ✅ | ≥ 14.0 | CTE SEARCH/CYCLE clauses |
| `supports_fetch_with_ties()` | ✅ | ≥ 13.0 | FETCH FIRST WITH TIES |

**Official Documentation**: https://www.postgresql.org/docs/current/sql-syntax.html

### PostgresLogicalReplicationSupport

**Feature Source**: Native support (no extension required)

| Feature | Support | Version | Description |
|---------|---------|---------|-------------|
| `supports_commit_timestamp()` | ✅ | ≥ 10.0 | Transaction commit timestamp tracking |
| `supports_streaming_transactions()` | ✅ | ≥ 14.0 | Streaming in-progress transactions |
| `supports_two_phase_decoding()` | ✅ | ≥ 14.0 | Two-phase commit decoding |
| `supports_binary_replication()` | ✅ | ≥ 14.0 | Binary transfer mode |

**Official Documentation**: https://www.postgresql.org/docs/current/logical-replication.html

## Detailed Extension Support

### Extension Version Detection

The framework supports version-aware feature detection for extensions. Each extension defines minimum version requirements for its features.

#### PostgresPgvectorSupport

**Extension**: pgvector  
**Repository**: https://github.com/pgvector/pgvector

| Feature | Min Version | Description |
|---------|-------------|-------------|
| `supports_pgvector_type()` | 0.1.0 | vector data type |
| `supports_pgvector_similarity_search()` | 0.1.0 | <-> distance operator |
| `supports_pgvector_ivfflat_index()` | 0.1.0 | IVFFlat index |
| `supports_pgvector_hnsw_index()` | 0.5.0 | HNSW index (requires 0.5.0+) |

**Installation**: `CREATE EXTENSION vector;`

#### PostgresPostGISSupport

**Extension**: PostGIS  
**Website**: https://postgis.net/

| Feature | Min Version | Description |
|---------|-------------|-------------|
| `supports_postgis_geometry_type()` | 2.0 | geometry type (planar) |
| `supports_postgis_geography_type()` | 2.0 | geography type (spherical) |
| `supports_postgis_spatial_index()` | 2.0 | GiST spatial indexes |
| `supports_postgis_spatial_functions()` | 2.0 | ST_* functions |

**Installation**: `CREATE EXTENSION postgis;`

#### PostgresPgTrgmSupport

**Extension**: pg_trgm

| Feature | Min Version | Description |
|---------|-------------|-------------|
| `supports_pg_trgm_similarity()` | 1.0 | similarity() function |
| `supports_pg_trgm_index()` | 1.0 | GiST/GIN trigram indexes |

**Installation**: `CREATE EXTENSION pg_trgm;`

#### PostgresHstoreSupport

**Extension**: hstore

| Feature | Min Version | Description |
|---------|-------------|-------------|
| `supports_hstore_type()` | 1.0 | hstore data type |
| `supports_hstore_operators()` | 1.0 | ->, ->>, @>, ? operators |

**Installation**: `CREATE EXTENSION hstore;`

#### PostgresLtreeSupport

**Extension**: ltree

| Feature | Min Version | Description |
|---------|-------------|-------------|
| `supports_ltree_type()` | 1.0 | ltree data type |
| `supports_ltree_operators()` | 1.0 | <@, @>, ~ operators |
| `supports_ltree_index()` | 1.0 | GiST/B-tree indexes |

**Installation**: `CREATE EXTENSION ltree;`

#### PostgresIntarraySupport

**Extension**: intarray

| Feature | Min Version | Description |
|---------|-------------|-------------|
| `supports_intarray_operators()` | 1.0 | &&, @>, <@ operators |
| `supports_intarray_functions()` | 1.0 | uniq(), sort(), idx() |
| `supports_intarray_index()` | 1.0 | GiST index support |

**Installation**: `CREATE EXTENSION intarray;`

#### PostgresEarthdistanceSupport

**Extension**: earthdistance (depends on cube)

| Feature | Min Version | Description |
|---------|-------------|-------------|
| `supports_earthdistance_type()` | 1.0 | earth data type |
| `supports_earthdistance_operators()` | 1.0 | <@> distance operator |

**Installation**: 
```sql
CREATE EXTENSION cube;
CREATE EXTENSION earthdistance;
```

#### PostgresTablefuncSupport

**Extension**: tablefunc

| Feature | Min Version | Description |
|---------|-------------|-------------|
| `supports_tablefunc_crosstab()` | 1.0 | crosstab() pivot tables |
| `supports_tablefunc_connectby()` | 1.0 | connectby() tree queries |
| `supports_tablefunc_normal_rand()` | 1.0 | normal_rand() function |

**Installation**: `CREATE EXTENSION tablefunc;`

#### PostgresPgStatStatementsSupport

**Extension**: pg_stat_statements  
**Requires**: `shared_preload_libraries = 'pg_stat_statements'` in postgresql.conf

| Feature | Min Version | Description |
|---------|-------------|-------------|
| `supports_pg_stat_statements_view()` | 1.0 | Query statistics view |
| `supports_pg_stat_statements_reset()` | 1.0 | Reset statistics function |

**Installation**: 
1. Add to postgresql.conf: `shared_preload_libraries = 'pg_stat_statements'`
2. Restart PostgreSQL
3. Execute: `CREATE EXTENSION pg_stat_statements;`

## Extension Version-Aware Feature Detection

### Using check_extension_feature()

The framework provides a convenient method for version-aware feature detection:

```python
from rhosocial.activerecord.backend.impl.postgres import PostgresBackend

# Connect and introspect
backend = PostgresBackend(...)
backend.connect()
backend.introspect_and_adapt()

# Check extension feature with version awareness
if backend.dialect.check_extension_feature('vector', 'hnsw_index'):
    # HNSW index is supported (pgvector >= 0.5.0)
    pass

# Alternative: use specific support method
if backend.dialect.supports_pgvector_hnsw_index():
    # Same result, more explicit
    pass
```

### Extension Detection Flow

1. **Connection**: Establish connection to PostgreSQL
2. **Introspection**: Call `introspect_and_adapt()`
3. **Caching**: Extension information cached in `dialect._extensions`
4. **Detection**: Use `check_extension_feature()` or specific methods

**Example**:
```python
# Automatic detection
backend.introspect_and_adapt()

# Check installed extensions
for ext_name, info in backend.dialect._extensions.items():
    if info.installed:
        print(f"{ext_name}: {info.version} (schema: {info.schema})")

# Programmatic checks
if backend.dialect.is_extension_installed('postgis'):
    version = backend.dialect.get_extension_version('postgis')
    print(f"PostGIS version: {version}")
```

## Version Compatibility Matrix

### PostgreSQL 8.x Series

| Version | Key Features Added |
|---------|-------------------|
| 8.2 | RETURNING clause |
| 8.3 | Window functions framework |
| 8.4 | Full window functions, recursive CTEs, TRUNCATE RESTART IDENTITY |

### PostgreSQL 9.x Series

| Version | Key Features Added |
|---------|-------------------|
| 9.2 | JSON data type |
| 9.3 | LATERAL joins, materialized views |
| 9.4 | JSONB, FILTER clause, ordered-set aggregates |
| 9.5 | UPSERT (ON CONFLICT), FOR UPDATE SKIP LOCKED, ROLLUP/CUBE/GROUPING SETS |
| 9.6 | Phrase full-text search |

### PostgreSQL 10.x Series

| Version | Key Features Added |
|---------|-------------------|
| 10 | Logical replication, Declarative partitioning, Identity columns, SCRAM-SHA-256, Hash index WAL logging, Commit timestamp tracking |

### PostgreSQL 11.x Series

| Version | Key Features Added |
|---------|-------------------|
| 11 | HASH/DEFAULT partitioning, Stored procedures (OUT params), JIT compilation, Partitionwise join/aggregate, Parallel CREATE INDEX, Domain arrays, Composite domains |

### PostgreSQL 12.x Series

| Version | Key Features Added |
|---------|-------------------|
| 12 | Generated columns, MATERIALIZED CTE hints, JSON_TABLE, REINDEX CONCURRENTLY, GiST INCLUDE, Nondeterministic ICU collations, Partition bounds expressions |

### PostgreSQL 13.x Series

| Version | Key Features Added |
|---------|-------------------|
| 13 | B-tree deduplication, Parallel VACUUM, Incremental sort, FETCH WITH TIES, xid8 type |

### PostgreSQL 14.x Series

| Version | Key Features Added |
|---------|-------------------|
| 14 | Multirange types, JSONB subscript, CTE SEARCH/CYCLE, BRIN multivalue/bloom, Concurrent DETACH PARTITION, Streaming transactions, Memoize, SP-GiST INCLUDE, Numeric Infinity |

### PostgreSQL 15.x Series

| Version | Key Features Added |
|---------|-------------------|
| 15 | MERGE statement |

## Usage Examples

### Checking Protocol Support

```python
from rhosocial.activerecord.backend.impl.postgres import PostgresDialect
from rhosocial.activerecord.backend.dialect.protocols import (
    SetOperationSupport, TruncateSupport, CTESupport
)

# Create dialect for PostgreSQL 13
dialect = PostgresDialect(version=(13, 0, 0))

# Check protocol implementation
assert isinstance(dialect, SetOperationSupport)
assert isinstance(dialect, TruncateSupport)
assert isinstance(dialect, CTESupport)

# Check specific features
assert dialect.supports_truncate_restart_identity()  # True (≥ 8.4)
assert dialect.supports_materialized_cte()  # True (≥ 12)
assert dialect.supports_merge_statement()  # False (requires ≥ 15)
```

### Version-Specific Feature Detection

```python
from rhosocial.activerecord.backend.impl.postgres import PostgresDialect

# Old PostgreSQL version
old_dialect = PostgresDialect(version=(8, 3, 0))
assert not old_dialect.supports_truncate_restart_identity()  # False (requires ≥ 8.4)

# New PostgreSQL version
new_dialect = PostgresDialect(version=(15, 0, 0))
assert new_dialect.supports_merge_statement()  # True (≥ 15)
assert new_dialect.supports_truncate_restart_identity()  # True (≥ 8.4)
```

## See Also

- [PostgreSQL Official Documentation](https://www.postgresql.org/docs/current/)
- [PostgreSQL Feature Matrix](https://www.postgresql.org/about/featurematrix/)
- [rhosocial.activerecord Backend Development Guide](../../../python-activerecord/docs/backend_development.md)
