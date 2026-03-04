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

In addition to standard protocols, PostgreSQL provides database-specific protocols:

| Protocol | Description |
|----------|-------------|
| **PostgresExtensionSupport** | Extension detection and management (PostGIS, pgvector, etc.) |
| **PostgresMaterializedViewSupport** | Materialized views with CONCURRENTLY refresh |
| **PostgresTableSupport** | Table-specific features (INHERITS, partitioning) |
| **PostgresVectorSupport** | pgvector extension for vector similarity search |
| **PostgresSpatialSupport** | PostGIS extension for spatial data |
| **PostgresTrigramSupport** | pg_trgm extension for trigram similarity |
| **PostgresHstoreSupport** | hstore extension for key-value storage |

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

### PostgreSQL 10.x - 14.x Series

| Version | Key Features Added |
|---------|-------------------|
| 12 | MATERIALIZED CTE hints, JSON_TABLE |
| 13 | (Incremental improvements) |
| 14 | (Performance improvements) |

### PostgreSQL 15.x and Later

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
