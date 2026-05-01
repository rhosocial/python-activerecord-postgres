# src/rhosocial/activerecord/backend/impl/postgres/examples/conftest.py
"""
Example metadata configuration.

This file defines metadata for all examples in this directory.
The inspector reads this file to get title, dialect_protocols, and priority.

PostgreSQL Version Support:
- Minimum: 9
- Maximum: 18

Version-specific features:
- Window Functions: PostgreSQL 8.4+
- JSONB data type: PostgreSQL 9.4+
- Array type: PostgreSQL 8.4+ (arrays existed before 8.4 but more limited)
- Full-Text Search (tsvector): PostgreSQL 8.3+
- CTE (WITH clause): PostgreSQL 8.4+
- Row-level security: PostgreSQL 9.5+
- JSON/JSONB operators: PostgreSQL 9.4+
- Materialized views: PostgreSQL 9.3+
- Range types: PostgreSQL 9.2+
- Hereditary tables: PostgreSQL 10+
- Identity columns: PostgreSQL 10+ (SERIAL is available since 8.2)
- Parallel queries: PostgreSQL 11+
- JSON path: PostgreSQL 12+
"""

EXAMPLES_META = {
    'connection/quickstart.py': {
        'title': 'Connect to PostgreSQL and Execute Queries',
        'dialect_protocols': [],
        'priority': 10,
        'min_version': '9',
        'max_version': '18',
    },
    'ddl/create_table.py': {
        'title': 'Create Table',
        'dialect_protocols': [],
        'priority': 10,
        'min_version': '9',
        'max_version': '18',
    },
    'ddl/create_index.py': {
        'title': 'Create Index',
        'dialect_protocols': [],
        'priority': 10,
        'min_version': '9',
        'max_version': '18',
    },
    'ddl/alter_table.py': {
        'title': 'Alter Table',
        'dialect_protocols': [],
        'priority': 10,
        'min_version': '9',
        'max_version': '18',
    },
    'ddl/drop_table.py': {
        'title': 'DROP TABLE',
        'dialect_protocols': [],
        'priority': 10,
        'min_version': '9',
        'max_version': '18',
    },
    'ddl/view.py': {
        'title': 'CREATE VIEW',
        'dialect_protocols': [],
        'priority': 10,
        'min_version': '9',
        'max_version': '18',
    },
    'ddl/materialized_view.py': {
        'title': 'CREATE MATERIALIZED VIEW',
        'dialect_protocols': [],
        'priority': 10,
        'min_version': '9',
        'max_version': '18',
        'note': 'Materialized views available since PostgreSQL 9.3+',
    },
    'ddl/unique_index.py': {
        'title': 'CREATE UNIQUE INDEX',
        'dialect_protocols': [],
        'priority': 10,
        'min_version': '9',
        'max_version': '18',
    },
    'ddl/partition.py': {
        'title': 'Table Partitioning',
        'dialect_protocols': [],
        'priority': 10,
        'min_version': '10',
        'max_version': '18',
        'note': 'Declarative partitioning available since PostgreSQL 10+',
    },
'insert/batch.py': {
        'title': 'Batch Insert',
        'dialect_protocols': [],
        'priority': 10,
        'min_version': '9',
        'max_version': '18',
    },
    'insert/single.py': {
        'title': 'Single Row Insert',
        'dialect_protocols': [],
        'priority': 10,
        'min_version': '9',
        'max_version': '18',
    },
    'insert/with_returning.py': {
        'title': 'INSERT with RETURNING Clause',
        'dialect_protocols': [],
        'priority': 10,
        'min_version': '9',
        'max_version': '18',
    },
    'insert/upsert.py': {
        'title': 'UPSERT (INSERT ON CONFLICT)',
        'dialect_protocols': [],
        'priority': 10,
        'min_version': '9.5',
        'max_version': '18',
        'note': 'ON CONFLICT requires PostgreSQL 9.5+',
    },
    'delete/basic.py': {
        'title': 'DELETE with RETURNING Clause',
        'dialect_protocols': [],
        'priority': 10,
        'min_version': '9',
        'max_version': '18',
    },
    'update/basic.py': {
        'title': 'UPDATE using UpdateExpression',
        'dialect_protocols': [],
        'priority': 10,
        'min_version': '9',
        'max_version': '18',
    },
    'query/basic.py': {
        'title': 'Basic SELECT Query',
        'dialect_protocols': [],
        'priority': 10,
        'min_version': '9',
        'max_version': '18',
    },
    'query/join.py': {
        'title': 'JOIN Query',
        'dialect_protocols': [],
        'priority': 10,
        'min_version': '9',
        'max_version': '18',
    },
    'query/aggregate.py': {
        'title': 'Aggregate Query',
        'dialect_protocols': [],
        'priority': 10,
        'min_version': '9',
        'max_version': '18',
    },
    'query/distinct.py': {
        'title': 'SELECT DISTINCT',
        'dialect_protocols': [],
        'priority': 10,
        'min_version': '9',
        'max_version': '18',
    },
    'query/union.py': {
        'title': 'UNION using SetOperationExpression',
        'dialect_protocols': [],
        'priority': 10,
        'min_version': '9',
        'max_version': '18',
    },
    'query/subquery.py': {
        'title': 'Subquery',
        'dialect_protocols': [],
        'priority': 10,
        'min_version': '9',
        'max_version': '18',
    },
    'query/window.py': {
        'title': 'Window Functions',
        'dialect_protocols': ['WindowFunctionSupport'],
        'priority': 10,
        'min_version': '9',
        'max_version': '18',
    },
    'query/text_search.py': {
        'title': 'Full-Text Search',
        'dialect_protocols': [],
        'priority': 10,
        'min_version': '9',
        'max_version': '18',
    },
    'query/array_func.py': {
        'title': 'Array Functions',
        'dialect_protocols': [],
        'priority': 10,
        'min_version': '9',
        'max_version': '18',
    },
    'types/json_basic.py': {
        'title': 'JSONB Operations',
        'dialect_protocols': ['JSONSupport'],
        'priority': 10,
        'min_version': '9',
        'max_version': '18',
        'note': 'JSON type is available since PostgreSQL 9.4+',
    },
    'types/array_postgres84.py': {
        'title': 'Array Type Operations (PostgreSQL 8.4+)',
        'dialect_protocols': [],
        'priority': 10,
        'min_version': '9',
        'max_version': '18',
    },
    'types/uuid.py': {
        'title': 'Native UUID Type',
        'dialect_protocols': [],
        'priority': 10,
        'min_version': '9',
        'max_version': '18',
        'note': 'gen_random_uuid() built-in since PostgreSQL 13+, uuid-ossp extension for earlier versions',
    },
    'query/predicate.py': {
        'title': 'Complex Predicates',
        'dialect_protocols': [],
        'priority': 10,
        'min_version': '9',
        'max_version': '18',
    },
    'query/pagination.py': {
        'title': 'Pagination with LIMIT/OFFSET',
        'dialect_protocols': [],
        'priority': 10,
        'min_version': '9',
        'max_version': '18',
    },
    'query/cte.py': {
        'title': 'CTE (Common Table Expressions)',
        'dialect_protocols': [],
        'priority': 10,
        'min_version': '9',
        'max_version': '18',
    },
    'query/window_functions.py': {
        'title': 'Window Functions (Advanced)',
        'dialect_protocols': ['WindowFunctionSupport'],
        'priority': 10,
        'min_version': '9',
        'max_version': '18',
    },
    'query/constants.py': {
        'title': 'Query Runtime Constants and Niladic Functions',
        'dialect_protocols': [],
        'priority': 10,
        'min_version': '9',
        'max_version': '18',
    },
    'query/explain.py': {
        'title': 'EXPLAIN Query Plan',
        'dialect_protocols': [],
        'priority': 10,
        'min_version': '9',
        'max_version': '18',
    },
    'transaction/basic.py': {
        'title': 'Transaction Control',
        'dialect_protocols': [],
        'priority': 10,
        'min_version': '9',
        'max_version': '18',
    },
    'transaction/for_update.py': {
        'title': 'FOR UPDATE Row Locking',
        'dialect_protocols': [],
        'priority': 10,
        'min_version': '9',
        'max_version': '18',
    },
    'transaction/exclusive.py': {
        'title': 'Transaction Isolation Levels',
        'dialect_protocols': [],
        'priority': 10,
        'min_version': '9',
        'max_version': '18',
    },
    # Extension examples
    'extensions/bloom.py': {
        'title': 'Bloom Filter Index Access Method',
        'dialect_protocols': [],
        'priority': 5,
        'min_version': '9',
        'max_version': '18',
    },
    'extensions/btree_gin.py': {
        'title': 'GIN Index for Btree-Comparable Types',
        'dialect_protocols': [],
        'priority': 5,
        'min_version': '9',
        'max_version': '18',
    },
    'extensions/btree_gist.py': {
        'title': 'GiST Index for Btree-Comparable Types',
        'dialect_protocols': [],
        'priority': 5,
        'min_version': '9',
        'max_version': '18',
    },
    'extensions/hypopg.py': {
        'title': 'Hypothetical Index Testing',
        'dialect_protocols': [],
        'priority': 5,
        'min_version': '10',
        'max_version': '18',
        'note': 'hypopg requires PostgreSQL 10+',
    },
    'extensions/orafce.py': {
        'title': 'Oracle-Compatible Functions',
        'dialect_protocols': [],
        'priority': 5,
        'min_version': '9',
        'max_version': '18',
    },
    'extensions/tablefunc.py': {
        'title': 'Pivot Tables and Hierarchical Queries',
        'dialect_protocols': [],
        'priority': 5,
        'min_version': '9',
        'max_version': '15',
        'note': 'tablefunc not available in PG13/14/16/17/18 docker images',
    },
    'extensions/pg_stat_statements.py': {
        'title': 'Query Execution Statistics',
        'dialect_protocols': [],
        'priority': 5,
        'min_version': '9',
        'max_version': '18',
        'note': 'Requires superuser or pg_read_all_stats role',
    },
    'extensions/address_standardizer.py': {
        'title': 'Address Standardization (Requires PostGIS)',
        'dialect_protocols': [],
        'priority': 3,
        'min_version': '9',
        'max_version': '18',
        'note': 'Requires PostGIS extension; not available in PG10-12/15 docker images',
    },
    'extensions/postgis_raster.py': {
        'title': 'Raster Data Support (Requires PostGIS)',
        'dialect_protocols': [],
        'priority': 3,
        'min_version': '9',
        'max_version': '18',
        'note': 'Requires PostGIS extension; not available in PG10-12/15 docker images',
    },
    'extensions/pgrouting.py': {
        'title': 'Geospatial Routing (Requires PostGIS)',
        'dialect_protocols': [],
        'priority': 3,
        'min_version': '9',
        'max_version': '18',
        'note': 'Requires PostGIS extension; not available in PG10-12/15 docker images',
    },
    'extensions/pg_partman.py': {
        'title': 'Partition Management',
        'dialect_protocols': [],
        'priority': 3,
        'min_version': '12',
        'max_version': '18',
        'note': 'Requires superuser; not available in PG9-11/15 docker images',
    },
    'extensions/pg_cron.py': {
        'title': 'Scheduled Job Execution (Requires Superuser)',
        'dialect_protocols': [],
        'priority': 3,
        'min_version': '10',
        'max_version': '18',
        'note': 'Requires superuser for CREATE EXTENSION',
    },
    'extensions/pg_repack.py': {
        'title': 'Table Repacking Without Locks (Requires Superuser)',
        'dialect_protocols': [],
        'priority': 3,
        'min_version': '9',
        'max_version': '18',
        'note': 'Requires superuser',
    },
    'extensions/pglogical.py': {
        'title': 'Logical Replication (Requires Superuser)',
        'dialect_protocols': [],
        'priority': 3,
        'min_version': '10',
        'max_version': '18',
        'note': 'Requires superuser for replication setup',
    },
    'extensions/pgaudit.py': {
        'title': 'Session Audit Logging (Requires Superuser)',
        'dialect_protocols': [],
        'priority': 3,
        'min_version': '10',
        'max_version': '18',
        'note': 'Requires superuser and shared_preload_libraries configuration',
    },
    'extensions/pg_surgery.py': {
        'title': 'Data Repair Functions (Requires Superuser)',
        'dialect_protocols': [],
        'priority': 3,
        'min_version': '14',
        'max_version': '18',
        'note': 'Requires superuser; only available in PG14+ docker images',
    },
    'extensions/pg_walinspect.py': {
        'title': 'WAL Inspection (Requires Superuser)',
        'dialect_protocols': [],
        'priority': 3,
        'min_version': '15',
        'max_version': '18',
        'note': 'Requires superuser or pg_read_server_files role; only available in PG15+ docker images',
    },
}
