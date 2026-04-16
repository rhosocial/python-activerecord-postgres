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
    'insert/batch.py': {
        'title': 'Batch Insert',
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
        'note': 'Requires PostgreSQL 9.5+',
    },
    'ddl/materialized_view.py': {
        'title': 'Materialized Views',
        'dialect_protocols': [],
        'priority': 10,
        'min_version': '9.3',
        'max_version': '18',
        'note': 'Requires PostgreSQL 9.3+',
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
}
