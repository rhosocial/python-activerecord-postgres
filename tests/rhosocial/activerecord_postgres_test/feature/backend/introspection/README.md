# introspection tests

The PostgreSQL introspection stack: core availability and database info, per-object metadata (tables, columns, indexes, foreign keys, triggers, views), cache management, config-aware default-schema resolution and the status introspector.

## Key files

- `test_introspection_basic.py` — introspector availability, database info
- `test_introspection_cache.py` — cache management
- `test_introspection_columns.py` — column metadata
- `test_introspection_default_schema.py` — config-aware default schema
- `test_introspection_foreign_keys.py` — foreign keys
- `test_introspection_indexes.py` — indexes incl. PG-specific index types
- `test_introspection_tables.py` — tables
- `test_introspection_triggers.py` — triggers incl. timing
- `test_introspection_views.py` — views
- `test_status_introspector.py` — status introspector
