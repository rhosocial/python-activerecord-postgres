# dialect tests

PostgreSQL dialect capabilities: ALTER TABLE IF [NOT] EXISTS qualifiers, dialect formatting on a live server, DROP TABLE CASCADE/RESTRICT, ON CONFLICT capability and rendering, property graph (PGQ) version gating and SQL formatting, SchemaSupport (database -> schema -> table) and version-specific feature detection.

## Key files

- `test_alter_table_if_exists.py` — IF [NOT] EXISTS qualifiers, USING, UNLOGGED
- `test_dialect.py` — dialect formatting integration
- `test_drop_table_cascade.py` — CASCADE / RESTRICT rendering
- `test_insert_on_conflict_clauses.py` — ON CONFLICT capabilities and rendering
- `test_property_graph_query_format.py` — PGQ format gating
- `test_schema_support.py` — schema capability and DDL formatting
- `test_version_features.py` — version-gated features
