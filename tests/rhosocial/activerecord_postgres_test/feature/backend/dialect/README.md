# dialect tests

PostgreSQL dialect capabilities: dialect formatting on a live server, property graph (PGQ) version gating and SQL formatting, and version-specific feature detection. DDL qualifiers (ALTER/DROP IF EXISTS), ON CONFLICT and SchemaSupport live in `../ddl/`, `../dml/` and `../schema/` respectively.

## Key files

- `test_dialect.py` — dialect formatting integration
- `test_property_graph_query_format.py` — PGQ format gating
- `test_version_features.py` — version-gated features
