# postgres tests

Vendor-specific subtree for PostgreSQL implementation details, organized by theme: ddl/ (diff, LIKE, RLS policies, partitions, indexes, triggers, routines, extensions DDL), dialect/ (security, ILIKE, protocol conformance), expression/ (round trips, EXPLAIN examples), extensions/ (unit + integration tests for a wide range of PostgreSQL extensions), functions/ (array, range, hstore, pgvector, PostGIS, UUID, sequence, network functions), query/ (advisory locks, PGQ scenarios, recursive CTE traversal), schema/ (schema namespaces), types/ (offline adapters plus integration for range/bitstring/enum/money/xml/oid/lsn types) and views/ (materialized view execution). Also holds PostgresConnectionConfig tests.

## Key files

- `test_connection_config.py` — PostgresConnectionConfig autocommit option
- `ddl/` — diff, LIKE, DDL objects/protocols, policies, partitions, indexes
- `dialect/` — security, ILIKE, protocol conformance, set ops/truncate
- `expression/` — expression classes, round trips, EXPLAIN examples
- `extensions/` — unit + integration tests for PostgreSQL extensions
- `functions/` — PG-specific function factories and versions
- `query/` — advisory locks, PGQ scenarios, recursive CTE graph traversal
- `schema/` — schema namespace support
- `types/` — PG-specific types: adapters, unit and integration tests
- `views/` — VIEW / MATERIALIZED VIEW execution

## Vendor-specific tests

Vendor-specific tests: everything under this directory exercises PostgreSQL-only implementation behavior that has no cross-backend equivalent.
