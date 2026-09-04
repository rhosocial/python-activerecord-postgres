# postgres tests

Vendor-specific subtree for PostgreSQL implementation details, organized by theme: ddl/ (diff, LIKE, RLS policies, indexes, triggers, routines, extensions DDL), dialect/ (security, ILIKE), expression/ (round trips, EXPLAIN examples), extensions/ (unit + integration tests for a wide range of PostgreSQL extensions), functions/ (array, range, hstore, pgvector, PostGIS, UUID, sequence, network functions), partition/ (declarative partition operations, advanced capabilities and EXPLAIN on partitioned tables), query/ (advisory locks, PGQ scenarios, recursive CTE traversal), schema/ (schema namespaces), types/ (offline adapters plus integration for range/bitstring/enum/money/xml/oid/lsn types) and views/ (materialized view execution). Protocol conformance lives in the top-level `../protocol/`. Also holds PostgresConnectionConfig tests.

## Key files

- `test_connection_config.py` — PostgresConnectionConfig autocommit option
- `ddl/` — diff, LIKE, DDL objects/protocols, policies, indexes
- `dialect/` — security, ILIKE, set ops/truncate
- `expression/` — expression classes, round trips, EXPLAIN examples
- `extensions/` — unit + integration tests for PostgreSQL extensions
- `functions/` — PG-specific function factories and versions
- `partition/` — partition operations, advanced capabilities, partition EXPLAIN
- `query/` — advisory locks, PGQ scenarios, recursive CTE graph traversal
- `schema/` — schema namespace support
- `types/` — PG-specific types: adapters, unit and integration tests
- `views/` — VIEW / MATERIALIZED VIEW execution

## Vendor-specific tests

Vendor-specific tests: everything under this directory exercises PostgreSQL-only implementation behavior that has no cross-backend equivalent.
