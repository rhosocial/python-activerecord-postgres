## [v1.0.0.dev11] - 2026-04-17

### Added

- Add comprehensive PostgreSQL backend examples covering connection, CRUD, transactions, CTEs, window functions, and more ([#30](https://github.com/rhosocial/python-activerecord-postgres/issues/30))


## [v1.0.0.dev10] - 2026-04-13

### Added

- Added PostgreSQL row-level lock strength support: FOR SHARE, FOR NO KEY UPDATE, and FOR KEY SHARE. These lock strengths provide finer control over row-level locking in PostgreSQL 9.0+ (KEY SHARE requires 9.3+). ([#23](https://github.com/rhosocial/python-activerecord-postgres/issues/23))
- Added enhanced mathematical functions to PostgreSQL backend, including round_, pow, power, sqrt, mod, ceil, floor, trunc, max_, min_, and avg, with proper naming to avoid conflicts with Python builtins. ([#25](https://github.com/rhosocial/python-activerecord-postgres/issues/25))
- Added comprehensive PostgreSQL function support: array (18), network address (16), UUID (8), hstore (33), and range constructors (6). ([#26](https://github.com/rhosocial/python-activerecord-postgres/issues/26))
- Add PostgreSQL constraint support protocol with EXCLUDE and NOT VALID options ([#27](https://github.com/rhosocial/python-activerecord-postgres/issues/27))



### Fixed

- Refactored SQL function generation utilities by migrating them from types/ to functions/ directory for better module responsibility separation. ([#24](https://github.com/rhosocial/python-activerecord-postgres/issues/24))


## [v1.0.0.dev9] - 2026-04-08

### Added

- Added connection pool context awareness support for PostgreSQL backend, enabling proper connection management across sync/async contexts and transaction boundaries. ([#19](https://github.com/rhosocial/python-activerecord-postgres/issues/19))
- Added PostgreSQL server status overview support with comprehensive introspection capabilities including WAL, replication, archive, security, and extensions info. ([#21](https://github.com/rhosocial/python-activerecord-postgres/issues/21))



### Fixed

- Fixed PostgreSQL transaction manager to respect optional BEGIN syntax - isolation level is no longer forced, allowing SESSION CHARACTERISTICS to take effect when not explicitly set. ([#20](https://github.com/rhosocial/python-activerecord-postgres/issues/20))


## [v1.0.0.dev8] - 2026-04-06

### Added

- Added EXPLAIN clause support with typed PostgresExplainResult and dual-layer connection recovery for PostgreSQL backend ([#16](https://github.com/rhosocial/python-activerecord-postgres/issues/16))
- Improved PostgreSQL backend CLI info command with connection status display and added comprehensive introspect usage examples. ([#17](https://github.com/rhosocial/python-activerecord-postgres/issues/17))


## [v1.0.0.dev7] - 2026-03-28

### Added

- Added PostgreSQL introspection support with AbstractIntrospector for database schema discovery, including tables, columns, indexes, foreign keys, views, and triggers. ([#14](https://github.com/rhosocial/python-activerecord-postgres/issues/14))


## [v1.0.0.dev6] - 2026-03-22

### Added

- Added comprehensive unit tests for PostgreSQL dialect features, improving code coverage from 69% to 74%. New tests cover stored procedures, extended statistics, triggers, indexes, range functions, and extensions (hstore, ltree, intarray, pg_trgm, pgvector, PostGIS). ([#12](https://github.com/rhosocial/python-activerecord-postgres/issues/12))


## [v1.0.0.dev5] - 2026-03-20

### Added

- Add PostgreSQL CLI protocol support display with comprehensive feature information ([#9](https://github.com/rhosocial/python-activerecord-postgres/issues/9))
- Added Python version-aware fixture selection for PostgreSQL backend testing, enabling Python 3.10+ UnionType syntax support. ([#10](https://github.com/rhosocial/python-activerecord-postgres/issues/10))


## [v1.0.0.dev4] - 2026-03-13

### Added

- Added PostgreSQL-specific DDL support including trigger DDL, TYPE DDL for ENUM/RANGE types, COMMENT ON, materialized views, extension detection, and ILIKE operator support. ([#7](https://github.com/rhosocial/python-activerecord-postgres/issues/7))


## [1.0.0.dev3] - 2026-02-27

### Added

- PostgreSQL backend adaptation to new expression-dialect architecture. ([#4](https://github.com/rhosocial/python-activerecord-postgres/issues/4))
- Implemented `introspect_and_adapt()` in PostgreSQL backends to achieve full sync/async symmetry with the core backend interface. ([#5](https://github.com/rhosocial/python-activerecord-postgres/issues/5))


## [1.0.0.dev2] - 2025-12-11

### Added

- Added support for new mapping and type adapter tests, implemented annotated adapter query tests and PostgreSQL provider for type adapter tests. Added column mapping tests and a comprehensive testing guide. ([#2](https://github.com/rhosocial/python-activerecord-postgres/issues/2))



### Fixed

- Adjusted schema for timezone-aware datetime in tests. ([#2](https://github.com/rhosocial/python-activerecord-postgres/issues/2))


## [1.0.0.dev1] - 2025-11-29

### Added

- Add backend CLI tool functionality for database operations ([#1](https://github.com/rhosocial/python-activerecord-postgres/issues/1))
