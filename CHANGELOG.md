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
