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
