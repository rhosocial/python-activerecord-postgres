# Project Overview: rhosocial-activerecord-postgres

## Project Name
- **Repository Name**: python-activerecord-postgres
- **Python Package Name**: rhosocial-activerecord-postgres

## Project Purpose

This project is a PostgreSQL backend implementation for the `rhosocial-activerecord` Python package. It provides PostgreSQL database support with the elegant ActiveRecord pattern interface.

## Key Design Principles

1. **Backend Implementation**: Extends core ActiveRecord with PostgreSQL-specific features
2. **Driver**: Uses `psycopg` (version 3) for database connectivity
3. **Namespace Package**: Integrates with the rhosocial namespace package architecture

## Current Status

This project is under active development. Key features implemented:

- Basic CRUD operations
- Connection management
- Transaction support
- Schema introspection
- PostgreSQL-specific data types
- Array types
- JSON/JSONB
- Range types
- PostGIS spatial types
- UUID types
- HStore
- Full-text search

## Version Control and Changelog

This project adheres to the same version control, branching, commit message, and changelog management standards as the main `python-activerecord` project.

## Async Backend and the Windows Event Loop

psycopg's async mode waits on the libpq socket with `loop.add_reader` /
`loop.add_writer`, which on Windows the platform default event loop
(`ProactorEventLoop`, since 3.8) does not implement. Therefore:

- The sync path is unaffected everywhere.
- `AsyncPostgresBackend` requires a selector-based event loop on Windows
  (`SelectorEventLoop(SelectSelector())`). psycopg 3.3+ detects the
  mismatch at connect time and raises with the fix named; older versions
  surface asyncio's raw `NotImplementedError`.
- To run async code on Windows, use the helper this backend ships:
  `run_coroutine(coro)` from
  `rhosocial.activerecord.backend.impl.postgres.async_support` -- it
  constructs the required loop and gives it `asyncio.run`-grade
  lifecycle (cancel on `KeyboardInterrupt`, shutdown of async generators,
  close, detach). It may only be called when no loop is running.
- The CLI's `--async` modes (`introspect`, `query`, `status`) already use
  it, so they work on Windows out of the box.
- Do NOT "fix" this by switching the global event loop policy:
  `WindowsSelectorEventLoopPolicy` is deprecated for removal in Python
  3.16, and a selector loop on Windows cannot serve asyncio subprocess or
  pipe I/O, which would break other parts of the process.

## Search Scope

- Use `rg` for repository searches; never use `grep`.
- Limit searches to this `python-activerecord` repository and its related testsuite/backend directories.
- Exclude `.git/`, `.venv*/`, `__pycache__/`, `build/`, `dist/`, and other cache directories.
