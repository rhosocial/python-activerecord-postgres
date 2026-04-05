# Scenarios

This chapter provides complete application examples of the PostgreSQL backend in real-world business scenarios, helping developers understand how to correctly use rhosocial-activerecord in actual projects.

## Scenario List

### [Parallel Worker Processing](parallel_workers.md)

Demonstrates correct usage of PostgreSQL backend in multi-process/async concurrent scenarios:

- **Multi-process Correct Usage**: `configure()` must be called within child processes
- **PostgreSQL Async Advantage**: psycopg native async support, true network I/O concurrency
- **Deadlock Handling**: PostgreSQL automatically detects deadlocks, recommended retry mechanism for production
- **Multi-threading Pitfalls**: psycopg connections also do not support multi-threaded concurrency

> 📖 **Companion Code**: Complete runnable experiment code is located in the `docs/examples/chapter_08_scenarios/parallel_workers/` directory.

## Relationship with Core Library Scenarios

This chapter is a PostgreSQL-specific supplement to the [Core Library Scenarios Documentation](https://github.com/Rhosocial/python-activerecord/tree/main/docs/en_US/scenarios), focusing on:

- PostgreSQL-specific concurrent behavior (MVCC, row-level locking, deadlock detection)
- psycopg async driver characteristics (single-connection model, true async I/O)
- RETURNING clause advantages (INSERT/UPDATE/DELETE returning data)
- Key differences compared to MySQL and SQLite

The general ActiveRecord usage patterns introduced in the core library scenarios (such as relationships, query building) also apply to the PostgreSQL backend.
