# expression tests

PostgreSQL expression tests: transaction expressions (BEGIN/SET TRANSACTION/SAVEPOINT) and SQL-snapshot tests for the pgvector similarity-search helpers built on a bare PostgresDialect.

## Key files

- `test_expressions_transaction.py` — transaction expression classes
- `vector_search_test.py` — pgvector distance/index SQL snapshots
