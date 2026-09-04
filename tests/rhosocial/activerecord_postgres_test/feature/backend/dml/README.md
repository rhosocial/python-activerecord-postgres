# dml tests

PostgreSQL DML integration: real-database CRUD, column type mapping and execute_many batch operations for sync and async backends, plus ON CONFLICT upsert clauses. Async tests are co-located with their sync twins (`_async` suffix).

## Key files

- `test_column_mapping_backend.py` — column type mapping (sync)
- `test_column_mapping_backend_async.py` — column type mapping (async)
- `test_crud_backend.py` — CRUD integration (sync)
- `test_crud_backend_async.py` — CRUD integration (async)
- `test_execute_many.py` — batch execution
- `test_insert_on_conflict_clauses.py` — ON CONFLICT capabilities and rendering
