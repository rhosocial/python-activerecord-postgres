# transactions tests

PostgreSQL transaction coverage: isolation levels, READ ONLY / READ WRITE mode effects, DEFERRABLE SERIALIZABLE transactions, SET TRANSACTION and session characteristics, savepoints and backend-level transaction handling for sync and async backends.

## Key files

- `test_transaction_advanced_features.py` — DEFERRABLE, SET TRANSACTION, session characteristics
- `test_transaction_backend.py` — backend transaction integration (sync)
- `test_transaction_backend_async.py` — backend transaction integration (async)
- `test_transaction_deferrable.py` — constraint deferral
- `test_transaction_isolation.py` — isolation levels
- `test_transaction_mode_effect.py` — READ ONLY / READ WRITE effects
- `test_transaction_postgres.py` — PG-specific transaction features
- `test_transaction_savepoint.py` — savepoints
