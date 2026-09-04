# query tests

PostgreSQL query-level features: EXPLAIN output for sync and async backends, and row-level locking (lock strengths FOR UPDATE, FOR NO KEY UPDATE, FOR SHARE, FOR KEY SHARE, clause formatting and async equivalents).

## Key files

- `test_explain.py` — Backend.explain() protocol and output (moved from `backend/`)
- `test_row_level_locks.py` — row-level lock strengths and formatting
