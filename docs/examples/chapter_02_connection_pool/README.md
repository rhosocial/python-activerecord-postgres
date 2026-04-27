# PostgreSQL Connection Pool Examples

This directory contains examples for testing the PostgreSQL connection pool.

## Available Examples

- **connection_pool_stress_test_sync.py** - Synchronous stress test for PostgreSQL connection pool
- **connection_pool_stress_test_async.py** - Asynchronous stress test for PostgreSQL connection pool

## Usage

Run in PostgreSQL virtual environment:

```bash
# Activate virtual environment
.venv_postgres\Scripts\activate

# Install dependencies
pip install psycopg

# Run sync stress test
python docs\examples\chapter_02_connection_pool\connection_pool_stress_test_sync.py

# Run async stress test
python docs\examples\chapter_02_connection_pool\connection_pool_stress_test_async.py
```

## Environment Variables

| Variable | Description | Required |
|----------|-------------|----------|
| PG_HOST | PostgreSQL server host | Yes |
| PG_PORT | PostgreSQL server port | Yes |
| PG_USER | Database username | Yes |
| PG_PASSWORD | Database password | Yes |
| PG_DATABASE | Database name | Yes |
| PG_SSLMODE | SSL mode (e.g., prefer) | No |

## Test Parameters

| Parameter | Value |
|-----------|-------|
| min_size | 10 |
| max_size | 50 |
| workers | 20 |
| iterations | 50 |
| total queries | 1000 |

## Expected Results

- **threadsafety**: 2 (psycopg - full thread-safe)
- **connection_mode**: `persistent` (auto-detected from threadsafety=2)
- All 1000 queries should execute successfully
- Pool should close cleanly