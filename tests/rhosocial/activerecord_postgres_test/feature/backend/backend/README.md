# backend tests

PostgresBackend behavior: backend initialization, connection config / range adapter mode, connection resilience (pg_terminate_backend recovery, idle timeouts, ping reconnect), cursor result-set pollution and server version handling. Concurrency protocol conformance lives in `../concurrency/`, explain() coverage in `../query/`.

## Key files

- `test_backend_initialization.py` — backend init
- `test_config.py` — connection config and type adapter mixin wiring
- `test_connection_resilience.py` — terminated-connection recovery
- `test_cursor_pollution.py` — result-set pollution after SELECT version()
- `test_version.py` — server version handling
