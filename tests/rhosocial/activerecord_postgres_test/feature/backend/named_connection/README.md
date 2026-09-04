# named_connection tests

Named connection support for PostgreSQL: NamedConnectionResolver unit and integration tests plus CLI parameter-resolution priority; example connections load from tests/config/postgres_scenarios.yaml.

## Key files

- `example_connections.py` — scenario-based example connections
- `test_named_connection_cli.py` — CLI parameter priority
- `test_resolver.py` — resolver unit + integration tests
