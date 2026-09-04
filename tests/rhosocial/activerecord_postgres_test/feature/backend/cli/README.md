# cli tests

PostgreSQL backend CLI: named-expression and named-procedure subcommand argument parsing and adapters, plus offline tests for resolve_connection_config_from_args (SSL parameter mapping).

## Key files

- `test_cli.py` — named-expression/procedure argument parsing
- `test_cli_connection_args.py` — connection args and SSL mapping
