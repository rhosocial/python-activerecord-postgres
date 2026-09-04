# cli tests

PostgreSQL backend CLI: named-expression and named-procedure subcommand argument parsing and adapters, offline tests for resolve_connection_config_from_args (SSL parameter mapping), and black-box tests of the backend CLI entry point against a live scenario server.

## Key files

- `test_cli.py` — named-expression/procedure argument parsing
- `test_cli_blackbox.py` — black-box CLI subcommands against a live server
- `test_cli_connection_args.py` — connection args and SSL mapping
