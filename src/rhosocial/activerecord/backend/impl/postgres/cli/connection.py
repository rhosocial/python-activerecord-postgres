# src/rhosocial/activerecord/backend/impl/postgres/cli/connection.py
"""Connection argument parsing and backend creation for PostgreSQL CLI."""

import os

from .output import RICH_AVAILABLE


def add_connection_args(parser):
    """Add PostgreSQL connection arguments to a subcommand parser.

    Each subcommand that needs a database connection calls this.
    """
    parser.add_argument(
        "--host",
        default=os.getenv("POSTGRES_HOST", "localhost"),
        help="Database host (env: POSTGRES_HOST, default: localhost)",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=int(os.getenv("POSTGRES_PORT", "5432")),
        help="Database port (env: POSTGRES_PORT, default: 5432)",
    )
    parser.add_argument(
        "--database",
        default=os.getenv("POSTGRES_DATABASE"),
        help="Database name (env: POSTGRES_DATABASE, optional for some operations)",
    )
    parser.add_argument(
        "--user",
        default=os.getenv("POSTGRES_USER", "postgres"),
        help="Database user (env: POSTGRES_USER, default: postgres)",
    )
    parser.add_argument(
        "--password",
        default=os.getenv("POSTGRES_PASSWORD", ""),
        help="Database password (env: POSTGRES_PASSWORD)",
    )
    parser.add_argument(
        "--use-async",
        action="store_true",
        help="Use asynchronous backend",
    )
    parser.add_argument(
        "--named-connection",
        dest="named_connection",
        metavar="QUALIFIED_NAME",
        help="Named connection from Python module (e.g., myapp.connections.prod_db).",
    )
    parser.add_argument(
        "--conn-param",
        action="append",
        metavar="KEY=VALUE",
        default=[],
        dest="connection_params",
        help="Connection parameter override for named connection. Can be specified multiple times.",
    )


def add_version_arg(parser):
    """Add --version argument (used only by info subcommand)."""
    parser.add_argument(
        "--version",
        type=str,
        default=None,
        help='PostgreSQL version to simulate (e.g., "15.0.0"). Default: auto-detect.',
    )


def create_connection_parent_parser():
    """Create a parent parser with connection and output arguments.

    Used by shared CLI helpers (named-query, named-procedure) that
    require a parent_parser containing connection parameters.
    """
    import argparse
    parent = argparse.ArgumentParser(add_help=False)
    add_connection_args(parent)
    # Output parameters
    parent.add_argument(
        "-o", "--output",
        choices=["table", "json", "csv", "tsv"],
        default="table",
        help='Output format. Defaults to "table" if rich is installed.',
    )
    parent.add_argument(
        "--rich-ascii",
        action="store_true",
        help="Use ASCII characters for rich table borders.",
    )
    return parent


def resolve_connection_config_from_args(args):
    """Resolve PostgreSQL connection config from parsed args.

    Priority order:
        1. --named-connection + --conn-param
        2. Explicit connection parameters (--host, --port, etc.)
        3. Default values
    """
    from rhosocial.activerecord.backend.impl.postgres.config import PostgresConnectionConfig
    from rhosocial.activerecord.backend.named_connection.cli import parse_params
    from rhosocial.activerecord.backend.named_connection import NamedConnectionResolver

    named_conn = getattr(args, "named_connection", None)
    conn_params = getattr(args, "connection_params", [])

    if conn_params:
        conn_params = parse_params(conn_params)
    else:
        conn_params = {}

    if named_conn:
        resolver = NamedConnectionResolver(named_conn).load()
        if conn_params:
            return resolver.resolve(conn_params)
        return resolver.resolve({})

    # Fallback to explicit connection parameters
    return PostgresConnectionConfig(
        host=args.host or "localhost",
        port=args.port or 5432,
        database=args.database,
        username=args.user,
        password=args.password,
    )


def create_backend(args):
    """Create, connect, and introspect a PostgreSQL backend from parsed args."""
    from rhosocial.activerecord.backend.impl.postgres import PostgresBackend
    config = resolve_connection_config_from_args(args)
    backend = PostgresBackend(connection_config=config)
    backend.connect()
    backend.introspect_and_adapt()
    return backend
