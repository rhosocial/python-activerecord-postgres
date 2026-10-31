# src/rhosocial/activerecord/backend/impl/postgres/cli/introspect.py
"""introspect subcommand - Database introspection.

PostgreSQL introspect includes the 'extensions' type in addition to
the standard types, plus the two schema-object families this backend
answers for on its own: materialized views and sequences.

A type either lists a family or, when it names a single catalogue object,
answers for that one object; ``NAME`` is required, optional or refused
according to which of the two it is, and never silently dropped.
"""

import argparse
import asyncio
import sys
from dataclasses import asdict, is_dataclass
from enum import Enum
from typing import Any, Dict, List

from rhosocial.activerecord.backend.impl.postgres.backend.async_backend import AsyncPostgresBackend
from rhosocial.activerecord.backend.impl.postgres.backend import PostgresBackend
from rhosocial.activerecord.backend.errors import ConnectionError, QueryError
from rhosocial.activerecord.backend.introspection.status import ExtensionInfo

from .connection import add_connection_args, resolve_connection_config_from_args
from .output import create_provider

OUTPUT_CHOICES = ["table", "json", "csv", "tsv"]

#: CLI type -> the dialect capability switch that answers for it.
#:
#: The advertised list is derived from this mapping, so a type cannot be
#: offered without saying which switch decides whether the connected server
#: can answer it. A type whose switch is False is refused when handled, rather
#: than run and answered with an empty list: "this server cannot report
#: sequences" and "this database has no sequences" are different answers.
#:
#: One entry per family the catalog reports independently. Partition metadata
#: has a switch too, but no introspector question of its own -- it is read as
#: part of the DDL a partitioned table carries -- so it is not a type here.
_TYPE_CAPABILITIES = {
    "tables": "supports_table_introspection",
    "views": "supports_view_introspection",
    "columns": "supports_column_introspection",
    "indexes": "supports_index_introspection",
    "foreign-keys": "supports_foreign_key_introspection",
    "triggers": "supports_trigger_introspection",
    "database": "supports_database_info",
    "extensions": "supports_extension_introspection",
    "materialized-views": "supports_materialized_view_introspection",
    "sequences": "supports_sequence_introspection",
}

INTROSPECT_TYPES = list(_TYPE_CAPABILITIES)


def _require_capability(backend, args) -> None:
    """Refuse a type the connected server cannot answer.

    The dialect is adapted before this runs, so the switches answer for the
    server in front of us rather than for the newest release this backend
    knows.
    """
    switch = _TYPE_CAPABILITIES.get(args.type)
    if switch is None:
        return
    dialect = getattr(backend, "dialect", None)
    if dialect is None:
        # A stub standing in for the backend (the offline CLI tests) has no
        # dialect to ask; a real one always does.
        return
    supported = getattr(dialect, switch, None)
    if supported is None or supported():
        return
    print(
        f"Error: {args.type} introspection is not supported by this server",
        file=sys.stderr,
    )
    sys.exit(2)


def _refuse_name(args: argparse.Namespace, hint: str) -> None:
    """Refuse a ``NAME`` the type has no question for.

    ``tables`` lists every table and ``database`` describes the database the
    run connected to; neither can be narrowed to one object. Dropping the
    name instead would print the family listing under a one-object question,
    which is a different answer than the one asked for.
    """
    print(f"Error: '{args.type}' does not take an object name; {hint}", file=sys.stderr)
    sys.exit(2)


def create_parser(subparsers):
    """Create the introspect subcommand parser."""
    parser = subparsers.add_parser(
        "introspect",
        help="Database introspection",
        epilog="""Examples:
  # List all tables in database
  %(prog)s tables --database mydb

  # List all views, or inspect one by name
  %(prog)s views --database mydb
  %(prog)s views active_users --database mydb

  # Get column details for a table
  %(prog)s columns users --database mydb

  # Get index information
  %(prog)s indexes users --database mydb

  # Get foreign key relationships
  %(prog)s foreign-keys users --database mydb

  # List triggers, optionally narrowed to one table
  %(prog)s triggers --database mydb
  %(prog)s triggers users --database mydb

  # Get database information
  %(prog)s database --database mydb

  # List installed extensions (PostgreSQL specific), or inspect one by name
  %(prog)s extensions --database mydb
  %(prog)s extensions pgcrypto --database mydb

  # List materialized views, or inspect one by name
  %(prog)s materialized-views --database mydb
  %(prog)s materialized-views sales_summary --database mydb

  # List sequences, or inspect one by name
  %(prog)s sequences --database mydb
  %(prog)s sequences order_id_seq --database mydb

  # Output as JSON
  %(prog)s tables --database mydb -o json

  # Using environment variables for connection
  export POSTGRES_HOST=localhost POSTGRES_DATABASE=mydb POSTGRES_USER=postgres POSTGRES_PASSWORD=secret
  %(prog)s tables
""",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    # Output format
    parser.add_argument(
        "-o",
        "--output",
        choices=OUTPUT_CHOICES,
        default="table",
        help="Output format (default: table)",
    )

    # Connection arguments
    add_connection_args(parser)

    # Rich display options
    parser.add_argument(
        "--rich-ascii",
        action="store_true",
        help="Use ASCII characters for rich table borders.",
    )

    # introspect-specific arguments
    # INTROSPECT_TYPES is this backend's own answer to what families Postgres
    # has; the CLI offers exactly that list.
    parser.add_argument(
        "type",
        choices=INTROSPECT_TYPES,
        help="Introspection type: " + ", ".join(INTROSPECT_TYPES),
    )
    parser.add_argument(
        "name",
        nargs="?",
        default=None,
        help="Object name. Required for columns, indexes and foreign-keys "
        "(the table they belong to); optional for triggers (a table) "
        "and for views, materialized-views, sequences and extensions "
        "(one object); refused for tables and database",
    )
    parser.add_argument(
        "--include-system",
        action="store_true",
        help="Include system tables in output",
    )

    return parser


def handle(args):
    """Handle the introspect subcommand."""
    provider = create_provider(args.output, ascii_borders=args.rich_ascii)

    named_conn = getattr(args, "named_connection", None)
    if not named_conn and not args.database:
        print("Error: --database is required for introspection", file=sys.stderr)
        sys.exit(1)

    config = resolve_connection_config_from_args(args)

    if args.is_async:
        backend = AsyncPostgresBackend(connection_config=config)
        asyncio.run(_handle_introspect_async(args, backend, provider))
    else:
        backend = PostgresBackend(connection_config=config)
        _handle_introspect_sync(args, backend, provider)


# ---------------------------------------------------------------------------
# Internal helper functions
# ---------------------------------------------------------------------------


def _serialize_for_output(obj: Any) -> Any:
    """Serialize object for JSON output, handling non-serializable types."""
    if obj is None:
        return None
    if hasattr(obj, "model_dump"):
        try:
            result = obj.model_dump(mode="json")
            return _serialize_for_output(result)
        except TypeError:
            result = obj.model_dump()
            return _serialize_for_output(result)
    if is_dataclass(obj) and not isinstance(obj, type):
        return {k: _serialize_for_output(v) for k, v in asdict(obj).items()}
    if isinstance(obj, Enum):
        return obj.value
    if isinstance(obj, dict):
        return {k: _serialize_for_output(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [_serialize_for_output(item) for item in obj]
    if isinstance(obj, (str, int, float, bool)):
        return obj
    return str(obj)


def _extension_rows(extensions: List[ExtensionInfo]) -> List[Dict[str, Any]]:
    """The installed-extension listing as the CLI reports it.

    The rows come from ``pg_extension`` through the introspector, so the
    answer is what this database has installed. The dialect's ``_extensions``
    registry is a catalogue of every extension the dialect *knows* -- dozens,
    regardless of the server -- and reading it here made the answer grow with
    the code rather than with the database.
    """
    return [
        {
            "name": ext.name,
            "version": ext.version,
            "schema": ext.schema,
            "description": ext.description,
        }
        for ext in extensions
    ]


def _handle_introspect_sync(args, backend: PostgresBackend, provider):
    """Handle introspect subcommand synchronously."""
    try:
        backend.connect()
        # Adapted once, at the entrance: the switch that decides whether the
        # family can be asked must answer for the server in front of us, and so
        # must every formatter downstream. Adapting inside the branches that
        # happened to need it made "can this be asked" depend on where the
        # question was read.
        backend.introspect_and_adapt()
        _require_capability(backend, args)

        introspector = backend.introspector

        if args.type == "tables":
            if args.name:
                _refuse_name(
                    args,
                    "use 'columns', 'indexes' or 'foreign-keys' with a table name",
                )
            tables = introspector.list_tables(schema=None, include_system=args.include_system)
            data = _serialize_for_output(tables)
            provider.display_results(data, title="Tables")

        elif args.type == "views":
            if args.name:
                info = introspector.get_view_info(args.name, schema=None)
                if info is None:
                    print(f"Error: View '{args.name}' not found", file=sys.stderr)
                    sys.exit(1)
                data = _serialize_for_output([info])
                provider.display_results(data, title=f"View {args.name}")
            else:
                views = introspector.list_views(schema=None)
                data = _serialize_for_output(views)
                provider.display_results(data, title="Views")

        elif args.type == "columns":
            if not args.name:
                print("Error: Table name is required for 'columns' introspection", file=sys.stderr)
                sys.exit(1)
            columns = introspector.list_columns(args.name, schema=None)
            data = _serialize_for_output(columns)
            provider.display_results(data, title=f"Columns of {args.name}")

        elif args.type == "indexes":
            if not args.name:
                print("Error: Table name is required for 'indexes' introspection", file=sys.stderr)
                sys.exit(1)
            indexes = introspector.list_indexes(args.name, schema=None)
            data = _serialize_for_output(indexes)
            provider.display_results(data, title=f"Indexes of {args.name}")

        elif args.type == "foreign-keys":
            if not args.name:
                print(
                    "Error: Table name is required for 'foreign-keys' introspection",
                    file=sys.stderr,
                )
                sys.exit(1)
            fks = introspector.list_foreign_keys(args.name, schema=None)
            data = _serialize_for_output(fks)
            provider.display_results(data, title=f"Foreign Keys of {args.name}")

        elif args.type == "triggers":
            triggers = introspector.list_triggers(table_name=args.name, schema=None)
            data = _serialize_for_output(triggers)
            provider.display_results(data, title="Triggers")

        elif args.type == "database":
            if args.name:
                _refuse_name(args, "it describes the database the run connected to")
            info = introspector.get_database_info()
            data = _serialize_for_output(info)
            provider.display_results([data], title="Database Info")

        elif args.type == "extensions":
            if args.name:
                info = introspector.get_extension_info(args.name)
                if info is None:
                    print(
                        f"Error: Extension '{args.name}' not installed",
                        file=sys.stderr,
                    )
                    sys.exit(1)
                data = _serialize_for_output(_extension_rows([info]))
                provider.display_results(data, title=f"Extension {args.name}")
            else:
                data = _serialize_for_output(_extension_rows(introspector.list_extensions()))
                provider.display_results(data, title="Extensions")

        elif args.type == "materialized-views":
            if args.name:
                info = introspector.get_materialized_view_info(args.name, schema=None)
                if info is None:
                    print(
                        f"Error: Materialized view '{args.name}' not found",
                        file=sys.stderr,
                    )
                    sys.exit(1)
                data = _serialize_for_output([info])
                provider.display_results(data, title=f"Materialized View {args.name}")
            else:
                views = introspector.list_materialized_views(
                    schema=None,
                    include_system=args.include_system,
                )
                data = _serialize_for_output(views)
                provider.display_results(data, title="Materialized Views")

        elif args.type == "sequences":
            if args.name:
                info = introspector.get_sequence_info(args.name, schema=None)
                if info is None:
                    print(f"Error: Sequence '{args.name}' not found", file=sys.stderr)
                    sys.exit(1)
                data = _serialize_for_output([info])
                provider.display_results(data, title=f"Sequence {args.name}")
            else:
                sequences = introspector.list_sequences(schema=None)
                data = _serialize_for_output(sequences)
                provider.display_results(data, title="Sequences")

    except ConnectionError as e:
        provider.display_connection_error(e)
        sys.exit(1)
    except QueryError as e:
        provider.display_query_error(e)
        sys.exit(1)
    except Exception as e:
        print(f"Error during introspection: {e}", file=sys.stderr)
        sys.exit(1)
    finally:
        if backend._connection:  # type: ignore
            backend.disconnect()


async def _handle_introspect_async(args, backend: AsyncPostgresBackend, provider):
    """Handle introspect subcommand asynchronously."""
    try:
        await backend.connect()
        # Same reason as the sync handler: the dialect must know the server
        # before a capability switch -- or a formatter -- can answer.
        await backend.introspect_and_adapt()
        _require_capability(backend, args)

        introspector = backend.introspector

        if args.type == "tables":
            if args.name:
                _refuse_name(
                    args,
                    "use 'columns', 'indexes' or 'foreign-keys' with a table name",
                )
            tables = await introspector.list_tables(schema=None, include_system=args.include_system)
            data = _serialize_for_output(tables)
            provider.display_results(data, title="Tables")

        elif args.type == "views":
            if args.name:
                info = await introspector.get_view_info(args.name, schema=None)
                if info is None:
                    print(f"Error: View '{args.name}' not found", file=sys.stderr)
                    sys.exit(1)
                data = _serialize_for_output([info])
                provider.display_results(data, title=f"View {args.name}")
            else:
                views = await introspector.list_views(schema=None)
                data = _serialize_for_output(views)
                provider.display_results(data, title="Views")

        elif args.type == "columns":
            if not args.name:
                print("Error: Table name is required for 'columns' introspection", file=sys.stderr)
                sys.exit(1)
            columns = await introspector.list_columns(args.name, schema=None)
            data = _serialize_for_output(columns)
            provider.display_results(data, title=f"Columns of {args.name}")

        elif args.type == "indexes":
            if not args.name:
                print("Error: Table name is required for 'indexes' introspection", file=sys.stderr)
                sys.exit(1)
            indexes = await introspector.list_indexes(args.name, schema=None)
            data = _serialize_for_output(indexes)
            provider.display_results(data, title=f"Indexes of {args.name}")

        elif args.type == "foreign-keys":
            if not args.name:
                print(
                    "Error: Table name is required for 'foreign-keys' introspection",
                    file=sys.stderr,
                )
                sys.exit(1)
            fks = await introspector.list_foreign_keys(args.name, schema=None)
            data = _serialize_for_output(fks)
            provider.display_results(data, title=f"Foreign Keys of {args.name}")

        elif args.type == "triggers":
            triggers = await introspector.list_triggers(table_name=args.name, schema=None)
            data = _serialize_for_output(triggers)
            provider.display_results(data, title="Triggers")

        elif args.type == "database":
            if args.name:
                _refuse_name(args, "it describes the database the run connected to")
            info = await introspector.get_database_info()
            data = _serialize_for_output(info)
            provider.display_results([data], title="Database Info")

        elif args.type == "extensions":
            if args.name:
                info = await introspector.get_extension_info(args.name)
                if info is None:
                    print(
                        f"Error: Extension '{args.name}' not installed",
                        file=sys.stderr,
                    )
                    sys.exit(1)
                data = _serialize_for_output(_extension_rows([info]))
                provider.display_results(data, title=f"Extension {args.name}")
            else:
                data = _serialize_for_output(_extension_rows(await introspector.list_extensions()))
                provider.display_results(data, title="Extensions")

        elif args.type == "materialized-views":
            if args.name:
                info = await introspector.get_materialized_view_info(args.name, schema=None)
                if info is None:
                    print(
                        f"Error: Materialized view '{args.name}' not found",
                        file=sys.stderr,
                    )
                    sys.exit(1)
                data = _serialize_for_output([info])
                provider.display_results(data, title=f"Materialized View {args.name}")
            else:
                views = await introspector.list_materialized_views(
                    schema=None,
                    include_system=args.include_system,
                )
                data = _serialize_for_output(views)
                provider.display_results(data, title="Materialized Views")

        elif args.type == "sequences":
            if args.name:
                info = await introspector.get_sequence_info(args.name, schema=None)
                if info is None:
                    print(
                        f"Error: Sequence '{args.name}' not found",
                        file=sys.stderr,
                    )
                    sys.exit(1)
                data = _serialize_for_output([info])
                provider.display_results(data, title=f"Sequence {args.name}")
            else:
                sequences = await introspector.list_sequences(schema=None)
                data = _serialize_for_output(sequences)
                provider.display_results(data, title="Sequences")

    except ConnectionError as e:
        provider.display_connection_error(e)
        sys.exit(1)
    except QueryError as e:
        provider.display_query_error(e)
        sys.exit(1)
    except Exception as e:
        print(f"Error during introspection: {e}", file=sys.stderr)
        sys.exit(1)
    finally:
        if backend._connection:  # type: ignore
            await backend.disconnect()
