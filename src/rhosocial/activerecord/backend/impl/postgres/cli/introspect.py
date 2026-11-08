# src/rhosocial/activerecord/backend/impl/postgres/cli/introspect.py
"""introspect subcommand - Database introspection.

PostgreSQL introspect includes the 'extensions' type in addition to
the standard types, plus the two schema-object families this backend
answers for on its own: materialized views and sequences. The families the
catalog and the statistics collector answer beside the schema structure --
unused indexes, usage counters, runtime metrics, dependencies, DDL and the
data profiles -- are offered too: the dialect declares it can answer them,
so the CLI has to be able to ask.

A type either lists a family or, when it names a single catalogue object,
answers for that one object; ``NAME`` is required, optional or refused
according to which of the two it is, and never silently dropped. The rule per
type lives in ``_NAME_RULE`` and is applied by one validator, so a family
cannot be added without deciding what its NAME means.
"""

import argparse
import sys
from dataclasses import asdict, is_dataclass
from enum import Enum
from typing import Any, Dict, List

from rhosocial.activerecord.backend.impl.postgres.async_support import run_coroutine
from rhosocial.activerecord.backend.impl.postgres.backend.async_backend import AsyncPostgresBackend
from rhosocial.activerecord.backend.impl.postgres.backend import PostgresBackend
from rhosocial.activerecord.backend.errors import ConnectionError, QueryError
from rhosocial.activerecord.backend.introspection.status import ExtensionInfo

from .connection import add_connection_args, resolve_connection_config_from_args
from .output import OUTPUT_CHOICES, create_provider

#: CLI type -> the dialect capability switch that answers for it.
#:
#: The advertised list is derived from this mapping, so a type cannot be
#: offered without saying which switch decides whether the connected server
#: can answer it. A type whose switch is False is refused when handled, rather
#: than run and answered with an empty list: "this server cannot report
#: sequences" and "this database has no sequences" are different answers.
#:
#: One entry per family this backend answers independently, whether the answer
#: comes from the catalog, from the statistics collector or from the rows
#: themselves. Partition metadata has a switch too, but no introspector
#: question of its own -- it is read as part of the DDL a partitioned table
#: carries -- so it is not a type here.
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
    # The per-table storage family answers for one table: the three size
    # figures ride the listing's projection, and this type asks it for one
    # name rather than making the caller read the whole listing.
    "table-storage": "supports_table_storage_introspection",
    # The operational families, read from the statistics collector rather
    # than the catalog: how the server is behaving, not what it contains.
    # They are listings over the connection's scope, so none takes a name --
    # ``dependencies`` may be narrowed to one object's edges but answers the
    # whole graph without one.
    "unused-indexes": "supports_unused_indexes_detection",
    "table-stats": "supports_table_stats",
    "runtime": "supports_runtime_stats",
    "index-stats": "supports_index_stats",
    "dependencies": "supports_object_dependencies",
    # The one-object families: the DDL that recreates a named table, and the
    # data profiles of one table or one column of it. They read the catalog,
    # pg_stats or the rows themselves, and every answer carries how it was
    # produced so the caller need not assume which.
    "ddl": "supports_ddl_extraction",
    "column-profiles": "supports_column_data_profile",
    "value-samples": "supports_column_value_sample",
    "join-coverage": "supports_join_coverage",
}

INTROSPECT_TYPES = list(_TYPE_CAPABILITIES)

#: What the optional NAME positional means for each type.
#:
#: ``required`` -- the type asks about one table and cannot be answered without
#: it. ``optional`` -- a catalogue family that can also answer for one member,
#: so a given name narrows it (and a name that matches nothing is an error, not
#: an empty list). ``forbidden`` -- the family has no single-member question
#: here, so a name would read as a filter that matched everything.
#:
#: The order is the order ``_TYPE_CAPABILITIES`` declares and the parser and
#: ``--help`` print, and the two tables cover the same keys: a type offered
#: without a rule is a type whose NAME decision nobody has made.
#:
#: ``tables`` stays one. Answering for one table would fill in its columns,
#: indexes and foreign keys, which is the aggregate this command deliberately
#: dropped. ``table-storage`` is the opposite case: its three figures are that
#: table's own, so the name *is* the question.
_NAME_RULE = {
    "tables": "forbidden",
    "views": "optional",
    "columns": "required",
    "indexes": "required",
    "foreign-keys": "required",
    "triggers": "optional",
    "database": "forbidden",
    "extensions": "optional",
    "materialized-views": "optional",
    "sequences": "optional",
    "table-storage": "required",
    # The four listings over the statistics collector's scope describe what the
    # connection can see, not one object in it, so none takes a name -- the way
    # ``tables`` does not. ``dependencies`` is not one of them: one object's
    # edges are a real question, so a name narrows rather than being refused.
    "unused-indexes": "forbidden",
    "table-stats": "forbidden",
    "runtime": "forbidden",
    "index-stats": "forbidden",
    "dependencies": "optional",
    # The one-object families: the DDL that recreates one table, the profiles
    # of its columns, and the samples of one of them.
    "ddl": "required",
    "column-profiles": "required",
    "value-samples": "required",
    # ``join-coverage`` asks about two tables; the left one is NAME, so it is
    # required here the way the right one is required on its own flag.
    "join-coverage": "required",
}


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


def _validate_name(args: argparse.Namespace) -> None:
    """Apply the NAME rule table, once, instead of leaving it to each branch.

    A silently ignored argument is the worst of the three outcomes: the run
    succeeds, prints a full listing, and reads as though it had been narrowed.
    So the table above is read here, once -- a branch that forgot would answer
    a wider question than the one asked and look successful doing it.

    A *missing* name is an incomplete invocation and leaves with 1, the code a
    missing ``--database`` gets; a name the type has no question for is a
    request this command declines and leaves with 2.
    """
    rule = _NAME_RULE.get(args.type)
    if rule == "required" and not args.name:
        print(
            f"Error: Table name is required for '{args.type}' introspection",
            file=sys.stderr,
        )
        sys.exit(1)
    if rule == "forbidden" and args.name:
        print(
            f"Error: '{args.type}' does not take an object name; "
            "it answers for the whole database",
            file=sys.stderr,
        )
        sys.exit(2)


def _refuse_unused_arguments(args: argparse.Namespace) -> None:
    """Refuse a flag the chosen type has no question for.

    The same defect as a silently dropped name, one flag over: ``--column``
    beside ``introspect tables`` would be ignored and the listing printed as
    if the column had been part of the question. Only the families whose
    probe takes a column read the flag, so only they may be given it.

    The flags are read with ``getattr`` because the handler is also driven
    with hand-built namespaces (the offline tests), which carry no flags at
    all -- an absent attribute is "not given", not an error.
    """
    column = getattr(args, "column", None)
    right_table = getattr(args, "right_table", None)
    right_column = getattr(args, "right_column", None)

    if column is not None and args.type not in ("value-samples", "join-coverage"):
        print(
            f"Error: '{args.type}' does not take --column; "
            "use 'value-samples' or 'join-coverage'",
            file=sys.stderr,
        )
        sys.exit(2)
    if (right_table is not None or right_column is not None) and args.type != "join-coverage":
        print(
            f"Error: '{args.type}' does not take --right-table or --right-column; "
            "use 'join-coverage'",
            file=sys.stderr,
        )
        sys.exit(2)


def _display_ddl(provider, statement, args) -> None:
    """Show a DDL statement in the form that stays readable.

    A ``CREATE TABLE`` is text: a one-row table folds it into a cell that
    wraps at the terminal width and interleaves with the other columns. Table
    output therefore prints the statement itself, while the structured
    formats keep the whole row so JSON and CSV still carry ``object_type``
    and ``native`` beside the text.
    """
    if args.output == "table":
        print(statement.ddl or "")
        return
    data = _serialize_for_output([statement])
    provider.display_results(data, title=f"DDL of {statement.object_name}")


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

  # Get one table's storage split (total, data, index bytes; row estimate)
  %(prog)s table-storage orders --database mydb

  # Indexes no query has used since the statistics were reset, and the
  # per-index counters that say how much the rest are used
  %(prog)s unused-indexes --database mydb
  %(prog)s index-stats --database mydb

  # Per-table usage counters and live runtime measurements
  %(prog)s table-stats --database mydb
  %(prog)s runtime --database mydb

  # The dependency graph, or one object's dependants
  %(prog)s dependencies --database mydb
  %(prog)s dependencies orders --database mydb

  # The DDL that recreates one table
  %(prog)s ddl orders --database mydb

  # Profile every column of a table from its data, and sample one column
  %(prog)s column-profiles orders --database mydb
  %(prog)s value-samples orders --column status --database mydb

  # Measure how two columns' value domains overlap
  %(prog)s join-coverage orders --column buyer_id --right-table users --right-column id

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
        help="Object name. Required for columns, indexes, foreign-keys, "
        "table-storage, ddl and column-profiles (the table they belong to), "
        "and for value-samples and join-coverage (the left table); "
        "optional for triggers (a table), dependencies (one object) "
        "and for views, materialized-views, sequences and extensions "
        "(one object); refused for tables, database and the four counter "
        "listings (unused-indexes, table-stats, runtime, index-stats). "
        "The rule per type is _NAME_RULE",
    )
    # The three families that ask about a column or a second table carry the
    # names the probe needs. They stay flags rather than more positionals
    # because ``name`` already means "the table" everywhere else, and
    # ``value-samples orders.email`` would read as a dotted object name.
    parser.add_argument(
        "--column",
        default=None,
        help="Column name: the column to sample for value-samples, and the "
        "left column for join-coverage",
    )
    parser.add_argument(
        "--right-table",
        default=None,
        help="Right-hand table for join-coverage",
    )
    parser.add_argument(
        "--right-column",
        default=None,
        help="Right-hand column for join-coverage",
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
        run_coroutine(_handle_introspect_async(args, backend, provider))
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
        _refuse_unused_arguments(args)
        _validate_name(args)

        introspector = backend.introspector

        if args.type == "tables":
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
            columns = introspector.list_columns(args.name, schema=None)
            data = _serialize_for_output(columns)
            provider.display_results(data, title=f"Columns of {args.name}")

        elif args.type == "indexes":
            indexes = introspector.list_indexes(args.name, schema=None)
            data = _serialize_for_output(indexes)
            provider.display_results(data, title=f"Indexes of {args.name}")

        elif args.type == "foreign-keys":
            fks = introspector.list_foreign_keys(args.name, schema=None)
            data = _serialize_for_output(fks)
            provider.display_results(data, title=f"Foreign Keys of {args.name}")

        elif args.type == "triggers":
            triggers = introspector.list_triggers(table_name=args.name, schema=None)
            data = _serialize_for_output(triggers)
            provider.display_results(data, title="Triggers")

        elif args.type == "database":
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
        elif args.type == "table-storage":
            # The storage figures ride the listing's projection, so the
            # question asks the listing and picks the row -- one read, and
            # none of the three per-table questions get_table_info would add
            # (columns, indexes, foreign keys) that a storage answer does not
            # need. ``_validate_name`` has already refused a run without one:
            # every figure below is that table's own.
            tables = introspector.list_tables(schema=None, include_system=args.include_system)
            info = next((t for t in tables if t.name == args.name), None)
            if info is None:
                print(f"Error: Table '{args.name}' not found", file=sys.stderr)
                sys.exit(1)
            data = _serialize_for_output([info])
            provider.display_results(data, title=f"Table storage {args.name}")

        # ---- the operational families -------------------------------------
        # These read the statistics collector, not the catalog: each one is a
        # listing of the whole connection's scope, so none takes a name (the
        # dependency graph may be narrowed to one object, and answers the
        # whole graph without it).

        elif args.type == "unused-indexes":
            indexes = introspector.list_unused_indexes(schema=None)
            data = _serialize_for_output(indexes)
            provider.display_results(data, title="Unused Indexes")

        elif args.type == "table-stats":
            stats = introspector.list_table_usage_stats(schema=None)
            data = _serialize_for_output(stats)
            provider.display_results(data, title="Table Usage Stats")

        elif args.type == "runtime":
            metrics = introspector.list_runtime_metrics(schema=None)
            data = _serialize_for_output(metrics)
            provider.display_results(data, title="Runtime Metrics")

        elif args.type == "index-stats":
            stats = introspector.list_index_usage_stats(schema=None)
            data = _serialize_for_output(stats)
            provider.display_results(data, title="Index Usage Stats")

        elif args.type == "dependencies":
            # A name narrows the graph to the edges that object carries --
            # "what breaks if I drop this" -- rather than asking a different
            # family, so it is optional here the way a trigger's table is.
            dependencies = introspector.list_object_dependencies(object_name=args.name, schema=None)
            data = _serialize_for_output(dependencies)
            title = f"Dependencies of {args.name}" if args.name else "Object Dependencies"
            provider.display_results(data, title=title)

        # ---- the one-object families --------------------------------------
        # The DDL that recreates a table, and the profiles of one table or one
        # column of it: the name is the table and the column is named on its
        # own flag.

        elif args.type == "ddl":
            statement = introspector.get_ddl_statement(args.name, object_type=None, schema=None)
            if statement is None:
                # Not "no such table": the object exists, and PostgreSQL
                # answers with no text rather than an assembled fragment that
                # would read as authoritative. The two are worth telling
                # apart, so this exits without printing an empty row.
                print(f"Error: no DDL available for '{args.name}'", file=sys.stderr)
                sys.exit(1)
            _display_ddl(provider, statement, args)

        elif args.type == "column-profiles":
            profiles = introspector.list_column_data_profiles(args.name, schema=None)
            data = _serialize_for_output(profiles)
            provider.display_results(data, title=f"Column profiles of {args.name}")

        elif args.type == "value-samples":
            if not args.column:
                print(
                    "Error: --column is required for 'value-samples' introspection",
                    file=sys.stderr,
                )
                sys.exit(1)
            samples = introspector.sample_column_values(args.name, args.column, schema=None)
            data = _serialize_for_output(samples)
            provider.display_results(data, title=f"Values of {args.name}.{args.column}")

        elif args.type == "join-coverage":
            if not args.column:
                print(
                    "Error: --column is required for 'join-coverage' introspection",
                    file=sys.stderr,
                )
                sys.exit(1)
            if not args.right_table or not args.right_column:
                print(
                    "Error: --right-table and --right-column are required for "
                    "'join-coverage' introspection",
                    file=sys.stderr,
                )
                sys.exit(1)
            coverage = introspector.estimate_join_coverage(
                args.name, args.column, args.right_table, args.right_column, schema=None
            )
            if coverage is None:
                # A column with nothing to match answers "no measurement",
                # which is not zero overlap; printing a row of nulls would
                # read as a measured zero.
                print(
                    f"Error: no coverage could be measured for {args.name}.{args.column} "
                    f"and {args.right_table}.{args.right_column}",
                    file=sys.stderr,
                )
                sys.exit(1)
            data = _serialize_for_output([coverage])
            provider.display_results(data, title="Join Coverage")

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
        _refuse_unused_arguments(args)
        _validate_name(args)

        introspector = backend.introspector

        if args.type == "tables":
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
            columns = await introspector.list_columns(args.name, schema=None)
            data = _serialize_for_output(columns)
            provider.display_results(data, title=f"Columns of {args.name}")

        elif args.type == "indexes":
            indexes = await introspector.list_indexes(args.name, schema=None)
            data = _serialize_for_output(indexes)
            provider.display_results(data, title=f"Indexes of {args.name}")

        elif args.type == "foreign-keys":
            fks = await introspector.list_foreign_keys(args.name, schema=None)
            data = _serialize_for_output(fks)
            provider.display_results(data, title=f"Foreign Keys of {args.name}")

        elif args.type == "triggers":
            triggers = await introspector.list_triggers(table_name=args.name, schema=None)
            data = _serialize_for_output(triggers)
            provider.display_results(data, title="Triggers")

        elif args.type == "database":
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
        elif args.type == "table-storage":
            # The async twin of the sync branch: the same listing read,
            # awaited, and the same pick.
            tables = await introspector.list_tables(schema=None, include_system=args.include_system)
            info = next((t for t in tables if t.name == args.name), None)
            if info is None:
                print(f"Error: Table '{args.name}' not found", file=sys.stderr)
                sys.exit(1)
            data = _serialize_for_output([info])
            provider.display_results(data, title=f"Table storage {args.name}")

        # The async twins of the nine branches above: same method, same
        # argument and same title, awaited. They are written out rather than
        # shared with the sync chain because the executor is the only thing
        # that differs, and a shared body would have to carry an ``await``
        # flag that reads worse than the duplicate.

        elif args.type == "unused-indexes":
            indexes = await introspector.list_unused_indexes(schema=None)
            data = _serialize_for_output(indexes)
            provider.display_results(data, title="Unused Indexes")

        elif args.type == "table-stats":
            stats = await introspector.list_table_usage_stats(schema=None)
            data = _serialize_for_output(stats)
            provider.display_results(data, title="Table Usage Stats")

        elif args.type == "runtime":
            metrics = await introspector.list_runtime_metrics(schema=None)
            data = _serialize_for_output(metrics)
            provider.display_results(data, title="Runtime Metrics")

        elif args.type == "index-stats":
            stats = await introspector.list_index_usage_stats(schema=None)
            data = _serialize_for_output(stats)
            provider.display_results(data, title="Index Usage Stats")

        elif args.type == "dependencies":
            dependencies = await introspector.list_object_dependencies(
                object_name=args.name, schema=None
            )
            data = _serialize_for_output(dependencies)
            title = f"Dependencies of {args.name}" if args.name else "Object Dependencies"
            provider.display_results(data, title=title)

        elif args.type == "ddl":
            statement = await introspector.get_ddl_statement(
                args.name, object_type=None, schema=None
            )
            if statement is None:
                print(f"Error: no DDL available for '{args.name}'", file=sys.stderr)
                sys.exit(1)
            _display_ddl(provider, statement, args)

        elif args.type == "column-profiles":
            profiles = await introspector.list_column_data_profiles(args.name, schema=None)
            data = _serialize_for_output(profiles)
            provider.display_results(data, title=f"Column profiles of {args.name}")

        elif args.type == "value-samples":
            if not args.column:
                print(
                    "Error: --column is required for 'value-samples' introspection",
                    file=sys.stderr,
                )
                sys.exit(1)
            samples = await introspector.sample_column_values(args.name, args.column, schema=None)
            data = _serialize_for_output(samples)
            provider.display_results(data, title=f"Values of {args.name}.{args.column}")

        elif args.type == "join-coverage":
            if not args.column:
                print(
                    "Error: --column is required for 'join-coverage' introspection",
                    file=sys.stderr,
                )
                sys.exit(1)
            if not args.right_table or not args.right_column:
                print(
                    "Error: --right-table and --right-column are required for "
                    "'join-coverage' introspection",
                    file=sys.stderr,
                )
                sys.exit(1)
            coverage = await introspector.estimate_join_coverage(
                args.name, args.column, args.right_table, args.right_column, schema=None
            )
            if coverage is None:
                print(
                    f"Error: no coverage could be measured for {args.name}.{args.column} "
                    f"and {args.right_table}.{args.right_column}",
                    file=sys.stderr,
                )
                sys.exit(1)
            data = _serialize_for_output([coverage])
            provider.display_results(data, title="Join Coverage")

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
