# src/rhosocial/activerecord/backend/impl/postgres/__main__.py
"""
PostgreSQL backend command-line interface.

Provides SQL execution and database information display capabilities.
"""

import argparse
import asyncio
import inspect
import json
import logging
import os
import sys
from dataclasses import asdict, is_dataclass
from enum import Enum
from typing import Dict, List, Any, Optional, Tuple

from . import PostgresBackend, AsyncPostgresBackend
from .config import PostgresConnectionConfig
from .dialect import PostgresDialect
from .protocols import (
    PostgresExtensionInfo,
    PostgresExtensionSupport,
    PostgresMaterializedViewSupport,
    PostgresTableSupport,
    PostgresPgvectorSupport,
    PostgresPostGISSupport,
    PostgresPgTrgmSupport,
    PostgresHstoreSupport,
    PostgresPartitionSupport,
    PostgresIndexSupport,
    PostgresVacuumSupport,
    PostgresQueryOptimizationSupport,
    PostgresDataTypeSupport,
    PostgresSQLSyntaxSupport,
    PostgresLogicalReplicationSupport,
    PostgresLtreeSupport,
    PostgresIntarraySupport,
    PostgresEarthdistanceSupport,
    PostgresTablefuncSupport,
    PostgresPgStatStatementsSupport,
    PostgresTriggerSupport,
    PostgresCommentSupport,
    PostgresTypeSupport,
    PostgresMultirangeSupport,
    PostgresEnumTypeSupport,
)
from rhosocial.activerecord.backend.errors import ConnectionError, QueryError
from rhosocial.activerecord.backend.output import JsonOutputProvider, CsvOutputProvider, TsvOutputProvider
from rhosocial.activerecord.backend.introspection.status import StatusCategory
from rhosocial.activerecord.backend.dialect.protocols import (
    WindowFunctionSupport,
    CTESupport,
    FilterClauseSupport,
    ReturningSupport,
    UpsertSupport,
    LateralJoinSupport,
    JoinSupport,
    JSONSupport,
    ExplainSupport,
    GraphSupport,
    SetOperationSupport,
    ViewSupport,
    TableSupport,
    TruncateSupport,
    GeneratedColumnSupport,
    TriggerSupport,
    FunctionSupport,
    AdvancedGroupingSupport,
    ArraySupport,
    ILIKESupport,
    IndexSupport,
    LockingSupport,
    MergeSupport,
    OrderedSetAggregationSupport,
    QualifyClauseSupport,
    SchemaSupport,
    SequenceSupport,
    TemporalTableSupport,
)
from rhosocial.activerecord.backend.named_query.cli import (
    create_named_query_parser,
    handle_named_query as handle_nq,
)
from rhosocial.activerecord.backend.options import ExecutionOptions

# Attempt to import rich for formatted output
try:
    from rich.logging import RichHandler
    from rhosocial.activerecord.backend.output_rich import RichOutputProvider

    RICH_AVAILABLE = True
except ImportError:
    RICH_AVAILABLE = False
    RichOutputProvider = None  # type: ignore[misc,assignment]

logger = logging.getLogger(__name__)

# Groups that are specific to PostgreSQL dialect
DIALECT_SPECIFIC_GROUPS = {"PostgreSQL Native", "PostgreSQL Extensions"}

# Supported introspection types for CLI
INTROSPECT_TYPES = [
    "tables", "views", "table", "columns",
    "indexes", "foreign-keys", "triggers", "database"
]

STATUS_TYPES = ["all", "config", "performance", "connections", "storage", "databases", "users"]


def _serialize_for_output(obj: Any) -> Any:
    """Serialize object for JSON output, handling non-serializable types.

    Handles Pydantic models, dataclasses, Enums, and nested structures.
    """
    if obj is None:
        return None
    if hasattr(obj, 'model_dump'):
        # Pydantic model
        try:
            result = obj.model_dump(mode='json')
            return _serialize_for_output(result)
        except TypeError:
            result = obj.model_dump()
            return _serialize_for_output(result)
    if is_dataclass(obj) and not isinstance(obj, type):
        # Dataclass instance
        return {k: _serialize_for_output(v) for k, v in asdict(obj).items()}
    if isinstance(obj, Enum):
        return obj.value
    if isinstance(obj, dict):
        return {k: _serialize_for_output(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [_serialize_for_output(item) for item in obj]
    if isinstance(obj, (str, int, float, bool)):
        return obj
    # Fallback: convert to string
    return str(obj)

# Protocol family groups for display
PROTOCOL_FAMILY_GROUPS: Dict[str, list] = {
    "Query Features": [
        WindowFunctionSupport,
        CTESupport,
        FilterClauseSupport,
        SetOperationSupport,
        AdvancedGroupingSupport,
    ],
    "JOIN Support": [JoinSupport, LateralJoinSupport],
    "Data Types": [JSONSupport, ArraySupport],
    "DML Features": [
        ReturningSupport,
        UpsertSupport,
        MergeSupport,
        OrderedSetAggregationSupport,
    ],
    "Transaction & Locking": [LockingSupport, TemporalTableSupport],
    "Query Analysis": [ExplainSupport, GraphSupport, QualifyClauseSupport],
    "DDL - Table": [TableSupport, TruncateSupport, GeneratedColumnSupport],
    "DDL - View": [ViewSupport, PostgresMaterializedViewSupport],
    "DDL - Schema & Index": [SchemaSupport, IndexSupport],
    "DDL - Sequence & Trigger": [SequenceSupport, TriggerSupport, FunctionSupport],
    "String Matching": [ILIKESupport],
    "PostgreSQL Native": [
        PostgresPartitionSupport,
        PostgresIndexSupport,
        PostgresVacuumSupport,
        PostgresQueryOptimizationSupport,
        PostgresDataTypeSupport,
        PostgresSQLSyntaxSupport,
        PostgresLogicalReplicationSupport,
        PostgresTriggerSupport,
        PostgresCommentSupport,
        PostgresTypeSupport,
        PostgresMultirangeSupport,
        PostgresEnumTypeSupport,
    ],
    "PostgreSQL Extensions": [
        PostgresExtensionSupport,
        PostgresTableSupport,
        PostgresPgvectorSupport,
        PostgresPostGISSupport,
        PostgresPgTrgmSupport,
        PostgresHstoreSupport,
        PostgresLtreeSupport,
        PostgresIntarraySupport,
        PostgresEarthdistanceSupport,
        PostgresTablefuncSupport,
        PostgresPgStatStatementsSupport,
    ],
}


def parse_args():
    # =========================================================================
    # Design Notes:
    # =========================================================================
    # Uses explicit subcommand mode: info, query and introspect are peers.
    # This avoids argparse misidentifying SQL queries as subcommand names.
    #
    # Connection parameters are shared between subcommands and placed in parent parser.
    # Only subcommand parsers inherit from parent, main parser does not.
    # =========================================================================

    # Parent parser: shared connection parameters
    parent_parser = argparse.ArgumentParser(add_help=False)
    parent_parser.add_argument(
        "--host",
        default=os.getenv("POSTGRES_HOST", "localhost"),
        help="Database host (env: POSTGRES_HOST, default: localhost)",
    )
    parent_parser.add_argument(
        "--port",
        type=int,
        default=int(os.getenv("POSTGRES_PORT", "5432")),
        help="Database port (env: POSTGRES_PORT, default: 5432)",
    )
    parent_parser.add_argument(
        "--database",
        default=os.getenv("POSTGRES_DATABASE"),
        help="Database name (env: POSTGRES_DATABASE, optional for some operations)",
    )
    parent_parser.add_argument(
        "--user",
        default=os.getenv("POSTGRES_USER", "postgres"),
        help="Database user (env: POSTGRES_USER, default: postgres)",
    )
    parent_parser.add_argument(
        "--password",
        default=os.getenv("POSTGRES_PASSWORD", ""),
        help="Database password (env: POSTGRES_PASSWORD)",
    )
    parent_parser.add_argument(
        "--use-async",
        action="store_true",
        help="Use asynchronous backend",
    )
    parent_parser.add_argument(
        "--version",
        type=str,
        default=None,
        help='PostgreSQL version to simulate (e.g., "15.0.0"). Default: auto-detect.',
    )
    parent_parser.add_argument(
        "-o", "--output",
        choices=["table", "json", "csv", "tsv"],
        default="table",
        help='Output format. Defaults to "table" if rich is installed.',
    )
    parent_parser.add_argument(
        "--log-level",
        default="INFO",
        help="Set logging level (e.g., DEBUG, INFO)",
    )
    parent_parser.add_argument(
        "--rich-ascii",
        action="store_true",
        help="Use ASCII characters for rich table borders.",
    )

    # Main parser does NOT inherit from parent
    parser = argparse.ArgumentParser(
        description="Execute SQL queries against a PostgreSQL backend.",
        formatter_class=argparse.RawTextHelpFormatter,
    )

    # Global options
    parser.add_argument(
        "-v", "--verbose",
        action="count",
        default=0,
        help="Increase verbosity. -v for families, -vv for details.",
    )

    # Subcommands: info, query and introspect
    subparsers = parser.add_subparsers(dest="command", help="Available commands")

    # info subcommand
    info_parser = subparsers.add_parser(
        "info",
        help="Display PostgreSQL environment information",
        parents=[parent_parser],
    )

    # query subcommand
    query_parser = subparsers.add_parser(
        "query",
        help="Execute SQL query",
        parents=[parent_parser],
    )
    query_parser.add_argument(
        "sql",
        nargs="?",
        default=None,
        help="SQL query to execute. If not provided, reads from --file or stdin.",
    )
    query_parser.add_argument(
        "-f", "--file",
        default=None,
        help="Path to a file containing SQL to execute.",
    )

    # introspect subcommand
    introspect_parser = subparsers.add_parser(
        "introspect",
        help="Database introspection commands",
        parents=[parent_parser],
        epilog="""Examples:
  # List all tables in database
  %(prog)s tables --database mydb

  # List all views
  %(prog)s views --database mydb

  # Get detailed table info (columns, indexes, foreign keys)
  %(prog)s table users --database mydb

  # Get column details for a table
  %(prog)s columns users --database mydb

  # Get index information
  %(prog)s indexes users --database mydb

  # Get foreign key relationships
  %(prog)s foreign-keys users --database mydb

  # List triggers
  %(prog)s triggers --database mydb

  # Get database information
  %(prog)s database --database mydb

  # Output as JSON
  %(prog)s tables --database mydb -o json

  # Specify schema (PostgreSQL specific)
  %(prog)s tables --database mydb --schema public

  # Using environment variables for connection
  export POSTGRES_HOST=localhost POSTGRES_DATABASE=mydb POSTGRES_USER=postgres POSTGRES_PASSWORD=secret
  %(prog)s tables
""",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    introspect_parser.add_argument(
        "type",
        choices=INTROSPECT_TYPES,
        help="Introspection type: tables, views, table, columns, indexes, foreign-keys, triggers, database",
    )
    introspect_parser.add_argument(
        "name",
        nargs="?",
        default=None,
        help="Table/view name (required for table, columns, indexes, foreign-keys types)",
    )
    introspect_parser.add_argument(
        "--schema",
        default=None,
        help='Schema name (defaults to "public" for PostgreSQL)',
    )
    introspect_parser.add_argument(
        "--include-system",
        action="store_true",
        help="Include system tables in output",
    )

    # status subcommand
    status_parser = subparsers.add_parser(
        "status",
        help="Display server status overview",
        parents=[parent_parser],
        epilog="""Examples:
  # Show complete status overview
  %(prog)s all --database mydb

  # Show configuration parameters only
  %(prog)s config --database mydb

  # Show performance metrics only
  %(prog)s performance --database mydb

  # Show connection information
  %(prog)s connections --database mydb

  # Show storage information
  %(prog)s storage --database mydb

  # Show databases list
  %(prog)s databases --database mydb

  # Show users list
  %(prog)s users --database mydb

  # Output as JSON
  %(prog)s all --database mydb -o json

  # Using environment variables for connection
  export POSTGRES_HOST=localhost POSTGRES_DATABASE=mydb POSTGRES_USER=postgres POSTGRES_PASSWORD=secret
  %(prog)s all
""",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    status_parser.add_argument(
        "type",
        nargs="?",
        default="all",
        choices=STATUS_TYPES,
        help="Status type: all (default), config, performance, connections, storage, databases, users",
    )

    # named-query subcommand (using shared CLI helper)
    create_named_query_parser(subparsers, parent_parser)

    return parser.parse_args()


def get_provider(args):
    """Factory function to get the correct output provider."""
    output_format = args.output
    if output_format == "table" and not RICH_AVAILABLE:
        output_format = "json"

    if output_format == "table" and RICH_AVAILABLE:
        from rich.console import Console

        return RichOutputProvider(console=Console(), ascii_borders=args.rich_ascii)
    if output_format == "json":
        return JsonOutputProvider()
    if output_format == "csv":
        return CsvOutputProvider()
    if output_format == "tsv":
        return TsvOutputProvider()

    return JsonOutputProvider()


def get_protocol_support_methods(protocol_class: type) -> List[str]:
    """Get all support check methods from a protocol class.

    Supports both 'supports_*' and 'is_*_available' naming patterns.
    """
    methods = []
    for name, member in inspect.getmembers(protocol_class):
        is_supports = name.startswith("supports_")
        is_available = name.startswith("is_") and name.endswith("_available")
        if callable(member) and (is_supports or is_available):
            methods.append(name)
    return sorted(methods)


# All possible test arguments for methods that require parameters
# This allows detailed display of which specific arguments are supported
SUPPORT_METHOD_ALL_ARGS: Dict[str, List[str]] = {
    # ExplainSupport: all possible format types
    "supports_explain_format": ["TEXT", "JSON", "XML", "YAML", "TREE", "DOT"],
}


def check_protocol_support(dialect: PostgresDialect, protocol_class: type) -> Dict[str, Any]:
    """Check all support methods for a protocol against the dialect.

    For methods requiring parameters, tests all possible arguments.

    Returns:
        Dict with method names as keys. For no-arg methods: bool value.
        For methods with parameters: dict with 'supported', 'total', 'args' keys.
    """
    results = {}
    methods = get_protocol_support_methods(protocol_class)
    for method_name in methods:
        if hasattr(dialect, method_name):
            try:
                method = getattr(dialect, method_name)
                # Check if method requires arguments (beyond self)
                sig = inspect.signature(method)
                params = [p for p in sig.parameters.values() if p.default == inspect.Parameter.empty]
                required_params = [p for p in params if p.name != "self"]

                if len(required_params) == 0:
                    # No required parameters, call directly
                    result = method()
                    results[method_name] = bool(result)
                elif method_name in SUPPORT_METHOD_ALL_ARGS:
                    # Test all possible arguments
                    all_args = SUPPORT_METHOD_ALL_ARGS[method_name]
                    arg_results = {}
                    for arg in all_args:
                        try:
                            arg_results[arg] = bool(method(arg))
                        except Exception:
                            arg_results[arg] = False
                    supported_count = sum(1 for v in arg_results.values() if v)
                    results[method_name] = {"supported": supported_count, "total": len(all_args), "args": arg_results}
                else:
                    # Unknown method requiring parameters, skip
                    results[method_name] = False
            except Exception:
                results[method_name] = False
        else:
            results[method_name] = False
    return results


def parse_version(version_str: str) -> Tuple[int, int, int]:
    """Parse version string like '15.0.0' to tuple."""
    parts = version_str.split(".")
    major = int(parts[0]) if len(parts) > 0 else 0
    minor = int(parts[1]) if len(parts) > 1 else 0
    patch = int(parts[2]) if len(parts) > 2 else 0
    return (major, minor, patch)


def handle_info(args, provider: Any):
    """Handle info subcommand.

    Display PostgreSQL environment information based on actual database connection
    or default values if no database is provided.
    """
    output_format = args.output if args.output != "table" or RICH_AVAILABLE else "json"

    # Track whether we're using actual database or defaults
    is_connected = False
    dialect = None
    extensions = None
    version_display = None

    if args.database:
        try:
            config = PostgresConnectionConfig(
                host=args.host,
                port=args.port,
                database=args.database,
                username=args.user,
                password=args.password,
            )
            backend = PostgresBackend(connection_config=config)
            backend.connect()
            backend.introspect_and_adapt()

            # Use the adapted dialect from backend
            dialect = backend.dialect
            version_tuple = backend.get_server_version()
            if version_tuple:
                version_display = f"{version_tuple[0]}.{version_tuple[1]}.{version_tuple[2]}"

            # Get extensions from dialect
            if hasattr(backend.dialect, "_extensions"):
                extensions = backend.dialect._extensions
            is_connected = True

            backend.disconnect()
        except Exception as e:
            logger.warning("Could not connect to database for introspection: %s", e)
            logger.warning("Using default values for dialect information.")
            # Fall back to command-line version or default

    # Create default dialect if not connected
    if dialect is None:
        # Parse version from command line or use default
        actual_version = args.version
        if actual_version:
            version = parse_version(actual_version)
        else:
            version = (13, 0, 0)  # Default version
        dialect = PostgresDialect(version=version)
        version_display = f"{version[0]}.{version[1]}.{version[2]}"

    # Unified structure for JSON output
    info = {
        "database": {
            "type": "postgresql",
            "version": version_display,
            "version_tuple": list(dialect.version),
            "connected": is_connected,
        },
        "features": {
            "extensions": {},
        },
        "protocols": {},
    }

    # Process extensions
    if extensions:
        for name, ext_info in extensions.items():
            ext_data = {
                "installed": ext_info.installed,
            }
            if ext_info.version:
                ext_data["version"] = ext_info.version
            if ext_info.schema:
                ext_data["schema"] = ext_info.schema
            info["features"]["extensions"][name] = ext_data

    for group_name, protocols in PROTOCOL_FAMILY_GROUPS.items():
        info["protocols"][group_name] = {}
        for protocol in protocols:
            protocol_name = protocol.__name__
            support_methods = check_protocol_support(dialect, protocol)

            # Calculate supported/total counts
            # For no-arg methods: value is bool
            # For methods with parameters: value is dict with 'supported', 'total', 'args'
            supported_count = 0
            total_count = 0
            for _method_name, value in support_methods.items():
                if isinstance(value, dict):
                    supported_count += value["supported"]
                    total_count += value["total"]
                else:
                    total_count += 1
                    if value:
                        supported_count += 1

            if args.verbose >= 2:
                info["protocols"][group_name][protocol_name] = {
                    "supported": supported_count,
                    "total": total_count,
                    "percentage": (round(supported_count / total_count * 100, 1) if total_count > 0 else 0),
                    "methods": support_methods,
                }
            else:
                info["protocols"][group_name][protocol_name] = {
                    "supported": supported_count,
                    "total": total_count,
                    "percentage": (round(supported_count / total_count * 100, 1) if total_count > 0 else 0),
                }

    if output_format == "json" or not RICH_AVAILABLE:
        print(json.dumps(info, indent=2))
    else:
        # Use legacy structure for rich display
        info_legacy = {
            "postgresql": info["database"],
            "extensions": info["features"]["extensions"],
            "protocols": info["protocols"],
        }
        _display_info_rich(info_legacy, args.verbose, version_display, extensions, is_connected)

    return info


def _display_info_rich(
    info: Dict,
    verbose: int,
    version_display: str,
    extensions: Optional[Dict[str, PostgresExtensionInfo]],
    is_connected: bool = True,
):
    """Display info using rich console.

    Args:
        info: Information dictionary containing database and protocol info
        verbose: Verbosity level for output detail
        version_display: Version string for display
        extensions: Optional dictionary of extension information
        is_connected: Whether the info is from actual database connection
    """
    from rich.console import Console

    console = Console(force_terminal=True)

    SYM_OK = "[OK]"
    SYM_PARTIAL = "[~]"
    SYM_FAIL = "[X]"

    console.print("\n[bold cyan]PostgreSQL Environment Information[/bold cyan]\n")

    # Show connection status
    if is_connected:
        console.print(f"[bold]PostgreSQL Version:[/bold] {version_display} [dim](from actual connection)[/dim]\n")
    else:
        console.print(f"[bold]PostgreSQL Version:[/bold] {version_display} [yellow](default value - no database connection)[/yellow]\n")

    # Display extensions if available
    if extensions:
        console.print("[bold green]Extension Support:[/bold green]")
        installed_exts = {k: v for k, v in extensions.items() if v.installed}
        if installed_exts:
            for name, ext in installed_exts.items():
                ver_str = f" ({ext.version})" if ext.version else ""
                schema_str = f" [schema: {ext.schema}]" if ext.schema else ""
                console.print(f"  [green][OK][/green] [bold]{name}[/bold]{ver_str}{schema_str}")
        else:
            console.print("  [dim]No extensions installed[/dim]")
        console.print()

    label = "Detailed" if verbose >= 2 else "Family Overview"
    console.print(f"[bold green]Protocol Support ({label}):[/bold green]")

    for group_name, protocols in info["protocols"].items():
        # Mark dialect-specific groups
        if group_name in DIALECT_SPECIFIC_GROUPS:
            console.print(f"\n  [bold underline]{group_name}:[/bold underline] [dim](dialect-specific)[/dim]")
        else:
            console.print(f"\n  [bold underline]{group_name}:[/bold underline]")
        for protocol_name, stats in protocols.items():
            pct = stats["percentage"]
            if pct == 100:
                color = "green"
                symbol = SYM_OK
            elif pct >= 50:
                color = "yellow"
                symbol = SYM_PARTIAL
            elif pct > 0:
                color = "red"
                symbol = SYM_PARTIAL
            else:
                color = "red"
                symbol = SYM_FAIL

            bar_len = 20
            filled = int(pct / 100 * bar_len)
            progress_bar = "#" * filled + "-" * (bar_len - filled)

            sup = stats["supported"]
            tot = stats["total"]
            console.print(
                f"    [{color}]{symbol}[/{color}] {protocol_name}: "
                f"[{color}]{progress_bar}[/{color}] {pct:.0f}% ({sup}/{tot})"
            )

            if verbose >= 2 and "methods" in stats:
                for method, value in stats["methods"].items():
                    # Format method name for display
                    method_display = (
                        method.replace("supports_", "").replace("_", " ").replace("is_", "").replace("_available", "")
                    )
                    if isinstance(value, dict):
                        # Method with parameters - show each arg's support
                        console.print(f"        [dim]{method_display}:[/dim]")
                        for arg, supported in value.get("args", {}).items():
                            m_status = "[green][OK][/green]" if supported else "[red][X][/red]"
                            console.print(f"            {m_status} {arg}")
                    else:
                        # No-arg method
                        m_status = "[green][OK][/green]" if value else "[red][X][/red]"
                        console.print(f"        {m_status} {method_display}")

    console.print()


def execute_query_sync(sql_query: str, backend: PostgresBackend, provider: Any, **kwargs):
    """Execute a SQL query synchronously."""
    try:
        backend.connect()
        provider.display_query(sql_query, is_async=False)
        result = backend.execute(sql_query)

        if not result:
            provider.display_no_result_object()
        else:
            provider.display_success(result.affected_rows, result.duration)
            if result.data:
                provider.display_results(result.data, **kwargs)
            else:
                provider.display_no_data()

    except ConnectionError as e:
        provider.display_connection_error(e)
        sys.exit(1)
    except QueryError as e:
        provider.display_query_error(e)
        sys.exit(1)
    except Exception as e:
        provider.display_unexpected_error(e, is_async=False)
        sys.exit(1)
    finally:
        if backend._connection:  # type: ignore
            backend.disconnect()
            provider.display_disconnect(is_async=False)


async def execute_query_async(sql_query: str, backend: AsyncPostgresBackend, provider: Any, **kwargs):
    """Execute a SQL query asynchronously."""
    try:
        await backend.connect()
        provider.display_query(sql_query, is_async=True)
        result = await backend.execute(sql_query)

        if not result:
            provider.display_no_result_object()
        else:
            provider.display_success(result.affected_rows, result.duration)
            if result.data:
                provider.display_results(result.data, **kwargs)
            else:
                provider.display_no_data()

    except ConnectionError as e:
        provider.display_connection_error(e)
        sys.exit(1)
    except QueryError as e:
        provider.display_query_error(e)
        sys.exit(1)
    except Exception as e:
        provider.display_unexpected_error(e, is_async=True)
        sys.exit(1)
    finally:
        if backend._connection:  # type: ignore
            await backend.disconnect()
            provider.display_disconnect(is_async=True)


def handle_introspect_sync(args, backend: PostgresBackend, provider: Any):
    """Handle introspect subcommand synchronously."""
    try:
        backend.connect()

        introspector = backend.introspector

        if args.type == "tables":
            tables = introspector.list_tables(
                schema=args.schema,
                include_system=args.include_system
            )
            data = _serialize_for_output(tables)
            provider.display_results(data, title="Tables")

        elif args.type == "views":
            views = introspector.list_views(schema=args.schema)
            data = _serialize_for_output(views)
            provider.display_results(data, title="Views")

        elif args.type == "table":
            if not args.name:
                print("Error: Table name is required for 'table' introspection", file=sys.stderr)
                sys.exit(1)
            info = introspector.get_table_info(args.name, schema=args.schema)
            if info:
                # Display columns
                provider.display_results(_serialize_for_output(info.columns), title=f"Columns of {args.name}")
                # Display indexes
                if info.indexes:
                    provider.display_results(_serialize_for_output(info.indexes), title=f"Indexes of {args.name}")
                # Display foreign keys
                if info.foreign_keys:
                    provider.display_results(_serialize_for_output(info.foreign_keys), title=f"Foreign Keys of {args.name}")
            else:
                print(f"Error: Table '{args.name}' not found", file=sys.stderr)
                sys.exit(1)

        elif args.type == "columns":
            if not args.name:
                print("Error: Table name is required for 'columns' introspection", file=sys.stderr)
                sys.exit(1)
            columns = introspector.list_columns(args.name, schema=args.schema)
            data = _serialize_for_output(columns)
            provider.display_results(data, title=f"Columns of {args.name}")

        elif args.type == "indexes":
            if not args.name:
                print("Error: Table name is required for 'indexes' introspection", file=sys.stderr)
                sys.exit(1)
            indexes = introspector.list_indexes(args.name, schema=args.schema)
            data = _serialize_for_output(indexes)
            provider.display_results(data, title=f"Indexes of {args.name}")

        elif args.type == "foreign-keys":
            if not args.name:
                print("Error: Table name is required for 'foreign-keys' introspection", file=sys.stderr)
                sys.exit(1)
            fks = introspector.list_foreign_keys(args.name, schema=args.schema)
            data = _serialize_for_output(fks)
            provider.display_results(data, title=f"Foreign Keys of {args.name}")

        elif args.type == "triggers":
            triggers = introspector.list_triggers(
                table_name=args.name,
                schema=args.schema
            )
            data = _serialize_for_output(triggers)
            provider.display_results(data, title="Triggers")

        elif args.type == "database":
            info = introspector.get_database_info()
            data = _serialize_for_output(info)
            provider.display_results([data], title="Database Info")

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


async def handle_introspect_async(args, backend: AsyncPostgresBackend, provider: Any):
    """Handle introspect subcommand asynchronously."""
    try:
        await backend.connect()

        introspector = backend.introspector

        if args.type == "tables":
            tables = await introspector.list_tables_async(
                schema=args.schema,
                include_system=args.include_system
            )
            data = _serialize_for_output(tables)
            provider.display_results(data, title="Tables")

        elif args.type == "views":
            views = await introspector.list_views_async(schema=args.schema)
            data = _serialize_for_output(views)
            provider.display_results(data, title="Views")

        elif args.type == "table":
            if not args.name:
                print("Error: Table name is required for 'table' introspection", file=sys.stderr)
                sys.exit(1)
            info = await introspector.get_table_info_async(args.name, schema=args.schema)
            if info:
                # Display columns
                provider.display_results(_serialize_for_output(info.columns), title=f"Columns of {args.name}")
                # Display indexes
                if info.indexes:
                    provider.display_results(_serialize_for_output(info.indexes), title=f"Indexes of {args.name}")
                # Display foreign keys
                if info.foreign_keys:
                    provider.display_results(_serialize_for_output(info.foreign_keys), title=f"Foreign Keys of {args.name}")
            else:
                print(f"Error: Table '{args.name}' not found", file=sys.stderr)
                sys.exit(1)

        elif args.type == "columns":
            if not args.name:
                print("Error: Table name is required for 'columns' introspection", file=sys.stderr)
                sys.exit(1)
            columns = await introspector.list_columns_async(args.name, schema=args.schema)
            data = _serialize_for_output(columns)
            provider.display_results(data, title=f"Columns of {args.name}")

        elif args.type == "indexes":
            if not args.name:
                print("Error: Table name is required for 'indexes' introspection", file=sys.stderr)
                sys.exit(1)
            indexes = await introspector.list_indexes_async(args.name, schema=args.schema)
            data = _serialize_for_output(indexes)
            provider.display_results(data, title=f"Indexes of {args.name}")

        elif args.type == "foreign-keys":
            if not args.name:
                print("Error: Table name is required for 'foreign-keys' introspection", file=sys.stderr)
                sys.exit(1)
            fks = await introspector.list_foreign_keys_async(args.name, schema=args.schema)
            data = _serialize_for_output(fks)
            provider.display_results(data, title=f"Foreign Keys of {args.name}")

        elif args.type == "triggers":
            triggers = await introspector.list_triggers_async(
                table_name=args.name,
                schema=args.schema
            )
            data = _serialize_for_output(triggers)
            provider.display_results(data, title="Triggers")

        elif args.type == "database":
            info = await introspector.get_database_info_async()
            data = _serialize_for_output(info)
            provider.display_results([data], title="Database Info")

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


def handle_status_sync(args, backend: PostgresBackend, provider: Any):
    """Handle status subcommand synchronously."""
    try:
        backend.connect()
        backend.introspect_and_adapt()

        status_introspector = backend.introspector.status
        status_type = args.type

        if status_type == "all":
            # Get complete overview
            status = status_introspector.get_overview()
            if args.output == "json" or not RICH_AVAILABLE:
                print(json.dumps(_serialize_for_output(status), indent=2))
            else:
                _display_status_rich(status, args.verbose)
        elif status_type == "config":
            config_items = status_introspector.list_configuration(StatusCategory.CONFIGURATION)
            data = _serialize_for_output(config_items)
            provider.display_results(data, title="Configuration")
        elif status_type == "performance":
            perf_items = status_introspector.list_configuration(StatusCategory.PERFORMANCE)
            data = _serialize_for_output(perf_items)
            provider.display_results(data, title="Performance")
        elif status_type == "connections":
            conn_info = status_introspector.get_connection_info()
            data = _serialize_for_output(conn_info)
            provider.display_results([data], title="Connections")
        elif status_type == "storage":
            storage_info = status_introspector.get_storage_info()
            data = _serialize_for_output(storage_info)
            provider.display_results([data], title="Storage")
        elif status_type == "databases":
            databases = status_introspector.list_databases()
            data = _serialize_for_output(databases)
            provider.display_results(data, title="Databases")
        elif status_type == "users":
            users = status_introspector.list_users()
            data = _serialize_for_output(users)
            provider.display_results(data, title="Users")

    except ConnectionError as e:
        provider.display_connection_error(e)
        sys.exit(1)
    except QueryError as e:
        provider.display_query_error(e)
        sys.exit(1)
    except Exception as e:
        print(f"Error during status introspection: {e}", file=sys.stderr)
        sys.exit(1)
    finally:
        if backend._connection:  # type: ignore
            backend.disconnect()


async def handle_status_async(args, backend: AsyncPostgresBackend, provider: Any):
    """Handle status subcommand asynchronously."""
    try:
        await backend.connect()
        await backend.introspect_and_adapt()

        status_introspector = backend.introspector.status
        status_type = args.type

        if status_type == "all":
            # Get complete overview
            status = await status_introspector.get_overview()
            if args.output == "json" or not RICH_AVAILABLE:
                print(json.dumps(_serialize_for_output(status), indent=2))
            else:
                _display_status_rich(status, args.verbose)
        elif status_type == "config":
            config_items = await status_introspector.list_configuration(StatusCategory.CONFIGURATION)
            data = _serialize_for_output(config_items)
            provider.display_results(data, title="Configuration")
        elif status_type == "performance":
            perf_items = await status_introspector.list_configuration(StatusCategory.PERFORMANCE)
            data = _serialize_for_output(perf_items)
            provider.display_results(data, title="Performance")
        elif status_type == "connections":
            conn_info = await status_introspector.get_connection_info()
            data = _serialize_for_output(conn_info)
            provider.display_results([data], title="Connections")
        elif status_type == "storage":
            storage_info = await status_introspector.get_storage_info()
            data = _serialize_for_output(storage_info)
            provider.display_results([data], title="Storage")
        elif status_type == "databases":
            databases = await status_introspector.list_databases()
            data = _serialize_for_output(databases)
            provider.display_results(data, title="Databases")
        elif status_type == "users":
            users = await status_introspector.list_users()
            data = _serialize_for_output(users)
            provider.display_results(data, title="Users")

    except ConnectionError as e:
        provider.display_connection_error(e)
        sys.exit(1)
    except QueryError as e:
        provider.display_query_error(e)
        sys.exit(1)
    except Exception as e:
        print(f"Error during status introspection: {e}", file=sys.stderr)
        sys.exit(1)
    finally:
        if backend._connection:  # type: ignore
            await backend.disconnect()


def _display_status_rich(status: Any, verbose: int = 0):
    """Display status using rich console.

    Args:
        status: ServerOverview object
        verbose: Verbosity level for output detail
    """
    from rich.console import Console
    from rich.table import Table

    console = Console(force_terminal=True)

    # Header
    console.print("\n[bold cyan]PostgreSQL Server Status[/bold cyan]\n")
    console.print(f"[bold]Version:[/bold] {status.server_version}")
    console.print(f"[bold]Vendor:[/bold] {status.server_vendor}")

    # Session info
    if hasattr(status, 'session') and status.session:
        session = status.session
        console.print()
        console.print("[bold green]Session[/bold green]")
        if session.user:
            console.print(f"  [bold]User:[/bold] {session.user}")
        if session.database:
            console.print(f"  [bold]Database:[/bold] {session.database}")
        if session.schema:
            console.print(f"  [bold]Schema:[/bold] {session.schema}")
        if session.host:
            console.print(f"  [bold]Host:[/bold] {session.host}")
        if session.ssl_enabled is not None:
            ssl_status = "Enabled" if session.ssl_enabled else "Disabled"
            console.print(f"  [bold]SSL:[/bold] {ssl_status}")
            if session.ssl_enabled and session.ssl_version:
                console.print(f"  [bold]SSL Version:[/bold] {session.ssl_version}")
            if session.ssl_enabled and session.ssl_cipher:
                console.print(f"  [bold]SSL Cipher:[/bold] {session.ssl_cipher}")
        if session.password_used is not None:
            auth_method = "Password" if session.password_used else "Other"
            console.print(f"  [bold]Auth Method:[/bold] {auth_method}")

    # Connection info
    if hasattr(status, 'connections') and status.connections:
        conn = status.connections
        if conn.active_count is not None:
            console.print(f"[bold]Active Connections:[/bold] {conn.active_count}")
            if conn.max_connections is not None:
                console.print(f"[bold]Max Connections:[/bold] {conn.max_connections}")

    console.print()

    # Configuration section
    config_items = [item for item in status.configuration
                    if item.category == StatusCategory.CONFIGURATION]
    if config_items:
        console.print("[bold green]Configuration[/bold green]")
        config_table = Table(show_header=True, header_style="bold")
        config_table.add_column("Parameter")
        config_table.add_column("Value")
        if verbose >= 1:
            config_table.add_column("Description")
            config_table.add_column("Readonly")

        for item in config_items:
            row = [item.name, str(item.value)]
            if verbose >= 1:
                row.extend([
                    item.description or "",
                    "Yes" if item.is_readonly else "No"
                ])
            config_table.add_row(*row)

        console.print(config_table)
        console.print()

    # Performance section
    perf_items = [item for item in status.configuration
                  if item.category == StatusCategory.PERFORMANCE]
    if perf_items:
        console.print("[bold green]Performance[/bold green]")
        perf_table = Table(show_header=True, header_style="bold")
        perf_table.add_column("Parameter")
        perf_table.add_column("Value")
        if verbose >= 1:
            perf_table.add_column("Unit")

        for item in perf_items:
            row = [item.name, str(item.value)]
            if verbose >= 1:
                row.append(item.unit or "")
            perf_table.add_row(*row)

        console.print(perf_table)
        console.print()

    # Storage section
    if hasattr(status, 'storage') and status.storage:
        console.print("[bold green]Storage[/bold green]")
        storage_table = Table(show_header=True, header_style="bold")
        storage_table.add_column("Metric")
        storage_table.add_column("Value")

        if status.storage.total_size_bytes is not None:
            storage_table.add_row("Total Size", _format_size(status.storage.total_size_bytes))

        console.print(storage_table)
        console.print()

    # Databases section
    if status.databases:
        console.print("[bold green]Databases[/bold green]")
        db_table = Table(show_header=True, header_style="bold")
        db_table.add_column("Name")
        db_table.add_column("Size")
        if verbose >= 1:
            db_table.add_column("Tables")
            db_table.add_column("Views")

        for db in status.databases:
            row = [db.name, _format_size(db.size_bytes) if db.size_bytes else "N/A"]
            if verbose >= 1:
                row.append(str(db.table_count) if db.table_count is not None else "N/A")
                row.append(str(db.view_count) if db.view_count is not None else "N/A")
            db_table.add_row(*row)

        console.print(db_table)
        console.print()

        # Show schema details for current database
        for db in status.databases:
            if db.extra and 'schemas' in db.extra:
                schemas = db.extra['schemas']
                if schemas:
                    console.print(f"[bold green]Schemas in {db.name}[/bold green]")
                    schema_table = Table(show_header=True, header_style="bold")
                    schema_table.add_column("Schema")
                    schema_table.add_column("Tables")
                    schema_table.add_column("Views")

                    for schema_name, counts in sorted(schemas.items()):
                        schema_table.add_row(
                            schema_name,
                            str(counts.get('tables', 0)),
                            str(counts.get('views', 0))
                        )

                    console.print(schema_table)
                    console.print()

    # Users section
    if status.users:
        console.print("[bold green]Users[/bold green]")
        user_table = Table(show_header=True, header_style="bold")
        user_table.add_column("Name")
        user_table.add_column("Superuser")

        for user in status.users:
            user_table.add_row(user.name, "Yes" if user.is_superuser else "No")

        console.print(user_table)
        console.print()

    # WAL section
    if hasattr(status, 'wal') and status.wal:
        wal = status.wal
        console.print("[bold green]WAL (Write-Ahead Logging)[/bold green]")
        wal_table = Table(show_header=True, header_style="bold")
        wal_table.add_column("Parameter")
        wal_table.add_column("Value")

        if wal.wal_level:
            wal_table.add_row("WAL Level", str(wal.wal_level))
        if wal.wal_files is not None:
            wal_table.add_row("WAL Files", str(wal.wal_files))
        if wal.wal_size_bytes is not None:
            wal_table.add_row("WAL Size", _format_size(wal.wal_size_bytes))
        if wal.checkpoint_count is not None:
            wal_table.add_row("Checkpoints", str(wal.checkpoint_count))
        if wal.checkpoint_time:
            wal_table.add_row("Last Checkpoint", str(wal.checkpoint_time))
        if wal.wal_segments is not None:
            wal_table.add_row("WAL Segments", str(wal.wal_segments))

        console.print(wal_table)
        console.print()

    # Replication section
    if hasattr(status, 'replication') and status.replication:
        repl = status.replication
        console.print("[bold green]Replication[/bold green]")
        repl_table = Table(show_header=True, header_style="bold")
        repl_table.add_column("Parameter")
        repl_table.add_column("Value")

        if repl.is_primary is not None:
            repl_table.add_row("Role", "Primary" if repl.is_primary else "Standby")
        if repl.replication_slots is not None:
            repl_table.add_row("Replication Slots", str(repl.replication_slots))
        if repl.active_replications is not None:
            repl_table.add_row("Active Replications", str(repl.active_replications))
        if repl.streaming is not None:
            repl_table.add_row("Streaming", "Yes" if repl.streaming else "No")
        if repl.lag_bytes is not None:
            repl_table.add_row("Replication Lag", _format_size(repl.lag_bytes))

        console.print(repl_table)
        console.print()

    # Archive section
    if hasattr(status, 'archive') and status.archive:
        archive = status.archive
        console.print("[bold green]Archive[/bold green]")
        archive_table = Table(show_header=True, header_style="bold")
        archive_table.add_column("Parameter")
        archive_table.add_column("Value")

        if archive.archive_mode:
            archive_table.add_row("Archive Mode", str(archive.archive_mode))
        if archive.archive_command:
            # Truncate long commands
            cmd = str(archive.archive_command)
            if len(cmd) > 50:
                cmd = cmd[:47] + "..."
            archive_table.add_row("Archive Command", cmd)
        if archive.archive_timeout is not None:
            archive_table.add_row("Archive Timeout", f"{archive.archive_timeout}s")
        if archive.archived_count is not None:
            archive_table.add_row("Archived Count", str(archive.archived_count))
        if archive.failed_count is not None:
            archive_table.add_row("Failed Archives", str(archive.failed_count))

        console.print(archive_table)
        console.print()

    # Security section
    if hasattr(status, 'security') and status.security:
        security = status.security
        console.print("[bold green]Security[/bold green]")
        security_table = Table(show_header=True, header_style="bold")
        security_table.add_column("Parameter")
        security_table.add_column("Value")

        if security.ssl_enabled is not None:
            security_table.add_row("SSL Enabled", "Yes" if security.ssl_enabled else "No")
        if security.ssl_cert_file:
            security_table.add_row("SSL Cert File", str(security.ssl_cert_file))
        if security.ssl_key_file:
            security_table.add_row("SSL Key File", str(security.ssl_key_file))
        if security.ssl_ca_file:
            security_table.add_row("SSL CA File", str(security.ssl_ca_file))
        if security.password_encryption:
            security_table.add_row("Password Encryption", str(security.password_encryption))
        if security.row_security is not None:
            security_table.add_row("Row-Level Security", "Enabled" if security.row_security else "Disabled")

        console.print(security_table)
        console.print()

    # Extensions section
    if hasattr(status, 'extensions') and status.extensions:
        console.print("[bold green]Extensions[/bold green]")
        ext_table = Table(show_header=True, header_style="bold")
        ext_table.add_column("Name")
        ext_table.add_column("Version")
        ext_table.add_column("Schema")

        for ext in status.extensions:
            ext_table.add_row(
                ext.name,
                ext.version or "N/A",
                ext.schema or "N/A"
            )

        console.print(ext_table)
        console.print()


def _create_postgres_backend(args) -> PostgresBackend:
    """Create and connect a PostgreSQL backend based on args."""
    config = PostgresConnectionConfig(
        host=args.host,
        port=args.port,
        database=args.database,
        username=args.user,
        password=args.password,
    )
    backend = PostgresBackend(connection_config=config)
    backend.connect()
    backend.introspect_and_adapt()
    return backend


def _get_postgres_dialect(backend: PostgresBackend) -> PostgresDialect:
    """Get the dialect from a connected PostgreSQL backend."""
    return backend.dialect


def _execute_postgres_query(backend: PostgresBackend, sql: str, params: tuple, stmt_type) -> Any:
    """Execute a query on the provided backend."""
    from rhosocial.activerecord.backend.options import ExecutionOptions
    return backend.execute(sql, params, options=ExecutionOptions(stmt_type=stmt_type))


_default_backend: Optional[PostgresBackend] = None


def _disconnect_postgres():
    """Disconnect the default backend."""
    global _default_backend
    if _default_backend and _default_backend._connection:
        _default_backend.disconnect()
        _default_backend = None


def _handle_named_query_postgres(args, provider):
    """Handle named-query subcommand for PostgreSQL."""
    global _default_backend

    def backend_factory():
        global _default_backend
        _default_backend = _create_postgres_backend(args)
        return _default_backend

    def get_dialect(b):
        return b.dialect

    def execute_query(sql, params, stmt_type):
        return _default_backend.execute(sql, params, options=ExecutionOptions(stmt_type=stmt_type))

    handle_nq(
        args,
        provider,
        backend_factory=backend_factory,
        get_dialect=get_dialect,
        execute_query=execute_query,
        disconnect=_disconnect_postgres,
    )


def _format_size(size_bytes: int) -> str:
    """Format byte size to human-readable string."""
    units = ["B", "KB", "MB", "GB", "TB"]
    size = float(size_bytes)
    for unit in units:
        if size < 1024:
            return f"{size:.1f} {unit}"
        size /= 1024
    return f"{size:.1f} PB"


def main():
    args = parse_args()

    # Must specify a subcommand
    if args.command is None:
        print("Error: Please specify a command: 'info', 'query', 'introspect', 'status', or 'named-query'", file=sys.stderr)
        print("Use --help for more information.", file=sys.stderr)
        sys.exit(1)

    provider = get_provider(args)

    # Handle info subcommand
    if args.command == "info":
        handle_info(args, provider)
        return

    # Handle introspect subcommand
    if args.command == "introspect":
        if not args.database:
            print("Error: --database is required for introspection", file=sys.stderr)
            sys.exit(1)

        config = PostgresConnectionConfig(
            host=args.host,
            port=args.port,
            database=args.database,
            username=args.user,
            password=args.password,
        )

        if args.use_async:
            backend = AsyncPostgresBackend(connection_config=config)
            asyncio.run(handle_introspect_async(args, backend, provider))
        else:
            backend = PostgresBackend(connection_config=config)
            handle_introspect_sync(args, backend, provider)
        return

    # Handle status subcommand
    if args.command == "status":
        if not args.database:
            print("Error: --database is required for status", file=sys.stderr)
            sys.exit(1)

        config = PostgresConnectionConfig(
            host=args.host,
            port=args.port,
            database=args.database,
            username=args.user,
            password=args.password,
        )

        if args.use_async:
            backend = AsyncPostgresBackend(connection_config=config)
            asyncio.run(handle_status_async(args, backend, provider))
        else:
            backend = PostgresBackend(connection_config=config)
            handle_status_sync(args, backend, provider)
        return

    # Handle named-query subcommand
    if args.command == "named-query":
        _handle_named_query_postgres(args, provider)
        return

    # Handle query subcommand
    numeric_level = getattr(logging, args.log_level.upper(), None)
    if not isinstance(numeric_level, int):
        raise ValueError(f"Invalid log level: {args.log_level}")

    if RICH_AVAILABLE and isinstance(provider, RichOutputProvider):
        from rich.console import Console

        handler = RichHandler(rich_tracebacks=True, show_path=False, console=Console(stderr=True))
        logging.basicConfig(level=numeric_level, format="%(message)s", datefmt="[%X]", handlers=[handler])
    else:
        logging.basicConfig(level=numeric_level, format="%(asctime)s - %(levelname)s - %(message)s", stream=sys.stderr)

    provider.display_greeting()

    sql_source = None
    if args.sql:
        sql_source = args.sql
    elif args.file:
        try:
            with open(args.file, "r", encoding="utf-8") as f:
                sql_source = f.read()
        except FileNotFoundError:
            logger.error(f"Error: File not found at {args.file}")
            sys.exit(1)
    elif not sys.stdin.isatty():
        sql_source = sys.stdin.read()

    if not sql_source:
        msg = "Error: No SQL query provided. Use SQL argument, --file, or stdin."
        print(msg, file=sys.stderr)
        sys.exit(1)

    # Ensure only one statement is provided
    if ";" in sql_source.strip().rstrip(";"):
        logger.error("Error: Multiple SQL statements are not supported.")
        sys.exit(1)

    config = PostgresConnectionConfig(
        host=args.host,
        port=args.port,
        database=args.database,
        username=args.user,
        password=args.password,
    )

    kwargs = {"use_ascii": args.rich_ascii}
    if args.use_async:
        backend = AsyncPostgresBackend(connection_config=config)
        asyncio.run(execute_query_async(sql_source, backend, provider, **kwargs))
    else:
        backend = PostgresBackend(connection_config=config)
        execute_query_sync(sql_source, backend, provider, **kwargs)


if __name__ == "__main__":
    main()
