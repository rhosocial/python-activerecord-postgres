# src/rhosocial/activerecord/backend/impl/postgres/cli/status.py
"""status subcommand - Display PostgreSQL server status overview.

PostgreSQL status includes WAL, Replication, Archive, Security, and
Extensions sections in addition to the standard sections.
"""

import argparse
import asyncio
import json
import sys
from dataclasses import asdict, is_dataclass
from enum import Enum
from typing import Any

from rhosocial.activerecord.backend.impl.postgres import PostgresBackend, AsyncPostgresBackend
from rhosocial.activerecord.backend.errors import ConnectionError, QueryError

from .connection import add_connection_args, resolve_connection_config_from_args
from .output import create_provider, RICH_AVAILABLE

OUTPUT_CHOICES = ['table', 'json', 'csv', 'tsv']

STATUS_TYPES = ["all", "config", "performance", "connections", "storage", "databases", "users"]


def create_parser(subparsers):
    """Create the status subcommand parser."""
    parser = subparsers.add_parser(
        'status',
        help='Display server status overview',
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

    # Output format (all, but 'all' type falls back to json for csv/tsv)
    parser.add_argument(
        '-o', '--output',
        choices=OUTPUT_CHOICES,
        default='table',
        help='Output format (default: table)',
    )

    # Connection arguments
    add_connection_args(parser)

    # Verbosity
    parser.add_argument(
        '-v', '--verbose',
        action='count',
        default=0,
        help='Increase verbosity for additional columns.',
    )

    # Rich display options
    parser.add_argument(
        '--rich-ascii',
        action='store_true',
        help='Use ASCII characters for rich table borders.',
    )

    # status-specific arguments
    parser.add_argument(
        "type",
        nargs="?",
        default="all",
        choices=STATUS_TYPES,
        help="Status type: all (default), config, performance, connections, storage, databases, users",
    )

    return parser


def handle(args):
    """Handle the status subcommand."""
    provider = create_provider(args.output, ascii_borders=args.rich_ascii)

    named_conn = getattr(args, "named_connection", None)
    if not named_conn and not args.database:
        print("Error: --database is required for status", file=sys.stderr)
        sys.exit(1)

    config = resolve_connection_config_from_args(args)

    if args.use_async:
        backend = AsyncPostgresBackend(connection_config=config)
        asyncio.run(_handle_status_async(args, backend, provider))
    else:
        backend = PostgresBackend(connection_config=config)
        _handle_status_sync(args, backend, provider)


# ---------------------------------------------------------------------------
# Internal helper functions
# ---------------------------------------------------------------------------

def _serialize_for_output(obj: Any) -> Any:
    """Serialize object for JSON output, handling non-serializable types."""
    if obj is None:
        return None
    if hasattr(obj, 'model_dump'):
        try:
            result = obj.model_dump(mode='json')
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


def _format_size(size_bytes: int) -> str:
    """Format byte size to human-readable string."""
    units = ["B", "KB", "MB", "GB", "TB"]
    size = float(size_bytes)
    for unit in units:
        if size < 1024:
            return f"{size:.1f} {unit}"
        size /= 1024
    return f"{size:.1f} PB"


def _handle_status_sync(args, backend: PostgresBackend, provider):
    """Handle status subcommand synchronously."""
    from rhosocial.activerecord.backend.introspection.status import StatusCategory

    try:
        backend.connect()
        backend.introspect_and_adapt()

        status_introspector = backend.introspector.status
        status_type = args.type

        if status_type == "all":
            status = status_introspector.get_overview()
            effective_output = args.output
            if effective_output in ("csv", "tsv"):
                effective_output = "json"

            if effective_output == "json" or not RICH_AVAILABLE:
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


async def _handle_status_async(args, backend: AsyncPostgresBackend, provider):
    """Handle status subcommand asynchronously."""
    from rhosocial.activerecord.backend.introspection.status import StatusCategory

    try:
        await backend.connect()
        await backend.introspect_and_adapt()

        status_introspector = backend.introspector.status
        status_type = args.type

        if status_type == "all":
            status = await status_introspector.get_overview()
            effective_output = args.output
            if effective_output in ("csv", "tsv"):
                effective_output = "json"

            if effective_output == "json" or not RICH_AVAILABLE:
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

    PostgreSQL-specific rich display includes WAL, Replication, Archive,
    Security, and Extensions sections.
    """
    from rich.console import Console
    from rich.table import Table
    from rhosocial.activerecord.backend.introspection.status import StatusCategory

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
