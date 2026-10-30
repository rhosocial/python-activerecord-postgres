# tests/rhosocial/activerecord_postgres_test/feature/backend/introspection/test_introspection_cli_objects.py
"""
Black-box CLI tests for the types that answer for a recognised server object:
``views``, ``materialized-views``, ``sequences`` and ``extensions``.

All four are driven through ``main()`` so the test exercises the same path a
terminal user takes: argument parsing, dispatch onto
``backend.introspector``, and rendering through the output provider.
"""

import io
import json
import socket
import sys
import time
import uuid
from contextlib import redirect_stderr, redirect_stdout

import pytest

from rhosocial.activerecord.backend.impl.postgres.__main__ import main
from rhosocial.activerecord.backend.impl.postgres.backend import PostgresBackend
from rhosocial.activerecord.backend.impl.postgres.cli.introspect import INTROSPECT_TYPES
from rhosocial.activerecord.backend.impl.postgres.config import PostgresConnectionConfig
from providers.scenarios import SCENARIO_MAP, get_scenario_raw

# The CLI runs its async handler inside ``asyncio.run``, which on Windows
# builds the ProactorEventLoop psycopg refuses to work with. That is the
# fate of every subcommand, not of these two, so the async cases are held
# out here and documented rather than left to fail for an unrelated reason.
async_cli = pytest.mark.skipif(
    sys.platform == "win32",
    reason="psycopg async mode needs the selector event loop; the CLI's "
    "asyncio.run gives it the ProactorEventLoop on Windows",
)


def _server_config():
    """The first reachable scenario's raw config, or ``None``."""
    for name in SCENARIO_MAP:
        _, config = get_scenario_raw(name)
        if _reachable(config):
            return config
    return None


@pytest.fixture(scope="module")
def conn_args():
    """Connection arguments for the first reachable scenario.

    Nothing names a schema: the connection fixes the scope, and the CLI reads
    the schema the connection resolves -- ``public`` for these scenarios.
    That is also where :func:`cli_objects` puts the objects, because a
    separate process inherits nothing from the test.
    """
    config = _server_config()
    if config is None:
        pytest.skip("No scenario server reachable")
    return [
        "--host",
        config.host,
        "--port",
        str(config.port),
        "--database",
        config.database,
        "--user",
        config.username,
        "--password",
        config.password,
    ]


@pytest.fixture(scope="module")
def cli_objects():
    """The objects the CLI subcommands list, created and dropped here.

    Names carry a per-process suffix. The matrix runs a dozen jobs against the
    same scenario servers, and a run that dies mid-way leaves its objects
    behind: with one fixed name, the next job's setup collides on
    ``pg_class_relname_nsp_index`` and every later assertion in the module
    reads whatever survived. A unique suffix makes each run's objects its own,
    so neither a leftover nor a concurrent job can be mistaken for it.

    They live in the connection's default schema, because that is the scope
    the CLI answers for; a run cannot name a schema of its own.

    The drops stay forgiving (``IF EXISTS``, ``CASCADE`` on the table because a
    view names its table) so a run that died half-way is cleaned up rather than
    blocking. The creates do not get the same licence: if a CREATE fails, the
    objects the tests assert on are genuinely absent, and that must be a loud
    error rather than a test that reads nothing.
    """
    config = _server_config()
    if config is None:
        pytest.skip("No scenario server reachable")

    suffix = uuid.uuid4().hex[:8]
    seq_name = f"arx_cli_demo_seq_{suffix}"
    table_name = f"arx_cli_demo_tbl_{suffix}"
    view_name = f"arx_cli_demo_mv_{suffix}"
    plain_view_name = f"arx_cli_demo_v_{suffix}"

    backend = PostgresBackend(
        connection_config=PostgresConnectionConfig(
            host=config.host,
            port=int(config.port),
            database=config.database,
            username=config.username,
            password=config.password,
        )
    )
    backend.connect()
    try:
        for sql in (
            f"DROP MATERIALIZED VIEW IF EXISTS {view_name}",
            f"DROP VIEW IF EXISTS {plain_view_name}",
            f"DROP TABLE IF EXISTS {table_name} CASCADE",
            f"DROP SEQUENCE IF EXISTS {seq_name}",
        ):
            try:
                backend.execute(sql)
            except Exception:
                pass
        for sql in (
            f"CREATE SEQUENCE {seq_name} START 42 INCREMENT 4",
            f"CREATE TABLE {table_name} (id INTEGER PRIMARY KEY)",
            f"CREATE MATERIALIZED VIEW {view_name} AS SELECT id FROM {table_name}",
            f"CREATE VIEW {plain_view_name} AS SELECT id FROM {table_name}",
        ):
            backend.execute(sql)
        yield {
            # The scope the CLI will answer for, spelled out so the assertions
            # below read it rather than assume it.
            "schema": "public",
            "seq": seq_name,
            "table": table_name,
            "view": view_name,
            "plain_view": plain_view_name,
        }
    finally:
        for sql in (
            f"DROP MATERIALIZED VIEW IF EXISTS {view_name}",
            f"DROP VIEW IF EXISTS {plain_view_name}",
            f"DROP TABLE IF EXISTS {table_name} CASCADE",
            f"DROP SEQUENCE IF EXISTS {seq_name}",
        ):
            try:
                backend.execute(sql)
            except Exception:
                pass
        backend.disconnect()


def _reachable(config) -> bool:
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.settimeout(2)
    try:
        sock.connect((config.host, int(config.port)))
    except OSError:
        return False
    finally:
        sock.close()
    return True


#: The error PostgreSQL answers when a catalogue row names a relation that is
#: gone by the time the row's definition is read.
_CATALOG_CHURN = "could not open relation with OID"


def run_cli(argv, attempts=3):
    """Run the CLI in-process, retrying a read that lost a relation mid-flight.

    A listing answers for every object of its type in the schema the connection
    resolves, and it reads each one's definition (a materialized view) or last
    value (a sequence) by OID. The rest of the suite runs in parallel against
    the same server, so a sibling worker can drop a relation between the
    snapshot that listed it and the lookup that describes it; PostgreSQL then
    raises ``could not open relation with OID`` rather than leaving the row
    out. That is a property of catalog snapshots, not of this backend, and a
    fresh process takes a fresh snapshot in which the dropped relation is
    absent altogether, so a bounded retry is the honest read.

    Only that one error is retried, and only ``attempts`` times: the last
    attempt's result is returned unchanged, so a persistent failure still fails
    the test with the server's own message.
    """
    out, err, exc = "", "", None
    for attempt in range(attempts):
        out_buf, err_buf = io.StringIO(), io.StringIO()
        exc = None
        with redirect_stdout(out_buf), redirect_stderr(err_buf):
            try:
                main(argv)
            except SystemExit as e:
                exc = e
        out, err = out_buf.getvalue(), err_buf.getvalue()
        if exc is None or _CATALOG_CHURN not in err:
            break
        if attempt < attempts - 1:
            time.sleep(0.2 * (attempt + 1))
    return out, err, exc


class TestParserContract:
    """Offline checks on what the introspect parser offers.

    The scope of a run is fixed by its connection, so the parser must not ask
    for a schema a second time: ``--schema`` is not a flag of this subcommand,
    and argparse refuses it like any other unknown option.
    """

    def test_schema_flag_is_refused(self, capsys):
        with pytest.raises(SystemExit):
            main(["introspect", "tables", "--database", "mydb", "--schema", "public"])
        assert "--schema" in capsys.readouterr().err

    def test_help_lists_every_type_and_no_schema_flag(self):
        out, _, exc = run_cli(["introspect", "--help"])
        assert exc is not None and exc.code == 0
        for type_name in INTROSPECT_TYPES:
            assert type_name in out
        assert "--schema" not in out


class TestSequencesCLI:
    """``introspect sequences`` against a live server."""

    def test_sequences_json(self, conn_args, cli_objects):
        out, err, exc = run_cli(["introspect", "sequences"] + conn_args + ["-o", "json"])
        assert exc is None, f"stderr: {err}\nstdout: {out}"
        rows = json.loads(out)
        assert isinstance(rows, list)
        assert len(rows) > 0
        # The row shape is SequenceInfo's own, so the key is "name" exactly
        # as it is for tables, views and every other listing.
        assert cli_objects["seq"] in [row["name"] for row in rows]
        demo = next(r for r in rows if r["name"] == cli_objects["seq"])
        assert demo["start_value"] == 42
        assert demo["increment"] == 4
        assert demo["schema"] == cli_objects["schema"]

    def test_single_sequence_json(self, conn_args, cli_objects):
        out, err, exc = run_cli(
            ["introspect", "sequences", cli_objects["seq"]] + conn_args + ["-o", "json"]
        )
        assert exc is None, f"stderr: {err}"
        rows = json.loads(out)
        assert len(rows) == 1
        assert rows[0]["name"] == cli_objects["seq"]

    def test_single_sequence_table(self, conn_args, cli_objects):
        out, err, exc = run_cli(["introspect", "sequences", cli_objects["seq"]] + conn_args)
        assert exc is None, f"stderr: {err}"
        assert cli_objects["seq"] in out

    def test_unknown_sequence_exits_nonzero(self, conn_args, cli_objects):
        _, err, exc = run_cli(
            ["introspect", "sequences", "arx_cli_no_seq"] + conn_args + ["-o", "json"]
        )
        assert exc is not None and exc.code == 1
        assert "arx_cli_no_seq" in err

    @async_cli
    def test_sequences_async(self, conn_args, cli_objects):
        out, err, exc = run_cli(["introspect", "sequences"] + conn_args + ["-o", "json", "--async"])
        assert exc is None, f"stderr: {err}"
        rows = json.loads(out)
        assert cli_objects["seq"] in [row["name"] for row in rows]


class TestMaterializedViewsCLI:
    """``introspect materialized-views`` against a live server."""

    def test_materialized_views_json(self, conn_args, cli_objects):
        out, err, exc = run_cli(["introspect", "materialized-views"] + conn_args + ["-o", "json"])
        assert exc is None, f"stderr: {err}\nstdout: {out}"
        rows = json.loads(out)
        assert isinstance(rows, list)
        assert cli_objects["view"] in [row["name"] for row in rows]
        demo = next(r for r in rows if r["name"] == cli_objects["view"])
        assert demo["extra"]["is_materialized"] is True

    def test_single_materialized_view_json(self, conn_args, cli_objects):
        out, err, exc = run_cli(
            ["introspect", "materialized-views", cli_objects["view"]] + conn_args + ["-o", "json"]
        )
        assert exc is None, f"stderr: {err}"
        rows = json.loads(out)
        assert len(rows) == 1
        assert rows[0]["name"] == cli_objects["view"]
        assert "SELECT" in rows[0]["definition"].upper()

    def test_single_materialized_view_table(self, conn_args, cli_objects):
        out, err, exc = run_cli(
            ["introspect", "materialized-views", cli_objects["view"]] + conn_args
        )
        assert exc is None, f"stderr: {err}"
        assert cli_objects["view"] in out

    def test_unknown_materialized_view_exits_nonzero(self, conn_args, cli_objects):
        _, err, exc = run_cli(
            ["introspect", "materialized-views", "arx_cli_no_mv"] + conn_args + ["-o", "json"]
        )
        assert exc is not None and exc.code == 1
        assert "arx_cli_no_mv" in err

    @async_cli
    def test_materialized_views_async(self, conn_args, cli_objects):
        out, err, exc = run_cli(
            ["introspect", "materialized-views"] + conn_args + ["-o", "json", "--async"]
        )
        assert exc is None, f"stderr: {err}"
        rows = json.loads(out)
        assert cli_objects["view"] in [row["name"] for row in rows]


class TestViewsCLI:
    """``introspect views`` against a live server, listing and one by name."""

    def test_views_json(self, conn_args, cli_objects):
        out, err, exc = run_cli(["introspect", "views"] + conn_args + ["-o", "json"])
        assert exc is None, f"stderr: {err}\nstdout: {out}"
        rows = json.loads(out)
        names = [row["name"] for row in rows]
        assert cli_objects["plain_view"] in names
        # The listing is ordinary views, not the materialized ones: the two
        # families answer through different questions.
        assert cli_objects["view"] not in names

    def test_single_view_json(self, conn_args, cli_objects):
        out, err, exc = run_cli(
            ["introspect", "views", cli_objects["plain_view"]] + conn_args + ["-o", "json"]
        )
        assert exc is None, f"stderr: {err}"
        rows = json.loads(out)
        assert len(rows) == 1
        assert rows[0]["name"] == cli_objects["plain_view"]
        assert "SELECT" in rows[0]["definition"].upper()

    def test_single_view_table(self, conn_args, cli_objects):
        out, err, exc = run_cli(["introspect", "views", cli_objects["plain_view"]] + conn_args)
        assert exc is None, f"stderr: {err}"
        assert cli_objects["plain_view"] in out

    def test_unknown_view_exits_nonzero(self, conn_args, cli_objects):
        _, err, exc = run_cli(
            ["introspect", "views", "arx_cli_no_view"] + conn_args + ["-o", "json"]
        )
        assert exc is not None and exc.code == 1
        assert "arx_cli_no_view" in err

    @async_cli
    def test_single_view_async(self, conn_args, cli_objects):
        out, err, exc = run_cli(
            ["introspect", "views", cli_objects["plain_view"]]
            + conn_args
            + ["-o", "json", "--async"]
        )
        assert exc is None, f"stderr: {err}"
        rows = json.loads(out)
        assert [row["name"] for row in rows] == [cli_objects["plain_view"]]


class TestExtensionsCLI:
    """``introspect extensions`` against a live server, listing and one by name.

    Which extensions a server has installed is the scenario's business, so the
    single-extension case reads the name out of the listing rather than naming
    an extension the server may not carry.
    """

    def test_extensions_json(self, conn_args, cli_objects):
        out, err, exc = run_cli(["introspect", "extensions"] + conn_args + ["-o", "json"])
        assert exc is None, f"stderr: {err}\nstdout: {out}"
        rows = json.loads(out)
        assert isinstance(rows, list)
        assert rows, "every PostgreSQL database has at least plpgsql"
        assert {"name", "version", "schema", "description"} <= set(rows[0])

    def test_single_extension_json(self, conn_args, cli_objects):
        listed, err, exc = run_cli(["introspect", "extensions"] + conn_args + ["-o", "json"])
        assert exc is None, f"stderr: {err}"
        installed = [row["name"] for row in json.loads(listed)]
        name = installed[0]

        out, err, exc = run_cli(["introspect", "extensions", name] + conn_args + ["-o", "json"])
        assert exc is None, f"stderr: {err}"
        rows = json.loads(out)
        assert len(rows) == 1
        assert rows[0]["name"] == name

    def test_unknown_extension_exits_nonzero(self, conn_args, cli_objects):
        _, err, exc = run_cli(["introspect", "extensions", "arx_cli_no_extension"] + conn_args)
        assert exc is not None and exc.code == 1
        assert "arx_cli_no_extension" in err

    @async_cli
    def test_extensions_async(self, conn_args, cli_objects):
        out, err, exc = run_cli(
            ["introspect", "extensions"] + conn_args + ["-o", "json", "--async"]
        )
        assert exc is None, f"stderr: {err}"
        rows = json.loads(out)
        assert rows
