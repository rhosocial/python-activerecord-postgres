# tests/rhosocial/activerecord_postgres_test/feature/backend/introspection/test_introspection_cli_objects.py
"""
Black-box CLI tests for the ``materialized-views`` and ``sequences`` types.

Both are driven through ``main()`` so the test exercises the same path a
terminal user takes: argument parsing, dispatch onto
``backend.introspector``, and rendering through the output provider.
"""

import io
import json
import socket
import sys
from contextlib import redirect_stderr, redirect_stdout

import pytest

from rhosocial.activerecord.backend.impl.postgres.__main__ import main
from rhosocial.activerecord.backend.impl.postgres.backend import PostgresBackend
from rhosocial.activerecord.backend.impl.postgres.config import PostgresConnectionConfig
from providers.scenarios import SCENARIO_MAP, get_scenario_raw

CLI_SEQUENCE = "arx_cli_demo_seq"
CLI_VIEW = "arx_cli_demo_mv"

# The CLI runs its async handler inside ``asyncio.run``, which on Windows
# builds the ProactorEventLoop psycopg refuses to work with. That is the
# fate of every subcommand, not of these two, so the async cases are held
# out here and documented rather than left to fail for an unrelated reason.
async_cli = pytest.mark.skipif(
    sys.platform == "win32",
    reason="psycopg async mode needs the selector event loop; the CLI's "
           "asyncio.run gives it the ProactorEventLoop on Windows",
)

# Order matters, and it is the order a previous run may have died in. A view
# depends on its table, so the table cannot be dropped while the view still
# names it; CASCADE on the table's drop is what takes a straggler view with it.
_SETUP_DROPS = [
    "DROP MATERIALIZED VIEW IF EXISTS arx_cli_demo_mv",
    "DROP TABLE IF EXISTS arx_cli_demo_tbl CASCADE",
    "DROP SEQUENCE IF EXISTS arx_cli_demo_seq",
]

_SETUP_CREATES = [
    "CREATE SEQUENCE arx_cli_demo_seq START 42 INCREMENT 4",
    "CREATE TABLE arx_cli_demo_tbl (id INTEGER PRIMARY KEY)",
    "CREATE MATERIALIZED VIEW arx_cli_demo_mv AS SELECT id FROM arx_cli_demo_tbl",
]

_CLEANUP_SQL = [
    "DROP MATERIALIZED VIEW IF EXISTS arx_cli_demo_mv",
    "DROP TABLE IF EXISTS arx_cli_demo_tbl CASCADE",
    "DROP SEQUENCE IF EXISTS arx_cli_demo_seq",
]


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


@pytest.fixture(scope="module")
def conn_args():
    """Connection arguments for the first reachable scenario."""
    for name in SCENARIO_MAP:
        _, config = get_scenario_raw(name)
        if _reachable(config):
            return [
                "--host", config.host,
                "--port", str(config.port),
                "--database", config.database,
                "--user", config.username,
                "--password", config.password,
            ]
    pytest.skip("No scenario server reachable")


@pytest.fixture(scope="module")
def cli_objects(conn_args):
    """The objects the CLI subcommands list, created and dropped here.

    The drops run twice, before and after, because a run that dies mid-way
    -- or an earlier run whose container vanished before its ``finally`` --
    leaves objects behind, and a fixture that cannot recover from that poisons
    every later test in the module with a half-built schema. Neither the drop
    nor the create is allowed to raise here: a duplicate means a previous run
    got there first, and ``CASCADE`` takes what depends on the table. What the
    tests assert is that the objects exist afterwards, not how they got there.
    """
    host, port, database, user, password = conn_args[1::2]
    config = PostgresConnectionConfig(
        host=host, port=int(port), database=database, username=user,
        password=password,
    )
    backend = PostgresBackend(connection_config=config)
    backend.connect()
    try:
        # Drops tolerate leftovers: a run that died half-way is not this
        # test's problem, and refusing to continue would leave the module
        # with a half-built schema. Creates do not get the same licence -- if
        # a CREATE fails the objects the tests assert on are genuinely absent,
        # and that must be a loud error rather than a passing test that read
        # nothing.
        for sql in _SETUP_DROPS:
            try:
                backend.execute(sql)
            except Exception:
                pass
        for sql in _SETUP_CREATES:
            backend.execute(sql)
        yield
    finally:
        for sql in _CLEANUP_SQL:
            try:
                backend.execute(sql)
            except Exception:
                pass
        backend.disconnect()


def run_cli(argv):
    out, err = io.StringIO(), io.StringIO()
    exc = None
    with redirect_stdout(out), redirect_stderr(err):
        try:
            main(argv)
        except SystemExit as e:
            exc = e
    return out.getvalue(), err.getvalue(), exc


class TestSequencesCLI:
    """``introspect sequences`` against a live server."""

    def test_sequences_json(self, conn_args, cli_objects):
        out, err, exc = run_cli(
            ["introspect", "sequences"] + conn_args + ["-o", "json"]
        )
        assert exc is None, f"stderr: {err}\nstdout: {out}"
        rows = json.loads(out)
        assert isinstance(rows, list)
        assert len(rows) > 0
        # The row shape is SequenceInfo's own, so the key is "name" exactly
        # as it is for tables, views and every other listing.
        assert CLI_SEQUENCE in [row["name"] for row in rows]
        demo = next(r for r in rows if r["name"] == CLI_SEQUENCE)
        assert demo["start_value"] == 42
        assert demo["increment"] == 4
        assert demo["schema"] == "public"

    def test_single_sequence_json(self, conn_args, cli_objects):
        out, err, exc = run_cli(
            ["introspect", "sequences", CLI_SEQUENCE] + conn_args + ["-o", "json"]
        )
        assert exc is None, f"stderr: {err}"
        rows = json.loads(out)
        assert len(rows) == 1
        assert rows[0]["name"] == CLI_SEQUENCE

    def test_single_sequence_table(self, conn_args, cli_objects):
        out, err, exc = run_cli(["introspect", "sequences", CLI_SEQUENCE] + conn_args)
        assert exc is None, f"stderr: {err}"
        assert CLI_SEQUENCE in out

    def test_unknown_sequence_exits_nonzero(self, conn_args, cli_objects):
        _, err, exc = run_cli(
            ["introspect", "sequences", "arx_cli_no_seq"] + conn_args + ["-o", "json"]
        )
        assert exc is not None and exc.code == 1
        assert "arx_cli_no_seq" in err

    @async_cli
    def test_sequences_async(self, conn_args, cli_objects):
        out, err, exc = run_cli(
            ["introspect", "sequences"] + conn_args + ["-o", "json", "--async"]
        )
        assert exc is None, f"stderr: {err}"
        rows = json.loads(out)
        assert CLI_SEQUENCE in [row["name"] for row in rows]


class TestMaterializedViewsCLI:
    """``introspect materialized-views`` against a live server."""

    def test_materialized_views_json(self, conn_args, cli_objects):
        out, err, exc = run_cli(
            ["introspect", "materialized-views"] + conn_args + ["-o", "json"]
        )
        assert exc is None, f"stderr: {err}\nstdout: {out}"
        rows = json.loads(out)
        assert isinstance(rows, list)
        assert CLI_VIEW in [row["name"] for row in rows]
        demo = next(r for r in rows if r["name"] == CLI_VIEW)
        assert demo["extra"]["is_materialized"] is True

    def test_single_materialized_view_json(self, conn_args, cli_objects):
        out, err, exc = run_cli(
            ["introspect", "materialized-views", CLI_VIEW] + conn_args + ["-o", "json"]
        )
        assert exc is None, f"stderr: {err}"
        rows = json.loads(out)
        assert len(rows) == 1
        assert rows[0]["name"] == CLI_VIEW
        assert "SELECT" in rows[0]["definition"].upper()

    def test_single_materialized_view_table(self, conn_args, cli_objects):
        out, err, exc = run_cli(
            ["introspect", "materialized-views", CLI_VIEW] + conn_args
        )
        assert exc is None, f"stderr: {err}"
        assert CLI_VIEW in out

    def test_unknown_materialized_view_exits_nonzero(self, conn_args, cli_objects):
        _, err, exc = run_cli(
            ["introspect", "materialized-views", "arx_cli_no_mv"]
            + conn_args + ["-o", "json"]
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
        assert CLI_VIEW in [row["name"] for row in rows]
