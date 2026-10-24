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
import uuid
from contextlib import redirect_stderr, redirect_stdout

import pytest

from rhosocial.activerecord.backend.impl.postgres.__main__ import main
from rhosocial.activerecord.backend.impl.postgres.backend import PostgresBackend
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

@pytest.fixture(scope="module")
def cli_objects(conn_args):
    """The objects the CLI subcommands list, created and dropped here.

    Names carry a per-process suffix. The matrix runs a dozen jobs against the
    same scenario servers, and a run that dies mid-way leaves its objects
    behind: with one fixed name, the next job's setup collides on
    ``pg_class_relname_nsp_index`` and every later assertion in the module
    reads whatever survived. A unique suffix makes each run's objects its own,
    so neither a leftover nor a concurrent job can be mistaken for it.

    The drops stay forgiving (``IF EXISTS``, ``CASCADE`` on the table because a
    view names its table) so a run that died half-way is cleaned up rather than
    blocking. The creates do not get the same licence: if a CREATE fails, the
    objects the tests assert on are genuinely absent, and that must be a loud
    error rather than a test that reads nothing.
    """
    suffix = uuid.uuid4().hex[:8]
    seq_name = f"arx_cli_demo_seq_{suffix}"
    table_name = f"arx_cli_demo_tbl_{suffix}"
    view_name = f"arx_cli_demo_mv_{suffix}"

    setup_drops = [
        f"DROP MATERIALIZED VIEW IF EXISTS {view_name}",
        f"DROP TABLE IF EXISTS {table_name} CASCADE",
        f"DROP SEQUENCE IF EXISTS {seq_name}",
    ]
    setup_creates = [
        f"CREATE SEQUENCE {seq_name} START 42 INCREMENT 4",
        f"CREATE TABLE {table_name} (id INTEGER PRIMARY KEY)",
        f"CREATE MATERIALIZED VIEW {view_name} AS SELECT id FROM {table_name}",
    ]
    cleanup_sql = [
        f"DROP MATERIALIZED VIEW IF EXISTS {view_name}",
        f"DROP TABLE IF EXISTS {table_name} CASCADE",
        f"DROP SEQUENCE IF EXISTS {seq_name}",
    ]

    host, port, database, user, password = conn_args[1::2]
    config = PostgresConnectionConfig(
        host=host, port=int(port), database=database, username=user,
        password=password,
    )
    backend = PostgresBackend(connection_config=config)
    backend.connect()
    try:
        for sql in setup_drops:
            try:
                backend.execute(sql)
            except Exception:
                pass
        for sql in setup_creates:
            backend.execute(sql)
        yield {"seq": seq_name, "table": table_name, "view": view_name}
    finally:
        for sql in cleanup_sql:
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
        assert cli_objects["seq"] in [row["name"] for row in rows]
        demo = next(r for r in rows if r["name"] == cli_objects["seq"])
        assert demo["start_value"] == 42
        assert demo["increment"] == 4
        assert demo["schema"] == "public"

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
        out, err, exc = run_cli(
            ["introspect", "sequences"] + conn_args + ["-o", "json", "--async"]
        )
        assert exc is None, f"stderr: {err}"
        rows = json.loads(out)
        assert cli_objects["seq"] in [row["name"] for row in rows]


class TestMaterializedViewsCLI:
    """``introspect materialized-views`` against a live server."""

    def test_materialized_views_json(self, conn_args, cli_objects):
        out, err, exc = run_cli(
            ["introspect", "materialized-views"] + conn_args + ["-o", "json"]
        )
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
        assert cli_objects["view"] in [row["name"] for row in rows]
