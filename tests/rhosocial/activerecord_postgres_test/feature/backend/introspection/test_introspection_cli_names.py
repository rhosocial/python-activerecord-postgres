# tests/rhosocial/activerecord_postgres_test/feature/backend/introspection/test_introspection_cli_names.py
"""Offline tests for what ``NAME`` means to each introspect type.

A type either answers for one catalogue object or lists a family. The ones
that answer for one object take a name -- optional, selecting that object --
and the ones that list a family refuse it. Nothing here needs a server: the
fake backend says what the catalogue would answer, so these tests pin the
dispatch rather than PostgreSQL's own bookkeeping.
"""

import argparse
import io
import json
from contextlib import redirect_stderr, redirect_stdout

import pytest

from rhosocial.activerecord.backend.impl.postgres.cli import introspect as introspect_mod
from rhosocial.activerecord.backend.introspection.status import ExtensionInfo
from rhosocial.activerecord.backend.introspection.types import IntrospectionViewInfo


def parse_introspect_args(argv):
    parser = argparse.ArgumentParser(prog="postgres")
    subparsers = parser.add_subparsers(dest="command")
    introspect_mod.create_parser(subparsers)
    return parser.parse_args(argv)


def _view(name, definition="SELECT 1"):
    return IntrospectionViewInfo(name=name, schema="public", definition=definition)


def _extension(name, version="1.0", schema="public", description=None):
    return ExtensionInfo(
        name=name,
        version=version,
        schema=schema,
        description=description,
    )


class FakeIntrospector:
    """The catalogue, answering with fixed payloads and recording each call."""

    def __init__(self):
        self.calls = []

    def list_tables(self, schema=None, include_system=False):
        self.calls.append(("list_tables", schema, include_system))
        return [{"name": "users"}]

    def list_views(self, schema=None):
        self.calls.append(("list_views", schema))
        return [_view("users_v")]

    def get_view_info(self, view_name, schema=None):
        self.calls.append(("get_view_info", view_name, schema))
        if view_name != "users_v":
            return None
        return _view(view_name)

    def get_database_info(self):
        self.calls.append(("get_database_info",))
        return {"name": "mydb"}

    def list_extensions(self):
        self.calls.append(("list_extensions",))
        return [_extension("pgcrypto"), _extension("plpgsql")]

    def get_extension_info(self, name):
        self.calls.append(("get_extension_info", name))
        if name != "pgcrypto":
            return None
        return _extension("pgcrypto")


class FakeBackend:
    last_instance = None

    def __init__(self, connection_config=None):
        self.connection_config = connection_config
        self._connection = None
        self.disconnect_calls = 0
        self.introspector = FakeIntrospector()
        FakeBackend.last_instance = self

    def connect(self):
        self._connection = object()

    def introspect_and_adapt(self):
        self.adapt_calls = getattr(self, "adapt_calls", 0) + 1

    def disconnect(self):
        self.disconnect_calls += 1
        self._connection = None


class AsyncFakeIntrospector:
    """The fake introspector with every method awaited."""

    def __init__(self, base=None):
        self._base = base if base is not None else FakeIntrospector()

    def __getattr__(self, name):
        attr = getattr(self._base, name)

        async def runner(*args, **kwargs):
            return attr(*args, **kwargs)

        return runner

    @property
    def calls(self):
        return self._base.calls


class AsyncFakeBackend(FakeBackend):
    def __init__(self, connection_config=None):
        super().__init__(connection_config=connection_config)
        self.introspector = AsyncFakeIntrospector()

    async def connect(self):
        self._connection = object()

    async def introspect_and_adapt(self):
        self.adapt_calls = getattr(self, "adapt_calls", 0) + 1

    async def disconnect(self):
        self.disconnect_calls += 1
        self._connection = None


@pytest.fixture
def run_cli(monkeypatch):
    """Run ``introspect`` in-process against the fake backend.

    Both backends are replaced, so ``--async`` exercises the asynchronous
    chain and everything else the synchronous one.
    """

    def runner(argv):
        monkeypatch.setattr(introspect_mod, "PostgresBackend", FakeBackend)
        monkeypatch.setattr(introspect_mod, "AsyncPostgresBackend", AsyncFakeBackend)
        args = parse_introspect_args(argv)
        out, err = io.StringIO(), io.StringIO()
        exc = None
        with redirect_stdout(out), redirect_stderr(err):
            try:
                introspect_mod.handle(args)
            except SystemExit as e:
                exc = e
        return out.getvalue(), err.getvalue(), exc

    return runner


class TestOneObjectByName:
    """``views`` and ``extensions`` answer for one object when given a name."""

    def test_single_view_carries_its_definition(self, run_cli):
        out, err, exc = run_cli(
            ["introspect", "views", "users_v", "--database", "mydb", "-o", "json"]
        )
        assert exc is None, f"stderr: {err}"
        rows = json.loads(out)
        assert len(rows) == 1
        assert rows[0]["name"] == "users_v"
        assert "SELECT" in rows[0]["definition"].upper()
        assert FakeBackend.last_instance.introspector.calls[0] == (
            "get_view_info",
            "users_v",
            None,
        )

    def test_unknown_view_exits_one(self, run_cli):
        _, err, exc = run_cli(["introspect", "views", "arx_cli_no_view", "--database", "mydb"])
        assert exc is not None and exc.code == 1
        assert "arx_cli_no_view" in err

    def test_views_without_a_name_still_lists(self, run_cli):
        out, _, exc = run_cli(["introspect", "views", "--database", "mydb", "-o", "json"])
        assert exc is None
        assert [row["name"] for row in json.loads(out)] == ["users_v"]
        assert FakeBackend.last_instance.introspector.calls[0] == ("list_views", None)

    def test_single_extension_keeps_the_listing_row_shape(self, run_cli):
        out, err, exc = run_cli(
            ["introspect", "extensions", "pgcrypto", "--database", "mydb", "-o", "json"]
        )
        assert exc is None, f"stderr: {err}"
        rows = json.loads(out)
        assert len(rows) == 1
        assert rows[0] == {
            "name": "pgcrypto",
            "version": "1.0",
            "schema": "public",
            "description": None,
        }
        assert FakeBackend.last_instance.introspector.calls[0] == (
            "get_extension_info",
            "pgcrypto",
        )

    def test_extension_that_is_not_installed_exits_one(self, run_cli):
        _, err, exc = run_cli(["introspect", "extensions", "nope", "--database", "mydb"])
        assert exc is not None and exc.code == 1
        assert "'nope'" in err and "not installed" in err

    def test_extensions_without_a_name_still_lists(self, run_cli):
        out, _, exc = run_cli(["introspect", "extensions", "--database", "mydb", "-o", "json"])
        assert exc is None
        assert [row["name"] for row in json.loads(out)] == ["pgcrypto", "plpgsql"]
        assert FakeBackend.last_instance.introspector.calls[0] == ("list_extensions",)

    @pytest.mark.parametrize(
        "argv",
        [
            ["introspect", "views", "users_v", "--database", "mydb", "-o", "json"],
            ["introspect", "extensions", "pgcrypto", "--database", "mydb", "-o", "json"],
        ],
    )
    def test_the_async_chain_answers_the_same_question(self, run_cli, argv):
        out, err, exc = run_cli(argv + ["--async"])
        assert exc is None, f"stderr: {err}"
        rows = json.loads(out)
        assert len(rows) == 1
        assert rows[0]["name"] in ("users_v", "pgcrypto")


class TestNameIsRefused:
    """A family listing takes no name; the name is an error, not a filter."""

    @pytest.mark.parametrize(
        "argv,marker",
        [
            (["introspect", "tables", "users"], "'tables'"),
            (["introspect", "database", "mydb"], "'database'"),
        ],
    )
    def test_a_family_listing_refuses_a_name(self, run_cli, argv, marker):
        _, err, exc = run_cli(argv + ["--database", "mydb"])
        assert exc is not None and exc.code == 2
        assert marker in err
        assert "does not take an object name" in err

    def test_a_refused_name_never_reaches_the_catalogue(self, run_cli):
        run_cli(["introspect", "tables", "users", "--database", "mydb"])
        assert FakeBackend.last_instance.introspector.calls == []
