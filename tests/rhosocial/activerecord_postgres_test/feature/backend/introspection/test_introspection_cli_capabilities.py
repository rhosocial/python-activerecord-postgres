# tests/rhosocial/activerecord_postgres_test/feature/backend/introspection/test_introspection_cli_capabilities.py
"""Offline gates on the introspect types and their capability switches.

The CLI advertises one type per catalogue family, and a type is only offered
if the dialect says the connected server can answer it. These run against a
fake backend, so nothing here needs a server: what they check is that the
advertised list, the capability mapping and the handler's branches stay
one-for-one, in both the synchronous and the asynchronous chain.
"""

import argparse
import asyncio
import io
from contextlib import redirect_stderr, redirect_stdout

import pytest

from rhosocial.activerecord.backend.errors import ConnectionError, QueryError
from rhosocial.activerecord.backend.impl.postgres.cli import introspect as introspect_mod
from rhosocial.activerecord.backend.impl.postgres.cli.introspect import INTROSPECT_TYPES
from rhosocial.activerecord.backend.impl.postgres.dialect import PostgresDialect


def parse_introspect_args(argv):
    parser = argparse.ArgumentParser(prog="postgres")
    subparsers = parser.add_subparsers(dest="command")
    introspect_mod.create_parser(subparsers)
    return parser.parse_args(argv)


class FakeDialect:
    """A dialect whose switches are all True unless a test says otherwise."""

    def __init__(self, refused=()):
        self._refused = set(refused)

    def __getattr__(self, name):
        if not name.startswith("supports_"):
            raise AttributeError(name)

        def switch():
            return name not in self._refused

        return switch


class FakeIntrospector:
    """One method per family, each recording the call it answered."""

    def __init__(self, exc=None):
        self.calls = []
        self._exc = exc

    def _record(self, name, *args):
        self.calls.append((name, *args))
        if self._exc is not None:
            raise self._exc

    def list_tables(self, schema=None, include_system=False):
        self._record("list_tables", schema, include_system)
        return [{"name": "users"}]

    def list_views(self, schema=None):
        self._record("list_views", schema)
        return [{"name": "users_v"}]

    def list_columns(self, name, schema=None):
        self._record("list_columns", name, schema)
        return [{"name": "id"}]

    def list_indexes(self, name, schema=None):
        self._record("list_indexes", name, schema)
        return [{"name": "users_pkey"}]

    def list_foreign_keys(self, name, schema=None):
        self._record("list_foreign_keys", name, schema)
        return [{"name": "users_role_fk"}]

    def list_triggers(self, table_name=None, schema=None):
        self._record("list_triggers", table_name, schema)
        return [{"name": "users_audit"}]

    def get_database_info(self):
        self._record("get_database_info")
        return {"name": "mydb"}

    def list_extensions(self):
        self._record("list_extensions")
        return []

    def list_materialized_views(self, schema=None, include_system=False):
        self._record("list_materialized_views", schema, include_system)
        return [{"name": "sales_summary"}]

    def list_sequences(self, schema=None):
        self._record("list_sequences", schema)
        return [{"name": "order_id_seq"}]


class FakeBackend:
    last_instance = None

    def __init__(self, connection_config=None):
        self.connection_config = connection_config
        self._connection = None
        self.disconnect_calls = 0
        self.adapt_calls = 0
        self.dialect = FakeDialect()
        self.introspector = FakeIntrospector()
        FakeBackend.last_instance = self

    def connect(self):
        self._connection = object()

    def introspect_and_adapt(self):
        self.adapt_calls += 1

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
        self.adapt_calls += 1

    async def disconnect(self):
        self.disconnect_calls += 1
        self._connection = None


#: The first introspector call each type is expected to make. A type that
#: lands in the wrong branch makes a different one, which is what the gate
#: detects -- the payloads themselves are the catalogue's business, not this
#: file's.
_FIRST_CALL = {
    "tables": "list_tables",
    "views": "list_views",
    "columns": "list_columns",
    "indexes": "list_indexes",
    "foreign-keys": "list_foreign_keys",
    "triggers": "list_triggers",
    "database": "get_database_info",
    "extensions": "list_extensions",
    "materialized-views": "list_materialized_views",
    "sequences": "list_sequences",
}

#: The name each type needs to reach its branch; the family listings take none.
_ARGV = {
    "columns": ["users"],
    "indexes": ["users"],
    "foreign-keys": ["users"],
}


@pytest.fixture
def run_cli(monkeypatch):
    def runner(argv, backend_cls=FakeBackend):
        monkeypatch.setattr(introspect_mod, "PostgresBackend", backend_cls)
        monkeypatch.setattr(
            introspect_mod,
            "AsyncPostgresBackend",
            AsyncFakeBackend if backend_cls is FakeBackend else backend_cls,
        )
        out, err = io.StringIO(), io.StringIO()
        exc = None
        with redirect_stdout(out), redirect_stderr(err):
            try:
                # argparse answers --help by exiting from the parse itself, so
                # the parse belongs inside the same capture as handle().
                args = parse_introspect_args(argv)
                introspect_mod.handle(args)
            except SystemExit as e:
                exc = e
        return out.getvalue(), err.getvalue(), exc

    return runner


class TestTypeVocabulary:
    def test_the_advertised_types_are_the_capability_mapping(self):
        """One source for the list: the mapping, not a second hand-written copy."""
        assert list(introspect_mod._TYPE_CAPABILITIES) == INTROSPECT_TYPES

    def test_every_type_names_a_switch_the_dialect_has(self):
        """A type whose switch does not exist would be un-gated, not gated."""
        dialect = PostgresDialect(version=(16, 0))
        missing = [
            (cli_type, switch)
            for cli_type, switch in introspect_mod._TYPE_CAPABILITIES.items()
            if not hasattr(dialect, switch)
        ]
        assert missing == []

    def test_every_family_the_dialect_answers_for_is_askable(self):
        """The other direction: no answerable family is left without a type.

        A switch that returns False is a different statement. PostgreSQL
        declares ``supports_partition_metadata_introspection`` because the
        protocol asks every dialect, and on the versions that have it the
        answer is True -- but partition metadata has no introspector question
        of its own: it is read as part of the DDL a partitioned table carries,
        so there is no family to list and no type to give it.
        """
        no_question = {"supports_partition_metadata_introspection"}
        dialect = PostgresDialect(version=(16, 0))
        answerable = {
            name
            for name in dir(PostgresDialect)
            if name.startswith("supports_")
            and name.endswith("_introspection")
            and name not in no_question
            and name != "supports_introspection"
            and getattr(dialect, name)()
        }
        used = set(introspect_mod._TYPE_CAPABILITIES.values())
        assert answerable - used == set()

    def test_the_aggregate_table_type_is_gone(self):
        assert "table" not in INTROSPECT_TYPES

    def test_help_lists_every_type(self, run_cli):
        out, _, exc = run_cli(["introspect", "--help"])
        assert exc is not None and exc.code == 0
        for type_name in INTROSPECT_TYPES:
            assert type_name in out


class TestEveryTypeLandsABranch:
    """One case per advertised type: the dispatch reaches the introspector.

    Both chains are covered, because the handler writes the branches twice and
    a type added to one and forgotten in the other would otherwise be offered
    to the user and answer nothing.
    """

    @pytest.mark.parametrize("type_name", INTROSPECT_TYPES)
    def test_sync(self, run_cli, type_name):
        argv = [
            "introspect",
            type_name,
            *_ARGV.get(type_name, []),
            "--database",
            "mydb",
            "-o",
            "json",
        ]
        _, err, exc = run_cli(argv)
        assert exc is None, f"stderr: {err}"
        calls = FakeBackend.last_instance.introspector.calls
        assert calls and calls[0][0] == _FIRST_CALL[type_name]
        assert FakeBackend.last_instance.disconnect_calls == 1

    @pytest.mark.parametrize("type_name", INTROSPECT_TYPES)
    def test_async(self, run_cli, type_name):
        argv = [
            "introspect",
            type_name,
            *_ARGV.get(type_name, []),
            "--database",
            "mydb",
            "-o",
            "json",
            "--async",
        ]
        _, err, exc = run_cli(argv)
        assert exc is None, f"stderr: {err}"
        calls = FakeBackend.last_instance.introspector.calls
        assert calls and calls[0][0] == _FIRST_CALL[type_name]
        assert FakeBackend.last_instance.disconnect_calls == 1


class TestCapabilityGate:
    """A switch that answers False refuses the type instead of the listing."""

    @pytest.mark.parametrize("type_name", INTROSPECT_TYPES)
    def test_an_unsupported_type_exits_two(self, run_cli, monkeypatch, type_name):
        switch = introspect_mod._TYPE_CAPABILITIES[type_name]

        class RefusingBackend(FakeBackend):
            def __init__(self, connection_config=None):
                super().__init__(connection_config=connection_config)
                self.dialect = FakeDialect(refused={switch})

        argv = ["introspect", type_name, *_ARGV.get(type_name, []), "--database", "mydb"]
        _, err, exc = run_cli(argv, backend_cls=RefusingBackend)
        assert exc is not None and exc.code == 2
        assert err.strip() == (f"Error: {type_name} introspection is not supported by this server")
        assert FakeBackend.last_instance.introspector.calls == []

    def test_the_async_chain_refuses_the_same_way(self, monkeypatch, capsys):
        class RefusingAsyncBackend(AsyncFakeBackend):
            def __init__(self, connection_config=None):
                super().__init__(connection_config=connection_config)
                self.dialect = FakeDialect(refused={"supports_sequence_introspection"})

        backend = RefusingAsyncBackend()
        args = parse_introspect_args(["introspect", "sequences", "--database", "mydb"])
        with pytest.raises(SystemExit) as excinfo:
            asyncio.run(
                introspect_mod._handle_introspect_async(
                    args, backend, introspect_mod.create_provider("json")
                )
            )
        assert excinfo.value.code == 2
        assert (
            "Error: sequences introspection is not supported by this server"
            in capsys.readouterr().err
        )
        assert backend.introspector.calls == []


class TestAdaptationAtTheEntrance:
    """The dialect is adapted once, before the capability switch is read."""

    @pytest.mark.parametrize(
        "type_name",
        ["tables", "materialized-views", "sequences", "extensions"],
    )
    def test_one_adaptation_per_run(self, run_cli, type_name):
        argv = ["introspect", type_name, "--database", "mydb", "-o", "json"]
        _, err, exc = run_cli(argv)
        assert exc is None, f"stderr: {err}"
        assert FakeBackend.last_instance.adapt_calls == 1

    def test_the_switch_is_read_after_adapting(self, run_cli):
        """An unadapted dialect would answer for the wrong server."""
        seen = []

        class RecordingBackend(FakeBackend):
            def introspect_and_adapt(self):
                super().introspect_and_adapt()
                seen.append("adapted")

            def __init__(self, connection_config=None):
                super().__init__(connection_config=connection_config)
                outer = self

                class Dialect:
                    def supports_table_introspection(self):
                        seen.append(f"asked after {outer.adapt_calls} adapt")
                        return True

                self.dialect = Dialect()

        _, _, exc = run_cli(
            ["introspect", "tables", "--database", "mydb"],
            backend_cls=RecordingBackend,
        )
        assert exc is None
        assert seen == ["adapted", "asked after 1 adapt"]


class TestErrorPathsStillExitOne:
    """Runtime failures stay exit 1: the request was servable, the run failed."""

    def test_connection_error_exits_one(self, run_cli):
        class RefusedBackend(FakeBackend):
            def connect(self):
                raise ConnectionError("unavailable")

        _, _, exc = run_cli(
            ["introspect", "tables", "--database", "mydb"], backend_cls=RefusedBackend
        )
        assert exc is not None and exc.code == 1

    def test_query_error_exits_one(self, run_cli):
        class BrokenBackend(FakeBackend):
            def __init__(self, connection_config=None):
                super().__init__(connection_config=connection_config)
                self.introspector = FakeIntrospector(exc=QueryError("no such table"))

        _, _, exc = run_cli(
            ["introspect", "tables", "--database", "mydb"], backend_cls=BrokenBackend
        )
        assert exc is not None and exc.code == 1

    def test_missing_database_exits_one(self, run_cli):
        _, err, exc = run_cli(["introspect", "tables"])
        assert exc is not None and exc.code == 1
        assert "--database is required" in err
