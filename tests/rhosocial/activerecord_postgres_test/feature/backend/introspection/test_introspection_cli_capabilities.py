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
from rhosocial.activerecord.backend.introspection.types import DDLStatement, JoinCoverage


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


class _Row(dict):
    """A fake catalogue row: a dict, and the attribute the real record has.

    The real ``list_tables`` answers ``IntrospectionTableInfo`` objects; the
    fake answers dicts because the serializers read both. A consumer that
    reads an attribute -- like the ``table-storage`` branch picking the named
    row -- gets the real contract's surface rather than a surprise.
    """

    def __getattr__(self, item):
        try:
            return self[item]
        except KeyError:
            raise AttributeError(item) from None


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
        return [_Row(name="users")]

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

    # The schema-object families added for catalogue coverage. Each answers a
    # listing; the optional name narrows rather than selects.

    def list_partitions(
        self, table_name, schema=None, include_partitions=True, include_statistics=True
    ):
        self._record(
            "list_partitions", table_name, schema, include_partitions, include_statistics
        )
        return [{"name": "orders_p2026_11", "table": table_name}]

    def list_routines(self, schema=None, routine_name=None):
        self._record("list_routines", schema, routine_name)
        return [{"name": routine_name or "app_total"}]

    def list_types(self, schema=None, type_name=None):
        self._record("list_types", schema, type_name)
        return [{"name": type_name or "order_status"}]

    def list_operators(self, schema=None, operator_name=None):
        self._record("list_operators", schema, operator_name)
        return [{"name": operator_name or "==="}]

    def list_operator_classes(self, schema=None, operator_class_name=None):
        self._record("list_operator_classes", schema, operator_class_name)
        return [{"name": operator_class_name or "gist_trgm_ops"}]

    def list_operator_families(self, schema=None, operator_family_name=None):
        self._record("list_operator_families", schema, operator_family_name)
        return [{"name": operator_family_name or "gist_trgm_ops"}]

    # The operational families: each answers a listing of its own, from the
    # statistics collector rather than the catalog.

    def list_unused_indexes(self, schema=None):
        self._record("list_unused_indexes", schema)
        return [{"schema": "public", "table_name": "users", "index_name": "users_email_idx"}]

    def list_table_usage_stats(self, schema=None):
        self._record("list_table_usage_stats", schema)
        return [{"table_name": "users", "sequential_scans": 4}]

    def list_runtime_metrics(self, schema=None):
        self._record("list_runtime_metrics", schema)
        return [{"name": "active_sessions", "value": 3}]

    def list_index_usage_stats(self, schema=None):
        self._record("list_index_usage_stats", schema)
        return [{"table_name": "users", "index_name": "users_pkey", "scans": 9}]

    def list_object_dependencies(self, object_name=None, schema=None):
        self._record("list_object_dependencies", object_name, schema)
        return [{"dependent": "users_v", "referenced": "users"}]

    def get_ddl_statement(self, object_name, object_type=None, schema=None):
        self._record("get_ddl_statement", object_name, object_type, schema)
        return DDLStatement(
            schema="public",
            object_type="r",
            object_name=object_name,
            ddl=f"CREATE TABLE {object_name} (id integer);",
            native=False,
        )

    # The data families: one table, or one column of it.

    def list_column_data_profiles(self, table_name, schema=None):
        self._record("list_column_data_profiles", table_name, schema)
        return [{"table_name": table_name, "column_name": "id"}]

    def sample_column_values(self, table_name, column_name, schema=None):
        self._record("sample_column_values", table_name, column_name, schema)
        return [{"value": "active", "count": 7}]

    def estimate_join_coverage(
        self, left_table, left_column, right_table, right_column, schema=None
    ):
        self._record(
            "estimate_join_coverage",
            left_table,
            left_column,
            right_table,
            right_column,
            schema,
        )
        return JoinCoverage(
            left_schema="public",
            left_table=left_table,
            left_column=left_column,
            right_schema="public",
            right_table=right_table,
            right_column=right_column,
            left_coverage=1.0,
            right_coverage=0.5,
        )


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
    "partitions": "list_partitions",
    "routines": "list_routines",
    "types": "list_types",
    "operators": "list_operators",
    "operator-classes": "list_operator_classes",
    "operator-families": "list_operator_families",
    # The storage figures ride the listing's projection, so the CLI asks the
    # listing and picks the row -- one read, not the four a per-table info
    # answer would spend.
    "table-storage": "list_tables",
    "unused-indexes": "list_unused_indexes",
    "table-stats": "list_table_usage_stats",
    "runtime": "list_runtime_metrics",
    "index-stats": "list_index_usage_stats",
    "dependencies": "list_object_dependencies",
    "ddl": "get_ddl_statement",
    "column-profiles": "list_column_data_profiles",
    "value-samples": "sample_column_values",
    "join-coverage": "estimate_join_coverage",
}

#: The name each type needs to reach its branch; the family listings take none.
#: ``dependencies`` accepts a name but answers the whole graph without one, so
#: the generic gate runs it unnamed and a case below pins the narrowed call.
_ARGV = {
    "columns": ["users"],
    "indexes": ["users"],
    "foreign-keys": ["users"],
    "partitions": ["users"],
    "table-storage": ["users"],
    "ddl": ["users"],
    "column-profiles": ["users"],
    "value-samples": ["users", "--column", "status"],
    "join-coverage": [
        "users",
        "--column",
        "id",
        "--right-table",
        "orders",
        "--right-column",
        "user_id",
    ],
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

    def test_the_new_families_have_a_supported_switch(self):
        dialect = PostgresDialect(version=(16, 0))
        for switch in (
            "supports_partition_introspection",
            "supports_routine_introspection",
            "supports_type_introspection",
            "supports_operator_introspection",
            "supports_operator_class_introspection",
            "supports_operator_family_introspection",
        ):
            assert getattr(dialect, switch)() is True

    def test_partition_info_delegates_to_the_family_switch(self):
        dialect = PostgresDialect(version=(16, 0))
        assert dialect.supports_partition_info() is True
        assert PostgresDialect(version=(9, 6)).supports_partition_info() is False

    def test_ddl_is_not_advertised_when_no_text_is_produced(self):
        """I-05: the family that returns None for ordinary tables says so."""
        assert PostgresDialect(version=(16, 0)).supports_ddl_extraction() is False

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


class TestTheFamiliesTheDialectAnswers:
    """One case per newly offered type: the method, and every argument with it.

    The parametrized gates above only prove that *some* introspector call is
    made. These pin which one and with what, because a branch that reached
    the right family with the wrong column would still pass a first-call
    check -- and the columns are the whole question for the data families.
    """

    def _calls(self, run_cli, argv):
        _, err, exc = run_cli(["introspect", *argv, "--database", "mydb", "-o", "json"])
        assert exc is None, f"stderr: {err}"
        return FakeBackend.last_instance.introspector.calls

    def test_partitions_asks_for_the_named_parent(self, run_cli):
        assert self._calls(run_cli, ["partitions", "orders"]) == [
            ("list_partitions", "orders", None, True, True)
        ]

    def test_routines_list_and_narrow_by_name(self, run_cli):
        assert self._calls(run_cli, ["routines"]) == [("list_routines", None, None)]
        assert self._calls(run_cli, ["routines", "app_total"]) == [
            ("list_routines", None, "app_total")
        ]

    def test_types_list_and_narrow_by_name(self, run_cli):
        assert self._calls(run_cli, ["types"]) == [("list_types", None, None)]
        assert self._calls(run_cli, ["types", "order_status"]) == [
            ("list_types", None, "order_status")
        ]

    def test_operators_list_and_narrow_by_name(self, run_cli):
        assert self._calls(run_cli, ["operators"]) == [("list_operators", None, None)]
        assert self._calls(run_cli, ["operators", "==="]) == [
            ("list_operators", None, "===")
        ]

    def test_operator_classes_list_and_narrow_by_name(self, run_cli):
        assert self._calls(run_cli, ["operator-classes"]) == [
            ("list_operator_classes", None, None)
        ]
        assert self._calls(run_cli, ["operator-classes", "gist_trgm_ops"]) == [
            ("list_operator_classes", None, "gist_trgm_ops")
        ]

    def test_operator_families_list_and_narrow_by_name(self, run_cli):
        assert self._calls(run_cli, ["operator-families"]) == [
            ("list_operator_families", None, None)
        ]
        assert self._calls(run_cli, ["operator-families", "gist_trgm_ops"]) == [
            ("list_operator_families", None, "gist_trgm_ops")
        ]

    def test_unused_indexes_reads_the_statistics_collector(self, run_cli):
        assert self._calls(run_cli, ["unused-indexes"]) == [("list_unused_indexes", None)]

    def test_table_stats_reads_the_usage_counters(self, run_cli):
        assert self._calls(run_cli, ["table-stats"]) == [("list_table_usage_stats", None)]

    def test_runtime_reads_the_live_metrics(self, run_cli):
        assert self._calls(run_cli, ["runtime"]) == [("list_runtime_metrics", None)]

    def test_index_stats_reads_the_per_index_counters(self, run_cli):
        assert self._calls(run_cli, ["index-stats"]) == [("list_index_usage_stats", None)]

    def test_dependencies_answers_the_whole_graph_unnamed(self, run_cli):
        assert self._calls(run_cli, ["dependencies"]) == [("list_object_dependencies", None, None)]

    def test_dependencies_narrows_to_one_object_by_name(self, run_cli):
        assert self._calls(run_cli, ["dependencies", "orders"]) == [
            ("list_object_dependencies", "orders", None)
        ]

    def test_ddl_asks_for_the_named_table(self, run_cli):
        assert self._calls(run_cli, ["ddl", "users"]) == [
            ("get_ddl_statement", "users", None, None)
        ]

    def test_ddl_prints_the_statement_in_table_mode(self, run_cli):
        """Table output is the SQL itself, not a one-cell table around it."""
        out, err, exc = run_cli(["introspect", "ddl", "users", "--database", "mydb"])
        assert exc is None, f"stderr: {err}"
        assert "CREATE TABLE users" in out

    def test_column_profiles_asks_for_the_named_table(self, run_cli):
        assert self._calls(run_cli, ["column-profiles", "users"]) == [
            ("list_column_data_profiles", "users", None)
        ]

    def test_value_samples_names_the_table_and_the_column(self, run_cli):
        assert self._calls(run_cli, ["value-samples", "users", "--column", "status"]) == [
            ("sample_column_values", "users", "status", None)
        ]

    def test_join_coverage_passes_both_sides_of_the_measurement(self, run_cli):
        assert self._calls(
            run_cli,
            [
                "join-coverage",
                "orders",
                "--column",
                "customer_ref",
                "--right-table",
                "customers",
                "--right-column",
                "code",
            ],
        ) == [
            (
                "estimate_join_coverage",
                "orders",
                "customer_ref",
                "customers",
                "code",
                None,
            )
        ]


class TestTheNewArgumentsAreRequiredOrRefused:
    """A missing name exits 1; a flag the type has no question for exits 2.

    Both are the request's fault rather than the run's, and both keep the
    catalogue untouched: an argument that is neither required nor refused is
    the silently-dropped one this handler keeps out.
    """

    def test_value_samples_without_a_column_exits_one(self, run_cli):
        _, err, exc = run_cli(["introspect", "value-samples", "users", "--database", "mydb"])
        assert exc is not None and exc.code == 1
        assert "--column is required" in err
        assert FakeBackend.last_instance.introspector.calls == []

    def test_join_coverage_without_the_right_side_exits_one(self, run_cli):
        _, err, exc = run_cli(
            [
                "introspect",
                "join-coverage",
                "orders",
                "--column",
                "customer_ref",
                "--database",
                "mydb",
            ]
        )
        assert exc is not None and exc.code == 1
        assert "--right-table and --right-column are required" in err
        assert FakeBackend.last_instance.introspector.calls == []

    def test_ddl_without_a_table_name_exits_one(self, run_cli):
        _, err, exc = run_cli(["introspect", "ddl", "--database", "mydb"])
        assert exc is not None and exc.code == 1
        assert "Table name is required for 'ddl'" in err
        assert FakeBackend.last_instance.introspector.calls == []

    def test_column_on_a_type_with_no_column_exits_two(self, run_cli):
        _, err, exc = run_cli(["introspect", "tables", "--column", "status", "--database", "mydb"])
        assert exc is not None and exc.code == 2
        assert "'tables' does not take --column" in err
        assert FakeBackend.last_instance.introspector.calls == []

    def test_right_side_on_a_one_table_type_exits_two(self, run_cli):
        _, err, exc = run_cli(
            [
                "introspect",
                "column-profiles",
                "users",
                "--right-table",
                "orders",
                "--database",
                "mydb",
            ]
        )
        assert exc is not None and exc.code == 2
        assert "does not take --right-table or --right-column" in err
        assert FakeBackend.last_instance.introspector.calls == []


class TestTheRealDialectRefusesDDL:
    """I-05 end to end: the advertised type meets a switch that is False."""

    def test_ddl_is_refused_before_the_catalogue_is_asked(self, capsys):
        backend = type(
            "RealDialectBackend",
            (),
            {"dialect": PostgresDialect(version=(16, 0))},
        )()
        with pytest.raises(SystemExit) as excinfo:
            introspect_mod._require_capability(
                backend, argparse.Namespace(type="ddl")
            )
        assert excinfo.value.code == 2
        assert "ddl introspection is not supported by this server" in capsys.readouterr().err


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
