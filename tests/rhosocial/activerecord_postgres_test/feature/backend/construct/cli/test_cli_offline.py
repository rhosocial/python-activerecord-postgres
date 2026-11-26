# tests/rhosocial/activerecord_postgres_test/feature/backend/construct/cli/test_cli_offline.py
"""Offline unit tests for the PostgreSQL CLI subcommands.

Every test here is pure: parser construction, serialization/formatting helpers,
validation maps and handler dispatch are exercised with MagicMock / AsyncMock
backends and providers. No PostgreSQL server, socket or driver is contacted.
"""

import argparse
import json
import sys
import types
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from rhosocial.activerecord.backend.errors import ConnectionError, QueryError
from rhosocial.activerecord.backend.impl.postgres.cli import info as info_mod
from rhosocial.activerecord.backend.impl.postgres.cli import introspect as introspect_mod
from rhosocial.activerecord.backend.impl.postgres.cli import query as query_mod
from rhosocial.activerecord.backend.impl.postgres.cli import status as status_mod
from rhosocial.activerecord.backend.impl.postgres.dialect import PostgresDialect
from rhosocial.activerecord.backend.introspection.types import (
    PartitionBound,
    PartitionScheme,
    PartitionState,
)
from rhosocial.activerecord.backend.introspection.status import (
    ConnectionInfo,
    DatabaseBriefInfo,
    ExtensionInfo,
    ServerOverview,
    SessionInfo,
    StatusCategory,
    StatusItem,
    StorageInfo,
    UserInfo,
)


def _args(**overrides):
    """A namespace carrying every flag the four handlers may read."""
    base = dict(
        output="json",
        rich_ascii=False,
        verbose=0,
        type="all",
        database="db",
        named_connection=None,
        is_async=False,
        version=None,
        sql=None,
        file=None,
        log_level="INFO",
        name=None,
        column=None,
        right_table=None,
        right_column=None,
        include_system=False,
        connection_params=[],
    )
    base.update(overrides)
    return argparse.Namespace(**base)


def _subparser(factory):
    return factory(argparse.ArgumentParser(prog="pgx").add_subparsers(dest="command"))


# ===========================================================================
# status
# ===========================================================================


class TestStatusParser:
    def test_defaults(self):
        args = _subparser(status_mod.create_parser).parse_args([])
        assert args.type == "all"
        assert args.output == "table"
        assert args.verbose == 0
        assert args.rich_ascii is False

    @pytest.mark.parametrize("status_type", status_mod.STATUS_TYPES)
    def test_every_status_type_accepted(self, status_type):
        parser = _subparser(status_mod.create_parser)
        assert parser.parse_args([status_type]).type == status_type

    def test_invalid_type_rejected(self):
        with pytest.raises(SystemExit):
            _subparser(status_mod.create_parser).parse_args(["bogus"])

    @pytest.mark.parametrize("output", ["table", "json", "csv", "tsv"])
    def test_output_choices(self, output):
        assert _subparser(status_mod.create_parser).parse_args(["-o", output]).output == output

    def test_invalid_output_rejected(self):
        with pytest.raises(SystemExit):
            _subparser(status_mod.create_parser).parse_args(["-o", "xml"])

    @pytest.mark.parametrize(
        "flags,attr,value",
        [
            (["-vvv"], "verbose", 3),
            (["--rich-ascii"], "rich_ascii", True),
            (["--async"], "is_async", True),
            (["--named-connection", "app.db"], "named_connection", "app.db"),
        ],
    )
    def test_flags(self, flags, attr, value):
        assert getattr(_subparser(status_mod.create_parser).parse_args(flags), attr) == value


class TestStatusHandleDispatch:
    def test_missing_database_exits(self, capsys):
        with pytest.raises(SystemExit) as exc:
            status_mod.handle(_args(database=None))
        assert exc.value.code == 1
        assert "database is required" in capsys.readouterr().err

    def test_sync_dispatch(self):
        args = _args(rich_ascii=True)
        with patch.object(status_mod, "create_provider") as provider_factory, patch.object(
            status_mod, "resolve_connection_config_from_args", return_value="cfg"
        ), patch.object(status_mod, "PostgresBackend") as backend_cls, patch.object(
            status_mod, "_handle_status_sync"
        ) as handler:
            status_mod.handle(args)
        provider_factory.assert_called_once_with("json", ascii_borders=True)
        backend_cls.assert_called_once_with(connection_config="cfg")
        handler.assert_called_once_with(
            args, backend_cls.return_value, provider_factory.return_value
        )

    def test_async_dispatch(self):
        args = _args(is_async=True)
        coroutine = object()
        with patch.object(status_mod, "create_provider"), patch.object(
            status_mod, "resolve_connection_config_from_args", return_value="cfg"
        ), patch.object(status_mod, "AsyncPostgresBackend") as backend_cls, patch.object(
            status_mod, "_handle_status_async", new=MagicMock(return_value=coroutine)
        ) as handler, patch.object(
            status_mod, "run_coroutine"
        ) as runner:
            status_mod.handle(args)
        backend_cls.assert_called_once_with(connection_config="cfg")
        handler.assert_called_once()
        runner.assert_called_once_with(coroutine)


class TestStatusSyncHandler:
    @pytest.mark.parametrize(
        "status_type,method,title,wrapped",
        [
            ("config", "list_configuration", "Configuration", False),
            ("performance", "list_configuration", "Performance", False),
            ("connections", "get_connection_info", "Connections", True),
            ("storage", "get_storage_info", "Storage", True),
            ("databases", "list_databases", "Databases", False),
            ("users", "list_users", "Users", False),
        ],
    )
    def test_section_dispatch(self, status_type, method, title, wrapped):
        backend = MagicMock()
        backend._connection = False
        sample = [{"name": "x", "value": 1}]
        getattr(backend.introspector.status, method).return_value = sample
        provider = MagicMock()
        args = _args(type=status_type)
        status_mod._handle_status_sync(args, backend, provider)
        backend.connect.assert_called_once_with()
        backend.introspect_and_adapt.assert_called_once_with()
        expected = [sample] if wrapped else sample
        provider.display_results.assert_called_once_with(expected, title=title)

    def test_all_prints_json(self, capsys):
        backend = MagicMock()
        backend._connection = False
        backend.introspector.status.get_overview.return_value = {"server_version": "16"}
        status_mod._handle_status_sync(_args(type="all"), backend, MagicMock())
        assert json.loads(capsys.readouterr().out) == {"server_version": "16"}

    @pytest.mark.parametrize("output", ["csv", "tsv"])
    def test_all_falls_back_to_json_for_tabular_formats(self, output, capsys):
        backend = MagicMock()
        backend._connection = False
        backend.introspector.status.get_overview.return_value = {"k": "v"}
        status_mod._handle_status_sync(_args(type="all", output=output), backend, MagicMock())
        assert json.loads(capsys.readouterr().out) == {"k": "v"}

    def test_all_uses_rich_when_available(self):
        backend = MagicMock()
        backend._connection = False
        backend.introspector.status.get_overview.return_value = {"k": "v"}
        with patch.object(status_mod, "RICH_AVAILABLE", True), patch.object(
            status_mod, "_display_status_rich"
        ) as rich:
            status_mod._handle_status_sync(_args(type="all", output="table"), backend, MagicMock())
        rich.assert_called_once_with({"k": "v"}, 0)

    def test_all_uses_json_without_rich(self, capsys):
        backend = MagicMock()
        backend._connection = False
        backend.introspector.status.get_overview.return_value = {"k": "v"}
        with patch.object(status_mod, "RICH_AVAILABLE", False):
            status_mod._handle_status_sync(_args(type="all", output="table"), backend, MagicMock())
        assert json.loads(capsys.readouterr().out) == {"k": "v"}

    def test_disconnect_when_connected(self):
        backend = MagicMock()
        backend._connection = object()
        backend.introspector.status.get_overview.return_value = {}
        status_mod._handle_status_sync(_args(type="all"), backend, MagicMock())
        backend.disconnect.assert_called_once_with()

    def test_connection_error_exits(self):
        backend = MagicMock()
        backend._connection = False
        backend.connect.side_effect = ConnectionError("no db")
        provider = MagicMock()
        with pytest.raises(SystemExit) as exc:
            status_mod._handle_status_sync(_args(), backend, provider)
        assert exc.value.code == 1
        provider.display_connection_error.assert_called_once()

    def test_query_error_exits(self):
        backend = MagicMock()
        backend._connection = False
        backend.introspect_and_adapt.side_effect = QueryError("bad sql")
        provider = MagicMock()
        with pytest.raises(SystemExit):
            status_mod._handle_status_sync(_args(), backend, provider)
        provider.display_query_error.assert_called_once()

    def test_unexpected_error_exits(self, capsys):
        backend = MagicMock()
        backend._connection = False
        backend.connect.side_effect = RuntimeError("boom")
        with pytest.raises(SystemExit):
            status_mod._handle_status_sync(_args(), backend, MagicMock())
        assert "Error during status introspection" in capsys.readouterr().err


class TestStatusAsyncHandler:
    @pytest.mark.parametrize(
        "status_type,method,title",
        [
            ("config", "list_configuration", "Configuration"),
            ("connections", "get_connection_info", "Connections"),
            ("users", "list_users", "Users"),
        ],
    )
    async def test_async_section_dispatch(self, status_type, method, title):
        backend = AsyncMock()
        backend._connection = False
        introspector = MagicMock()
        setattr(introspector, method, AsyncMock(return_value=[{"a": 1}]))
        backend.introspector = MagicMock(status=introspector)
        provider = MagicMock()
        await status_mod._handle_status_async(_args(type=status_type), backend, provider)
        backend.connect.assert_awaited_once_with()
        backend.introspect_and_adapt.assert_awaited_once_with()
        expected = [[{"a": 1}]] if method == "get_connection_info" else [{"a": 1}]
        provider.display_results.assert_called_once_with(expected, title=title)

    async def test_async_all_prints_json(self, capsys):
        backend = AsyncMock()
        backend._connection = False
        backend.introspector = MagicMock(
            status=MagicMock(get_overview=AsyncMock(return_value={"v": 16}))
        )
        await status_mod._handle_status_async(_args(type="all"), backend, MagicMock())
        assert json.loads(capsys.readouterr().out) == {"v": 16}

    async def test_async_connection_error_exits(self):
        backend = AsyncMock()
        backend._connection = False
        backend.connect.side_effect = ConnectionError("no db")
        provider = MagicMock()
        with pytest.raises(SystemExit):
            await status_mod._handle_status_async(_args(), backend, provider)
        provider.display_connection_error.assert_called_once()

    async def test_async_disconnect_when_connected(self):
        backend = AsyncMock()
        backend._connection = object()
        backend.introspector = MagicMock(status=MagicMock(get_overview=AsyncMock(return_value={})))
        await status_mod._handle_status_async(_args(type="all"), backend, MagicMock())
        backend.disconnect.assert_awaited_once_with()


# ===========================================================================
# info
# ===========================================================================


class TestInfoParser:
    def test_defaults(self):
        args = _subparser(info_mod.create_parser).parse_args([])
        assert args.output == "table"
        assert args.verbose == 0
        assert args.version is None
        assert args.rich_ascii is False

    @pytest.mark.parametrize("output", info_mod.INFO_OUTPUT_CHOICES)
    def test_output_choices(self, output):
        assert _subparser(info_mod.create_parser).parse_args(["-o", output]).output == output

    def test_output_rejects_csv(self):
        with pytest.raises(SystemExit):
            _subparser(info_mod.create_parser).parse_args(["-o", "csv"])

    def test_version_and_verbosity(self):
        args = _subparser(info_mod.create_parser).parse_args(["--version", "15.4.1", "-vv"])
        assert args.version == "15.4.1"
        assert args.verbose == 2


class _Proto:
    def supports_alpha(self):
        return True

    def supports_beta(self):
        return False

    def is_gamma_available(self):
        return True

    def supports_arg_probe(self, probe):
        return probe in ("TEXT", "JSON")

    def supports_needs_arg(self, required):
        return True

    def helper_not_a_check(self):
        return True


class _ProtoDialect:
    def supports_alpha(self):
        return True

    def supports_beta(self):
        return 0

    def is_gamma_available(self):
        return 1

    def supports_arg_probe(self, probe):
        return probe in ("TEXT", "JSON")

    def supports_needs_arg(self, required):
        return True


class TestInfoHelpers:
    @pytest.mark.parametrize(
        "text,expected",
        [
            ("15.4.1", (15, 4, 1)),
            ("13", (13, 0, 0)),
            ("15.2", (15, 2, 0)),
            ("9.6.1", (9, 6, 1)),
            ("16.0.0", (16, 0, 0)),
        ],
    )
    def test_parse_version(self, text, expected):
        assert info_mod.parse_version(text) == expected

    def test_get_protocol_support_methods(self):
        assert info_mod.get_protocol_support_methods(_Proto) == [
            "is_gamma_available",
            "supports_alpha",
            "supports_arg_probe",
            "supports_beta",
            "supports_needs_arg",
        ]

    def test_check_protocol_support(self, monkeypatch):
        monkeypatch.setitem(
            info_mod.SUPPORT_METHOD_ALL_ARGS,
            "supports_arg_probe",
            ["TEXT", "JSON", "DOT"],
        )
        result = info_mod.check_protocol_support(_ProtoDialect(), _Proto)
        assert result["supports_alpha"] is True
        assert result["supports_beta"] is False
        assert result["is_gamma_available"] is True
        assert result["supports_needs_arg"] is False
        probe = result["supports_arg_probe"]
        assert probe["supported"] == 2
        assert probe["total"] == 3
        assert probe["args"]["TEXT"] is True
        assert probe["args"]["DOT"] is False

    def test_check_protocol_support_missing_method(self):
        class _Empty:
            pass

        result = info_mod.check_protocol_support(object(), _Proto)
        assert set(result) == set(info_mod.get_protocol_support_methods(_Proto))
        assert all(value is False for value in result.values())

    @pytest.mark.parametrize(
        "methods,expected",
        [
            ({"a": True, "b": False}, (1, 2)),
            ({"a": {"supported": 2, "total": 6}}, (2, 6)),
            ({"a": True, "b": {"supported": 3, "total": 3}}, (4, 4)),
        ],
    )
    def test_calculate_protocol_stats(self, methods, expected):
        assert info_mod._calculate_protocol_stats(methods) == expected

    def test_build_protocol_info(self):
        info = info_mod._build_protocol_info(_ProtoDialect(), "G", [_Proto], 0)
        stats = info["_Proto"]
        assert set(stats) == {"supported", "total", "percentage"}
        assert stats["total"] == 5
        assert stats["percentage"] == 40.0

    def test_build_protocol_info_verbose_includes_methods(self):
        info = info_mod._build_protocol_info(_ProtoDialect(), "G", [_Proto], 2)
        assert "methods" in info["_Proto"]

    @pytest.mark.parametrize(
        "pct,expected",
        [
            (100, ("green", "[OK]")),
            (75, ("yellow", "[~]")),
            (25, ("red", "[~]")),
            (0, ("red", "[X]")),
        ],
    )
    def test_get_status_style(self, pct, expected):
        assert info_mod._get_status_style(pct) == expected

    @pytest.mark.parametrize(
        "method,expected",
        [
            ("supports_lock_strength_for_update", "lock strength for update"),
            ("supports_x", "x"),
        ],
    )
    def test_format_method_display(self, method, expected):
        assert info_mod._format_method_display(method) == expected

    def test_constants(self):
        assert info_mod.DIALECT_SPECIFIC_GROUPS == {"PostgreSQL Native", "PostgreSQL Extensions"}
        assert info_mod.SUPPORT_METHOD_ALL_ARGS == {}


class TestInfoHandle:
    def test_default_version_without_connection(self, capsys):
        args = _args(output="json", version=None)
        with patch.object(info_mod, "create_provider") as factory, patch.object(
            info_mod, "_build_protocol_info", return_value={}
        ):
            info_mod.handle(args)
        factory.assert_called_once_with("json", ascii_borders=False)
        payload = json.loads(capsys.readouterr().out)
        assert payload["database"] == {
            "type": "postgresql",
            "version": "13.0.0",
            "version_tuple": [13, 0, 0],
            "connected": False,
        }
        assert payload["features"]["extensions"] == {}

    def test_version_override(self, capsys):
        args = _args(output="json", version="15.2.3")
        with patch.object(info_mod, "create_provider"), patch.object(
            info_mod, "_build_protocol_info", return_value={}
        ):
            info_mod.handle(args)
        payload = json.loads(capsys.readouterr().out)
        assert payload["database"]["version"] == "15.2.3"
        assert payload["database"]["version_tuple"] == [15, 2, 3]

    def test_connected_path_reports_extensions(self, capsys):
        dialect = PostgresDialect(version=(15, 2, 3))
        ext = SimpleNamespace(installed=True, available=True, version="1.3", schema="public")
        dialect._extensions = {"pgcrypto": ext}
        backend = MagicMock()
        backend.dialect = dialect
        backend.get_server_version.return_value = (15, 2, 3)
        args = _args(output="json", database="mydb")
        with patch.object(info_mod, "create_provider"), patch.object(
            info_mod, "_build_protocol_info", return_value={}
        ), patch.object(info_mod, "resolve_connection_config_from_args", return_value="cfg"), patch(
            "rhosocial.activerecord.backend.impl.postgres.backend.PostgresBackend",
            return_value=backend,
        ) as backend_cls:
            info_mod.handle(args)
        backend_cls.assert_called_once_with(connection_config="cfg")
        payload = json.loads(capsys.readouterr().out)
        assert payload["database"]["connected"] is True
        assert payload["database"]["version"] == "15.2.3"
        assert payload["features"]["extensions"]["pgcrypto"] == {
            "installed": True,
            "available": True,
            "version": "1.3",
            "schema": "public",
        }
        backend.disconnect.assert_called_once_with()

    def test_connection_failure_falls_back_to_defaults(self, capsys):
        args = _args(output="json", database="mydb", version="14.1.0")
        with patch.object(info_mod, "create_provider"), patch.object(
            info_mod, "_build_protocol_info", return_value={}
        ), patch.object(
            info_mod, "resolve_connection_config_from_args", side_effect=RuntimeError("no db")
        ):
            info_mod.handle(args)
        payload = json.loads(capsys.readouterr().out)
        assert payload["database"]["connected"] is False
        assert payload["database"]["version"] == "14.1.0"


# ===========================================================================
# introspect
# ===========================================================================


class TestIntrospectParser:
    def test_defaults(self):
        args = _subparser(introspect_mod.create_parser).parse_args(["tables"])
        assert args.type == "tables"
        assert args.output == "table"
        assert args.name is None
        assert args.include_system is False

    @pytest.mark.parametrize("kind", introspect_mod.INTROSPECT_TYPES)
    def test_all_types_accepted(self, kind):
        assert _subparser(introspect_mod.create_parser).parse_args([kind]).type == kind

    def test_invalid_type_rejected(self):
        with pytest.raises(SystemExit):
            _subparser(introspect_mod.create_parser).parse_args(["bogus"])

    def test_name_and_flags(self):
        args = _subparser(introspect_mod.create_parser).parse_args(
            ["value-samples", "orders", "--column", "email", "--include-system", "--async"]
        )
        assert args.name == "orders"
        assert args.column == "email"
        assert args.include_system is True
        assert args.is_async is True

    def test_join_coverage_flags(self):
        args = _subparser(introspect_mod.create_parser).parse_args(
            ["join-coverage", "a", "--column", "x", "--right-table", "b", "--right-column", "y"]
        )
        assert (args.right_table, args.right_column) == ("b", "y")


class TestValidateName:
    @pytest.mark.parametrize(
        "kind", [k for k, v in introspect_mod._NAME_RULE.items() if v == "required"]
    )
    def test_required_missing_exits(self, kind):
        with pytest.raises(SystemExit) as exc:
            introspect_mod._validate_name(_args(type=kind, name=None))
        assert exc.value.code == 1

    @pytest.mark.parametrize(
        "kind", [k for k, v in introspect_mod._NAME_RULE.items() if v == "forbidden"]
    )
    def test_forbidden_with_name_exits(self, kind):
        with pytest.raises(SystemExit) as exc:
            introspect_mod._validate_name(_args(type=kind, name="obj"))
        assert exc.value.code == 2

    @pytest.mark.parametrize("kind,rule", sorted(introspect_mod._NAME_RULE.items()))
    def test_valid_invocation_accepted(self, kind, rule):
        name = None if rule == "forbidden" else "obj"
        introspect_mod._validate_name(_args(type=kind, name=name))


class TestRequireCapability:
    @pytest.mark.parametrize("kind,switch", sorted(introspect_mod._TYPE_CAPABILITIES.items()))
    def test_unsupported_switch_exits(self, kind, switch):
        backend = MagicMock()
        dialect = MagicMock()
        setattr(dialect, switch, MagicMock(return_value=False))
        backend.dialect = dialect
        with pytest.raises(SystemExit) as exc:
            introspect_mod._require_capability(backend, _args(type=kind))
        assert exc.value.code == 2

    @pytest.mark.parametrize("kind,switch", sorted(introspect_mod._TYPE_CAPABILITIES.items()))
    def test_supported_switch_passes(self, kind, switch):
        backend = MagicMock()
        dialect = MagicMock()
        setattr(dialect, switch, MagicMock(return_value=True))
        backend.dialect = dialect
        introspect_mod._require_capability(backend, _args(type=kind))

    def test_missing_switch_passes(self):
        backend = MagicMock()
        backend.dialect = object()
        introspect_mod._require_capability(backend, _args(type="tables"))

    def test_missing_dialect_passes(self):
        backend = MagicMock()
        backend.dialect = None
        introspect_mod._require_capability(backend, _args(type="tables"))


class TestRefuseUnusedArguments:
    @pytest.mark.parametrize("kind", ["tables", "columns", "database", "table-stats"])
    def test_column_on_wrong_type_exits(self, kind):
        with pytest.raises(SystemExit) as exc:
            introspect_mod._refuse_unused_arguments(_args(type=kind, column="c"))
        assert exc.value.code == 2

    @pytest.mark.parametrize("kind", ["value-samples", "join-coverage"])
    def test_column_on_probe_type_accepted(self, kind):
        introspect_mod._refuse_unused_arguments(_args(type=kind, column="c"))

    @pytest.mark.parametrize("kind", ["tables", "columns", "value-samples"])
    def test_right_flags_on_wrong_type_exits(self, kind):
        with pytest.raises(SystemExit) as exc:
            introspect_mod._refuse_unused_arguments(_args(type=kind, right_table="b"))
        assert exc.value.code == 2

    def test_right_flags_on_join_coverage_accepted(self):
        introspect_mod._refuse_unused_arguments(
            _args(type="join-coverage", right_table="b", right_column="y")
        )


class TestExtensionRows:
    def test_extension_rows(self):
        rows = introspect_mod._extension_rows(
            [
                ExtensionInfo(
                    name="pgcrypto", version="1.3", schema="public", description="crypto"
                ),
                ExtensionInfo(name="hstore"),
            ]
        )
        assert rows == [
            {"name": "pgcrypto", "version": "1.3", "schema": "public", "description": "crypto"},
            {"name": "hstore", "version": None, "schema": None, "description": None},
        ]


class TestIntrospectHandleDispatch:
    def test_missing_database_exits(self, capsys):
        with pytest.raises(SystemExit) as exc:
            introspect_mod.handle(_args(type="tables", database=None))
        assert exc.value.code == 1
        assert "database is required" in capsys.readouterr().err

    def test_sync_dispatch(self):
        args = _args(type="tables")
        with patch.object(introspect_mod, "create_provider") as factory, patch.object(
            introspect_mod, "resolve_connection_config_from_args", return_value="cfg"
        ), patch.object(introspect_mod, "PostgresBackend") as backend_cls, patch.object(
            introspect_mod, "_handle_introspect_sync"
        ) as handler:
            introspect_mod.handle(args)
        factory.assert_called_once_with("json", ascii_borders=False)
        backend_cls.assert_called_once_with(connection_config="cfg")
        handler.assert_called_once_with(args, backend_cls.return_value, factory.return_value)

    def test_async_dispatch(self):
        args = _args(type="tables", is_async=True)
        coroutine = object()
        with patch.object(introspect_mod, "create_provider"), patch.object(
            introspect_mod, "resolve_connection_config_from_args", return_value="cfg"
        ), patch.object(introspect_mod, "AsyncPostgresBackend") as backend_cls, patch.object(
            introspect_mod, "_handle_introspect_async", new=MagicMock(return_value=coroutine)
        ) as handler, patch.object(
            introspect_mod, "run_coroutine"
        ) as runner:
            introspect_mod.handle(args)
        backend_cls.assert_called_once_with(connection_config="cfg")
        handler.assert_called_once()
        runner.assert_called_once_with(coroutine)


_SYNC_CASES = [
    ("tables", None, "list_tables", dict(schema=None, include_system=False), "Tables", False),
    ("views", None, "list_views", dict(schema=None), "Views", False),
    ("columns", "users", "list_columns", dict(schema=None), "Columns of users", False),
    ("indexes", "users", "list_indexes", dict(schema=None), "Indexes of users", False),
    (
        "foreign-keys",
        "users",
        "list_foreign_keys",
        dict(schema=None),
        "Foreign Keys of users",
        False,
    ),
    ("triggers", None, "list_triggers", dict(table_name=None, schema=None), "Triggers", False),
    ("database", None, "get_database_info", {}, "Database Info", True),
    (
        "materialized-views",
        None,
        "list_materialized_views",
        dict(schema=None, include_system=False),
        "Materialized Views",
        False,
    ),
    ("sequences", None, "list_sequences", dict(schema=None), "Sequences", False),
    ("partitions", "orders", "get_partition_state", dict(schema=None), "Partitions of orders", True),
    ("routines", None, "list_routines", dict(schema=None, routine_name=None), "Routines", False),
    ("types", None, "list_types", dict(schema=None, type_name=None), "Types", False),
    (
        "operators",
        None,
        "list_operators",
        dict(schema=None, operator_name=None),
        "Operators",
        False,
    ),
    (
        "operator-classes",
        None,
        "list_operator_classes",
        dict(schema=None, operator_class_name=None),
        "Operator Classes",
        False,
    ),
    (
        "operator-families",
        None,
        "list_operator_families",
        dict(schema=None, operator_family_name=None),
        "Operator Families",
        False,
    ),
    ("unused-indexes", None, "list_unused_indexes", dict(schema=None), "Unused Indexes", False),
    ("table-stats", None, "list_table_usage_stats", dict(schema=None), "Table Usage Stats", False),
    ("runtime", None, "list_runtime_metrics", dict(schema=None), "Runtime Metrics", False),
    ("index-stats", None, "list_index_usage_stats", dict(schema=None), "Index Usage Stats", False),
    (
        "dependencies",
        None,
        "list_object_dependencies",
        dict(object_name=None, schema=None),
        "Object Dependencies",
        False,
    ),
    (
        "column-profiles",
        "orders",
        "list_column_data_profiles",
        dict(schema=None),
        "Column profiles of orders",
        False,
    ),
]


class TestIntrospectSyncHandler:
    @pytest.mark.parametrize("kind,name,method,kwargs,title,wrapped", _SYNC_CASES)
    def test_family_dispatch(self, kind, name, method, kwargs, title, wrapped):
        backend = MagicMock()
        backend._connection = False
        sample = [{"k": "v"}]
        if method == "get_partition_state":
            # The family reads the neutral state's scheme, so the fake answers
            # a PartitionState rather than the bare listing it used to.
            sample = PartitionState(
                scheme=PartitionScheme.RANGE,
                bounds=(PartitionBound(name="orders_p2026_11"),),
            )
        getattr(backend.introspector, method).return_value = sample
        provider = MagicMock()
        introspect_mod._handle_introspect_sync(_args(type=kind, name=name), backend, provider)
        backend.connect.assert_called_once_with()
        backend.introspect_and_adapt.assert_called_once_with()
        if name and method in (
            "list_columns",
            "list_indexes",
            "list_foreign_keys",
            "get_partition_state",
            "list_column_data_profiles",
        ):
            getattr(backend.introspector, method).assert_called_once_with(name, **kwargs)
        else:
            getattr(backend.introspector, method).assert_called_once_with(**kwargs)
        expected = [sample] if wrapped else sample
        provider.display_results.assert_called_once_with(
            introspect_mod._serialize_for_output(expected), title=title
        )

    def test_partitions_reports_an_unpartitioned_table(self):
        """``scheme=None`` is the "not partitioned" answer; ``None`` is not one."""
        backend = MagicMock()
        backend._connection = False
        backend.introspector.get_partition_state.return_value = PartitionState(
            scheme=None
        )
        with pytest.raises(SystemExit) as exc:
            introspect_mod._handle_introspect_sync(
                _args(type="partitions", name="users"), backend, MagicMock()
            )
        assert exc.value.code == 1

    def test_extensions_family(self):
        backend = MagicMock()
        backend._connection = False
        backend.introspector.list_extensions.return_value = [
            ExtensionInfo(name="pgcrypto", version="1.3", schema="public", description=None)
        ]
        provider = MagicMock()
        introspect_mod._handle_introspect_sync(_args(type="extensions"), backend, provider)
        provider.display_results.assert_called_once_with(
            [{"name": "pgcrypto", "version": "1.3", "schema": "public", "description": None}],
            title="Extensions",
        )

    def test_value_samples(self):
        backend = MagicMock()
        backend._connection = False
        backend.introspector.sample_column_values.return_value = [{"value": 1}]
        provider = MagicMock()
        introspect_mod._handle_introspect_sync(
            _args(type="value-samples", name="orders", column="email"), backend, provider
        )
        backend.introspector.sample_column_values.assert_called_once_with(
            "orders", "email", schema=None
        )
        provider.display_results.assert_called_once_with(
            [{"value": 1}], title="Values of orders.email"
        )

    def test_join_coverage(self):
        backend = MagicMock()
        backend._connection = False
        backend.introspector.estimate_join_coverage.return_value = {"overlap": 0.5}
        provider = MagicMock()
        introspect_mod._handle_introspect_sync(
            _args(
                type="join-coverage",
                name="orders",
                column="buyer",
                right_table="users",
                right_column="id",
            ),
            backend,
            provider,
        )
        backend.introspector.estimate_join_coverage.assert_called_once_with(
            "orders", "buyer", "users", "id", schema=None
        )
        provider.display_results.assert_called_once_with([{"overlap": 0.5}], title="Join Coverage")

    @pytest.mark.parametrize(
        "kind,name,method",
        [
            ("views", "missing", "get_view_info"),
            ("extensions", "missing", "get_extension_info"),
            ("materialized-views", "missing", "get_materialized_view_info"),
            ("sequences", "missing", "get_sequence_info"),
        ],
    )
    def test_named_object_not_found_exits(self, kind, name, method):
        backend = MagicMock()
        backend._connection = False
        getattr(backend.introspector, method).return_value = None
        with pytest.raises(SystemExit) as exc:
            introspect_mod._handle_introspect_sync(
                _args(type=kind, name=name), backend, MagicMock()
            )
        assert exc.value.code == 1

    def test_value_samples_requires_column(self):
        backend = MagicMock()
        backend._connection = False
        with pytest.raises(SystemExit) as exc:
            introspect_mod._handle_introspect_sync(
                _args(type="value-samples", name="orders", column=None), backend, MagicMock()
            )
        assert exc.value.code == 1

    def test_join_coverage_requires_right_flags(self):
        backend = MagicMock()
        backend._connection = False
        with pytest.raises(SystemExit):
            introspect_mod._handle_introspect_sync(
                _args(type="join-coverage", name="orders", column="buyer"), backend, MagicMock()
            )

    def test_connection_error_exits(self):
        backend = MagicMock()
        backend._connection = False
        backend.connect.side_effect = ConnectionError("down")
        provider = MagicMock()
        with pytest.raises(SystemExit):
            introspect_mod._handle_introspect_sync(_args(type="tables"), backend, provider)
        provider.display_connection_error.assert_called_once()


class TestIntrospectAsyncHandler:
    @pytest.mark.parametrize("kind,name,method,kwargs,title,wrapped", _SYNC_CASES[:6])
    async def test_async_family_dispatch(self, kind, name, method, kwargs, title, wrapped):
        backend = AsyncMock()
        backend._connection = False
        introspector = MagicMock()
        setattr(introspector, method, AsyncMock(return_value=[{"k": "v"}]))
        backend.introspector = introspector
        backend.dialect = None
        provider = MagicMock()
        await introspect_mod._handle_introspect_async(
            _args(type=kind, name=name), backend, provider
        )
        backend.connect.assert_awaited_once_with()
        backend.introspect_and_adapt.assert_awaited_once_with()
        expected = [[{"k": "v"}]] if wrapped else [{"k": "v"}]
        provider.display_results.assert_called_once_with(expected, title=title)

    async def test_async_value_samples(self):
        backend = AsyncMock()
        backend._connection = False
        backend.introspector = MagicMock(
            sample_column_values=AsyncMock(return_value=[{"value": 2}])
        )
        backend.dialect = None
        provider = MagicMock()
        await introspect_mod._handle_introspect_async(
            _args(type="value-samples", name="t", column="c"), backend, provider
        )
        provider.display_results.assert_called_once_with([{"value": 2}], title="Values of t.c")

    async def test_async_unsupported_capability_exits(self):
        backend = AsyncMock()
        backend._connection = False
        dialect = MagicMock()
        dialect.supports_table_introspection = MagicMock(return_value=False)
        backend.dialect = dialect
        with pytest.raises(SystemExit) as exc:
            await introspect_mod._handle_introspect_async(
                _args(type="tables"), backend, MagicMock()
            )
        assert exc.value.code == 2


# ===========================================================================
# query
# ===========================================================================


class TestQueryParser:
    def test_defaults(self):
        args = _subparser(query_mod.create_parser).parse_args([])
        assert args.output == "table"
        assert args.sql is None
        assert args.file is None
        assert args.log_level == "INFO"
        assert args.rich_ascii is False

    @pytest.mark.parametrize("output", ["table", "json", "csv", "tsv"])
    def test_output_choices(self, output):
        assert _subparser(query_mod.create_parser).parse_args(["-o", output]).output == output

    def test_sql_and_file(self):
        parser = _subparser(query_mod.create_parser)
        assert parser.parse_args(["SELECT 1"]).sql == "SELECT 1"
        assert parser.parse_args(["-f", "q.sql"]).file == "q.sql"


class TestQueryHandleValidation:
    def test_invalid_log_level_raises(self):
        with pytest.raises(ValueError, match="Invalid log level"):
            query_mod.handle(_args(sql="SELECT 1", log_level="NOPE"))

    def test_missing_sql_exits(self, capsys):
        fake_stdin = MagicMock()
        fake_stdin.isatty.return_value = True
        with patch.object(query_mod.sys, "stdin", fake_stdin):
            with pytest.raises(SystemExit) as exc:
                query_mod.handle(_args(sql=None, file=None))
        assert exc.value.code == 1
        assert "No SQL query provided" in capsys.readouterr().err

    def test_multiple_statements_exit(self):
        with pytest.raises(SystemExit) as exc:
            query_mod.handle(_args(sql="SELECT 1; SELECT 2;"))
        assert exc.value.code == 1

    def test_single_trailing_semicolon_allowed(self):
        with patch.object(query_mod, "create_provider"), patch.object(
            query_mod.logging, "basicConfig"
        ), patch.object(
            query_mod, "resolve_connection_config_from_args", return_value="cfg"
        ), patch.object(
            query_mod, "PostgresBackend"
        ), patch.object(
            query_mod, "_execute_query_sync"
        ) as executor:
            query_mod.handle(_args(sql="SELECT 1;"))
        executor.assert_called_once()

    def test_file_not_found_exits(self):
        with pytest.raises(SystemExit) as exc:
            query_mod.handle(_args(sql=None, file="no/such/query.sql"))
        assert exc.value.code == 1

    def test_file_source(self, tmp_path):
        path = tmp_path / "q.sql"
        path.write_text("SELECT 42", encoding="utf-8")
        with patch.object(query_mod, "create_provider"), patch.object(
            query_mod.logging, "basicConfig"
        ), patch.object(
            query_mod, "resolve_connection_config_from_args", return_value="cfg"
        ), patch.object(
            query_mod, "PostgresBackend"
        ), patch.object(
            query_mod, "_execute_query_sync"
        ) as executor:
            query_mod.handle(_args(sql=None, file=str(path)))
        assert executor.call_args.args[0] == "SELECT 42"

    def test_stdin_source(self):
        fake_stdin = MagicMock()
        fake_stdin.isatty.return_value = False
        fake_stdin.read.return_value = "SELECT 7"
        with patch.object(query_mod.sys, "stdin", fake_stdin), patch.object(
            query_mod, "create_provider"
        ), patch.object(query_mod.logging, "basicConfig"), patch.object(
            query_mod, "resolve_connection_config_from_args", return_value="cfg"
        ), patch.object(
            query_mod, "PostgresBackend"
        ), patch.object(
            query_mod, "_execute_query_sync"
        ) as executor:
            query_mod.handle(_args(sql=None, file=None))
        assert executor.call_args.args[0] == "SELECT 7"


class TestQueryHandleDispatch:
    def test_sync_dispatch(self):
        args = _args(sql="SELECT 1", log_level="DEBUG", rich_ascii=True)
        with patch.object(query_mod, "create_provider") as factory, patch.object(
            query_mod.logging, "basicConfig"
        ) as logging_mod, patch.object(
            query_mod, "resolve_connection_config_from_args", return_value="cfg"
        ), patch.object(
            query_mod, "PostgresBackend"
        ) as backend_cls, patch.object(
            query_mod, "_execute_query_sync"
        ) as executor:
            query_mod.handle(args)
        factory.assert_called_once_with("json", ascii_borders=True)
        logging_mod.assert_called_once()
        factory.return_value.display_greeting.assert_called_once_with()
        backend_cls.assert_called_once_with(connection_config="cfg")
        executor.assert_called_once_with(
            "SELECT 1", backend_cls.return_value, factory.return_value, use_ascii=True
        )

    def test_async_dispatch(self):
        args = _args(sql="SELECT 1", is_async=True)
        coroutine = object()
        with patch.object(query_mod, "create_provider"), patch.object(
            query_mod.logging, "basicConfig"
        ), patch.object(
            query_mod, "resolve_connection_config_from_args", return_value="cfg"
        ), patch.object(
            query_mod, "AsyncPostgresBackend"
        ) as backend_cls, patch.object(
            query_mod, "_execute_query_async", new=MagicMock(return_value=coroutine)
        ) as executor, patch.object(
            query_mod, "run_coroutine"
        ) as runner:
            query_mod.handle(args)
        backend_cls.assert_called_once_with(connection_config="cfg")
        executor.assert_called_once()
        runner.assert_called_once_with(coroutine)


class TestQueryExecuteSync:
    def test_result_with_data(self):
        backend = MagicMock()
        backend._connection = True
        backend.execute.return_value = SimpleNamespace(
            affected_rows=2, duration=0.5, data=[{"a": 1}]
        )
        provider = MagicMock()
        query_mod._execute_query_sync("SELECT 1", backend, provider, use_ascii=False)
        provider.display_query.assert_called_once_with("SELECT 1", is_async=False)
        provider.display_success.assert_called_once_with(2, 0.5)
        provider.display_results.assert_called_once_with([{"a": 1}], use_ascii=False)
        backend.disconnect.assert_called_once_with()
        provider.display_disconnect.assert_called_once_with(is_async=False)

    def test_result_without_data(self):
        backend = MagicMock()
        backend._connection = False
        backend.execute.return_value = SimpleNamespace(affected_rows=0, duration=0.1, data=[])
        provider = MagicMock()
        query_mod._execute_query_sync("UPDATE t", backend, provider)
        provider.display_no_data.assert_called_once_with()
        provider.display_results.assert_not_called()

    def test_no_result_object(self):
        backend = MagicMock()
        backend._connection = False
        backend.execute.return_value = None
        provider = MagicMock()
        query_mod._execute_query_sync("SET x", backend, provider)
        provider.display_no_result_object.assert_called_once_with()

    def test_connection_error_exits(self):
        backend = MagicMock()
        backend._connection = False
        backend.connect.side_effect = ConnectionError("down")
        provider = MagicMock()
        with pytest.raises(SystemExit):
            query_mod._execute_query_sync("SELECT 1", backend, provider)
        provider.display_connection_error.assert_called_once()

    def test_query_error_exits(self):
        backend = MagicMock()
        backend._connection = False
        backend.execute.side_effect = QueryError("bad")
        provider = MagicMock()
        with pytest.raises(SystemExit):
            query_mod._execute_query_sync("SELECT 1", backend, provider)
        provider.display_query_error.assert_called_once()

    def test_unexpected_error_exits(self):
        backend = MagicMock()
        backend._connection = False
        backend.execute.side_effect = RuntimeError("boom")
        provider = MagicMock()
        with pytest.raises(SystemExit):
            query_mod._execute_query_sync("SELECT 1", backend, provider)
        provider.display_unexpected_error.assert_called_once()


class TestQueryExecuteAsync:
    async def test_result_with_data(self):
        backend = AsyncMock()
        backend._connection = True
        backend.execute.return_value = SimpleNamespace(
            affected_rows=1, duration=0.2, data=[{"b": 2}]
        )
        provider = MagicMock()
        await query_mod._execute_query_async("SELECT 1", backend, provider, use_ascii=True)
        backend.connect.assert_awaited_once_with()
        provider.display_query.assert_called_once_with("SELECT 1", is_async=True)
        provider.display_results.assert_called_once_with([{"b": 2}], use_ascii=True)
        backend.disconnect.assert_awaited_once_with()
        provider.display_disconnect.assert_called_once_with(is_async=True)

    async def test_no_result_object(self):
        backend = AsyncMock()
        backend._connection = False
        backend.execute.return_value = None
        provider = MagicMock()
        await query_mod._execute_query_async("SELECT 1", backend, provider)
        provider.display_no_result_object.assert_called_once_with()

    async def test_connection_error_exits(self):
        backend = AsyncMock()
        backend._connection = False
        backend.connect.side_effect = ConnectionError("down")
        provider = MagicMock()
        with pytest.raises(SystemExit):
            await query_mod._execute_query_async("SELECT 1", backend, provider)
        provider.display_connection_error.assert_called_once()

    async def test_unexpected_error_exits(self):
        backend = AsyncMock()
        backend._connection = False
        backend.execute.side_effect = RuntimeError("boom")
        provider = MagicMock()
        with pytest.raises(SystemExit):
            await query_mod._execute_query_async("SELECT 1", backend, provider)
        provider.display_unexpected_error.assert_called_once_with(
            backend.execute.side_effect, is_async=True
        )


# ===========================================================================
# rich rendering (fake rich modules -- no optional dependency required)
# ===========================================================================


class _FakeTable:
    def __init__(self, *args, **kwargs):
        self.columns = []
        self.rows = []

    def add_column(self, *args, **kwargs):
        self.columns.append(args)

    def add_row(self, *args, **kwargs):
        self.rows.append(args)


class _FakeConsole:
    printed = []

    def __init__(self, *args, **kwargs):
        pass

    def print(self, *args, **kwargs):
        for item in args:
            if isinstance(item, _FakeTable):
                for names in item.columns:
                    _FakeConsole.printed.extend(str(name) for name in names)
                for row in item.rows:
                    _FakeConsole.printed.extend(str(value) for value in row)
            else:
                _FakeConsole.printed.append(str(item))


@pytest.fixture
def fake_rich():
    console_mod = types.ModuleType("rich.console")
    console_mod.Console = _FakeConsole
    table_mod = types.ModuleType("rich.table")
    table_mod.Table = _FakeTable
    rich_mod = types.ModuleType("rich")
    rich_mod.console = console_mod
    rich_mod.table = table_mod
    with patch.dict(
        sys.modules, {"rich": rich_mod, "rich.console": console_mod, "rich.table": table_mod}
    ):
        _FakeConsole.printed = []
        yield _FakeConsole


def _full_overview():
    overview = ServerOverview(server_version="16.2", server_vendor="PostgreSQL")
    overview.session = SessionInfo(
        user="bob",
        database="db",
        schema="public",
        host="h",
        ssl_enabled=True,
        ssl_version="TLS1.3",
        ssl_cipher="AES",
        password_used=True,
    )
    overview.connections = ConnectionInfo(active_count=5, max_connections=100)
    overview.storage = StorageInfo(total_size_bytes=1024**3)
    overview.configuration = [
        StatusItem(
            "shared_buffers",
            "128MB",
            StatusCategory.CONFIGURATION,
            description="buffer cache",
            is_readonly=True,
        ),
        StatusItem("work_mem", "4MB", StatusCategory.PERFORMANCE, unit="MB"),
    ]
    overview.databases = [
        DatabaseBriefInfo(
            name="db",
            size_bytes=2048,
            table_count=3,
            view_count=1,
            extra={"schemas": {"public": {"tables": 2, "views": 1}}},
        )
    ]
    overview.users = [UserInfo(name="bob", is_superuser=True)]
    overview.wal = SimpleNamespace(
        wal_level="replica",
        wal_files=2,
        wal_size_bytes=4096,
        checkpoint_count=7,
        checkpoint_time="now",
        wal_segments=3,
    )
    overview.replication = SimpleNamespace(
        is_primary=True,
        replication_slots=1,
        active_replications=2,
        streaming=True,
        lag_bytes=512,
    )
    overview.archive = SimpleNamespace(
        archive_mode="on",
        archive_command="x" * 60,
        archive_timeout=30,
        archived_count=4,
        failed_count=0,
    )
    overview.security = SimpleNamespace(
        ssl_enabled=True,
        ssl_cert_file="c",
        ssl_key_file="k",
        ssl_ca_file="ca",
        password_encryption="scram",
        row_security=False,
    )
    overview.extensions = [SimpleNamespace(name="pgcrypto", version="1.3", schema="public")]
    return overview


class TestStatusRichDisplay:
    def test_all_sections_render(self, fake_rich):
        status_mod._display_status_rich(_full_overview(), verbose=1)
        out = " ".join(fake_rich.printed)
        for label in (
            "Server Status",
            "Session",
            "Configuration",
            "Performance",
            "Storage",
            "Databases",
            "Schemas in db",
            "Users",
            "WAL",
            "Replication",
            "Archive",
            "Security",
            "Extensions",
        ):
            assert label in out
        assert "1.0 GB" in out
        assert "..." in out  # the long archive command is truncated

    def test_non_verbose_paths(self, fake_rich):
        status_mod._display_status_rich(_full_overview(), verbose=0)
        out = " ".join(fake_rich.printed)
        assert "buffer cache" not in out
        assert "16.2" in out

    def test_optional_sections_absent(self, fake_rich):
        overview = ServerOverview(server_version="16", server_vendor="PostgreSQL")
        status_mod._display_status_rich(overview, verbose=2)
        out = " ".join(fake_rich.printed)
        assert "Server Status" in out
        assert "Session" not in out
        assert "WAL" not in out


class TestInfoRichDisplay:
    def _legacy_info(self):
        return {
            "postgresql": {"version": "16.0"},
            "extensions": {
                "pgcrypto": SimpleNamespace(
                    installed=True, available=True, version="1.3", schema="public"
                ),
                "hstore": SimpleNamespace(
                    installed=False, available=True, version="1.4", schema=None
                ),
            },
            "protocols": {
                "Query Features": {
                    "ProtoA": {
                        "percentage": 100.0,
                        "supported": 1,
                        "total": 1,
                        "methods": {
                            "supports_x": True,
                            "supports_arg_probe": {"args": {"TEXT": True, "YAML": False}},
                        },
                    }
                },
                "PostgreSQL Native": {"ProtoB": {"percentage": 0.0, "supported": 0, "total": 2}},
            },
        }

    def test_connected_detailed_render(self, fake_rich):
        extensions = self._legacy_info()["extensions"]
        info_mod._display_info_rich(self._legacy_info(), 2, "16.0", extensions, is_connected=True)
        out = " ".join(fake_rich.printed)
        for label in (
            "Environment Information",
            "actual connection",
            "Extension Support",
            "Installed",
            "Available (not installed)",
            "Protocol Support",
            "Detailed",
            "dialect-specific",
            "ProtoA",
            "ProtoB",
            "arg probe",
        ):
            assert label in out

    def test_disconnected_family_overview(self, fake_rich):
        info_mod._display_info_rich(self._legacy_info(), 0, "13.0.0", None, is_connected=False)
        out = " ".join(fake_rich.printed)
        assert "default value" in out
        assert "Family Overview" in out


# ===========================================================================
# introspect: remaining per-object and error branches
# ===========================================================================


class TestIntrospectExtraBranches:
    @pytest.mark.parametrize(
        "kind,name,method,title",
        [
            ("views", "v", "get_view_info", "View v"),
            ("materialized-views", "mv", "get_materialized_view_info", "Materialized View mv"),
            ("sequences", "s", "get_sequence_info", "Sequence s"),
        ],
    )
    def test_named_object_found(self, kind, name, method, title):
        backend = MagicMock()
        backend._connection = False
        getattr(backend.introspector, method).return_value = {"name": name}
        provider = MagicMock()
        introspect_mod._handle_introspect_sync(_args(type=kind, name=name), backend, provider)
        provider.display_results.assert_called_once_with([{"name": name}], title=title)

    def test_named_extension_found(self):
        backend = MagicMock()
        backend._connection = False
        backend.introspector.get_extension_info.return_value = ExtensionInfo(
            name="pgcrypto", version="1.3", schema="public", description="crypto"
        )
        provider = MagicMock()
        introspect_mod._handle_introspect_sync(
            _args(type="extensions", name="pgcrypto"), backend, provider
        )
        provider.display_results.assert_called_once_with(
            [{"name": "pgcrypto", "version": "1.3", "schema": "public", "description": "crypto"}],
            title="Extension pgcrypto",
        )

    def test_table_storage_found(self):
        backend = MagicMock()
        backend._connection = False
        backend.introspector.list_tables.return_value = [
            DatabaseBriefInfo(name="other"),
            DatabaseBriefInfo(name="orders", size_bytes=128),
        ]
        provider = MagicMock()
        introspect_mod._handle_introspect_sync(
            _args(type="table-storage", name="orders"), backend, provider
        )
        provider.display_results.assert_called_once()
        assert provider.display_results.call_args.kwargs["title"] == "Table storage orders"

    def test_table_storage_not_found_exits(self):
        backend = MagicMock()
        backend._connection = False
        backend.introspector.list_tables.return_value = [DatabaseBriefInfo(name="other")]
        with pytest.raises(SystemExit) as exc:
            introspect_mod._handle_introspect_sync(
                _args(type="table-storage", name="orders"), backend, MagicMock()
            )
        assert exc.value.code == 1

    def test_dependencies_with_name(self):
        backend = MagicMock()
        backend._connection = False
        backend.introspector.list_object_dependencies.return_value = [{"edge": 1}]
        provider = MagicMock()
        introspect_mod._handle_introspect_sync(
            _args(type="dependencies", name="orders"), backend, provider
        )
        provider.display_results.assert_called_once_with(
            [{"edge": 1}], title="Dependencies of orders"
        )

    def test_join_coverage_without_measurement_exits(self):
        backend = MagicMock()
        backend._connection = False
        backend.introspector.estimate_join_coverage.return_value = None
        args = _args(type="join-coverage", name="a", column="x", right_table="b", right_column="y")
        with pytest.raises(SystemExit) as exc:
            introspect_mod._handle_introspect_sync(args, backend, MagicMock())
        assert exc.value.code == 1

    def test_query_error_exits(self):
        backend = MagicMock()
        backend._connection = False
        backend.introspector.list_tables.side_effect = QueryError("bad")
        provider = MagicMock()
        with pytest.raises(SystemExit):
            introspect_mod._handle_introspect_sync(_args(type="tables"), backend, provider)
        provider.display_query_error.assert_called_once()

    def test_unexpected_error_exits(self, capsys):
        backend = MagicMock()
        backend._connection = False
        backend.introspector.list_tables.side_effect = RuntimeError("boom")
        with pytest.raises(SystemExit):
            introspect_mod._handle_introspect_sync(_args(type="tables"), backend, MagicMock())
        assert "Error during introspection" in capsys.readouterr().err

    async def test_named_object_found_async(self):
        backend = AsyncMock()
        backend._connection = False
        backend.dialect = None
        backend.introspector = MagicMock(get_view_info=AsyncMock(return_value={"name": "v"}))
        provider = MagicMock()
        await introspect_mod._handle_introspect_async(
            _args(type="views", name="v"), backend, provider
        )
        provider.display_results.assert_called_once_with([{"name": "v"}], title="View v")

    async def test_table_storage_async(self):
        backend = AsyncMock()
        backend._connection = False
        backend.dialect = None
        backend.introspector = MagicMock(
            list_tables=AsyncMock(return_value=[DatabaseBriefInfo(name="orders", size_bytes=8)])
        )
        provider = MagicMock()
        await introspect_mod._handle_introspect_async(
            _args(type="table-storage", name="orders"), backend, provider
        )
        assert provider.display_results.call_args.kwargs["title"] == "Table storage orders"

    async def test_join_coverage_without_measurement_async(self):
        backend = AsyncMock()
        backend._connection = False
        backend.dialect = None
        backend.introspector = MagicMock(estimate_join_coverage=AsyncMock(return_value=None))
        args = _args(type="join-coverage", name="a", column="x", right_table="b", right_column="y")
        with pytest.raises(SystemExit):
            await introspect_mod._handle_introspect_async(args, backend, MagicMock())

    async def test_query_error_async(self):
        backend = AsyncMock()
        backend._connection = False
        backend.dialect = None
        backend.introspector = MagicMock(list_tables=AsyncMock(side_effect=QueryError("bad")))
        provider = MagicMock()
        with pytest.raises(SystemExit):
            await introspect_mod._handle_introspect_async(_args(type="tables"), backend, provider)
        provider.display_query_error.assert_called_once()
