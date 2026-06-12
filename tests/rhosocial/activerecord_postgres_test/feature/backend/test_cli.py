# tests/rhosocial/activerecord_postgres_test/feature/backend/test_cli.py
"""
Tests for PostgreSQL backend CLI.

Tests argument parsing for named-expression and named-procedure subcommands.
"""

import asyncio
import sys
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch


class TestCLINamedExpressionArgs:
    """Tests for named-expression subcommand argument parsing."""

    def test_parse_args_named_expression_basic(self):
        """Test basic named-expression parsing."""
        from rhosocial.activerecord.backend.impl.postgres.__main__ import parse_args

        with patch.object(sys, 'argv', ['postgres', 'named-expression', 'myapp.queries.test']):
            args = parse_args()

            assert args.command == 'named-expression'
            assert args.qualified_name == 'myapp.queries.test'

    def test_parse_args_named_expression_with_params(self):
        """Test named-expression with parameters."""
        from rhosocial.activerecord.backend.impl.postgres.__main__ import parse_args

        with patch.object(sys, 'argv', [
            'postgres', 'named-expression', 'myapp.queries.test',
            '--param', 'limit=50',
            '--param', 'status=active',
        ]):
            args = parse_args()

            assert args.command == 'named-expression'
            assert args.params == ['limit=50', 'status=active']

    def test_parse_args_named_expression_dry_run(self):
        """Test named-expression with --dry-run."""
        from rhosocial.activerecord.backend.impl.postgres.__main__ import parse_args

        with patch.object(sys, 'argv', ['postgres', 'named-expression', 'myapp.queries.test', '--dry-run']):
            args = parse_args()

            assert args.command == 'named-expression'
            assert args.dry_run is True

    def test_parse_args_named_expression_describe(self):
        """Test named-expression with --describe."""
        from rhosocial.activerecord.backend.impl.postgres.__main__ import parse_args

        with patch.object(sys, 'argv', ['postgres', 'named-expression', 'myapp.queries.test', '--describe']):
            args = parse_args()

            assert args.command == 'named-expression'
            assert args.describe is True

    def test_parse_args_named_expression_list(self):
        """Test named-expression with --list."""
        from rhosocial.activerecord.backend.impl.postgres.__main__ import parse_args

        with patch.object(sys, 'argv', ['postgres', 'named-expression', 'myapp.queries', '--list']):
            args = parse_args()

            assert args.command == 'named-expression'
            assert args.list_queries is True

    def test_parse_args_named_expression_async(self):
        """Test named-expression with --async."""
        from rhosocial.activerecord.backend.impl.postgres.__main__ import parse_args

        with patch.object(sys, 'argv', ['postgres', 'named-expression', 'myapp.queries.test', '--async']):
            args = parse_args()

            assert args.command == 'named-expression'
            assert args.is_async is True

    def test_parse_args_named_expression_dialect_version(self):
        from rhosocial.activerecord.backend.impl.postgres.__main__ import parse_args

        with patch.object(sys, 'argv', [
            'postgres', 'named-expression', 'myapp.queries.test',
            '--dialect-version', '15.2.1',
        ]):
            args = parse_args()

        assert args.command == 'named-expression'
        assert args.dialect_version == '15.2.1'


class TestCLINamedExpressionAdapter:
    def _args(self, **overrides):
        values = {
            'output': 'table',
            'rich_ascii': False,
            'is_async': False,
            'dialect_version': None,
            'host': 'localhost',
            'port': 5432,
            'database': 'test_db',
            'user': 'postgres',
            'password': '',
            'named_connection': None,
            'connection_params': [],
        }
        values.update(overrides)
        return SimpleNamespace(**values)

    def test_handle_sync_wires_backend_provider_and_dialect_version(self):
        from rhosocial.activerecord.backend.impl.postgres.cli import named_expression

        captured = {}
        backend = MagicMock()
        backend._connection = object()
        backend.dialect = object()

        def fake_handle(args, provider, **kwargs):
            captured['args'] = args
            captured['provider'] = provider
            captured.update(kwargs)

        args = self._args(dialect_version='15.2.1')

        with patch.object(
            named_expression, 'create_provider', return_value='provider'
        ) as create_provider, patch.object(
            named_expression,
            'resolve_connection_config_from_args',
            return_value='config',
        ) as resolve_config, patch.object(
            named_expression, 'PostgresBackend', return_value=backend
        ) as backend_cls, patch(
            'rhosocial.activerecord.backend.named_expression.cli.handle_named_expression',
            side_effect=fake_handle,
        ):
            named_expression.handle(args)
            create_provider.assert_called_once_with('table', ascii_borders=False)
            resolved_backend = captured['backend_factory']()
            assert resolved_backend is backend
            resolve_config.assert_called_once_with(args)
            backend_cls.assert_called_once_with(connection_config='config')
            backend.connect.assert_called_once_with()
            backend.introspect_and_adapt.assert_called_once_with()
            assert backend._dialect.version == (15, 2, 1)
            assert captured['get_dialect'](backend) is backend.dialect

            captured['execute_query']('SELECT 1', (), 'dql')
            backend.execute.assert_called_once()

            captured['disconnect']()
            backend.disconnect.assert_called_once_with()

    def test_handle_sync_creates_standalone_dialect_for_dry_run_paths(self):
        from rhosocial.activerecord.backend.impl.postgres.cli import named_expression

        captured = {}

        def fake_handle(args, provider, **kwargs):
            captured.update(kwargs)

        args = self._args(dialect_version='14.0.0')

        with patch.object(
            named_expression, 'create_provider', return_value='provider'
        ), patch(
            'rhosocial.activerecord.backend.named_expression.cli.handle_named_expression',
            side_effect=fake_handle,
        ):
            named_expression.handle(args)

        assert captured['create_dialect']().version == (14, 0, 0)

    def test_handle_async_wires_async_callbacks(self):
        from rhosocial.activerecord.backend.impl.postgres.cli import named_expression

        captured = {}
        async_backend = MagicMock()
        async_backend._connection = object()
        async_backend.dialect = object()
        async_backend.execute = AsyncMock(return_value='result')
        async_backend.disconnect = AsyncMock()

        def fake_handle(args, provider, **kwargs):
            captured.update(kwargs)

        args = self._args(is_async=True)

        with patch.object(
            named_expression, 'create_provider', return_value='provider'
        ), patch.object(
            named_expression,
            'resolve_connection_config_from_args',
            return_value='config',
        ), patch.object(
            named_expression, 'AsyncPostgresBackend', return_value=async_backend
        ) as backend_cls, patch(
            'rhosocial.activerecord.backend.named_expression.cli.handle_named_expression',
            side_effect=fake_handle,
        ):
            named_expression.handle(args)
            resolved_backend = captured['backend_async_factory']()
            assert resolved_backend is async_backend
            backend_cls.assert_called_once_with(connection_config='config')
            dialect = asyncio.run(captured['get_dialect_async'](async_backend))
            assert dialect is async_backend.dialect
            result = asyncio.run(
                captured['execute_query_async']('SELECT 1', (), 'dql')
            )
            assert result == 'result'
            asyncio.run(captured['disconnect_async']())
            async_backend.disconnect.assert_awaited_once_with()


class TestCLINamedProcedureArgs:
    """Tests for named-procedure subcommand argument parsing."""

    def test_parse_args_named_procedure_basic(self):
        """Test basic named-procedure parsing."""
        from rhosocial.activerecord.backend.impl.postgres.__main__ import parse_args

        with patch.object(sys, 'argv', ['postgres', 'named-procedure', 'myapp.procedures.monthly_cleanup']):
            args = parse_args()

            assert args.command == 'named-procedure'
            assert args.qualified_name == 'myapp.procedures.monthly_cleanup'

    def test_parse_args_named_procedure_with_params(self):
        """Test named-procedure with parameters."""
        from rhosocial.activerecord.backend.impl.postgres.__main__ import parse_args

        with patch.object(sys, 'argv', [
            'postgres', 'named-procedure', 'myapp.procedures.monthly_cleanup',
            '--param', 'month=2026-03',
        ]):
            args = parse_args()

            assert args.command == 'named-procedure'
            assert args.params == ['month=2026-03']

    def test_parse_args_named_procedure_transaction_auto(self):
        """Test named-procedure with --transaction auto."""
        from rhosocial.activerecord.backend.impl.postgres.__main__ import parse_args

        with patch.object(sys, 'argv', ['postgres', 'named-procedure', 'myapp.procedures.test', '--transaction', 'auto']):  # noqa: E501
            args = parse_args()

            assert args.command == 'named-procedure'
            assert args.transaction == 'auto'

    def test_parse_args_named_procedure_transaction_step(self):
        """Test named-procedure with --transaction step."""
        from rhosocial.activerecord.backend.impl.postgres.__main__ import parse_args

        with patch.object(sys, 'argv', ['postgres', 'named-procedure', 'myapp.procedures.test', '--transaction', 'step']):  # noqa: E501
            args = parse_args()

            assert args.command == 'named-procedure'
            assert args.transaction == 'step'

    def test_parse_args_named_procedure_transaction_none(self):
        """Test named-procedure with --transaction none."""
        from rhosocial.activerecord.backend.impl.postgres.__main__ import parse_args

        with patch.object(sys, 'argv', ['postgres', 'named-procedure', 'myapp.procedures.test', '--transaction', 'none']):  # noqa: E501
            args = parse_args()

            assert args.command == 'named-procedure'
            assert args.transaction == 'none'

    def test_parse_args_named_procedure_dry_run(self):
        """Test named-procedure with --dry-run."""
        from rhosocial.activerecord.backend.impl.postgres.__main__ import parse_args

        with patch.object(sys, 'argv', ['postgres', 'named-procedure', 'myapp.procedures.test', '--dry-run']):
            args = parse_args()

            assert args.command == 'named-procedure'
            assert args.dry_run is True

    def test_parse_args_named_procedure_describe(self):
        """Test named-procedure with --describe."""
        from rhosocial.activerecord.backend.impl.postgres.__main__ import parse_args

        with patch.object(sys, 'argv', ['postgres', 'named-procedure', 'myapp.procedures.test', '--describe']):
            args = parse_args()

            assert args.command == 'named-procedure'
            assert args.describe is True

    def test_parse_args_named_procedure_list(self):
        """Test named-procedure with --list."""
        from rhosocial.activerecord.backend.impl.postgres.__main__ import parse_args

        with patch.object(sys, 'argv', ['postgres', 'named-procedure', 'myapp.procedures', '--list']):
            args = parse_args()

            assert args.command == 'named-procedure'
            assert args.list_procedures is True

    def test_parse_args_named_procedure_async(self):
        """Test named-procedure with --async."""
        from rhosocial.activerecord.backend.impl.postgres.__main__ import parse_args

        with patch.object(sys, 'argv', ['postgres', 'named-procedure', 'myapp.procedures.test', '--async']):
            args = parse_args()

            assert args.command == 'named-procedure'
            assert args.is_async is True


class TestCLINamedProcedureGraphArgs:
    def test_parse_args_named_procedure_graph_run(self):
        from rhosocial.activerecord.backend.impl.postgres.__main__ import parse_args

        with patch.object(sys, 'argv', [
            'postgres', 'named-procedure-graph', 'myapp.graphs.monthly_report',
            '--params', '{"month":"2026-04"}',
        ]):
            args = parse_args()

        assert args.command == 'named-procedure-graph'
        assert args.qualified_name == 'myapp.graphs.monthly_report'
        assert args.params_json == '{"month":"2026-04"}'

    def test_parse_args_named_procedure_graph_list(self):
        from rhosocial.activerecord.backend.impl.postgres.__main__ import parse_args

        with patch.object(sys, 'argv', [
            'postgres', 'named-procedure-graph', 'myapp.graphs', '--list',
        ]):
            args = parse_args()

        assert args.command == 'named-procedure-graph'
        assert args.qualified_name == 'myapp.graphs'
        assert args.list_procedure_graphs is True

    def test_parse_args_named_procedure_graph_validate_async(self):
        from rhosocial.activerecord.backend.impl.postgres.__main__ import parse_args

        with patch.object(sys, 'argv', [
            'postgres', 'named-procedure-graph', 'myapp.graphs.monthly_report',
            '--validate', '--async',
        ]):
            args = parse_args()

        assert args.command == 'named-procedure-graph'
        assert args.qualified_name == 'myapp.graphs.monthly_report'
        assert args.validate is True
        assert args.is_async is True


class TestCLINamedProcedureGraphAdapter:
    def _args(self, **overrides):
        values = {
            'output': 'table',
            'rich_ascii': True,
            'is_async': False,
            'host': 'localhost',
            'port': 5432,
            'database': 'test_db',
            'user': 'postgres',
            'password': '',
            'named_connection': None,
            'connection_params': [],
        }
        values.update(overrides)
        return SimpleNamespace(**values)

    def test_handle_sync_wires_backend_and_disconnect(self):
        from rhosocial.activerecord.backend.impl.postgres.cli import named_procedure_graph

        captured = {}
        backend = MagicMock()
        backend._connection = object()

        def fake_handle(args, provider, **kwargs):
            captured['args'] = args
            captured['provider'] = provider
            captured.update(kwargs)

        args = self._args()

        with patch.object(
            named_procedure_graph, 'create_provider', return_value='provider'
        ) as create_provider, patch.object(
            named_procedure_graph,
            'resolve_connection_config_from_args',
            return_value='config',
        ) as resolve_config, patch.object(
            named_procedure_graph, 'PostgresBackend', return_value=backend
        ) as backend_cls, patch(
            'rhosocial.activerecord.backend.named_expression.cli_procedure_graph.'
            'handle_named_procedure_graph',
            side_effect=fake_handle,
        ):
            named_procedure_graph.handle(args)
            create_provider.assert_called_once_with('table', ascii_borders=True)
            resolved_backend = captured['backend_factory']()
            assert resolved_backend is backend
            resolve_config.assert_called_once_with(args)
            backend_cls.assert_called_once_with(connection_config='config')
            backend.connect.assert_called_once_with()
            backend.introspect_and_adapt.assert_called_once_with()

            captured['disconnect']()
            backend.disconnect.assert_called_once_with()

    def test_handle_async_wires_async_backend_and_disconnect(self):
        from rhosocial.activerecord.backend.impl.postgres.cli import named_procedure_graph

        captured = {}
        async_backend = MagicMock()
        async_backend._connection = object()
        async_backend.disconnect = AsyncMock()

        def fake_handle(args, provider, **kwargs):
            captured.update(kwargs)

        args = self._args(is_async=True)

        with patch.object(
            named_procedure_graph, 'create_provider', return_value='provider'
        ), patch.object(
            named_procedure_graph,
            'resolve_connection_config_from_args',
            return_value='config',
        ), patch(
            'rhosocial.activerecord.backend.impl.postgres.AsyncPostgresBackend',
            return_value=async_backend,
        ) as backend_cls, patch(
            'rhosocial.activerecord.backend.named_expression.cli_procedure_graph.'
            'handle_named_procedure_graph',
            side_effect=fake_handle,
        ):
            named_procedure_graph.handle(args)
            resolved_backend = captured['backend_async_factory']()
            assert resolved_backend is async_backend
            backend_cls.assert_called_once_with(connection_config='config')
            asyncio.run(captured['disconnect_async']())
            async_backend.disconnect.assert_awaited_once_with()
