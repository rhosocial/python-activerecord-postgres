# tests/rhosocial/activerecord_postgres_test/feature/backend/test_cli.py
"""
Tests for PostgreSQL backend CLI.

Tests argument parsing for named-expression and named-procedure subcommands.
"""

import pytest
import sys
from unittest.mock import patch


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

        with patch.object(sys, 'argv', ['postgres', 'named-procedure', 'myapp.procedures.test', '--transaction', 'auto']):
            args = parse_args()

            assert args.command == 'named-procedure'
            assert args.transaction == 'auto'

    def test_parse_args_named_procedure_transaction_step(self):
        """Test named-procedure with --transaction step."""
        from rhosocial.activerecord.backend.impl.postgres.__main__ import parse_args

        with patch.object(sys, 'argv', ['postgres', 'named-procedure', 'myapp.procedures.test', '--transaction', 'step']):
            args = parse_args()

            assert args.command == 'named-procedure'
            assert args.transaction == 'step'

    def test_parse_args_named_procedure_transaction_none(self):
        """Test named-procedure with --transaction none."""
        from rhosocial.activerecord.backend.impl.postgres.__main__ import parse_args

        with patch.object(sys, 'argv', ['postgres', 'named-procedure', 'myapp.procedures.test', '--transaction', 'none']):
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
