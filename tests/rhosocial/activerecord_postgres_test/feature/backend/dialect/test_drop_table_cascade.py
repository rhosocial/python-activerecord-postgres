# tests/rhosocial/activerecord_postgres_test/feature/backend/dialect/test_drop_table_cascade.py
"""Tests for DROP TABLE ... CASCADE/RESTRICT rendering on PostgreSQL.

PostgreSQL declares full SQL-standard CASCADE/RESTRICT support; the generic
``TableMixin.format_drop_table_statement`` helper (optimistic True/True)
emits the correct token without backend override. These tests pin that
contract so a future override that drops support is caught early.
"""

import pytest

from rhosocial.activerecord.backend.expression import DropTableExpression
from rhosocial.activerecord.backend.impl.postgres.dialect import PostgresDialect


@pytest.fixture
def dialect():
    return PostgresDialect(version=(15, 0, 0))


class TestPostgresDropTableCascade:
    def test_capability_switches(self, dialect):
        assert dialect.supports_drop_table_cascade() is True
        assert dialect.supports_drop_table_restrict() is True

    def test_cascade_renders_standard_token(self, dialect):
        expr = DropTableExpression(dialect, table="users", cascade=True)
        sql, params = expr.to_sql()
        assert sql.endswith(" CASCADE")
        assert "CASCADE CONSTRAINTS" not in sql
        assert params == ()

    def test_restrict_renders_standard_token(self, dialect):
        expr = DropTableExpression(dialect, table="users", cascade=False)
        sql, params = expr.to_sql()
        assert sql.endswith(" RESTRICT")
        assert params == ()

    def test_cascade_none_omits_token(self, dialect):
        expr = DropTableExpression(dialect, table="users", cascade=None)
        sql, params = expr.to_sql()
        assert "CASCADE" not in sql
        assert "RESTRICT" not in sql
        assert params == ()

    def test_if_exists_combined_with_cascade(self, dialect):
        expr = DropTableExpression(
            dialect, table="users", if_exists=True, cascade=True
        )
        sql, params = expr.to_sql()
        assert sql.startswith("DROP TABLE IF EXISTS")
        assert sql.endswith(" CASCADE")
        assert params == ()
