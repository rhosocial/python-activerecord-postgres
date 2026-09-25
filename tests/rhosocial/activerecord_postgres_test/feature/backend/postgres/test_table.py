from unittest.mock import patch

import pytest

from rhosocial.activerecord.backend.dialect.exceptions import UnsupportedFeatureError
from rhosocial.activerecord.backend.expression import (
    ColumnDefinition,
    CreateTableExpression,
)
from rhosocial.activerecord.backend.expression.types import IntegerType
from rhosocial.activerecord.backend.impl.postgres.dialect import PostgresDialect
from rhosocial.activerecord.backend.impl.postgres.mixins.ddl_table import PostgresTableMixin


class TestTableSupport:
    """Test PostgresTableMixin feature detection."""

    def test_supports_if_not_exists_table_pg95(self):
        assert PostgresDialect((9, 5, 0)).supports_if_not_exists_table() is True

    def test_supports_if_not_exists_table_pg94(self):
        assert PostgresDialect((9, 4, 0)).supports_if_not_exists_table() is False

    def test_supports_if_exists_table(self):
        assert PostgresDialect().supports_if_exists_table() is True

    def test_supports_temporary_table(self):
        assert PostgresDialect().supports_temporary_table() is True

    def test_supports_table_inheritance(self):
        assert PostgresDialect().supports_table_inheritance() is True

    def test_supports_table_tablespace(self):
        assert PostgresDialect().supports_table_tablespace() is True


class TestPostgresTableMixinDirect:
    """Test PostgresTableMixin directly (not through PostgresDialect)."""

    class _Host:
        version = (15, 0, 0)

    class _LowHost:
        version = (9, 4, 0)

    class _TableMixin(_Host, PostgresTableMixin):
        pass

    class _TableMixinLow(_LowHost, PostgresTableMixin):
        pass

    def test_supports_if_not_exists_table_direct(self):
        assert self._TableMixin().supports_if_not_exists_table() is True

    def test_supports_if_not_exists_table_low(self):
        assert not self._TableMixinLow().supports_if_not_exists_table()

    def test_supports_if_exists_table_direct(self):
        assert self._TableMixin().supports_if_exists_table() is True

    def test_supports_temporary_table_direct(self):
        assert self._TableMixin().supports_temporary_table() is True

    def test_supports_table_inheritance_direct(self):
        assert self._TableMixin().supports_table_inheritance() is True

    def test_supports_table_tablespace_direct(self):
        assert self._TableMixin().supports_table_tablespace() is True


class TestPostgresTableDDLDeclarations:
    @staticmethod
    def _expression(dialect, table="child", *, inherits=None, tablespace=None):
        return CreateTableExpression(
            dialect,
            table,
            [ColumnDefinition(dialect, "id", IntegerType(dialect))],
            inherits=inherits,
            tablespace=tablespace,
        )

    def test_table_declaration_defaults_are_absent(self):
        expression = self._expression(PostgresDialect(version=(16, 0, 0)))
        assert expression.inherits == []
        assert expression.tablespace is None

    def test_table_inherits_is_carried_and_rendered(self):
        dialect = PostgresDialect(version=(16, 0, 0))
        expression = self._expression(
            dialect, inherits=["parent_a", "parent_b"]
        )
        assert expression.inherits == ["parent_a", "parent_b"]
        sql, _ = expression.to_sql()
        assert 'INHERITS ("parent_a", "parent_b")' in sql

    def test_table_tablespace_is_carried_and_rendered(self):
        dialect = PostgresDialect(version=(16, 0, 0))
        expression = self._expression(dialect, tablespace="ts_data")
        assert expression.tablespace == "ts_data"
        sql, _ = expression.to_sql()
        assert 'TABLESPACE "ts_data"' in sql

    def test_table_inherits_fails_fast_when_capability_disabled(self):
        dialect = PostgresDialect(version=(16, 0, 0))
        with patch.object(dialect, "supports_table_inheritance", return_value=False):
            with pytest.raises(UnsupportedFeatureError, match="INHERITS"):
                self._expression(
                    dialect, inherits=["parent_a", "parent_b"]
                ).to_sql()

    def test_table_tablespace_fails_fast_when_capability_disabled(self):
        dialect = PostgresDialect(version=(16, 0, 0))
        with patch.object(dialect, "supports_table_tablespace", return_value=False):
            with pytest.raises(UnsupportedFeatureError, match="TABLESPACE"):
                self._expression(dialect, tablespace="ts_data").to_sql()
