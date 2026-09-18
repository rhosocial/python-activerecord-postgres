# tests/rhosocial/activerecord_postgres_test/feature/backend/dialect/test_returning_capabilities.py
"""PostgreSQL RETURNING capability tests (incl. PG17 OLD/NEW references)."""

import pytest

from rhosocial.activerecord.backend.dialect.exceptions import UnsupportedFeatureError
from rhosocial.activerecord.backend.expression.core import Column
from rhosocial.activerecord.backend.expression.statements import ReturningClause
from rhosocial.activerecord.backend.impl.postgres.dialect import PostgresDialect

pytestmark = [pytest.mark.feature, pytest.mark.backend]


class TestPostgresReturningCapabilities:
    def test_dml_returning_supported(self):
        dialect = PostgresDialect(version=(14, 0, 0))
        assert dialect.supports_returning_insert() is True
        assert dialect.supports_returning_update() is True
        assert dialect.supports_returning_delete() is True

    def test_old_new_pre_17(self):
        dialect = PostgresDialect(version=(16, 0, 0))
        assert dialect.supports_returning_old_new() is False

    def test_old_new_at_17(self):
        dialect = PostgresDialect(version=(17, 0, 0))
        assert dialect.supports_returning_old_new() is True

    def test_defaults_for_generic_switches(self):
        dialect = PostgresDialect(version=(17, 0, 0))
        assert dialect.supports_returning_expressions() is True
        assert dialect.supports_returning_alias() is True
        assert dialect.supports_returning_wildcard() is True
        assert dialect.supports_returning_single_row() is False
        assert dialect.supports_returning_into() is False


class TestPostgresOldNewFormatting:
    def test_old_new_rejected_pre_17(self):
        dialect = PostgresDialect(version=(16, 0, 0))
        clause = ReturningClause(dialect, expressions=[Column(dialect, "name", table="OLD")])
        with pytest.raises(UnsupportedFeatureError, match="OLD/NEW in RETURNING"):
            dialect.format_returning_clause(clause)

    def test_old_new_allowed_at_17(self):
        dialect = PostgresDialect(version=(17, 0, 0))
        clause = ReturningClause(
            dialect,
            expressions=[
                Column(dialect, "name", table="OLD"),
                Column(dialect, "name", table="NEW"),
            ],
        )
        sql, _ = dialect.format_returning_clause(clause)
        assert '"OLD"."name"' in sql
        assert '"NEW"."name"' in sql
