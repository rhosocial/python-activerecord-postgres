# tests/rhosocial/activerecord_postgres_test/feature/backend/dialect/test_dql_capabilities.py
"""PostgreSQL DQL capability tests: WITH TIES (PG13+) and NULLS ordering."""

import pytest

from rhosocial.activerecord.backend.expression import (
    Column,
    LimitOffsetClause,
    OrderByClause,
    OrderByExpression,
)
from rhosocial.activerecord.backend.impl.postgres.dialect import PostgresDialect


def test_fetch_with_ties_pre_13():
    dialect = PostgresDialect(version=(12, 0, 0))
    assert dialect.supports_fetch_with_ties() is False


def test_fetch_with_ties_at_13():
    dialect = PostgresDialect(version=(13, 0, 0))
    assert dialect.supports_fetch_with_ties() is True
    sql, _ = dialect.format_limit_offset_clause(LimitOffsetClause(dialect, limit=5, with_ties=True))
    assert "WITH TIES" in sql


def test_nulls_first_last_supported():
    dialect = PostgresDialect(version=(17, 0, 0))
    assert dialect.supports_nulls_first_last() is True
    sql, _ = OrderByClause(
        dialect,
        expressions=[OrderByExpression(dialect, Column(dialect, "name"), nulls_last=True)],
    ).to_sql()
    assert "NULLS LAST" in sql
