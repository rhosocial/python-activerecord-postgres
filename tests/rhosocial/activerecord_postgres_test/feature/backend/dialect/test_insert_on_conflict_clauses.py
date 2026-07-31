# tests/rhosocial/activerecord_postgres_test/feature/backend/dialect/test_insert_on_conflict_clauses.py
"""Tests for PostgreSQL ON CONFLICT clause capability and rendering.

Covers:
- Capability switches: single clause supported, multiple clauses rejected.
- Single ON CONFLICT DO NOTHING / DO UPDATE rendering (incl. EXCLUDED).
- Multiple ON CONFLICT clauses rejected by the generic gate.
"""

import pytest

from rhosocial.activerecord.backend.dialect import UnsupportedFeatureError
from rhosocial.activerecord.backend.expression import (
    Column,
    InsertExpression,
    Literal,
    OnConflictClause,
    ValuesSource,
)
from rhosocial.activerecord.backend.impl.postgres.dialect import PostgresDialect


@pytest.fixture
def dialect():
    return PostgresDialect(version=(15, 0, 0))


class TestPostgresOnConflictCapabilities:
    """Capability switch tests."""

    def test_supports_on_conflict_clause(self, dialect):
        assert dialect.supports_on_conflict_clause() is True

    def test_does_not_support_multiple_on_conflict_clauses(self, dialect):
        assert dialect.supports_multiple_on_conflict_clauses() is False

    def test_multiple_on_conflict_clauses_rejected(self, dialect):
        """PostgreSQL grammar allows only one ON CONFLICT clause per INSERT."""
        source = ValuesSource(dialect, values_list=[[Literal(dialect, 1)]])
        clause1 = OnConflictClause(dialect, conflict_target=["col_a"], do_nothing=True)
        clause2 = OnConflictClause(dialect, conflict_target=["col_b"], do_nothing=True)
        expr = InsertExpression(dialect, into="t", source=source, on_conflict=[clause1, clause2])

        with pytest.raises(UnsupportedFeatureError, match="multiple ON CONFLICT clauses"):
            expr.to_sql()


class TestPostgresOnConflictRendering:
    """SQL rendering tests for a single ON CONFLICT clause."""

    def test_do_nothing(self, dialect):
        source = ValuesSource(dialect, values_list=[[Literal(dialect, 1)]])
        clause = OnConflictClause(dialect, conflict_target=["id"], do_nothing=True)
        expr = InsertExpression(dialect, into="users", columns=["id"], source=source, on_conflict=clause)
        sql, params = expr.to_sql()
        assert sql == 'INSERT INTO "users" ("id") VALUES (%s) ON CONFLICT ("id") DO NOTHING'
        assert params == (1,)

    def test_do_update_with_excluded(self, dialect):
        """EXCLUDED pseudo-table must not be double-quoted in PostgreSQL."""
        source = ValuesSource(
            dialect, values_list=[[Literal(dialect, 1), Literal(dialect, "new_name")]]
        )
        clause = OnConflictClause(
            dialect,
            conflict_target=["id"],
            update_assignments={"name": Column(dialect, "name", "EXCLUDED")},
        )
        expr = InsertExpression(
            dialect, into="users", columns=["id", "name"], source=source, on_conflict=clause
        )
        sql, params = expr.to_sql()
        assert sql == (
            'INSERT INTO "users" ("id", "name") VALUES (%s, %s) '
            'ON CONFLICT ("id") DO UPDATE SET "name" = EXCLUDED."name"'
        )
        assert params == (1, "new_name")

    def test_do_update_with_where(self, dialect):
        source = ValuesSource(
            dialect, values_list=[[Literal(dialect, 1), Literal(dialect, 10)]]
        )
        clause = OnConflictClause(
            dialect,
            conflict_target=["id"],
            update_assignments={"qty": Column(dialect, "qty", "EXCLUDED")},
            update_where=Column(dialect, "qty", "users") > Column(dialect, "qty", "EXCLUDED"),
        )
        expr = InsertExpression(
            dialect, into="users", columns=["id", "qty"], source=source, on_conflict=clause
        )
        sql, params = expr.to_sql()
        assert sql == (
            'INSERT INTO "users" ("id", "qty") VALUES (%s, %s) '
            'ON CONFLICT ("id") DO UPDATE SET "qty" = EXCLUDED."qty" '
            'WHERE "users"."qty" > "EXCLUDED"."qty"'
        )
        assert params == (1, 10)
