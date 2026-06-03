# tests/rhosocial/activerecord_postgres_test/feature/query/test_collation_expression.py
"""
Tests for expression-level COLLATE support on PostgreSQL.
"""

import pytest

from rhosocial.activerecord.backend.expression import CollationName, Column, Literal
from rhosocial.activerecord.backend.impl.postgres import PostgresCollation, PostgresDialect


@pytest.fixture
def dialect():
    return PostgresDialect(version=(15, 0, 0))


class TestPostgresCollationExpression:
    def test_column_collate_generates_sql(self, dialect):
        expr = Column(dialect, "name", table="users").collate(PostgresCollation.C)

        sql, params = expr.to_sql()

        assert sql == '"users"."name" COLLATE "C"'
        assert params == ()

    def test_literal_collate_preserves_parameter_binding(self, dialect):
        expr = Literal(dialect, "Alice").collate(PostgresCollation.POSIX)

        sql, params = expr.to_sql()

        assert sql == '%s COLLATE "POSIX"'
        assert params == ("Alice",)

    def test_schema_qualified_collation_generates_sql(self, dialect):
        expr = Column(dialect, "name").collate(
            CollationName(PostgresCollation.C.value, schema="pg_catalog")
        )

        sql, params = expr.to_sql()

        assert sql == '"name" COLLATE "pg_catalog"."C"'
        assert params == ()

    def test_rejects_unsupported_collation(self, dialect):
        expr = Column(dialect, "name").collate("unknown_ci")

        with pytest.raises(ValueError, match="Unsupported PostgreSQL collation"):
            expr.to_sql()

    def test_rejects_version_sensitive_collation_on_older_version(self):
        dialect = PostgresDialect(version=(9, 6, 0))
        expr = Column(dialect, "name").collate(PostgresCollation.UND_X_ICU)

        with pytest.raises(ValueError, match="requires PostgreSQL 10.0"):
            expr.to_sql()

    def test_collate_executes_order_by(self, annotated_query_fixtures):
        SearchableItem = (
            annotated_query_fixtures[0]
            if isinstance(annotated_query_fixtures, tuple)
            else annotated_query_fixtures
        )
        SearchableItem(name="b", tags=[]).save()
        SearchableItem(name="A", tags=[]).save()
        SearchableItem(name="c", tags=[]).save()

        results = SearchableItem.query().order_by(
            SearchableItem.c.name.collate(PostgresCollation.C)
        ).all()

        assert [item.name for item in results] == ["A", "b", "c"]
