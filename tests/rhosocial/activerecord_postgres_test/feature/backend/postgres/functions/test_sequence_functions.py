# tests/rhosocial/activerecord_postgres_test/feature/backend/postgres/functions/test_sequence_functions.py
"""
Tests for PostgreSQL Sequence functions.

Functions: nextval, currval, lastval, setval
"""

from rhosocial.activerecord.backend.impl.postgres.dialect import PostgresDialect
from rhosocial.activerecord.backend.impl.postgres.functions.sequence import (
    nextval,
    currval,
    lastval,
    setval,
)


class TestPostgresSequenceFunctions:
    """Tests for PostgreSQL sequence functions."""

    def test_nextval(self, postgres_dialect: PostgresDialect):
        """Test nextval() function."""
        result = nextval(postgres_dialect, "user_id_seq")
        assert result == "nextval(user_id_seq)"

    def test_nextval_expression(self, postgres_dialect: PostgresDialect):
        """Test nextval() with an expression argument."""
        sql, params = nextval(postgres_dialect, "user_id_seq").to_sql()
        assert sql == "NEXTVAL(%s)"
        assert params == ("user_id_seq",)

    def test_currval(self, postgres_dialect: PostgresDialect):
        """Test currval() function."""
        result = currval(postgres_dialect, "user_id_seq")
        assert result == "currval(user_id_seq)"

    def test_lastval(self, postgres_dialect: PostgresDialect):
        """Test lastval() function (no arguments)."""
        result = lastval(postgres_dialect)
        assert result == "lastval()"

    def test_setval_two_args(self, postgres_dialect: PostgresDialect):
        """Test setval() with two arguments."""
        result = setval(postgres_dialect, "user_id_seq", 1000)
        assert result == "setval(user_id_seq, 1000)"

    def test_setval_three_args(self, postgres_dialect: PostgresDialect):
        """Test setval() with three arguments (is_called)."""
        result = setval(postgres_dialect, "user_id_seq", 1000, False)
        assert result == "setval(user_id_seq, 1000, False)"

    def test_setval_schema_qualified(self, postgres_dialect: PostgresDialect):
        """Test setval() with a schema-qualified sequence name."""
        result = setval(postgres_dialect, "public.user_id_seq", 1000)
        assert result == "setval(public.user_id_seq, 1000)"

    def test_setval_is_called_true(self, postgres_dialect: PostgresDialect):
        """Test setval() with is_called=True."""
        result = setval(postgres_dialect, "user_id_seq", 1000, True)
        assert result == "setval(user_id_seq, 1000, True)"
