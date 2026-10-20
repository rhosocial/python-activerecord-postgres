# tests/rhosocial/activerecord_postgres_test/feature/backend/dialect/test_database_size_introspection.py
"""The PostgreSQL database-size query nodes (SQL generation only).

``get_storage_info`` used to spell ``SELECT pg_database_size(%s) as
size_bytes`` at the call site. The statement now belongs to
``PostgresDatabaseSizeQueryExpression`` and the call inside it to
``PostgresDatabaseSizeFunction``, so the introspector asks a node for the
whole statement; these tests pin the rendered bytes each of them owes.
"""

from rhosocial.activerecord.backend.impl.postgres.dialect import PostgresDialect
from rhosocial.activerecord.backend.impl.postgres.expression.introspection import (
    PostgresDatabaseSizeFunction,
    PostgresDatabaseSizeQueryExpression,
)


def _dialect():
    return PostgresDialect(version=(16, 0, 0))


class TestDatabaseSizeFunction:
    """The function node owns the call's spelling and its bind parameter."""

    def test_call_renders_with_a_placeholder_for_the_name(self):
        sql, params = PostgresDatabaseSizeFunction(_dialect(), database_name="app").to_sql()
        # The name is the FunctionCall node's, so the spelling is its renderer's:
        # unquoted identifiers fold to lower case on the server, so the upper
        # case here is the same function the concatenated form named.
        assert sql == "PG_DATABASE_SIZE(%s)"
        assert params == ("app",)

    def test_name_travels_as_a_parameter_not_as_text(self):
        # A name with a quote must not be able to break out of the call: it is
        # handed to the driver, never spelled into the SQL text.
        sql, params = PostgresDatabaseSizeFunction(
            _dialect(), database_name='ap"p'
        ).to_sql()
        assert sql == "PG_DATABASE_SIZE(%s)"
        assert params == ('ap"p',)


class TestDatabaseSizeQuery:
    """The query node owns the row shape, including the column name."""

    def test_query_renders_the_size_call_under_the_size_bytes_alias(self):
        sql, params = PostgresDatabaseSizeQueryExpression(
            _dialect(), database_name="app"
        ).to_sql()
        assert sql == 'SELECT PG_DATABASE_SIZE(%s) AS "size_bytes"'
        assert params == ("app",)

    def test_alias_is_the_column_callers_read(self):
        sql, _ = PostgresDatabaseSizeQueryExpression(_dialect(), database_name="app").to_sql()
        # Quoted or not, the label the driver reports is ``size_bytes``.
        assert 'AS "size_bytes"' in sql
