# tests/rhosocial/activerecord_postgres_test/feature/backend/test_create_table_like.py
"""
PostgreSQL CREATE TABLE ... LIKE syntax tests.

This module tests the PostgreSQL-specific LIKE syntax for CREATE TABLE statements,
including INCLUDING/EXCLUDING options.

The LIKE form is modelled by :class:`CreateTableLikeExpression` and rendered by
the dialect's ``format_create_table_like_statement`` into PostgreSQL's clause
form ``CREATE TABLE t (LIKE src [INCLUDING ...])``.
"""
from rhosocial.activerecord.backend.expression import CreateTableLikeExpression
from rhosocial.activerecord.backend.impl.postgres.dialect import PostgresDialect


class TestPostgreSQLCreateTableLike:
    """Tests for PostgreSQL CREATE TABLE ... LIKE syntax."""

    def test_basic_like_syntax(self):
        """Test basic CREATE TABLE ... LIKE syntax."""
        dialect = PostgresDialect()
        expr = CreateTableLikeExpression(dialect, table="users_copy", like_table="users")
        sql, params = expr.to_sql()

        assert sql == 'CREATE TABLE "users_copy" (LIKE "users")'
        assert params == ()

    def test_like_with_including_defaults(self):
        """Test LIKE with INCLUDING DEFAULTS option."""
        dialect = PostgresDialect()
        expr = CreateTableLikeExpression(
            dialect,
            table="users_copy",
            like_table="users",
            like_options={'including': ['defaults']},
        )
        sql, params = expr.to_sql()

        assert sql == 'CREATE TABLE "users_copy" (LIKE "users" INCLUDING DEFAULTS)'
        assert params == ()

    def test_like_with_including_constraints(self):
        """Test LIKE with INCLUDING CONSTRAINTS option."""
        dialect = PostgresDialect()
        expr = CreateTableLikeExpression(
            dialect,
            table="users_copy",
            like_table="users",
            like_options={'including': ['constraints']},
        )
        sql, params = expr.to_sql()

        assert sql == 'CREATE TABLE "users_copy" (LIKE "users" INCLUDING CONSTRAINTS)'
        assert params == ()

    def test_like_with_including_indexes(self):
        """Test LIKE with INCLUDING INDEXES option."""
        dialect = PostgresDialect()
        expr = CreateTableLikeExpression(
            dialect,
            table="users_copy",
            like_table="users",
            like_options={'including': ['indexes']},
        )
        sql, params = expr.to_sql()

        assert sql == 'CREATE TABLE "users_copy" (LIKE "users" INCLUDING INDEXES)'
        assert params == ()

    def test_like_with_multiple_including_options(self):
        """Test LIKE with multiple INCLUDING options (dictionary format)."""
        dialect = PostgresDialect()
        expr = CreateTableLikeExpression(
            dialect,
            table="users_copy",
            like_table="users",
            like_options={'including': ['defaults', 'constraints', 'indexes']},
        )
        sql, params = expr.to_sql()

        assert sql == (
            'CREATE TABLE "users_copy" (LIKE "users" '
            'INCLUDING DEFAULTS INCLUDING CONSTRAINTS INCLUDING INDEXES)'
        )
        assert params == ()

    def test_like_with_including_and_excluding_options(self):
        """Test LIKE with both INCLUDING and EXCLUDING options."""
        dialect = PostgresDialect()
        expr = CreateTableLikeExpression(
            dialect,
            table="users_copy",
            like_table="users",
            like_options={
                'including': ['defaults', 'constraints'],
                'excluding': ['indexes', 'comments'],
            },
        )
        sql, params = expr.to_sql()

        assert sql == (
            'CREATE TABLE "users_copy" (LIKE "users" '
            'INCLUDING DEFAULTS INCLUDING CONSTRAINTS '
            'EXCLUDING INDEXES EXCLUDING COMMENTS)'
        )
        assert params == ()

    def test_like_with_list_format_options(self):
        """Test LIKE with options in list format (plain strings => INCLUDING)."""
        dialect = PostgresDialect()
        expr = CreateTableLikeExpression(
            dialect,
            table="users_copy",
            like_table="users",
            like_options=['defaults', 'constraints'],
        )
        sql, params = expr.to_sql()

        assert sql == (
            'CREATE TABLE "users_copy" (LIKE "users" '
            'INCLUDING DEFAULTS INCLUDING CONSTRAINTS)'
        )
        assert params == ()

    def test_like_with_tuple_format_options(self):
        """Test LIKE with options in (action, feature) tuple format."""
        dialect = PostgresDialect()
        expr = CreateTableLikeExpression(
            dialect,
            table="users_copy",
            like_table="users",
            like_options=[
                ('INCLUDING', 'DEFAULTS'),
                ('EXCLUDING', 'INDEXES'),
            ],
        )
        sql, params = expr.to_sql()

        assert sql == (
            'CREATE TABLE "users_copy" (LIKE "users" '
            'INCLUDING DEFAULTS EXCLUDING INDEXES)'
        )
        assert params == ()

    def test_like_with_lowercase_option_names(self):
        """Option feature names are upper-cased in the rendered clause."""
        dialect = PostgresDialect()
        expr = CreateTableLikeExpression(
            dialect,
            table="users_copy",
            like_table="users",
            like_options={'including': ['comments', 'defaults']},
        )
        sql, params = expr.to_sql()

        assert sql == (
            'CREATE TABLE "users_copy" (LIKE "users" '
            'INCLUDING COMMENTS INCLUDING DEFAULTS)'
        )
        assert params == ()

    def test_like_with_if_not_exists(self):
        """Test CREATE TABLE ... LIKE with IF NOT EXISTS."""
        dialect = PostgresDialect()
        expr = CreateTableLikeExpression(
            dialect,
            table="users_copy",
            like_table="users",
            if_not_exists=True,
        )
        sql, params = expr.to_sql()

        assert sql == 'CREATE TABLE IF NOT EXISTS "users_copy" (LIKE "users")'
        assert params == ()

    def test_like_with_temporary(self):
        """Test CREATE TEMPORARY TABLE ... LIKE."""
        dialect = PostgresDialect()
        expr = CreateTableLikeExpression(
            dialect,
            table="temp_users",
            like_table="users",
            temporary=True,
        )
        sql, params = expr.to_sql()

        assert sql == 'CREATE TEMPORARY TABLE "temp_users" (LIKE "users")'
        assert params == ()

    def test_like_with_schema_qualified_table(self):
        """Test CREATE TABLE ... LIKE with schema-qualified source table."""
        dialect = PostgresDialect()
        expr = CreateTableLikeExpression(
            dialect,
            table="users_copy",
            like_table=('public', 'users'),
        )
        sql, params = expr.to_sql()

        assert sql == 'CREATE TABLE "users_copy" (LIKE "public"."users")'
        assert params == ()

    def test_like_with_temporary_and_if_not_exists(self):
        """Test CREATE TEMPORARY TABLE ... LIKE with IF NOT EXISTS."""
        dialect = PostgresDialect()
        expr = CreateTableLikeExpression(
            dialect,
            table="temp_users_copy",
            like_table=('public', 'users'),
            temporary=True,
            if_not_exists=True,
        )
        sql, params = expr.to_sql()

        assert sql == (
            'CREATE TEMPORARY TABLE IF NOT EXISTS "temp_users_copy" '
            '(LIKE "public"."users")'
        )
        assert params == ()

    def test_like_with_all_option(self):
        """Test LIKE with INCLUDING ALL option."""
        dialect = PostgresDialect()
        expr = CreateTableLikeExpression(
            dialect,
            table="users_copy",
            like_table="users",
            like_options={'including': ['all']},
        )
        sql, params = expr.to_sql()

        assert sql == 'CREATE TABLE "users_copy" (LIKE "users" INCLUDING ALL)'
        assert params == ()

    def test_supports_create_table_like(self):
        """PostgreSQL advertises CREATE TABLE ... (LIKE ...) support."""
        dialect = PostgresDialect()
        assert dialect.supports_create_table_like() is True
